package service

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/broker"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/repository"
)

// Scheduler orchestrates job submission, dispatch, retries, completion and
// outbox reconciliation (ADR 007/008/010/011).
type Scheduler struct {
	jobs    repository.JobRepository
	tenants repository.TenantRepository
	events  repository.JobEventRepository
	outbox  repository.OutboxRepository
	tx      repository.Transactor
	bus     broker.Broker
	backoff domain.BackoffStrategy
	// ReconcileThreshold is the minimum age of an outbox row before the
	// reconciler picks it up. Production default is 5s so the live
	// dispatcher gets first attempt; tests set it to 0 for determinism.
	ReconcileThreshold time.Duration
}

// NewScheduler wires the service. backoff may be nil (defaults applied).
func NewScheduler(
	jobs repository.JobRepository,
	tenants repository.TenantRepository,
	events repository.JobEventRepository,
	outbox repository.OutboxRepository,
	tx repository.Transactor,
	bus broker.Broker,
	backoff domain.BackoffStrategy,
) *Scheduler {
	if backoff == nil {
		backoff = domain.NewDefaultBackoffStrategy()
	}
	return &Scheduler{jobs: jobs, tenants: tenants, events: events, outbox: outbox, tx: tx, bus: bus, backoff: backoff, ReconcileThreshold: 5 * time.Second}
}

// SubmitInput carries validated job-submission fields.
type SubmitInput struct {
	TenantID    uuid.UUID
	Name        string
	Payload     json.RawMessage
	Priority    domain.JobPriority
	MaxAttempts int
	DependsOn   *uuid.UUID
	ScheduledAt time.Time
}

// SubmitJob persists a job + audit event + outbox row atomically, rejecting
// dependency cycles (W4) and unknown parents.
func (s *Scheduler) SubmitJob(ctx context.Context, in SubmitInput) (*domain.Job, error) {
	if _, err := s.tenants.GetByID(ctx, in.TenantID); err != nil {
		return nil, err
	}
	if !domain.IsValidPriority(in.Priority) {
		in.Priority = domain.PriorityDefault
	}
	if in.MaxAttempts <= 0 {
		in.MaxAttempts = 3
	}
	job, err := domain.NewJob(in.TenantID, in.Name, in.Payload, in.Priority, in.MaxAttempts, in.DependsOn, in.ScheduledAt)
	if err != nil {
		return nil, err
	}
	if in.DependsOn != nil {
		parent, err := s.jobs.GetByID(ctx, *in.DependsOn)
		if err != nil {
			return nil, domain.ErrDependencyNotMet
		}
		if err := domain.DetectDependencyCycle(job.ID, *in.DependsOn, func(id uuid.UUID) (*uuid.UUID, error) {
			if id == job.ID {
				return job.DependsOn, nil
			}
			j, err := s.jobs.GetByID(ctx, id)
			if err != nil {
				return nil, err
			}
			return j.DependsOn, nil
		}); err != nil {
			return nil, err
		}
		_ = parent
	}
	outboxEvent := domain.NewOutboxEvent(job.ID, "jobs.dispatch", mustMarshal(job))
	audit := &domain.JobEvent{
		ID:         uuid.New(),
		JobID:      job.ID,
		FromStatus: domain.StatusPending,
		ToStatus:   domain.StatusPending,
		OccurredAt: time.Now().UTC(),
	}
	if err := s.tx.WithinTransaction(ctx, func(ctx context.Context) error {
		if err := s.jobs.Create(ctx, job); err != nil {
			return err
		}
		if err := s.events.Record(ctx, audit); err != nil {
			return err
		}
		return s.outbox.Store(ctx, outboxEvent)
	}); err != nil {
		return nil, err
	}
	return job, nil
}

// GetJob returns one job.
func (s *Scheduler) GetJob(ctx context.Context, id uuid.UUID) (*domain.Job, error) {
	return s.jobs.GetByID(ctx, id)
}

// ListJobs lists a tenant's jobs.
func (s *Scheduler) ListJobs(ctx context.Context, tenantID uuid.UUID, filter repository.JobListFilter) ([]*domain.Job, int64, error) {
	return s.jobs.ListByTenant(ctx, tenantID, filter)
}

// CancelJob cancels a pending job and cascades atomically to dependents.
func (s *Scheduler) CancelJob(ctx context.Context, id uuid.UUID, reason string) (*domain.Job, error) {
	var job *domain.Job
	err := s.tx.WithinTransaction(ctx, func(ctx context.Context) error {
		var err error
		job, err = s.jobs.Cancel(ctx, id)
		if err != nil { return err }
		if err := s.recordTransition(ctx, id, domain.StatusPending, domain.StatusCancelled, "", reason); err != nil { return err }
		_, err = s.cancelDependents(ctx, id, "parent cancelled")
		return err
	})
	if err != nil { return nil, err }
	return job, nil
}

// ReportCompletion serializes reports with claims/reaping and commits all
// status, attempt, retry, audit and cascade effects in one transaction.
func (s *Scheduler) ReportCompletion(ctx context.Context, jobID uuid.UUID, workerID string, final domain.JobStatus, fencingToken int64, errMsg string) (int, error) {
	if final != domain.StatusSucceeded && final != domain.StatusFailed { return 0, domain.ErrInvalidTransition }
	n := 0
	err := s.tx.WithinTransaction(ctx, func(ctx context.Context) error {
		job, err := s.jobs.GetByIDForUpdate(ctx, jobID)
		if err != nil { return err }
		if job.FencingToken != fencingToken { return domain.ErrFencingTokenStale }
		if job.Status != domain.StatusRunning { return domain.ErrInvalidTransition }
		if job.LeaseOwner == nil || *job.LeaseOwner != workerID || job.LeaseExpiresAt == nil || !job.LeaseExpiresAt.After(time.Now().UTC()) { return domain.ErrLeaseLost }
		if err := s.jobs.TransitionStatus(ctx, jobID, domain.StatusRunning, final, fencingToken); err != nil { return err }
		if err := job.TransitionTo(final); err != nil { return err }
		if final == domain.StatusFailed {
			job.AttemptCount++
			if err := s.jobs.UpdateWithFencingToken(ctx, job, fencingToken); err != nil { return err }
		}
		if err := s.recordTransition(ctx, jobID, domain.StatusRunning, final, workerID, errMsg); err != nil { return err }
		if final == domain.StatusSucceeded {
			deps, err := s.jobs.GetDependentJobs(ctx, jobID)
			if err != nil { return err }
			n = len(deps)
			return nil
		}
		n, err = s.finishFailure(ctx, job)
		return err
	})
	if err != nil { return 0, err }
	return n, nil
}

// finishFailure preserves running -> failed -> pending/dead for both worker
// reports and expired leases. Its caller owns the transaction and row lock.
func (s *Scheduler) finishFailure(ctx context.Context, job *domain.Job) (int, error) {
	to, reason := domain.StatusDead, "retries exhausted"
	job.NextRetryAt = nil
	if job.AttemptCount < job.MaxAttempts {
		to, reason = domain.StatusPending, "retry scheduled"
		next := time.Now().UTC().Add(s.backoff.NextDelay(job.AttemptCount))
		job.NextRetryAt = &next
	}
	if err := s.jobs.UpdateWithFencingToken(ctx, job, job.FencingToken); err != nil { return 0, err }
	if err := s.jobs.TransitionStatus(ctx, job.ID, domain.StatusFailed, to, job.FencingToken); err != nil { return 0, err }
	if err := job.TransitionTo(to); err != nil { return 0, err }
	if err := s.recordTransition(ctx, job.ID, domain.StatusFailed, to, "", reason); err != nil { return 0, err }
	if to == domain.StatusPending {
		return 0, s.outbox.Store(ctx, domain.NewOutboxEvent(job.ID, "jobs.dispatch", mustMarshal(job)))
	}
	return s.cancelDependents(ctx, job.ID, "parent terminal failure")
}

// ReapExpiredLeases locks a batch with SKIP LOCKED, invalidates each dead
// holder's token and atomically retries or dead-letters the abandoned attempt.
func (s *Scheduler) ReapExpiredLeases(ctx context.Context, limit int) (int, error) {
	n := 0
	err := s.tx.WithinTransaction(ctx, func(ctx context.Context) error {
		jobs, err := s.jobs.GetExpiredLeasesForUpdate(ctx, limit)
		if err != nil { return err }
		for _, job := range jobs {
			failed, err := s.jobs.ExpireLease(ctx, job.ID, job.FencingToken)
			if err != nil { return err }
			workerID := ""
			if job.LeaseOwner != nil { workerID = *job.LeaseOwner }
			if err := s.recordTransition(ctx, job.ID, domain.StatusRunning, domain.StatusFailed, workerID, "lease expired"); err != nil { return err }
			if _, err := s.finishFailure(ctx, failed); err != nil { return err }
			n++
		}
		return nil
	})
	if err != nil { return 0, err }
	return n, nil
}

func (s *Scheduler) recordTransition(ctx context.Context, id uuid.UUID, from, to domain.JobStatus, workerID, reason string) error {
	return s.events.Record(ctx, &domain.JobEvent{
		ID: uuid.New(), JobID: id, FromStatus: from, ToStatus: to,
		OccurredAt: time.Now().UTC(), WorkerID: strptr(workerID), ErrorMessage: strptr(reason),
	})
}

// DispatchReady publishes announcements only. Workers own the atomic claim;
// duplicate or stale announcements cannot start a second execution.
func (s *Scheduler) DispatchReady(ctx context.Context, batchSize int) (int, error) {
	if batchSize <= 0 {
		batchSize = 100
	}
	ready, err := s.jobs.GetReadyJobs(ctx, batchSize)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, j := range ready {
		if s.bus == nil { continue }
		if err := s.bus.Publish(ctx, "jobs.execute", mustMarshal(j), j.ID.String()); err != nil { return n, err }
		n++
	}
	return n, nil
}

// RetryDue moves due failed jobs back to pending (called on a ticker before DispatchReady).
func (s *Scheduler) RetryDue(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		limit = 100
	}
	due, err := s.jobs.GetPendingRetryJobs(ctx, limit)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, j := range due {
		if err := s.jobs.TransitionStatus(ctx, j.ID, domain.StatusFailed, domain.StatusPending, j.FencingToken); err != nil { return n, err }
		n++
	}
	return n, nil
}

// ReconcileOutbox republishes stranded outbox rows (ADR 008). Rows past
// MaxOutboxRetries are already excluded by the repository layer.
func (s *Scheduler) ReconcileOutbox(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		limit = 100
	}
	events, err := s.outbox.FetchUnpublished(ctx, limit, s.ReconcileThreshold)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, e := range events {
		if s.bus == nil {
			continue
		}
		if err := s.bus.Publish(ctx, e.RoutingKey, e.Payload, e.JobID.String()); err != nil {
			if markErr := s.outbox.MarkFailed(ctx, e.ID, err.Error()); markErr != nil { return n, markErr }
			continue
		}
		if err := s.outbox.MarkPublished(ctx, e.ID); err != nil { return n, err }
		n++
	}
	return n, nil
}

func (s *Scheduler) cancelDependents(ctx context.Context, parentID uuid.UUID, reason string) (int, error) {
	deps, err := s.jobs.GetDependentJobs(ctx, parentID)
	if err != nil { return 0, err }
	n := 0
	for _, d := range deps {
		if d.IsTerminal() { continue }
		if _, err := s.jobs.Cancel(ctx, d.ID); err != nil { return 0, err }
		if err := s.recordTransition(ctx, d.ID, d.Status, domain.StatusCancelled, "", reason); err != nil { return 0, err }
		children, err := s.cancelDependents(ctx, d.ID, reason)
		if err != nil { return 0, err }
		n += 1 + children
	}
	return n, nil
}

func mustMarshal(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

func strptr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
