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

// CancelJob cancels a pending job and cascades to dependents (ADR 010).
func (s *Scheduler) CancelJob(ctx context.Context, id uuid.UUID, reason string) (*domain.Job, error) {
	job, err := s.jobs.Cancel(ctx, id)
	if err != nil {
		return nil, err
	}
	_ = s.events.Record(ctx, &domain.JobEvent{
		ID: job.ID, JobID: job.ID,
		FromStatus: domain.StatusPending, ToStatus: domain.StatusCancelled,
		OccurredAt: time.Now().UTC(), ErrorMessage: strptr(reason),
	})
	// Cascade: dependents of a cancelled parent can never run.
	_ = s.cancelDependents(ctx, id, "parent cancelled")
	return job, nil
}

// ReportCompletion applies a worker's terminal report with fencing-token
// protection (ADR 009). It handles retries with backoff and cascade-abort.
func (s *Scheduler) ReportCompletion(ctx context.Context, jobID uuid.UUID, workerID string, final domain.JobStatus, fencingToken int64, errMsg string) (dependentsUnlocked int, err error) {
	job, err := s.jobs.GetByID(ctx, jobID)
	if err != nil {
		return 0, err
	}
	from := job.Status
	var to domain.JobStatus
	switch final {
	case domain.StatusSucceeded:
		to = domain.StatusSucceeded
	case domain.StatusFailed:
		// Retry or die: decided below after incrementing attempt count.
		to = domain.StatusFailed
	default:
		return 0, domain.ErrInvalidTransition
	}

	// Fencing: stale zombie writes fail harmlessly.
	if job.FencingToken > fencingToken {
		return 0, domain.ErrFencingTokenStale
	}

	updated := *job
	if to == domain.StatusFailed {
		updated.AttemptCount++
	}
	if err := s.jobs.UpdateWithFencingToken(ctx, &updated, fencingToken); err != nil {
		return 0, err
	}
	if err := s.jobs.TransitionStatus(ctx, jobID, from, to, fencingToken); err != nil {
		// UpdateWithFencingToken already persisted attempt bump; surface error.
		return 0, err
	}
	_ = s.events.Record(ctx, &domain.JobEvent{
		ID: uuid.New(), JobID: jobID, FromStatus: from, ToStatus: to,
		OccurredAt: time.Now().UTC(), WorkerID: strptr(workerID), ErrorMessage: strptr(errMsg),
	})

	if to == domain.StatusSucceeded {
		deps, _ := s.jobs.GetDependentJobs(ctx, jobID)
		return len(deps), nil
	}

	// Failed: retry with backoff if attempts remain, else dead + cascade.
	refresh, _ := s.jobs.GetByID(ctx, jobID)
	if refresh.AttemptCount < refresh.MaxAttempts {
		delay := s.backoff.NextDelay(refresh.AttemptCount)
		next := time.Now().UTC().Add(delay)
		refresh.NextRetryAt = &next
		_ = s.jobs.UpdateWithFencingToken(ctx, refresh, fencingToken)
		_ = s.jobs.TransitionStatus(ctx, jobID, domain.StatusFailed, domain.StatusPending, fencingToken)
		_ = s.events.Record(ctx, &domain.JobEvent{
			ID: uuid.New(), JobID: jobID,
			FromStatus: domain.StatusFailed, ToStatus: domain.StatusPending,
			OccurredAt: time.Now().UTC(), ErrorMessage: strptr("retry scheduled"),
		})
		// Re-enqueue for redispatch.
		_ = s.outbox.Store(ctx, domain.NewOutboxEvent(jobID, "jobs.dispatch", mustMarshal(refresh)))
		return 0, nil
	}
	_ = s.jobs.TransitionStatus(ctx, jobID, domain.StatusFailed, domain.StatusDead, fencingToken)
	_ = s.events.Record(ctx, &domain.JobEvent{
		ID: uuid.New(), JobID: jobID,
		FromStatus: domain.StatusFailed, ToStatus: domain.StatusDead,
		OccurredAt: time.Now().UTC(), ErrorMessage: strptr("retries exhausted"),
	})
	n := s.cancelDependents(ctx, jobID, "parent terminal failure")
	return n, nil
}

// DispatchReady moves up to batchSize ready jobs to running and publishes
// each to the broker (message_id = job_id for idempotent redelivery).
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
		token, err := s.jobs.IncrementFencingToken(ctx, j.ID)
		if err != nil {
			continue
		}
		if err := s.jobs.TransitionStatus(ctx, j.ID, domain.StatusPending, domain.StatusRunning, token); err != nil {
			continue
		}
		_ = s.events.Record(ctx, &domain.JobEvent{
			ID: uuid.New(), JobID: j.ID,
			FromStatus: domain.StatusPending, ToStatus: domain.StatusRunning,
			OccurredAt: time.Now().UTC(),
		})
		if s.bus != nil {
			_ = s.bus.Publish(ctx, "jobs.execute", mustMarshal(j), j.ID.String())
		}
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
		if err := s.jobs.TransitionStatus(ctx, j.ID, domain.StatusFailed, domain.StatusPending, j.FencingToken); err == nil {
			n++
		}
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
			_ = s.outbox.MarkFailed(ctx, e.ID, err.Error())
			continue
		}
		_ = s.outbox.MarkPublished(ctx, e.ID)
		n++
	}
	return n, nil
}

func (s *Scheduler) cancelDependents(ctx context.Context, parentID uuid.UUID, reason string) int {
	deps, err := s.jobs.GetDependentJobs(ctx, parentID)
	if err != nil {
		return 0
	}
	n := 0
	for _, d := range deps {
		if d.IsTerminal() {
			continue
		}
		if _, err := s.jobs.Cancel(ctx, d.ID); err == nil {
			n++
			_ = s.events.Record(ctx, &domain.JobEvent{
				ID: uuid.New(), JobID: d.ID,
				FromStatus: d.Status, ToStatus: domain.StatusCancelled,
				OccurredAt: time.Now().UTC(), ErrorMessage: strptr(reason),
			})
			// Recurse one level: transitively cancel grandchildren.
			n += s.cancelDependents(ctx, d.ID, reason)
		}
	}
	return n
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
