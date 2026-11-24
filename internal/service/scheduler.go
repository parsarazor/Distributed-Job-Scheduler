package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/parsarazor/distributed-job-scheduler/internal/broker"
	"github.com/parsarazor/distributed-job-scheduler/internal/domain"
	"github.com/parsarazor/distributed-job-scheduler/internal/repository"
	"github.com/parsarazor/distributed-job-scheduler/internal/telemetry"
	"go.opentelemetry.io/otel/propagation"
)

// Scheduler orchestrates job submission, dispatch, retries, completion and
// outbox reconciliation (ADR 007/008/010/011).
type Scheduler struct {
	jobs    repository.JobRepository
	tenants repository.TenantRepository
	// workerCreds backs the worker credential admin RPCs; nil disables them.
	workerCreds repository.WorkerCredentialRepository
	events      repository.JobEventRepository
	outbox      repository.OutboxRepository
	tx          repository.Transactor
	bus         broker.Broker
	backoff     domain.BackoffStrategy
	// ReconcileThreshold is the minimum age of an outbox row before the
	// publisher picks it up. The default of zero publishes ready work immediately.
	ReconcileThreshold time.Duration
	// RedispatchAfter controls how long a confirmed announcement may wait for a claim.
	// Zero uses the default, five minutes.
	RedispatchAfter time.Duration
	// NoWorkerRetry is how long a job whose job name has no worker queue waits
	// before it is dispatched again (default 1m).
	NoWorkerRetry time.Duration
	// NoWorkerTimeout is how long such a job may stay ready before it goes dead
	// with ReasonNoWorker (default 24h).
	NoWorkerTimeout time.Duration
	TenantQuota     int
	TenantRate      int

	dispatchWake chan struct{}
	publishWake  chan struct{}
}

// DispatchWake fires when new work arrives or a dispatch slot frees, so the
// dispatch loop need not wait for its next tick. Signals are coalesced.
func (s *Scheduler) DispatchWake() <-chan struct{} { return s.dispatchWake }

// PublishWake fires when dispatch stages outbox announcements.
func (s *Scheduler) PublishWake() <-chan struct{} { return s.publishWake }

func notifyWake(c chan struct{}) {
	select {
	case c <- struct{}{}:
	default:
	}
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
	return &Scheduler{jobs: jobs, tenants: tenants, events: events, outbox: outbox, tx: tx, bus: bus, backoff: backoff, TenantQuota: 1000, TenantRate: 600,
		dispatchWake: make(chan struct{}, 1), publishWake: make(chan struct{}, 1)}
}

// SubmitInput carries validated job-submission fields.
type SubmitInput struct {
	TenantID       uuid.UUID
	Name           string
	Payload        json.RawMessage
	Priority       domain.JobPriority
	MaxAttempts    int
	DependsOn      *uuid.UUID
	ScheduledAt    time.Time
	IdempotencyKey string
	Timeout        time.Duration
}

// SubmitJob persists a job and audit event atomically. DispatchReady creates
// its outbox announcement once the schedule, retry and dependency gates open.
func (s *Scheduler) SubmitJob(ctx context.Context, in SubmitInput) (*domain.Job, error) {
	if strings.TrimSpace(in.Name) == "" || len(in.Name) > 255 || len(in.Payload) > 1<<20 || (len(in.Payload) > 0 && !json.Valid(in.Payload)) || len(in.IdempotencyKey) > 255 || in.Timeout < 0 || in.Timeout > 24*time.Hour || in.MaxAttempts > 100 {
		return nil, fmt.Errorf("%w: invalid name, payload, attempts, idempotency key or timeout", domain.ErrInvalidInput)
	}
	if !payloadExpansionFits(in.Payload) {
		return nil, fmt.Errorf("%w: numeric payload expansion exceeds storage limits", domain.ErrInvalidInput)
	}
	if len(in.Payload) == 0 {
		in.Payload = json.RawMessage(`{}`)
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
	job.SubmissionKey = in.IdempotencyKey
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	job.TraceParent, job.TraceState = carrier.Get("traceparent"), carrier.Get("tracestate")
	if in.Timeout > 0 {
		job.Timeout = in.Timeout
	}
	if in.IdempotencyKey != "" {
		hash, err := submissionHash(job, in.ScheduledAt)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid submission payload", domain.ErrInvalidInput)
		}
		job.SubmissionHash = hash
	}
	audit := &domain.JobEvent{
		ID:         uuid.New(),
		JobID:      job.ID,
		FromStatus: domain.StatusPending,
		ToStatus:   domain.StatusPending,
	}
	if err := s.tx.WithinTransaction(ctx, func(ctx context.Context) error {
		if err := s.tenants.Lock(ctx, in.TenantID); err != nil {
			return err
		}
		if in.IdempotencyKey != "" {
			existing, err := s.jobs.GetBySubmissionKey(ctx, in.TenantID, in.IdempotencyKey)
			if err == nil {
				if existing.SubmissionHash == "" {
					// Legacy rows have no original request fingerprint. Compare
					// the persisted configuration, recognizing the old default
					// schedule as the creation timestamp.
					scheduledAt := existing.ScheduledAt
					if in.ScheduledAt.IsZero() && existing.CreatedAt.Sub(scheduledAt).Abs() <= time.Microsecond {
						scheduledAt = time.Time{}
					}
					existing.SubmissionHash, err = submissionHash(existing, scheduledAt)
					if err != nil {
						return domain.ErrIdempotencyConflict
					}
				}
				if existing.SubmissionHash != job.SubmissionHash {
					return domain.ErrIdempotencyConflict
				}
				job = existing
				return nil
			}
			if !errors.Is(err, domain.ErrJobNotFound) {
				return err
			}
		}
		if err := s.jobs.CheckAdmission(ctx, in.TenantID, s.TenantQuota, s.TenantRate); err != nil {
			return err
		}
		if in.DependsOn != nil {
			// Serialize child insertion with the parent's terminal transition.
			parent, err := s.jobs.GetDependency(ctx, in.TenantID, *in.DependsOn, true)
			if err != nil {
				return err
			}
			if parent.Status == domain.StatusDead || parent.Status == domain.StatusCancelled {
				return domain.ErrDependencyNotMet
			}
			if err := domain.DetectDependencyCycle(job.ID, parent.ID, func(id uuid.UUID) (*uuid.UUID, error) {
				if id == parent.ID {
					return parent.DependsOn, nil
				}
				j, err := s.jobs.GetDependency(ctx, in.TenantID, id, false)
				if err != nil {
					return nil, err
				}
				return j.DependsOn, nil
			}); err != nil {
				return err
			}
		}
		now, err := s.jobs.Now(ctx)
		if err != nil {
			return err
		}
		job.CreatedAt, job.UpdatedAt, audit.OccurredAt = now, now, now
		if in.ScheduledAt.IsZero() {
			job.ScheduledAt = now
		}
		if err := s.jobs.Create(ctx, job); err != nil {
			return err
		}
		return s.events.Record(ctx, audit)
	}); err != nil {
		return nil, err
	}
	notifyWake(s.dispatchWake)
	return job, nil
}

func submissionHash(job *domain.Job, scheduledAt time.Time) (string, error) {
	var value any
	decoder := json.NewDecoder(bytes.NewReader(job.Payload))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return "", err
	}
	data, err := json.Marshal(struct {
		Name        string
		Payload     any
		Priority    domain.JobPriority
		MaxAttempts int
		DependsOn   *uuid.UUID
		ScheduledAt time.Time
		Timeout     time.Duration
	}{job.Name, value, job.Priority, job.MaxAttempts, job.DependsOn, scheduledAt.UTC().Truncate(time.Microsecond), job.Timeout})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// RequeueDead starts another retry budget for an explicitly selected dead job.
func (s *Scheduler) RequeueDead(ctx context.Context, id uuid.UUID) (*domain.Job, error) {
	var job *domain.Job
	err := s.tx.WithinTransaction(ctx, func(ctx context.Context) error {
		before, err := s.jobs.GetByID(ctx, id)
		if err != nil {
			return err
		}
		if before.PayloadExpired {
			return domain.ErrPayloadExpired
		}
		if err := s.tenants.Lock(ctx, before.TenantID); err != nil {
			return err
		}
		if err := s.jobs.CheckAdmission(ctx, before.TenantID, s.TenantQuota, s.TenantRate); err != nil {
			return err
		}
		job, err = s.jobs.RequeueDead(ctx, id)
		if err != nil {
			return err
		}
		// Use the database clock, like the outbox rows that
		// TerminalizeOutboxExhausted compares against this event.
		now, err := s.jobs.Now(ctx)
		if err != nil {
			return err
		}
		return s.events.Record(ctx, &domain.JobEvent{
			ID: uuid.New(), JobID: id, FromStatus: domain.StatusDead, ToStatus: domain.StatusPending,
			OccurredAt: now, ErrorMessage: strptr("operator requeue"),
		})
	})
	return job, err
}

// GetJob returns one job.
func (s *Scheduler) GetJob(ctx context.Context, id uuid.UUID) (*domain.Job, error) {
	return s.jobs.GetByID(ctx, id)
}

// ListJobs lists a tenant's jobs.
func (s *Scheduler) ListJobs(ctx context.Context, tenantID uuid.UUID, filter repository.JobListFilter) ([]*domain.Job, int64, error) {
	if filter.Limit < 0 || filter.Offset < 0 {
		return nil, 0, domain.ErrInvalidInput
	}
	return s.jobs.ListByTenant(ctx, tenantID, filter)
}

// CancelJob cancels a pending job and cascades atomically to dependents.
func (s *Scheduler) CancelJob(ctx context.Context, id uuid.UUID, reason string) (*domain.Job, error) {
	var job *domain.Job
	err := s.tx.WithinTransaction(ctx, func(ctx context.Context) error {
		var err error
		job, err = s.jobs.Cancel(ctx, id)
		if err != nil {
			return err
		}
		if err := s.outbox.RetireUnpublished(ctx, id); err != nil {
			return err
		}
		if err := s.recordTransition(ctx, id, domain.StatusPending, domain.StatusCancelled, "", reason); err != nil {
			return err
		}
		return s.cancelDependents(ctx, id, "parent cancelled")
	})
	if err != nil {
		return nil, err
	}
	return job, nil
}

// ReportCompletion serializes reports with claims/reaping and commits all
// status, attempt, retry, audit and cascade effects in one transaction.
func (s *Scheduler) ReportCompletion(ctx context.Context, jobID uuid.UUID, workerID string, final domain.JobStatus, fencingToken int64, errMsg string) (int, error) {
	return s.ReportCompletionWith(ctx, jobID, workerID, final, fencingToken, errMsg, false)
}

// ReportCompletionWith is ReportCompletion where nonRetryable dead-letters a
// failed job without using its remaining retry budget.
func (s *Scheduler) ReportCompletionWith(ctx context.Context, jobID uuid.UUID, workerID string, final domain.JobStatus, fencingToken int64, errMsg string, nonRetryable bool) (int, error) {
	if final != domain.StatusSucceeded && final != domain.StatusFailed {
		return 0, domain.ErrInvalidTransition
	}
	if final == domain.StatusSucceeded {
		errMsg = ""
	} else {
		errMsg = domain.SafeHandlerMessage(errMsg)
		if errMsg == "" {
			errMsg = "handler failed"
		}
	}
	n := 0
	var tenantID uuid.UUID
	var persistedStatus domain.JobStatus
	err := s.tx.WithinTransaction(ctx, func(ctx context.Context) error {
		job, err := s.jobs.CompleteLease(ctx, jobID, workerID, fencingToken, final)
		if err != nil {
			return err
		}
		tenantID = job.TenantID
		if err := s.recordTransition(ctx, jobID, domain.StatusRunning, final, workerID, errMsg); err != nil {
			return err
		}
		if final == domain.StatusSucceeded {
			n, err = s.jobs.CountWaitingDependents(ctx, jobID)
			return err
		}
		err = s.finishFailure(ctx, job, nonRetryable || errMsg == domain.ReasonHandlerNotRegistered)
		persistedStatus = job.Status
		return err
	})
	if err != nil {
		return 0, err
	}
	notifyWake(s.dispatchWake)
	telemetry.Transitions.WithLabelValues(string(final)).Inc()
	if final == domain.StatusFailed {
		telemetry.Transitions.WithLabelValues(string(persistedStatus)).Inc()
	}
	slog.Info("job completed", "job_id", jobID, "tenant_id", tenantID, "worker_id", domain.AuditText(workerID, 128), "status", final, "fencing_token", fencingToken)
	return n, nil
}

// finishFailure preserves running -> failed -> pending/dead for both worker
// reports and expired leases. Its caller owns the transaction and row lock.
// A non-retryable failure (e.g. no worker registered the handler) skips the
// remaining retry budget and dead-letters immediately.
func (s *Scheduler) finishFailure(ctx context.Context, job *domain.Job, nonRetryable bool) error {
	to, reason := domain.StatusDead, "retries exhausted"
	job.NextRetryAt = nil
	if !nonRetryable && job.AttemptCount < job.MaxAttempts {
		to, reason = domain.StatusPending, "retry scheduled"
		if err := s.scheduleRetry(ctx, job); err != nil {
			return err
		}
	}
	if err := s.jobs.TransitionStatus(ctx, job.ID, domain.StatusFailed, to, job.FencingToken); err != nil {
		return err
	}
	if err := job.TransitionTo(to); err != nil {
		return err
	}
	if err := s.recordTransition(ctx, job.ID, domain.StatusFailed, to, "", reason); err != nil {
		return err
	}
	if to == domain.StatusPending {
		return nil
	}
	return s.cancelDependents(ctx, job.ID, "parent terminal failure")
}

func (s *Scheduler) scheduleRetry(ctx context.Context, job *domain.Job) error {
	now, err := s.jobs.Now(ctx)
	if err != nil {
		return err
	}
	next := now.Add(s.backoff.NextDelay(max(job.AttemptCount, 1)))
	if err := s.jobs.SetNextRetryAt(ctx, job.ID, job.FencingToken, next); err != nil {
		return err
	}
	job.NextRetryAt = &next
	return nil
}

// GrantLeaseGrace runs once at scheduler startup. Workers cannot renew leases
// while no scheduler is reachable, so leases that lapsed during the outage get
// grace to be renewed or completed before the reaper re-runs their jobs.
func (s *Scheduler) GrantLeaseGrace(ctx context.Context, grace time.Duration) (int, error) {
	if grace <= 0 {
		return 0, nil
	}
	return s.jobs.ExtendRunningLeases(ctx, grace)
}

// ReapExpiredLeases locks a batch with SKIP LOCKED, invalidates each dead
// holder's token and atomically retries or dead-letters the abandoned attempt.
func (s *Scheduler) ReapExpiredLeases(ctx context.Context, limit int) (int, error) {
	n := 0
	counts := make(map[domain.JobStatus]int)
	err := s.tx.WithinTransaction(ctx, func(ctx context.Context) error {
		jobs, err := s.jobs.GetExpiredLeasesForUpdate(ctx, limit)
		if err != nil {
			return err
		}
		for _, job := range jobs {
			failed, err := s.jobs.ExpireLease(ctx, job.ID, job.FencingToken)
			if err != nil {
				return err
			}
			workerID := ""
			if job.LeaseOwner != nil {
				workerID = *job.LeaseOwner
			}
			if err := s.recordTransition(ctx, job.ID, domain.StatusRunning, domain.StatusFailed, workerID, "lease expired"); err != nil {
				return err
			}
			if err := s.finishFailure(ctx, failed, false); err != nil {
				return err
			}
			counts[failed.Status]++
			n++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	for state, count := range counts {
		telemetry.Transitions.WithLabelValues(string(state)).Add(float64(count))
	}
	telemetry.Transitions.WithLabelValues("failed").Add(float64(n))
	if n > 0 {
		notifyWake(s.dispatchWake)
	}
	return n, nil
}

func (s *Scheduler) recordTransition(ctx context.Context, id uuid.UUID, from, to domain.JobStatus, workerID, reason string) error {
	return s.events.Record(ctx, &domain.JobEvent{
		ID: uuid.New(), JobID: id, FromStatus: from, ToStatus: to,
		OccurredAt: time.Now().UTC(), WorkerID: strptr(domain.AuditText(workerID, 128)), ErrorMessage: strptr(domain.AuditText(reason, 1024)),
	})
}

// DispatchReady stages one outbox announcement per ready attempt. The row
// locks and dispatched marker prevent duplicate staging across replicas.
func (s *Scheduler) DispatchReady(ctx context.Context, batchSize int) (int, error) {
	if batchSize <= 0 {
		batchSize = 100
	}
	batchSize = min(batchSize, 1000)
	redispatchAfter := s.RedispatchAfter
	if redispatchAfter <= 0 {
		redispatchAfter = 5 * time.Minute
	}
	n := 0
	err := s.tx.WithinTransaction(ctx, func(ctx context.Context) error {
		exhausted, err := s.jobs.TerminalizeOutboxExhausted(ctx, batchSize, domain.MaxDeadLetteredDispatches)
		if err != nil {
			return err
		}
		for _, id := range exhausted {
			if err := s.recordTransition(ctx, id, domain.StatusPending, domain.StatusDead, "", "outbox publish exhausted: announcements dead-lettered"); err != nil {
				return err
			}
			if err := s.cancelDependents(ctx, id, "parent terminal failure"); err != nil {
				return err
			}
			telemetry.OutboxExhaustedJobs.Inc()
			slog.Error("job terminalized: outbox publish exhausted", "job_id", id, "dead_lettered", domain.MaxDeadLetteredDispatches)
		}
		if err := s.jobs.RecoverStaleDispatches(ctx, batchSize, redispatchAfter); err != nil {
			return err
		}
		ready, err := s.jobs.GetUndispatchedJobsForUpdate(ctx, batchSize)
		if err != nil {
			return err
		}
		if len(ready) == 0 {
			return nil
		}
		now, err := s.jobs.Now(ctx)
		if err != nil {
			return err
		}
		for _, j := range ready {
			payload, err := json.Marshal(struct {
				ID           uuid.UUID `json:"id"`
				AttemptCount int       `json:"attempt_count"`
				FencingToken int64     `json:"fencing_token"`
				TraceParent  string    `json:"traceparent,omitempty"`
				TraceState   string    `json:"tracestate,omitempty"`
			}{j.ID, j.AttemptCount, j.FencingToken, j.TraceParent, j.TraceState})
			if err != nil {
				return err
			}
			event := domain.NewOutboxEvent(j.ID, broker.JobQueue(j.Name), payload)
			event.ID = domain.DispatchID(j.ID, j.AttemptCount, j.FencingToken)
			event.CreatedAt = now
			if err := s.outbox.Store(ctx, event); err != nil {
				return err
			}
			if err := s.jobs.MarkDispatched(ctx, j.ID, j.FencingToken); err != nil {
				return err
			}
			n++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	if n > 0 {
		notifyWake(s.publishWake)
	}
	return n, nil
}

// RetryDue recovers legacy failed rows and terminalizes exhausted budgets.
func (s *Scheduler) RetryDue(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		limit = 100
	}
	limit = min(limit, 1000)
	n := 0
	err := s.tx.WithinTransaction(ctx, func(ctx context.Context) error {
		due, err := s.jobs.GetPendingRetryJobs(ctx, limit)
		if err != nil {
			return err
		}
		for _, j := range due {
			to, reason := domain.StatusPending, "retry recovered"
			if j.AttemptCount >= j.MaxAttempts {
				to, reason = domain.StatusDead, "retries exhausted"
			} else if j.NextRetryAt == nil {
				if err := s.scheduleRetry(ctx, j); err != nil {
					if errors.Is(err, domain.ErrInvalidTransition) {
						current, getErr := s.jobs.GetByID(ctx, j.ID)
						if getErr == nil && current.Status == domain.StatusCancelled {
							continue
						}
					}
					return err
				}
			}
			if err := s.jobs.TransitionStatus(ctx, j.ID, domain.StatusFailed, to, j.FencingToken); err != nil {
				// A terminalized parent in this batch may have cancelled this child.
				if errors.Is(err, domain.ErrInvalidTransition) {
					current, getErr := s.jobs.GetByID(ctx, j.ID)
					if getErr != nil {
						return getErr
					}
					if current.Status == domain.StatusCancelled {
						continue
					}
				}
				return err
			}
			if err := s.recordTransition(ctx, j.ID, domain.StatusFailed, to, "", reason); err != nil {
				return err
			}
			if to == domain.StatusDead {
				if err := s.cancelDependents(ctx, j.ID, "parent terminal failure"); err != nil {
					return err
				}
			}
			n++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return n, nil
}

// ReconcileOutbox publishes ready announcements using claim-then-publish: each
// row is claimed in a short transaction (stamping publishing_since), the
// RabbitMQ publish happens with no transaction or row lock held, and the
// outcome is persisted in a second short transaction. A crashed publisher's
// stale claims are released by ReclaimStalePublishing on the next pass.
// Broker outages are retried with backoff up to domain.MaxOutboxRetries, then
// the row is dead-lettered and kept for inspection.
func (s *Scheduler) ReconcileOutbox(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		limit = 100
	}
	limit = min(limit, 1000)
	if s.bus == nil {
		return 0, nil
	}
	// Give the pass a finite budget. Each row gets its own claim transaction and
	// a shorter publish deadline so there's time left to persist its outcome.
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	n := 0
	var passErr error
	var excluded []uuid.UUID
	if _, err := s.outbox.ReclaimStalePublishing(ctx); err != nil {
		return 0, err
	}
	for range limit {
		if err := ctx.Err(); err != nil {
			return n, errors.Join(passErr, err)
		}
		rowCtx, stop := context.WithTimeout(ctx, 4*time.Second)
		var event *domain.OutboxEvent
		// 1. Claim: short transaction, committed before the publish.
		err := s.tx.WithinTransaction(rowCtx, func(txCtx context.Context) error {
			var err error
			event, err = s.outbox.FetchPublishable(txCtx, s.ReconcileThreshold, excluded)
			return err
		})
		stop()
		if err != nil {
			if event == nil {
				return n, errors.Join(passErr, err)
			}
			excluded = append(excluded, event.ID)
			passErr = errors.Join(passErr, fmt.Errorf("claim outbox event %s: %w", event.ID, err))
			continue
		}
		if event == nil {
			break
		}
		// 2. Publish with no transaction or row lock held.
		publishCtx, endPublish := context.WithTimeout(ctx, 2*time.Second)
		pubErr := s.bus.Publish(publishCtx, event.RoutingKey, event.Payload, event.ID.String())
		endPublish()
		// 3. Persist the outcome in a second short transaction.
		outcomeCtx, stopOutcome := context.WithTimeout(ctx, 4*time.Second)
		outcomeErr := s.tx.WithinTransaction(outcomeCtx, func(txCtx context.Context) error {
			if errors.Is(pubErr, broker.ErrUnroutable) && broker.IsJobTypeQueue(event.RoutingKey) {
				return s.deferUnroutable(txCtx, event)
			}
			if pubErr != nil {
				telemetry.OutboxPublishFailures.Inc()
				if event.RetryCount+1 >= domain.MaxOutboxRetries {
					telemetry.OutboxDeadLettered.Inc()
					slog.Error("outbox announcement dead-lettered", "outbox_id", event.ID,
						"job_id", event.JobID, "attempts", event.RetryCount+1)
				}
				reason := "broker publish failed"
				if errors.Is(pubErr, context.DeadlineExceeded) {
					reason = "broker publish timed out"
				}
				return s.outbox.MarkFailed(txCtx, event.ID, reason)
			}
			return s.outbox.MarkPublished(txCtx, event.ID)
		})
		stopOutcome()
		if outcomeErr != nil {
			excluded = append(excluded, event.ID)
			passErr = errors.Join(passErr, fmt.Errorf("persist outbox event %s: %w", event.ID, outcomeErr))
			continue
		}
		if pubErr == nil {
			n++
		}
	}
	return n, passErr
}

// Defaults for jobs whose job name has no worker queue.
const (
	DefaultNoWorkerRetry   = time.Minute
	DefaultNoWorkerTimeout = 24 * time.Hour
)

// deferUnroutable handles an announcement refused because no worker has
// declared a queue for the job name. It holds the job without using publish
// retry budget, and makes it dead once NoWorkerTimeout has passed.
func (s *Scheduler) deferUnroutable(ctx context.Context, event *domain.OutboxEvent) error {
	var announcement struct {
		FencingToken int64 `json:"fencing_token"`
	}
	if err := json.Unmarshal(event.Payload, &announcement); err != nil {
		return fmt.Errorf("decode announcement %s: %w", event.ID, err)
	}
	if err := s.outbox.MarkUnroutable(ctx, event.ID, domain.ReasonNoWorker); err != nil {
		return err
	}
	retry, timeout := s.NoWorkerRetry, s.NoWorkerTimeout
	if retry <= 0 {
		retry = DefaultNoWorkerRetry
	}
	if timeout <= 0 {
		timeout = DefaultNoWorkerTimeout
	}
	dead, err := s.jobs.DeferUnroutable(ctx, event.JobID, announcement.FencingToken, retry, timeout)
	if errors.Is(err, domain.ErrJobNotClaimable) {
		return nil
	}
	if err != nil {
		return err
	}
	if !dead {
		telemetry.NoWorkerDeferrals.Inc()
		slog.Debug("no worker queue for job name; job deferred", "job_id", event.JobID, "queue", event.RoutingKey, "retry_in", retry)
		return nil
	}
	telemetry.NoWorkerDeadJobs.Inc()
	slog.Error("job dead: no worker handles its job name", "job_id", event.JobID, "queue", event.RoutingKey, "timeout", timeout)
	if err := s.recordTransition(ctx, event.JobID, domain.StatusPending, domain.StatusDead, "", domain.ReasonNoWorker); err != nil {
		return err
	}
	return s.cancelDependents(ctx, event.JobID, "parent terminal failure")
}

func (s *Scheduler) cancelDependents(ctx context.Context, parentID uuid.UUID, reason string) error {
	deps, err := s.jobs.CancelDependents(ctx, parentID)
	if err != nil {
		return err
	}
	for _, d := range deps {
		if err := s.outbox.RetireUnpublished(ctx, d.ID); err != nil {
			return err
		}
		if err := s.recordTransition(ctx, d.ID, d.Status, domain.StatusCancelled, "", reason); err != nil {
			return err
		}
	}
	return nil
}

func strptr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
