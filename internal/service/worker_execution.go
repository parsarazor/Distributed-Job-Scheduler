package service

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/telemetry"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/semaphore"
)

var (
	errHandlerPanic = errors.New("handler panicked")
	errNoHandler    = errors.New("handler not registered")
)

// Detached executions carry only tracing, never caller credentials, baggage or
// other RPC context values. Cancellation is connected explicitly where needed.
func traceOnlyContext(ctx context.Context) context.Context {
	return trace.ContextWithSpanContext(context.Background(), trace.SpanContextFromContext(ctx))
}

func (w *Worker) execute(ctx context.Context, announcement *domain.Job) (bool, error) {
	base, cancelBase := context.WithCancelCause(traceOnlyContext(ctx))
	stopCaller := context.AfterFunc(ctx, func() { cancelBase(context.Cause(ctx)) })
	stopWorker := context.AfterFunc(w.lifetime, func() { cancelBase(context.Cause(w.lifetime)) })
	defer func() { stopCaller(); stopWorker(); cancelBase(nil) }()
	if ctx.Err() != nil {
		cancelBase(context.Cause(ctx))
	}
	if w.lifetime.Err() != nil {
		cancelBase(context.Cause(w.lifetime))
	}
	base = propagation.TraceContext{}.Extract(base, propagation.MapCarrier{"traceparent": announcement.TraceParent, "tracestate": announcement.TraceState})
	base, span := otel.Tracer("scheduler").Start(base, "job.execute")
	defer span.End()
	claimCtx, cancelClaim := context.WithTimeout(base, 10*time.Second)
	defer cancelClaim()
	// The local lease deadline starts before the claim request so it can only
	// expire earlier than the lease the scheduler stores.
	claimedAt := time.Now()
	job, token, err := w.ctl.ClaimExecution(claimCtx, announcement, w.id, w.leaseTTL)
	// A deleted job or an obsolete announcement has no remaining work to do.
	if errors.Is(err, domain.ErrJobNotClaimable) || errors.Is(err, domain.ErrJobNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	span.SetAttributes(attribute.String("job.id", job.ID.String()), attribute.String("tenant.id", job.TenantID.String()), attribute.Int64("job.fencing_token", token))
	slog.Info("job started", "job_id", job.ID, "tenant_id", job.TenantID, "worker_id", w.id, "attempt", job.AttemptCount+1)

	jobCtx, cancel := context.WithCancelCause(base)
	defer cancel(nil)
	timeout := job.Timeout
	if timeout <= 0 || (w.jobTimeout > 0 && w.jobTimeout < timeout) {
		timeout = w.jobTimeout
	}
	if timeout > 0 {
		var cancelTimeout context.CancelFunc
		jobCtx, cancelTimeout = context.WithTimeout(jobCtx, timeout)
		defer cancelTimeout()
	}
	a := &activeJob{
		info:   ActiveInfo{JobID: job.ID, TenantID: job.TenantID, Name: job.Name, StartedAt: time.Now().UTC(), FencingToken: token},
		cancel: cancel, ctx: jobCtx, gate: semaphore.NewWeighted(1), deadline: claimedAt.Add(w.leaseTTL),
	}
	if !time.Now().Before(a.deadline) {
		a.leaseLost = true
		cancel(domain.ErrLeaseLost)
	}
	w.mu.Lock()
	h := w.handlers[job.Name] // Registration is exact; "default" is an ordinary name.
	w.active[job.ID] = a
	w.mu.Unlock()
	defer func() {
		w.mu.Lock()
		if w.active[job.ID] == a {
			delete(w.active, job.ID)
		}
		w.mu.Unlock()
	}()
	hbCtx, stopHeartbeat := context.WithCancel(context.Background())
	hbDone := make(chan struct{})
	go func() { defer close(hbDone); w.heartbeatLoop(hbCtx, a) }()
	defer func() { stopHeartbeat(); <-hbDone }()

	started := time.Now()
	attempt := job.AttemptCount
	runErr := invokeHandler(jobCtx, h, job)
	telemetry.Latency.Observe(time.Since(started).Seconds())

	// Finalization has its own bounded budget so cancellation can still persist a
	// failed attempt. Each database operation is shorter than a lease interval;
	// successful handlers retain heartbeat renewal between retries.
	completionCtx, stopCompletion := context.WithTimeout(traceOnlyContext(base), 5*time.Second)
	defer stopCompletion()
	if err := a.gate.Acquire(completionCtx, 1); err != nil {
		return true, err
	}
	a.completing = true // Cancel after this point must not acknowledge success.
	if cause := context.Cause(jobCtx); cause != nil {
		runErr = cause
	}
	final, msg, kind := handlerResult(runErr)
	a.gate.Release(1)
	if runErr != nil {
		telemetry.HandlerErrors.WithLabelValues(kind).Inc()
		span.RecordError(errors.New(msg))
		span.SetStatus(codes.Error, msg)
		slog.Warn("job handler failed", "job_id", job.ID, "worker_id", w.id, "kind", kind)
	}
	err = w.complete(completionCtx, a, attempt, final, msg)
	if errors.Is(err, domain.ErrLeaseLost) || errors.Is(err, domain.ErrFencingTokenStale) {
		telemetry.LeaseLosses.Inc()
		slog.Warn("job lease lost", "worker_id", w.id, "job_id", job.ID)
	}
	return true, err
}

func invokeHandler(ctx context.Context, h Handler, job *domain.Job) (err error) {
	defer func() {
		if recover() != nil {
			// Never format a panic value: Error/String methods can panic again,
			// and user values can contain payloads, credentials or personal data.
			err = errHandlerPanic
		}
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	if h == nil {
		return errNoHandler
	}
	return h(ctx, job)
}

func handlerResult(err error) (final domain.JobStatus, message, kind string) {
	defer func() {
		if recover() != nil {
			final, message, kind = domain.StatusFailed, "handler failed", "error"
		}
	}()
	if err == nil {
		return domain.StatusSucceeded, "", ""
	}
	switch {
	case errors.Is(err, errHandlerPanic):
		return domain.StatusFailed, "handler panicked", "panic"
	case errors.Is(err, context.DeadlineExceeded):
		return domain.StatusFailed, "handler timed out", "timeout"
	case errors.Is(err, context.Canceled):
		return domain.StatusFailed, "handler canceled", "canceled"
	case errors.Is(err, errNoHandler):
		return domain.StatusFailed, domain.ReasonHandlerNotRegistered, "error"
	default:
		return domain.StatusFailed, "handler failed", "error"
	}
}

func (w *Worker) complete(ctx context.Context, a *activeJob, attempt int, final domain.JobStatus, msg string) error {
	var lastErr error
	for tries := 0; tries < 6; tries++ {
		if err := a.gate.Acquire(ctx, 1); err != nil {
			return err
		}
		// Reconcile before trusting a renewal failure: the previous completion
		// may have committed even when its response was lost.
		if lastErr != nil {
			readCtx, cancel := context.WithTimeout(ctx, min(w.leaseTTL/3, time.Second))
			stored, err := w.ctl.GetExecutionJob(readCtx, a.info.JobID)
			cancel()
			if err == nil && completionObserved(stored, a.info.FencingToken, attempt, final) {
				a.finished = true
				a.gate.Release(1)
				return nil
			}
			if err == nil && (stored.FencingToken != a.info.FencingToken || stored.Status != domain.StatusRunning) {
				a.finished = true
				a.gate.Release(1)
				return domain.ErrLeaseLost
			}
		}
		// The local timer is advisory only: report anyway and let the
		// server-side fencing token decide. This avoids clock-skew-induced
		// duplicate runs when the local deadline drifted past the real lease.
		if a.leaseLost {
			a.leaseLost, a.finished = true, true
			a.cancel(domain.ErrLeaseLost)
			a.gate.Release(1)
			return domain.ErrLeaseLost
		}
		opCtx, cancel := context.WithDeadline(ctx, minTime(a.deadline, time.Now().Add(min(w.leaseTTL/3, time.Second))))
		lastErr = w.ctl.ReportCompletion(opCtx, a.info.JobID, w.id, final, a.info.FencingToken, msg)
		cancel()
		if lastErr == nil {
			a.finished = true
			a.gate.Release(1)
			return nil
		}
		a.gate.Release(1)
		if !waitCompletionRetry(ctx, min((25*time.Millisecond)<<tries, w.leaseTTL/6)) {
			return ctx.Err()
		}
	}
	return lastErr
}

func completionObserved(job *domain.Job, token int64, attempt int, final domain.JobStatus) bool {
	if job.FencingToken != token || job.LeaseOwner != nil || job.LeaseExpiresAt != nil {
		return false
	}
	if final == domain.StatusSucceeded {
		return job.Status == domain.StatusSucceeded && job.AttemptCount == attempt
	}
	return (job.Status == domain.StatusPending || job.Status == domain.StatusDead) && job.AttemptCount == attempt+1
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func waitCompletionRetry(ctx context.Context, delay time.Duration) bool {
	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func (w *Worker) heartbeatLoop(ctx context.Context, a *activeJob) {
	interval := max(w.leaseTTL/3, time.Nanosecond)
	for {
		if err := a.gate.Acquire(ctx, 1); err != nil {
			return
		}
		remaining := time.Until(a.deadline)
		finished := a.finished || a.leaseLost
		if remaining <= 0 && !finished {
			a.leaseLost = true
			a.cancel(domain.ErrLeaseLost)
		}
		a.gate.Release(1)
		if finished || remaining <= 0 {
			return
		}
		t := time.NewTimer(min(interval, remaining))
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-a.ctx.Done():
			t.Stop()
			return
		case <-t.C:
			if err := w.renewLease(ctx, a, true); err != nil {
				if errors.Is(err, domain.ErrLeaseLost) || a.ctx.Err() != nil {
					return
				}
				if ctx.Err() == nil {
					slog.Warn("renew job lease failed", "worker_id", w.id, "job_id", a.info.JobID)
				}
			}
		}
	}
}

func (w *Worker) renewLease(ctx context.Context, a *activeJob, automatic bool) error {
	if err := a.gate.Acquire(ctx, 1); err != nil {
		return err
	}
	defer a.gate.Release(1)
	if a.finished || a.leaseLost || (!automatic && a.completing) || a.ctx.Err() != nil {
		return domain.ErrLeaseLost
	}
	if !time.Now().Before(a.deadline) {
		a.leaseLost = true
		a.cancel(domain.ErrLeaseLost)
		return domain.ErrLeaseLost
	}
	started := time.Now()
	renewCtx, cancel := context.WithDeadline(ctx, a.deadline)
	stop := context.AfterFunc(a.ctx, cancel)
	defer func() { stop(); cancel() }()
	err := w.ctl.RenewLease(renewCtx, a.info.JobID, w.id, a.info.FencingToken, w.leaseTTL)
	if a.ctx.Err() != nil {
		return domain.ErrLeaseLost
	}
	if errors.Is(err, domain.ErrLeaseLost) || (err != nil && ctx.Err() == nil) || !time.Now().Before(a.deadline) {
		a.leaseLost = true
		a.cancel(domain.ErrLeaseLost)
		return domain.ErrLeaseLost
	}
	if err == nil {
		a.deadline = started.Add(w.leaseTTL)
	}
	return err
}

// Heartbeat never renews an execution whose handler has been aborted or finalized.
func (w *Worker) Heartbeat(ctx context.Context, jobID uuid.UUID, token int64) (renewed bool, abort bool) {
	w.mu.RLock()
	a := w.active[jobID]
	w.mu.RUnlock()
	if a == nil || a.info.FencingToken != token || a.ctx.Err() != nil {
		return false, true
	}
	err := w.renewLease(ctx, a, false)
	return err == nil, errors.Is(err, domain.ErrLeaseLost) || a.ctx.Err() != nil
}

// CancelExecution acknowledges only a matching execution that has not begun
// finalization. Admission and the result decision share the same gate.
func (w *Worker) CancelExecution(ctx context.Context, jobID uuid.UUID, token int64) bool {
	w.mu.RLock()
	a := w.active[jobID]
	w.mu.RUnlock()
	if a == nil || token <= 0 || a.info.FencingToken != token {
		return false
	}
	if err := a.gate.Acquire(ctx, 1); err != nil {
		return false
	}
	defer a.gate.Release(1)
	if a.completing || a.finished || a.leaseLost || a.ctx.Err() != nil {
		return false
	}
	a.cancel(context.Canceled)
	return true
}
