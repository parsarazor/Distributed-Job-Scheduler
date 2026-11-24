package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/parsarazor/distributed-job-scheduler/internal/broker"
	"github.com/parsarazor/distributed-job-scheduler/internal/domain"
	"golang.org/x/sync/semaphore"
)

// Handler executes one job. It must respect ctx cancellation, including lease loss.
type Handler func(ctx context.Context, job *domain.Job) error

// Worker claims jobs atomically through its JobControl, executes them with
// bounded concurrency and renews their leases until execution finishes.
type Worker struct {
	id             string
	sched          *Scheduler // in-process only; nil for remote workers
	ctl            JobControl
	leaseTTL       time.Duration
	sem            *semaphore.Weighted
	maxConcurrency int64
	handlers       map[string]Handler
	mu             sync.RWMutex
	active         map[uuid.UUID]*activeJob
	draining       bool
	startedAt      time.Time
	jobTimeout     time.Duration
	completionWait time.Duration
	intakeCancel   context.CancelFunc
	lifetime       context.Context
	stop           context.CancelCauseFunc
}

type activeJob struct {
	info       ActiveInfo
	cancel     context.CancelCauseFunc
	gate       *semaphore.Weighted // context-aware renewal/completion/cancel admission
	ctx        context.Context
	deadline   time.Time
	finished   bool
	completing bool
	// handlerDone is set without the gate when the handler returns, so a
	// renewal stalled until the deadline cannot discard a finished result.
	handlerDone atomic.Bool
	leaseLost   bool
}

// NewWorker creates a worker with maxConcurrency slots and a repository-owned lease.
// Nonpositive values select the defaults of 10 slots and a 30-second lease.
func NewWorker(id string, sched *Scheduler, maxConcurrency int64, leaseTTL time.Duration) *Worker {
	return newWorker(id, sched, schedulerControl{s: sched}, maxConcurrency, leaseTTL)
}

// NewRemoteWorker creates a database-free worker that claims, renews and
// completes jobs through the scheduler. It consumes announcements only.
func NewRemoteWorker(id string, ctl JobControl, maxConcurrency int64, leaseTTL time.Duration) *Worker {
	return newWorker(id, nil, ctl, maxConcurrency, leaseTTL)
}

// DefaultCompletionWindow is how long a finished handler's result is retried
// while the scheduler is unreachable.
const DefaultCompletionWindow = time.Minute

// SetCompletionWindow bounds completion retries after the handler returns.
// Nonpositive values select DefaultCompletionWindow.
func (w *Worker) SetCompletionWindow(d time.Duration) {
	if d <= 0 {
		d = DefaultCompletionWindow
	}
	w.completionWait = d
}

func newWorker(id string, sched *Scheduler, ctl JobControl, maxConcurrency int64, leaseTTL time.Duration) *Worker {
	if maxConcurrency <= 0 {
		maxConcurrency = 10
	}
	if leaseTTL <= 0 {
		leaseTTL = 30 * time.Second
	}
	lifetime, stop := context.WithCancelCause(context.Background())
	return &Worker{
		id: id, sched: sched, ctl: ctl, leaseTTL: leaseTTL,
		sem: semaphore.NewWeighted(maxConcurrency), maxConcurrency: maxConcurrency,
		handlers: make(map[string]Handler), active: make(map[uuid.UUID]*activeJob),
		startedAt:      time.Now().UTC(),
		jobTimeout:     5 * time.Minute,
		completionWait: DefaultCompletionWindow,
		lifetime:       lifetime, stop: stop,
	}
}

// RegisterHandler registers a named job handler.
func (w *Worker) RegisterHandler(name string, h Handler) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.handlers[name] = h
}

// ID returns the worker id.
func (w *Worker) ID() string { return w.id }

// Drain stops accepting new jobs; in-flight jobs run to completion.
func (w *Worker) Drain() {
	w.mu.Lock()
	w.draining = true
	cancel := w.intakeCancel
	w.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// SetIntakeCancel connects drain controls to the real broker/poller intake.
// It is safe to wire after an RPC has already requested drain.
func (w *Worker) SetIntakeCancel(cancel context.CancelFunc) {
	w.mu.Lock()
	w.intakeCancel = cancel
	draining := w.draining
	w.mu.Unlock()
	if draining && cancel != nil {
		cancel()
	}
}

// CancelAll escalates a drain by canceling admitted claims and handlers.
// Handlers must cooperate with cancellation; a process may bound Wait separately.
func (w *Worker) CancelAll() {
	w.Drain()
	w.stop(context.Canceled)
}

// Wait drains admitted executions, including work still acquiring its DB claim.
func (w *Worker) Wait(ctx context.Context) error {
	w.Drain()
	if err := w.sem.Acquire(ctx, w.maxConcurrency); err != nil {
		return err
	}
	w.sem.Release(w.maxConcurrency)
	return nil
}

// SetJobTimeout sets the default deadline for executions. Configure before use.
func (w *Worker) SetJobTimeout(timeout time.Duration) { w.jobTimeout = timeout }

// IsDraining reports drain state.
func (w *Worker) IsDraining() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.draining
}

// ActiveJobs returns a snapshot of in-flight jobs.
func (w *Worker) ActiveJobs() []ActiveInfo {
	w.mu.RLock()
	defer w.mu.RUnlock()
	out := make([]ActiveInfo, 0, len(w.active))
	for _, a := range w.active {
		out = append(out, a.info)
	}
	return out
}

// ActiveInfo describes one in-flight job.
type ActiveInfo struct {
	JobID        uuid.UUID
	TenantID     uuid.UUID
	Name         string
	StartedAt    time.Time
	FencingToken int64
}

// RunOnce polls ready jobs and executes one batch synchronously.
func (w *Worker) RunOnce(ctx context.Context, batchSize int) (int, error) {
	if w.IsDraining() {
		return 0, nil
	}
	if w.sched == nil {
		return 0, errors.New("polling requires an in-process scheduler")
	}
	ready, err := w.sched.jobs.GetReadyJobs(ctx, batchSize)
	if err != nil {
		return 0, err
	}
	done := 0
	for _, job := range ready {
		if w.IsDraining() {
			break
		}
		if err := w.sem.Acquire(ctx, 1); err != nil {
			return done, err
		}
		if w.IsDraining() {
			w.sem.Release(1)
			break
		}
		ran, err := w.execute(ctx, job)
		w.sem.Release(1)
		if ran {
			done++
		}
		if err != nil && !errors.Is(err, domain.ErrLeaseLost) && !errors.Is(err, domain.ErrFencingTokenStale) {
			return done, err
		}
	}
	return done, nil
}

// StartExecution accepts an announcement without exceeding the execution limit.
// Acceptance is not ownership: execute still has to win the repository claim.
func (w *Worker) StartExecution(ctx context.Context, job *domain.Job) bool {
	w.mu.Lock()
	if job == nil || w.draining {
		w.mu.Unlock()
		return false
	}
	accepted := w.sem.TryAcquire(1)
	w.mu.Unlock()
	if !accepted {
		return false
	}
	go func() {
		defer w.sem.Release(1)
		if _, err := w.execute(ctx, job); err != nil {
			slog.Error("execute job", "worker_id", w.id, "job_id", job.ID, "tenant_id", job.TenantID, "error", err)
		}
	}()
	return true
}

// HandleMessage processes a durable announcement synchronously. The consumer
// acknowledges only after the execution result has committed. Redeliveries of
// a finished or superseded attempt are harmless.
func (w *Worker) HandleMessage(ctx context.Context, payload []byte) error {
	var job domain.Job
	if err := json.Unmarshal(payload, &job); err != nil || job.ID == uuid.Nil || job.AttemptCount < 0 || job.FencingToken < 0 {
		return broker.Permanent(errors.New("invalid job announcement"))
	}
	if err := w.sem.Acquire(ctx, 1); err != nil {
		return err
	}
	defer w.sem.Release(1)
	if w.IsDraining() {
		return fmt.Errorf("worker is draining")
	}
	_, err := w.execute(ctx, &job)
	return err
}

// MaxConcurrency returns the configured execution limit.
func (w *Worker) MaxConcurrency() int64 { return w.maxConcurrency }
