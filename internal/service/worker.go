package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/lease"
	"golang.org/x/sync/semaphore"
)

// Handler executes one job. It must respect ctx cancellation (Drain support).
type Handler func(ctx context.Context, job *domain.Job) error

// Worker pulls jobs from the Scheduler, executes them with bounded
// concurrency (ADR 005), renews leases via heartbeat, and reports completion
// with fencing tokens (ADR 002/009).
type Worker struct {
	id             string
	sched          *Scheduler
	leases         lease.LeaseManager
	sem            *semaphore.Weighted
	maxConcurrency int64
	handlers       map[string]Handler
	mu             sync.RWMutex
	active         map[uuid.UUID]*activeJob
	draining       bool
	startedAt      time.Time
}

type activeJob struct {
	job        *domain.Job
	token      int64 // database fencing token used for completion reports
	leaseToken int64 // lease-manager token used for renewals
	started    time.Time
	cancel     context.CancelFunc
}

// NewWorker creates a worker with maxConcurrency execution slots.
func NewWorker(id string, sched *Scheduler, leases lease.LeaseManager, maxConcurrency int64) *Worker {
	if maxConcurrency <= 0 {
		maxConcurrency = 10
	}
	return &Worker{
		id:             id,
		sched:          sched,
		leases:         leases,
		sem:            semaphore.NewWeighted(maxConcurrency),
		maxConcurrency: maxConcurrency,
		handlers:       make(map[string]Handler),
		active:         make(map[uuid.UUID]*activeJob),
		startedAt:      time.Now().UTC(),
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
func (w *Worker) Drain() { w.mu.Lock(); w.draining = true; w.mu.Unlock() }

// IsDraining reports drain state.
func (w *Worker) IsDraining() bool { w.mu.RLock(); defer w.mu.RUnlock(); return w.draining }

// ActiveJobs returns a snapshot of in-flight jobs.
func (w *Worker) ActiveJobs() []ActiveInfo {
	w.mu.RLock()
	defer w.mu.RUnlock()
	out := make([]ActiveInfo, 0, len(w.active))
	for id, a := range w.active {
		out = append(out, ActiveInfo{JobID: id, TenantID: a.job.TenantID, Name: a.job.Name, StartedAt: a.started, FencingToken: a.token})
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

// RunOnce dispatches ready jobs and executes one batch synchronously.
// It is the functional core used by the demo binary and tests.
func (w *Worker) RunOnce(ctx context.Context, batchSize int) (int, error) {
	w.mu.RLock()
	draining := w.draining
	w.mu.RUnlock()
	if draining {
		return 0, nil
	}
	ready, err := w.sched.jobs.GetReadyJobs(ctx, batchSize)
	if err != nil {
		return 0, err
	}
	done := 0
	for _, job := range ready {
		if err := w.sem.Acquire(ctx, 1); err != nil {
			break
		}
		if err := w.execute(ctx, job); err == nil {
			done++
		}
		w.sem.Release(1)
	}
	return done, nil
}

// StartExecution accepts a broker-delivered job without exceeding the worker's
// concurrency limit. It returns false when draining or saturated so callers
// can requeue the delivery instead of silently losing it.
func (w *Worker) StartExecution(ctx context.Context, job *domain.Job) bool {
	w.mu.RLock()
	draining := w.draining
	w.mu.RUnlock()
	if draining || !w.sem.TryAcquire(1) {
		return false
	}
	go func() {
		defer w.sem.Release(1)
		_ = w.execute(ctx, job)
	}()
	return true
}

// MaxConcurrency returns the configured execution limit.
func (w *Worker) MaxConcurrency() int64 { return w.maxConcurrency }

func (w *Worker) execute(ctx context.Context, job *domain.Job) error {
	w.mu.RLock()
	h := w.handlers[job.Name]
	fallback := w.handlers["default"]
	w.mu.RUnlock()
	if h == nil {
		h = fallback
	}
	if h == nil {
		h = func(context.Context, *domain.Job) error { return nil }
	}

	leaseToken, err := w.leases.Acquire(ctx, job.ID, w.id, 30*time.Second)
	if err != nil {
		return err
	}
	current, err := w.sched.jobs.GetByID(ctx, job.ID)
	if err != nil {
		_ = w.leases.Release(ctx, job.ID, w.id)
		return err
	}
	if current.IsTerminal() {
		_ = w.leases.Release(ctx, job.ID, w.id)
		return nil // duplicate delivery of an already-completed job
	}
	if current.Status != domain.StatusPending && current.Status != domain.StatusRunning {
		_ = w.leases.Release(ctx, job.ID, w.id)
		return domain.ErrInvalidTransition
	}
	token, err := w.sched.jobs.IncrementFencingToken(ctx, job.ID)
	if err != nil {
		_ = w.leases.Release(ctx, job.ID, w.id)
		return err
	}
	if current.Status == domain.StatusPending {
		if err := w.sched.jobs.TransitionStatus(ctx, job.ID, domain.StatusPending, domain.StatusRunning, token); err != nil {
			_ = w.leases.Release(ctx, job.ID, w.id)
			return err
		}
	}

	job = current
	job.FencingToken = token
	jobCtx, cancel := context.WithCancel(ctx)
	w.mu.Lock()
	w.active[job.ID] = &activeJob{job: job, token: token, leaseToken: leaseToken, started: time.Now().UTC(), cancel: cancel}
	w.mu.Unlock()

	// Heartbeat renews the lease until execution finishes.
	stopHb := make(chan struct{})
	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-stopHb:
				return
			case <-t.C:
				_ = w.leases.Renew(context.Background(), job.ID, w.id, leaseToken, 30*time.Second)
			case <-jobCtx.Done():
				return
			}
		}
	}()

	// A panicking handler must fail the job, not crash the worker or strand
	// the lease: recover, report failure (→ retry path), always clean up.
	runErr := func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("handler panic: %v", r)
			}
		}()
		return h(jobCtx, job)
	}()
	close(stopHb)
	cancel()
	_ = w.leases.Release(ctx, job.ID, w.id)

	w.mu.Lock()
	delete(w.active, job.ID)
	w.mu.Unlock()

	final := domain.StatusSucceeded
	msg := ""
	if runErr != nil {
		final = domain.StatusFailed
		msg = runErr.Error()
	}
	_, repErr := w.sched.ReportCompletion(ctx, job.ID, w.id, final, token, msg)
	return repErr
}

// Heartbeat renews the lease for an in-flight job; abort is requested when
// the job no longer exists or is terminal.
func (w *Worker) Heartbeat(ctx context.Context, jobID uuid.UUID, token int64) (renewed bool, abort bool) {
	w.mu.RLock()
	a, active := w.active[jobID]
	w.mu.RUnlock()
	if !active || a.token != token {
		return false, true
	}
	if err := w.leases.Renew(ctx, jobID, w.id, a.leaseToken, 30*time.Second); err != nil {
		return false, true
	}
	j, err := w.sched.jobs.GetByID(ctx, jobID)
	if err != nil || j.IsTerminal() {
		return true, true
	}
	return true, false
}

// CancelExecution aborts context for an in-flight job.
func (w *Worker) CancelExecution(jobID uuid.UUID) bool {
	w.mu.RLock()
	a, ok := w.active[jobID]
	w.mu.RUnlock()
	if !ok {
		return false
	}
	a.cancel()
	return true
}
