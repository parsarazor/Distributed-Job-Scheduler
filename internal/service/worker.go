package service

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
	"golang.org/x/sync/semaphore"
)

// Handler executes one job. It must respect ctx cancellation, including lease loss.
type Handler func(ctx context.Context, job *domain.Job) error

// Worker claims jobs atomically through the repository, executes them with
// bounded concurrency and renews their leases until execution finishes.
type Worker struct {
	id             string
	sched          *Scheduler
	leaseTTL       time.Duration
	sem            *semaphore.Weighted
	maxConcurrency int64
	handlers       map[string]Handler
	mu             sync.RWMutex
	active         map[uuid.UUID]*activeJob
	draining       bool
	startedAt      time.Time
}

type activeJob struct {
	info     ActiveInfo
	cancel   context.CancelCauseFunc
	mu       sync.Mutex // serializes automatic and RPC renewals with completion
	deadline time.Time
	finished bool
}

// NewWorker creates a worker with maxConcurrency slots and a repository-owned lease.
// Nonpositive values select the defaults of 10 slots and a 30-second lease.
func NewWorker(id string, sched *Scheduler, maxConcurrency int64, leaseTTL time.Duration) *Worker {
	if maxConcurrency <= 0 {
		maxConcurrency = 10
	}
	if leaseTTL <= 0 {
		leaseTTL = 30 * time.Second
	}
	return &Worker{
		id: id, sched: sched, leaseTTL: leaseTTL,
		sem: semaphore.NewWeighted(maxConcurrency), maxConcurrency: maxConcurrency,
		handlers: make(map[string]Handler), active: make(map[uuid.UUID]*activeJob),
		startedAt: time.Now().UTC(),
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
	defer w.mu.Unlock()
	w.draining = true
}

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
		ran, err := w.execute(ctx, job)
		w.sem.Release(1)
		if err != nil {
			return done, err
		}
		if ran {
			done++
		}
	}
	return done, nil
}

// StartExecution accepts an announcement without exceeding the execution limit.
// Acceptance is not ownership: execute still has to win the repository claim.
func (w *Worker) StartExecution(ctx context.Context, job *domain.Job) bool {
	if job == nil || w.IsDraining() || !w.sem.TryAcquire(1) {
		return false
	}
	go func() {
		defer w.sem.Release(1)
		if _, err := w.execute(ctx, job); err != nil {
			log.Printf("worker %s execute %s: %v", w.id, job.ID, err)
		}
	}()
	return true
}

// MaxConcurrency returns the configured execution limit.
func (w *Worker) MaxConcurrency() int64 { return w.maxConcurrency }

func (w *Worker) execute(ctx context.Context, announcement *domain.Job) (bool, error) {
	var job *domain.Job
	var token int64
	var claimedAt time.Time
	err := w.sched.tx.WithinTransaction(ctx, func(ctx context.Context) error {
		claimedAt = time.Now()
		var err error
		token, err = w.sched.jobs.ClaimJob(ctx, announcement.ID, w.id, w.leaseTTL)
		if err != nil {
			return err
		}
		job, err = w.sched.jobs.GetByID(ctx, announcement.ID)
		if err != nil {
			return err
		}
		return w.sched.recordTransition(ctx, job.ID, domain.StatusPending, domain.StatusRunning, w.id, "")
	})
	if errors.Is(err, domain.ErrJobNotClaimable) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	jobCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	a := &activeJob{
		info: ActiveInfo{JobID: job.ID, TenantID: job.TenantID, Name: job.Name, StartedAt: time.Now().UTC(), FencingToken: token},
		cancel: cancel, deadline: claimedAt.Add(w.leaseTTL),
	}
	w.mu.Lock()
	h := w.handlers[job.Name]
	if h == nil {
		h = w.handlers["default"]
	}
	w.active[job.ID] = a
	w.mu.Unlock()
	defer func() {
		w.mu.Lock()
		delete(w.active, job.ID)
		w.mu.Unlock()
	}()

	hbCtx, stopHeartbeat := context.WithCancel(ctx)
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		w.heartbeatLoop(hbCtx, a)
	}()

	// A handler panic fails the attempt without crashing the worker.
	runErr := func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("handler panic: %v", r)
			}
		}()
		if h == nil {
			return nil
		}
		return h(jobCtx, job)
	}()
	stopHeartbeat()
	<-hbDone
	a.mu.Lock()
	a.finished = true
	if !time.Now().Before(a.deadline) {
		cancel(domain.ErrLeaseLost)
	}
	a.mu.Unlock()
	if errors.Is(context.Cause(jobCtx), domain.ErrLeaseLost) {
		log.Printf("worker %s lost lease for %s; completion withheld", w.id, a.info.JobID)
		return true, nil
	}

	final, msg := domain.StatusSucceeded, ""
	if runErr != nil {
		final, msg = domain.StatusFailed, runErr.Error()
	}
	_, err = w.sched.ReportCompletion(ctx, a.info.JobID, w.id, final, token, msg)
	return true, err
}

func (w *Worker) heartbeatLoop(ctx context.Context, a *activeJob) {
	interval := w.leaseTTL / 3
	if interval <= 0 {
		interval = time.Nanosecond
	}
	for {
		a.mu.Lock()
		remaining := time.Until(a.deadline)
		a.mu.Unlock()
		if remaining <= 0 {
			a.cancel(domain.ErrLeaseLost)
			return
		}
		timer := time.NewTimer(min(interval, remaining))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			if err := w.renewLease(ctx, a); err != nil {
				if errors.Is(err, domain.ErrLeaseLost) {
					return
				}
				if ctx.Err() == nil {
					log.Printf("worker %s renew %s: %v", w.id, a.info.JobID, err)
				}
			}
		}
	}
}

func (w *Worker) renewLease(ctx context.Context, a *activeJob) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.finished {
		return domain.ErrLeaseLost
	}
	if !time.Now().Before(a.deadline) {
		a.cancel(domain.ErrLeaseLost)
		return domain.ErrLeaseLost
	}
	// A stuck renewal cannot outlive the last known lease deadline. Use the
	// request start, not the reply time, for a conservative local deadline.
	started := time.Now()
	renewCtx, cancel := context.WithDeadline(ctx, a.deadline)
	defer cancel()
	err := w.sched.jobs.RenewLease(renewCtx, a.info.JobID, w.id, a.info.FencingToken, w.leaseTTL)
	if errors.Is(err, domain.ErrLeaseLost) || !time.Now().Before(a.deadline) {
		a.cancel(domain.ErrLeaseLost)
		return domain.ErrLeaseLost
	}
	if err == nil {
		a.deadline = started.Add(w.leaseTTL)
	}
	return err
}

// Heartbeat shares the automatic renewal path; lost ownership requests abort.
func (w *Worker) Heartbeat(ctx context.Context, jobID uuid.UUID, token int64) (renewed bool, abort bool) {
	w.mu.RLock()
	a, active := w.active[jobID]
	w.mu.RUnlock()
	if !active || a.info.FencingToken != token {
		return false, true
	}
	err := w.renewLease(ctx, a)
	return err == nil, errors.Is(err, domain.ErrLeaseLost)
}

// CancelExecution aborts context for an in-flight job.
func (w *Worker) CancelExecution(jobID uuid.UUID) bool {
	w.mu.RLock()
	a, ok := w.active[jobID]
	w.mu.RUnlock()
	if !ok {
		return false
	}
	a.cancel(context.Canceled)
	return true
}
