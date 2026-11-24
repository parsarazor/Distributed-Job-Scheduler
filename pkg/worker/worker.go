// Package worker is the SDK for running job handlers against the scheduler.
//
// Delivery is at-least-once: a handler can run more than once for the same
// job (after a crash, a lost lease or a redelivered message). Handlers must be
// idempotent and should deduplicate side effects on Job.ID, which stays the
// same across retries and duplicates.
//
// A handler's return value decides the attempt:
//
//	nil             the job succeeded
//	Permanent(err)  the job goes dead now, without further attempts
//	any other error the job is retried with backoff until MaxAttempts, then goes dead
//
// A panic is recovered, its stack trace is logged to Config.Logger, and the
// attempt is retried like an ordinary error. Error text stays in the worker's
// logs; the scheduler records only a fixed outcome such as "handler failed",
// so payload data cannot leak into job history.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/parsarazor/distributed-job-scheduler/internal/broker"
	"github.com/parsarazor/distributed-job-scheduler/internal/domain"
	"github.com/parsarazor/distributed-job-scheduler/internal/service"
	"github.com/parsarazor/distributed-job-scheduler/internal/workersdk"
	apiv1 "github.com/parsarazor/distributed-job-scheduler/pkg/api/v1"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

func init() {
	workersdk.Service = func(w any) *service.Worker { return w.(*Worker).svc }
}

const (
	startupTimeout = 15 * time.Second
	probeInterval  = 5 * time.Second
	unwindTimeout  = 5 * time.Second
)

// Job is one execution attempt handed to a Handler.
type Job struct {
	ID, TenantID, Name string
	Payload            []byte
	// Attempt starts at 1 and counts every execution, including this one.
	Attempt, MaxAttempts int
	ScheduledAt          time.Time
}

// Decode unmarshals the job's JSON payload into v.
func (j *Job) Decode(v any) error {
	if len(j.Payload) == 0 {
		return errors.New("worker: job has no payload")
	}
	return json.Unmarshal(j.Payload, v)
}

// Handler runs one job. ctx is cancelled when the job times out, the lease is
// lost, an operator cancels the execution or shutdown runs out of time;
// context.Cause(ctx) tells which. A handler must return promptly once ctx is done.
type Handler interface {
	Handle(ctx context.Context, job *Job) error
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(ctx context.Context, job *Job) error

// Handle calls f(ctx, job).
func (f HandlerFunc) Handle(ctx context.Context, job *Job) error { return f(ctx, job) }

// Worker consumes job announcements and runs registered handlers.
type Worker struct {
	cfg         Config
	conn        *grpc.ClientConn
	scheduler   apiv1.SchedulerServiceClient
	health      healthpb.HealthClient
	svc         *service.Worker
	mu          sync.Mutex
	names       map[string]struct{}
	started     atomic.Bool
	running     atomic.Bool
	schedulerOK atomic.Bool
	broker      atomic.Pointer[broker.RabbitMQBroker]
}

// New validates cfg and prepares a worker. It does no network I/O; Run connects.
func New(cfg Config) (*Worker, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	var transport credentials.TransportCredentials
	switch {
	case cfg.Insecure:
		transport = insecure.NewCredentials()
	case cfg.TLS != nil:
		transport = credentials.NewTLS(cfg.TLS.Clone())
	default:
		transport = credentials.NewTLS(nil)
	}
	conn, err := grpc.NewClient(cfg.SchedulerAddr,
		grpc.WithTransportCredentials(transport),
		grpc.WithPerRPCCredentials(bearer{token: cfg.Token, secure: !cfg.Insecure}),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig":[{"round_robin":{}}]}`),
	)
	if err != nil {
		return nil, fmt.Errorf("worker: configure scheduler client: %w", err)
	}
	scheduler := apiv1.NewSchedulerServiceClient(conn)
	svc := service.NewRemoteWorker(cfg.WorkerID, service.NewRemoteJobControl(scheduler), int64(cfg.Concurrency), cfg.LeaseTTL)
	svc.SetJobTimeout(cfg.JobTimeout)
	svc.SetCompletionWindow(cfg.CompletionWindow)
	svc.SetLogger(cfg.Logger)
	return &Worker{
		cfg: cfg, conn: conn, scheduler: scheduler, health: healthpb.NewHealthClient(conn),
		svc: svc, names: make(map[string]struct{}),
	}, nil
}

// Handle registers h for jobs named name. It panics if name is empty, h is
// nil, name is already registered, or Run has started.
func (w *Worker) Handle(name string, h Handler) {
	if name == "" {
		panic("worker: empty job name")
	}
	if h == nil {
		panic("worker: nil handler for " + name)
	}
	if f, ok := h.(HandlerFunc); ok && f == nil {
		panic("worker: nil handler for " + name)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.started.Load() {
		panic("worker: Handle called after Run")
	}
	if _, dup := w.names[name]; dup {
		panic("worker: duplicate handler for " + name)
	}
	w.names[name] = struct{}{}
	w.svc.RegisterHandler(name, func(ctx context.Context, j *domain.Job) error {
		return h.Handle(ctx, toJob(j))
	})
}

// HandleFunc registers fn for jobs named name, like Handle.
func (w *Worker) HandleFunc(name string, fn func(ctx context.Context, job *Job) error) {
	if fn == nil {
		panic("worker: nil handler for " + name)
	}
	w.Handle(name, HandlerFunc(fn))
}

// Ready reports whether the worker is accepting jobs: Run has connected, the
// RabbitMQ subscription is live, the latest scheduler probe accepted the
// token, and the worker is not draining. Use it for readiness probes.
func (w *Worker) Ready() bool {
	rb := w.broker.Load()
	return w.running.Load() && w.schedulerOK.Load() && !w.svc.IsDraining() && rb != nil && rb.ConsumerReady()
}

// Run connects to the scheduler and RabbitMQ and runs handlers until ctx is
// cancelled, then stops taking jobs and waits up to Config.ShutdownTimeout
// for running handlers. It returns nil after a clean drain. It returns an
// error if startup fails, intake stops unexpectedly, or ShutdownTimeout
// expires with handlers still running (their contexts are then cancelled
// with ErrShutdown). Run may be called once.
func (w *Worker) Run(ctx context.Context) error {
	w.mu.Lock()
	first := w.started.CompareAndSwap(false, true)
	w.mu.Unlock()
	if !first {
		return errors.New("worker: Run called more than once")
	}
	defer func() { _ = w.conn.Close() }()
	startCtx, cancelStart := context.WithTimeout(ctx, startupTimeout)
	if err := w.probeScheduler(startCtx); err != nil {
		cancelStart()
		return fmt.Errorf("worker: connect scheduler: %w", err)
	}
	w.schedulerOK.Store(true)
	rb, err := broker.NewRabbitMQBrokerContext(startCtx, w.cfg.RabbitMQURL, "jobs")
	cancelStart()
	if err != nil {
		return fmt.Errorf("worker: connect RabbitMQ: %w", err)
	}
	w.broker.Store(rb)
	probeCtx, stopProbe := context.WithCancel(context.Background())
	defer stopProbe()
	go w.probeLoop(probeCtx)

	intakeCtx, stopIntake := context.WithCancel(context.Background())
	defer stopIntake()
	w.svc.SetIntakeCancel(stopIntake)
	intakeDone := make(chan error, 1)
	go func() {
		intakeDone <- rb.Consume(intakeCtx, broker.JobsQueue, broker.JobsRoutingKey, w.cfg.Concurrency, w.svc.HandleMessage)
	}()
	w.running.Store(true)
	w.cfg.Logger.Info("worker consuming", "worker_id", w.cfg.WorkerID)

	var fatal error
	intakeExited := false
	select {
	case <-ctx.Done():
	case err := <-intakeDone:
		intakeExited = true
		if w.svc.IsDraining() {
			// An operator drain stops intake; stay up until asked to stop.
			<-ctx.Done()
		} else {
			fatal = fmt.Errorf("worker: job intake stopped: %w", err)
		}
	}
	w.running.Store(false)
	return errors.Join(fatal, w.shutdown(rb, intakeDone, intakeExited))
}

func (w *Worker) shutdown(rb *broker.RabbitMQBroker, intakeDone <-chan error, intakeExited bool) error {
	w.cfg.Logger.Info("worker draining", "worker_id", w.cfg.WorkerID)
	drainCtx, cancel := context.WithTimeout(context.Background(), w.cfg.ShutdownTimeout)
	defer cancel()
	var result error
	if err := w.svc.Wait(drainCtx); err != nil {
		inFlight := len(w.svc.ActiveJobs())
		w.svc.CancelAll()
		unwind, stop := context.WithTimeout(context.Background(), unwindTimeout)
		_ = w.svc.Wait(unwind)
		stop()
		result = fmt.Errorf("worker: shutdown timeout %s expired with %d jobs in flight", w.cfg.ShutdownTimeout, inFlight)
	}
	if !intakeExited {
		select {
		case <-intakeDone:
		case <-drainCtx.Done():
		}
	}
	closed := make(chan struct{})
	go func() { _ = rb.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
	}
	return result
}

// probeScheduler checks health and then a worker-role RPC, so a rejected
// token fails startup and readiness instead of every claim.
func (w *Worker) probeScheduler(ctx context.Context) error {
	resp, err := w.health.Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		return fmt.Errorf("scheduler health: %w", err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		return fmt.Errorf("scheduler is %s", resp.GetStatus())
	}
	_, err = w.scheduler.GetExecutionJob(ctx, &apiv1.GetExecutionJobRequest{JobId: uuid.Nil.String(), WorkerId: w.cfg.WorkerID})
	switch status.Code(err) {
	case codes.OK, codes.NotFound, codes.Aborted:
		return nil
	case codes.Unauthenticated, codes.PermissionDenied:
		return errors.New("scheduler rejected the worker token")
	default:
		return fmt.Errorf("scheduler worker RPCs: %w", err)
	}
}

func (w *Worker) probeLoop(ctx context.Context) {
	t := time.NewTicker(probeInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			err := w.probeScheduler(probeCtx)
			cancel()
			if ok := err == nil; w.schedulerOK.Swap(ok) != ok && ctx.Err() == nil {
				if ok {
					w.cfg.Logger.Info("scheduler reachable again", "worker_id", w.cfg.WorkerID)
				} else {
					w.cfg.Logger.Warn("scheduler probe failed; worker not ready", "worker_id", w.cfg.WorkerID, "error", err)
				}
			}
		}
	}
}

func toJob(j *domain.Job) *Job {
	return &Job{
		ID: j.ID.String(), TenantID: j.TenantID.String(), Name: j.Name,
		Payload: append([]byte(nil), j.Payload...), Attempt: j.AttemptCount + 1,
		MaxAttempts: j.MaxAttempts, ScheduledAt: j.ScheduledAt,
	}
}

type bearer struct {
	token  string
	secure bool
}

func (b bearer) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + b.token}, nil
}

func (b bearer) RequireTransportSecurity() bool { return b.secure }
