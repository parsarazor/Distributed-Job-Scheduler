package worker

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/parsarazor/distributed-job-scheduler/internal/broker"
	"github.com/parsarazor/distributed-job-scheduler/internal/domain"
	"github.com/parsarazor/distributed-job-scheduler/internal/service"
)

// Defaults applied by New to zero Config fields.
const (
	DefaultConcurrency      = 10
	DefaultLeaseTTL         = 30 * time.Second
	DefaultJobTimeout       = 5 * time.Minute
	DefaultCompletionWindow = service.DefaultCompletionWindow
	DefaultShutdownTimeout  = 20 * time.Second
)

// Config configures a Worker. The worker has no database access: it consumes
// job announcements from RabbitMQ and reaches job state only through the
// scheduler's gRPC API.
type Config struct {
	// SchedulerAddr is the scheduler's gRPC target. A name that resolves to
	// several replicas spreads calls over all of them.
	SchedulerAddr string
	// RabbitMQURL must use amqps:// unless Insecure is set. RabbitMQ TLS (CA
	// and client certificates) is configured through the URL, not TLS.
	RabbitMQURL string
	// WorkerID must match ^[A-Za-z0-9._-]{1,255}$ and the ID the Token was
	// issued for.
	WorkerID string
	// Token is this worker's credential from `schedctl worker create`, or the
	// shared WORKER_TOKEN while that is still enabled.
	Token string
	// TLS secures the gRPC connection to the scheduler only. Nil selects the
	// system roots with TLS 1.2 or later.
	TLS *tls.Config
	// Insecure allows plaintext gRPC and amqp:// for local development and
	// skips production credential checks. It cannot be combined with TLS.
	Insecure bool
	// Concurrency bounds handlers running at once (default 10, max 65535).
	Concurrency int
	// LeaseTTL is the lease renewed while a handler runs (default 30s, max 10m).
	LeaseTTL time.Duration
	// JobTimeout caps every execution; a job's own shorter timeout wins (default 5m).
	JobTimeout time.Duration
	// CompletionWindow is how long a finished result is retried while the
	// scheduler is unreachable (default 1m).
	CompletionWindow time.Duration
	// ShutdownTimeout bounds how long Run waits for running handlers after
	// its context is cancelled (default 20s).
	ShutdownTimeout time.Duration
	// Logger receives execution logs, including handler panic stack traces.
	// Nil selects slog.Default().
	Logger *slog.Logger
}

func (c Config) withDefaults() Config {
	if c.Concurrency == 0 {
		c.Concurrency = DefaultConcurrency
	}
	if c.LeaseTTL == 0 {
		c.LeaseTTL = DefaultLeaseTTL
	}
	if c.JobTimeout == 0 {
		c.JobTimeout = DefaultJobTimeout
	}
	if c.CompletionWindow == 0 {
		c.CompletionWindow = DefaultCompletionWindow
	}
	if c.ShutdownTimeout == 0 {
		c.ShutdownTimeout = DefaultShutdownTimeout
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return c
}

func (c Config) validate() error {
	if c.SchedulerAddr == "" {
		return errors.New("worker: SchedulerAddr is required")
	}
	if c.RabbitMQURL == "" {
		return errors.New("worker: RabbitMQURL is required")
	}
	if !domain.ValidWorkerID(c.WorkerID) {
		return fmt.Errorf("worker: WorkerID %q must match %s", c.WorkerID, domain.WorkerIDPattern)
	}
	if c.Token == "" || strings.ContainsFunc(c.Token, func(r rune) bool { return r < '!' || r > '~' }) {
		return errors.New("worker: Token is required and must be visible ASCII")
	}
	if c.Insecure && c.TLS != nil {
		return errors.New("worker: Insecure cannot be combined with TLS")
	}
	if !c.Insecure && !domain.ValidProductionToken(c.Token) {
		return errors.New("worker: Token must contain at least 32 visible ASCII characters unless Insecure is set")
	}
	if err := broker.ValidateURL(c.RabbitMQURL, c.Insecure); err != nil {
		return fmt.Errorf("worker: %w", err)
	}
	if c.Concurrency < 1 || c.Concurrency > 65535 {
		return errors.New("worker: Concurrency must be between 1 and 65535")
	}
	if c.LeaseTTL < time.Millisecond || c.LeaseTTL > service.MaxWorkerLease {
		return fmt.Errorf("worker: LeaseTTL must be between 1ms and %s", service.MaxWorkerLease)
	}
	if c.JobTimeout < 0 || c.CompletionWindow < 0 || c.ShutdownTimeout < 0 {
		return errors.New("worker: durations must not be negative")
	}
	return nil
}

// ConfigFromEnv reads the environment variables cmd/worker uses:
//
//	SCHEDULER_ADDR, RABBITMQ_URL, WORKER_TOKEN   required
//	WORKER_ID                                    default worker-1
//	SCHEDULER_CA_FILE, SCHEDULER_TLS_SERVER_NAME gRPC TLS roots and server name
//	APP_ENV=development                          Insecure, unless SCHEDULER_CA_FILE is set
//	MAX_CONCURRENCY, LEASE_TTL, JOB_TIMEOUT, COMPLETION_RETRY_WINDOW, SHUTDOWN_TIMEOUT
//
// It refuses DATABASE_URL: job state belongs to the scheduler.
func ConfigFromEnv() (Config, error) {
	if os.Getenv("DATABASE_URL") != "" {
		return Config{}, errors.New("workers must not receive DATABASE_URL: the scheduler owns job state; set SCHEDULER_ADDR")
	}
	development := false
	switch os.Getenv("APP_ENV") {
	case "development":
		development = true
	case "", "production":
	default:
		return Config{}, errors.New("APP_ENV must be development or production")
	}
	cfg := Config{
		SchedulerAddr: os.Getenv("SCHEDULER_ADDR"), RabbitMQURL: os.Getenv("RABBITMQ_URL"),
		WorkerID: os.Getenv("WORKER_ID"), Token: os.Getenv("WORKER_TOKEN"),
	}
	if cfg.SchedulerAddr == "" || cfg.RabbitMQURL == "" {
		return cfg, errors.New("SCHEDULER_ADDR and RABBITMQ_URL are required")
	}
	if cfg.Token == "" {
		return cfg, errors.New("WORKER_TOKEN is required to call the scheduler")
	}
	if cfg.WorkerID == "" {
		cfg.WorkerID = "worker-1"
	}
	if !domain.ValidWorkerID(cfg.WorkerID) {
		return cfg, fmt.Errorf("WORKER_ID %q must match %s", cfg.WorkerID, domain.WorkerIDPattern)
	}
	caFile := os.Getenv("SCHEDULER_CA_FILE")
	if development && caFile == "" {
		cfg.Insecure = true
	} else {
		cfg.TLS = &tls.Config{MinVersion: tls.VersionTLS12, ServerName: os.Getenv("SCHEDULER_TLS_SERVER_NAME")}
		if caFile != "" {
			pem, err := os.ReadFile(caFile)
			if err != nil {
				return cfg, fmt.Errorf("read SCHEDULER_CA_FILE: %w", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return cfg, errors.New("SCHEDULER_CA_FILE contains no PEM certificates")
			}
			cfg.TLS.RootCAs = pool
		}
	}
	if raw := os.Getenv("MAX_CONCURRENCY"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 || n > 65535 {
			return cfg, fmt.Errorf("invalid MAX_CONCURRENCY %q", raw)
		}
		cfg.Concurrency = n
	}
	for _, d := range []struct {
		name string
		dst  *time.Duration
	}{
		{"LEASE_TTL", &cfg.LeaseTTL}, {"JOB_TIMEOUT", &cfg.JobTimeout},
		{"COMPLETION_RETRY_WINDOW", &cfg.CompletionWindow}, {"SHUTDOWN_TIMEOUT", &cfg.ShutdownTimeout},
	} {
		raw := os.Getenv(d.name)
		if raw == "" {
			continue
		}
		v, err := time.ParseDuration(raw)
		if err != nil || v <= 0 {
			return cfg, fmt.Errorf("invalid %s %q: expected a positive duration", d.name, raw)
		}
		*d.dst = v
	}
	return cfg, nil
}
