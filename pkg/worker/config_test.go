package worker

import (
	"crypto/tls"
	"strings"
	"testing"
	"time"
)

const prodToken = "djwk_0123456789abcdef0123456789abcdef"

func validConfig() Config {
	return Config{SchedulerAddr: "scheduler:50051", RabbitMQURL: "amqps://app:s3cret@rabbit/", WorkerID: "edge-1", Token: prodToken}
}

func TestNew_AppliesDefaults(t *testing.T) {
	w, err := New(validConfig())
	if err != nil {
		t.Fatal(err)
	}
	c := w.cfg
	if c.Concurrency != DefaultConcurrency || c.LeaseTTL != DefaultLeaseTTL || c.JobTimeout != DefaultJobTimeout ||
		c.CompletionWindow != DefaultCompletionWindow || c.ShutdownTimeout != DefaultShutdownTimeout || c.Logger == nil {
		t.Fatalf("defaults not applied: %+v", c)
	}
	if w.Ready() {
		t.Fatal("worker ready before Run")
	}
}

func TestNew_RejectsInvalidConfig(t *testing.T) {
	for name, mutate := range map[string]func(*Config){
		"no scheduler":           func(c *Config) { c.SchedulerAddr = "" },
		"no broker":              func(c *Config) { c.RabbitMQURL = "" },
		"space in worker id":     func(c *Config) { c.WorkerID = "edge 1" },
		"colon in worker id":     func(c *Config) { c.WorkerID = "edge:1" },
		"empty worker id":        func(c *Config) { c.WorkerID = "" },
		"no token":               func(c *Config) { c.Token = "" },
		"short production token": func(c *Config) { c.Token = "short" },
		"plaintext broker":       func(c *Config) { c.RabbitMQURL = "amqp://app:s3cret@rabbit/" },
		"dev broker password":    func(c *Config) { c.RabbitMQURL = "amqps://app:guest@rabbit/" },
		"negative concurrency":   func(c *Config) { c.Concurrency = -1 },
		"lease too long":         func(c *Config) { c.LeaseTTL = 11 * time.Minute },
		"lease too short":        func(c *Config) { c.LeaseTTL = 100 * time.Microsecond },
		"negative shutdown":      func(c *Config) { c.ShutdownTimeout = -time.Second },
	} {
		t.Run(name, func(t *testing.T) {
			c := validConfig()
			mutate(&c)
			if _, err := New(c); err == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}
}

func TestNew_InsecureAllowsLocalDevelopment(t *testing.T) {
	c := validConfig()
	c.Insecure, c.RabbitMQURL, c.Token = true, "amqp://guest:guest@localhost/", "local-development-worker-token-change-me"
	if _, err := New(c); err != nil {
		t.Fatal(err)
	}
	c.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	if w, err := New(c); err != nil || w.cfg.TLS == nil {
		t.Fatalf("development worker with scheduler TLS rejected: %v", err)
	}
}

func setEnv(t *testing.T, env map[string]string) {
	t.Helper()
	for _, k := range []string{"DATABASE_URL", "APP_ENV", "SCHEDULER_ADDR", "RABBITMQ_URL", "WORKER_ID", "WORKER_TOKEN",
		"SCHEDULER_CA_FILE", "SCHEDULER_TLS_SERVER_NAME", "MAX_CONCURRENCY", "LEASE_TTL", "JOB_TIMEOUT", "COMPLETION_RETRY_WINDOW", "SHUTDOWN_TIMEOUT"} {
		t.Setenv(k, env[k])
	}
}

func devEnv() map[string]string {
	return map[string]string{"APP_ENV": "development", "SCHEDULER_ADDR": "scheduler:50051", "RABBITMQ_URL": "amqp://u:p@q/", "WORKER_TOKEN": "local-development-worker-token-change-me"}
}

func TestConfigFromEnv_Development(t *testing.T) {
	env := devEnv()
	env["SHUTDOWN_TIMEOUT"], env["MAX_CONCURRENCY"] = "7s", "3"
	setEnv(t, env)
	c, err := ConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if !c.Insecure || c.TLS != nil || c.WorkerID != "worker-1" || c.ShutdownTimeout != 7*time.Second || c.Concurrency != 3 {
		t.Fatalf("unexpected config: %+v", c)
	}
	if _, err := New(c); err != nil {
		t.Fatal(err)
	}
}

func TestConfigFromEnv_ProductionUsesTLS(t *testing.T) {
	setEnv(t, map[string]string{"SCHEDULER_ADDR": "scheduler:50051", "RABBITMQ_URL": "amqps://app:s3cret@rabbit/", "WORKER_TOKEN": prodToken, "SCHEDULER_TLS_SERVER_NAME": "scheduler.internal"})
	c, err := ConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if c.Insecure || c.TLS == nil || c.TLS.ServerName != "scheduler.internal" || c.TLS.MinVersion != tls.VersionTLS12 {
		t.Fatalf("production config is not TLS: %+v", c)
	}
	c.RabbitMQURL = "amqp://app:s3cret@rabbit/"
	if _, err := New(c); err == nil {
		t.Fatal("production config accepted plaintext RabbitMQ")
	}
}

func TestConfigFromEnv_Rejects(t *testing.T) {
	for name, change := range map[string][2]string{
		"database credentials": {"DATABASE_URL", "postgres://scheduler:scheduler@db/scheduler"},
		"no scheduler":         {"SCHEDULER_ADDR", ""},
		"no broker":            {"RABBITMQ_URL", ""},
		"no token":             {"WORKER_TOKEN", ""},
		"bad worker id":        {"WORKER_ID", "édge"},
		"bad app env":          {"APP_ENV", "staging"},
		"bad lease":            {"LEASE_TTL", "soon"},
		"bad concurrency":      {"MAX_CONCURRENCY", "0"},
		"missing CA file":      {"SCHEDULER_CA_FILE", "/nonexistent/ca.pem"},
	} {
		t.Run(name, func(t *testing.T) {
			env := devEnv()
			env[change[0]] = change[1]
			setEnv(t, env)
			if _, err := ConfigFromEnv(); err == nil {
				t.Fatalf("accepted %s=%q", change[0], change[1])
			} else if change[0] == "WORKER_ID" && !strings.Contains(err.Error(), "WORKER_ID") {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
