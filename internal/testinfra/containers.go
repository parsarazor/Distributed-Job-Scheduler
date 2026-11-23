// Package testinfra provides opt-in, disposable services for live contract tests.
// Importing it never contacts Docker; callers must explicitly set INTEGRATION=1.
package testinfra

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// RequireIntegration gates all Docker access, including container discovery.
func RequireIntegration(t *testing.T) {
	t.Helper()
	if os.Getenv("INTEGRATION") != "1" {
		t.Skip("live integration test requires INTEGRATION=1 (Docker or explicit disposable TEST_POSTGRES_DSN/TEST_RABBITMQ_URL)")
	}
}

// Postgres owns a migrated, disposable PostgreSQL server.
type Postgres struct {
	DSN string
}

// StartPostgres starts an isolated server and applies every *.up.sql migration.
func StartPostgres(t *testing.T) *Postgres {
	t.Helper()
	RequireIntegration(t)
	// An explicitly selected test server lets the same contract suite run on
	// hosts without Docker. Always create a fresh database: DATABASE_URL is an
	// application setting and must never opt an ordinary test into live writes.
	if dsn := os.Getenv("TEST_POSTGRES_DSN"); dsn != "" {
		admin := &Postgres{DSN: dsn}
		return &Postgres{DSN: admin.NewDatabase(t)}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	ctr := startContainer(t, ctx, testcontainers.ContainerRequest{
		Image:        "postgres:17-alpine@sha256:526f86d93c6ea08fd3f1fe5605650cacc781b2c6013169a0ad8f42c7d3d84f32",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "scheduler",
			"POSTGRES_PASSWORD": "scheduler",
			"POSTGRES_DB":       "scheduler",
		},
		WaitingFor: wait.ForAll(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
			wait.ForListeningPort("5432/tcp"),
		).WithDeadline(90 * time.Second),
	})
	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatalf("Postgres host: %v", err)
	}
	port, err := ctr.MappedPort(ctx, "5432/tcp")
	if err != nil {
		t.Fatalf("Postgres mapped port: %v", err)
	}
	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword("scheduler", "scheduler"),
		Host:     net.JoinHostPort(host, port.Port()),
		Path:     "/scheduler",
		RawQuery: "sslmode=disable",
	}
	p := &Postgres{DSN: u.String()}
	migrate(t, ctx, p.DSN)
	return p
}

// NewDatabase gives each subtest its own migrated database on the same server.
// Scheduler scans are global, so tenant isolation alone is insufficient.
func (p *Postgres) NewDatabase(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, p.DSN)
	if err != nil {
		t.Fatalf("connect to create test database: %v", err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	name := "test_" + uuid.NewString()
	identifier := pgx.Identifier{name}.Sanitize()
	if _, err := conn.Exec(ctx, "CREATE DATABASE "+identifier); err != nil {
		t.Fatalf("create test database: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		admin, err := pgx.Connect(cleanupCtx, p.DSN)
		if err != nil {
			t.Errorf("connect to drop test database: %v", err)
			return
		}
		defer func() { _ = admin.Close(cleanupCtx) }()
		if _, err := admin.Exec(cleanupCtx, "DROP DATABASE "+identifier+" WITH (FORCE)"); err != nil {
			t.Errorf("drop test database: %v", err)
		}
	})
	u, err := url.Parse(p.DSN)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name
	dsn := u.String()
	migrate(t, ctx, dsn)
	return dsn
}

func migrate(t *testing.T, ctx context.Context, dsn string) {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			break
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("cannot find repository migrations from test working directory")
		}
		dir = parent
	}
	files, err := filepath.Glob(filepath.Join(dir, "migrations", "*.up.sql"))
	if err != nil || len(files) == 0 {
		t.Fatalf("find up migrations: files=%v err=%v", files, err)
	}
	sort.Strings(files)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect for migrations: %v", err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin migrations: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	for _, path := range files {
		sql, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read migration %s: %v", filepath.Base(path), err)
		}
		if _, err := tx.Exec(ctx, string(sql)); err != nil {
			t.Fatalf("apply migration %s: %v", filepath.Base(path), err)
		}
	}
	// Mirror the deployment runner's version marker so runtime schema checks
	// are exercised by live tests as well as by the Compose migration smoke.
	last := strings.SplitN(filepath.Base(files[len(files)-1]), "_", 2)[0]
	version, err := strconv.ParseInt(last, 10, 64)
	if err != nil {
		t.Fatalf("read migration version: %v", err)
	}
	if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (version bigint NOT NULL PRIMARY KEY, dirty boolean NOT NULL)`); err != nil {
		t.Fatalf("create migration version marker: %v", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version, dirty) VALUES ($1, false)`, version); err != nil {
		t.Fatalf("record migration version: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit migrations: %v", err)
	}
}

// RabbitMQ exposes lifecycle control for reconnect and persistence tests.
type RabbitMQ struct {
	Container testcontainers.Container
	URL       string
}

// Restart exercises broker persistence and client reconnection. Native test
// servers opt in with an explicit executable that controls only that server.
func (r *RabbitMQ) Restart(ctx context.Context) error {
	if r.Container != nil {
		timeout := time.Second
		if err := r.Container.Stop(ctx, &timeout); err != nil {
			return err
		}
		return r.Container.Start(ctx)
	}
	command := os.Getenv("TEST_RABBITMQ_RESTART")
	if !filepath.IsAbs(command) {
		return fmt.Errorf("native broker restart requires an absolute TEST_RABBITMQ_RESTART executable")
	}
	output, err := exec.CommandContext(ctx, command).CombinedOutput()
	if err != nil {
		return fmt.Errorf("restart disposable RabbitMQ: %w: %.4096s", err, output)
	}
	return nil
}

func StartRabbitMQ(t *testing.T) *RabbitMQ {
	t.Helper()
	RequireIntegration(t)
	// This URL must select a disposable broker/vhost dedicated to this suite;
	// integration tests declare and delete their canonical execution queues.
	if rawURL := os.Getenv("TEST_RABBITMQ_URL"); rawURL != "" {
		parsed, err := url.Parse(rawURL)
		if err != nil || parsed.Host == "" || (parsed.Scheme != "amqp" && parsed.Scheme != "amqps") {
			t.Fatal("TEST_RABBITMQ_URL must be an explicit amqp(s) URL for a disposable test broker")
		}
		return &RabbitMQ{URL: rawURL}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	ctr := startContainer(t, ctx, testcontainers.ContainerRequest{
		Image:        "rabbitmq:4.1-alpine@sha256:cd4009416f83d7e5895020cd71b0963f3b8a4dfc3de74bbb6fb3bfe5f81f01e0",
		ExposedPorts: []string{"5672/tcp"},
		Env: map[string]string{
			"RABBITMQ_DEFAULT_USER": "scheduler",
			"RABBITMQ_DEFAULT_PASS": "scheduler",
		},
		WaitingFor: wait.ForListeningPort("5672/tcp").WithStartupTimeout(90 * time.Second),
	})
	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatalf("RabbitMQ host: %v", err)
	}
	port, err := ctr.MappedPort(ctx, "5672/tcp")
	if err != nil {
		t.Fatalf("RabbitMQ mapped port: %v", err)
	}
	u := url.URL{Scheme: "amqp", User: url.UserPassword("scheduler", "scheduler"), Host: net.JoinHostPort(host, port.Port()), Path: "/"}
	return &RabbitMQ{Container: ctr, URL: u.String()}
}

func startContainer(t *testing.T, ctx context.Context, req testcontainers.ContainerRequest) testcontainers.Container {
	t.Helper()
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	testcontainers.CleanupContainer(t, ctr)
	if ctr != nil {
		t.Cleanup(func() {
			if !t.Failed() {
				return
			}
			logCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			logs, err := ctr.Logs(logCtx)
			if err == nil {
				defer func() { _ = logs.Close() }()
				body, _ := io.ReadAll(io.LimitReader(logs, 64<<10))
				t.Logf("%s container logs:\n%s", req.Image, body)
			}
		})
	}
	if err != nil {
		t.Fatalf("start %s testcontainer (INTEGRATION=1 requires working Docker): %v", req.Image, err)
	}
	return ctr
}
