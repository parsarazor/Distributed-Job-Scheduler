package app

import (
	"strings"
	"testing"
)

// Pool bounds and session timeouts must be applied on every database config so
// a slow query or wedged transaction cannot pin the connection pool (review C2).
func TestParseDatabaseConfig_AppliesPoolAndTimeoutDefaults(t *testing.T) {
	cfg, err := parseDatabaseConfig("postgres://app:custom@localhost/scheduler?sslmode=verify-full", true)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxConns < 4 || cfg.MinConns < 1 {
		t.Fatalf("pool bounds not defaulted: max=%d min=%d", cfg.MaxConns, cfg.MinConns)
	}
	opts := cfg.ConnConfig.RuntimeParams["options"]
	if !strings.Contains(opts, "statement_timeout=") || !strings.Contains(opts, "idle_in_transaction_session_timeout=") {
		t.Fatalf("session timeouts missing from runtime params: %q", opts)
	}
}

// Overrides via environment must win over the defaults.
func TestParseDatabaseConfig_EnvironmentOverrides(t *testing.T) {
	t.Setenv("DB_POOL_MAX_CONNS", "64")
	t.Setenv("DB_POOL_MIN_CONNS", "2")
	t.Setenv("DB_STATEMENT_TIMEOUT", "5s")
	t.Setenv("DB_IDLE_TX_TIMEOUT", "1s")
	cfg, err := parseDatabaseConfig("postgres://app:custom@localhost/scheduler?sslmode=verify-full", true)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxConns != 64 || cfg.MinConns != 2 {
		t.Fatalf("pool overrides ignored: max=%d min=%d", cfg.MaxConns, cfg.MinConns)
	}
	opts := cfg.ConnConfig.RuntimeParams["options"]
	if !strings.Contains(opts, "statement_timeout=5000") || !strings.Contains(opts, "idle_in_transaction_session_timeout=1000") {
		t.Fatalf("timeout overrides ignored: %q", opts)
	}
}
