package domain

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestTransitionTo_RefreshesUpdatedAt(t *testing.T) {
	old := time.Now().UTC().Add(-time.Hour)
	job := &Job{Status: StatusPending, UpdatedAt: old}
	if err := job.TransitionTo(StatusRunning); err != nil {
		t.Fatal(err)
	}
	if !job.UpdatedAt.After(old) {
		t.Fatalf("UpdatedAt not refreshed: %v", job.UpdatedAt)
	}
}

func TestNewTenant_HashesKey(t *testing.T) {
	tn, err := NewTenant("acme", "raw-secret")
	if err != nil {
		t.Fatal(err)
	}
	if tn.APIKeyHash == "raw-secret" {
		t.Fatal("plaintext key stored")
	}
	if len(tn.APIKeyHash) != 64 {
		t.Fatalf("expected 64-char hex SHA-256, got %q", tn.APIKeyHash)
	}
	if !tn.VerifyAPIKey("raw-secret") || tn.VerifyAPIKey("wrong") {
		t.Fatal("VerifyAPIKey broken")
	}
	if _, err := NewTenant("acme", ""); err == nil {
		t.Fatal("empty key must be rejected")
	}
	if _, err := NewTenant("", "k"); err == nil {
		t.Fatal("empty name must be rejected")
	}
}

func TestOutbox_Ceiling(t *testing.T) {
	e := NewOutboxEvent(uuid.New(), "jobs.dispatch", nil)
	if e.ExceededMaxRetries() {
		t.Fatal("fresh event must not be abandoned")
	}
	for i := 0; i < MaxOutboxRetries; i++ {
		e.RecordFailure("boom")
	}
	if !e.ExceededMaxRetries() {
		t.Fatalf("after %d failures must be abandoned", MaxOutboxRetries)
	}
}

func TestDetectDependencyCycle(t *testing.T) {
	a, b, c := uuid.New(), uuid.New(), uuid.New()
	// b -> a, c -> b. Adding a -> c must fail.
	parents := map[uuid.UUID]*uuid.UUID{a: nil, b: &a, c: &b}
	lookup := func(id uuid.UUID) (*uuid.UUID, error) { return parents[id], nil }

	// Reparenting c (c->b->a) directly under a is still acyclic.
	if err := DetectDependencyCycle(c, a, lookup); err != nil {
		t.Fatalf("unexpected cycle reparenting c->a: %v", err)
	}

	// Real cycle: make a depend on c (a->c->b->a).
	if err := DetectDependencyCycle(a, c, lookup); err == nil {
		t.Fatal("expected cycle adding a->c")
	}
	// Self dependency.
	if err := DetectDependencyCycle(a, a, lookup); err == nil {
		t.Fatal("expected self-dependency error")
	}
	// Acyclic: d -> a is fine.
	d := uuid.New()
	if err := DetectDependencyCycle(d, a, lookup); err != nil {
		t.Fatalf("unexpected cycle: %v", err)
	}
}

func TestNewJob_Defaults(t *testing.T) {
	j, err := NewJob(uuid.New(), "n", nil, PriorityHigh, 0, nil, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if j.Status != StatusPending || j.MaxAttempts != 3 || string(j.Payload) != `{}` {
		t.Fatalf("bad defaults: %+v", j)
	}
	if _, err := NewJob(uuid.New(), "n", nil, JobPriority(99), 3, nil, time.Now()); err == nil {
		t.Fatal("invalid priority must be rejected")
	}
}
