package domain

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

// Principle: table-driven validation. Every constructor rejection rule gets
// its own row so a future relaxation breaks exactly one named case.
func TestNewJob_ValidationTable(t *testing.T) {
	t.Parallel()
	validTenant := uuid.New()
	cases := []struct {
		name     string
		tenant   uuid.UUID
		jobName  string
		priority JobPriority
		wantErr  bool
	}{
		{"ok low", validTenant, "j", PriorityLow, false},
		{"ok default", validTenant, "j", PriorityDefault, false},
		{"ok high", validTenant, "j", PriorityHigh, false},
		{"ok critical", validTenant, "j", PriorityCritical, false},
		{"nil tenant", uuid.Nil, "j", PriorityDefault, true},
		{"empty name", validTenant, "", PriorityDefault, true},
		{"negative priority", validTenant, "j", JobPriority(-1), true},
		{"gap priority 1", validTenant, "j", JobPriority(1), true},
		{"gap priority 3", validTenant, "j", JobPriority(3), true},
		{"gap priority 99", validTenant, "j", JobPriority(99), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewJob(tc.tenant, tc.jobName, nil, tc.priority, 3, nil, time.Now().UTC())
			if tc.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestIsValidPriority_Table(t *testing.T) {
	t.Parallel()
	for _, p := range []JobPriority{PriorityLow, PriorityDefault, PriorityHigh, PriorityCritical} {
		if !IsValidPriority(p) {
			t.Errorf("defined weight %d must be valid", int(p))
		}
	}
	for _, p := range []JobPriority{-10, -1, 1, 3, 4, 6, 9, 11, 100} {
		if IsValidPriority(p) {
			t.Errorf("undefined weight %d must be invalid", int(p))
		}
	}
}

func TestCanCancelAndIsTerminal_Table(t *testing.T) {
	t.Parallel()
	cases := []struct {
		status      JobStatus
		cancellable bool
		terminal    bool
	}{
		{StatusPending, true, false},
		{StatusRunning, false, false},
		{StatusFailed, false, false},
		{StatusSucceeded, false, true},
		{StatusCancelled, false, true},
		{StatusDead, false, true},
	}
	for _, tc := range cases {
		t.Run(string(tc.status), func(t *testing.T) {
			t.Parallel()
			j := &Job{Status: tc.status}
			if j.CanCancel() != tc.cancellable {
				t.Errorf("CanCancel=%v want %v", j.CanCancel(), tc.cancellable)
			}
			if j.IsTerminal() != tc.terminal {
				t.Errorf("IsTerminal=%v want %v", j.IsTerminal(), tc.terminal)
			}
		})
	}
}

// Principle: error-contract test. Callers branch on sentinels with errors.Is;
// the wrapping in ValidateTransition must preserve them.
func TestValidateTransition_ErrorContract(t *testing.T) {
	t.Parallel()
	if err := ValidateTransition(JobStatus("bogus"), StatusRunning); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("unknown source must wrap ErrInvalidTransition, got %v", err)
	}
	j := &Job{Status: StatusPending}
	if err := j.TransitionTo(StatusSucceeded); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("illegal jump must wrap ErrInvalidTransition, got %v", err)
	}
	// Sentinel identity: all domain sentinels must be distinct non-nil errors.
	sentinels := []error{ErrJobNotFound, ErrTenantNotFound, ErrInvalidTransition, ErrFencingTokenStale, ErrDependencyNotMet, ErrJobCannotBeCancelled, ErrMaxAttemptsReached, ErrDependencyCycle}
	seen := map[string]bool{}
	for _, s := range sentinels {
		if s == nil {
			t.Fatal("nil sentinel")
		}
		if seen[s.Error()] {
			t.Fatalf("duplicate sentinel message: %q", s.Error())
		}
		seen[s.Error()] = true
	}
}

func TestOutbox_Methods(t *testing.T) {
	t.Parallel()
	t.Run("NewOutboxEvent defaults", func(t *testing.T) {
		t.Parallel()
		e := NewOutboxEvent(uuid.New(), "k", nil)
		if e.Published || e.RetryCount != 0 || string(e.Payload) != `{}` || e.PublishedAt != nil {
			t.Fatalf("bad defaults: %+v", e)
		}
	})
	t.Run("MarkPublished stamps time", func(t *testing.T) {
		t.Parallel()
		e := NewOutboxEvent(uuid.New(), "k", nil)
		before := time.Now().UTC()
		e.MarkPublished()
		if !e.Published || e.PublishedAt == nil || e.PublishedAt.Before(before) {
			t.Fatalf("bad publish stamp: %+v", e)
		}
	})
	t.Run("RecordFailure accumulates", func(t *testing.T) {
		t.Parallel()
		e := NewOutboxEvent(uuid.New(), "k", nil)
		e.RecordFailure("a")
		e.RecordFailure("b")
		if e.RetryCount != 2 || e.LastError == nil || *e.LastError != "b" {
			t.Fatalf("bad failure record: %+v", e)
		}
	})
	t.Run("ceiling boundary", func(t *testing.T) {
		t.Parallel()
		e := NewOutboxEvent(uuid.New(), "k", nil)
		for i := 0; i < MaxOutboxRetries-1; i++ {
			e.RecordFailure("x")
		}
		if e.ExceededMaxRetries() {
			t.Fatal("must not trip one below the ceiling")
		}
		e.RecordFailure("x")
		if !e.ExceededMaxRetries() {
			t.Fatal("must trip exactly at the ceiling")
		}
	})
}

func TestTenant_HashContract(t *testing.T) {
	t.Parallel()
	t.Run("deterministic", func(t *testing.T) {
		t.Parallel()
		if HashAPIKey("k") != "8254c329a92850f6d539dd376f4816ee2764517da5e0235514af433164480d7a" || HashAPIKey("a") == HashAPIKey("b") {
			t.Fatal("hash must be deterministic and distinguishing")
		}
	})
	t.Run("wrong length never verifies", func(t *testing.T) {
		t.Parallel()
		tn := &Tenant{APIKeyHash: "short"}
		if tn.VerifyAPIKey("short") {
			t.Fatal("length mismatch must not verify")
		}
	})
	t.Run("whitespace name rejected", func(t *testing.T) {
		t.Parallel()
		if _, err := NewTenant("   ", "k"); err == nil {
			t.Fatal("blank name must be rejected")
		}
	})
}

func TestNewDefaultBackoffStrategy_Values(t *testing.T) {
	t.Parallel()
	s := NewDefaultBackoffStrategy()
	if s.BaseDelay != time.Second || s.MaxDelay != time.Hour || s.Factor != 2.0 {
		t.Fatalf("defaults changed: %+v", s)
	}
	if d := s.NextDelay(1); d < 0 || d > time.Second {
		t.Fatalf("attempt-1 delay %v outside [0, 1s]", d)
	}
}
