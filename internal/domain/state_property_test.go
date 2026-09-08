package domain

import (
	"errors"
	"testing"
	"time"
)

// Principle: exhaustive model test. The transition table IS the spec, so test
// the full 6x6 matrix instead of cherry-picked pairs. Any future table edit
// that opens an illegal edge fails here first.
func TestStateMachine_FullMatrix(t *testing.T) {
	t.Parallel()
	all := []JobStatus{StatusPending, StatusRunning, StatusSucceeded, StatusFailed, StatusCancelled, StatusDead}
	legal := map[JobStatus]map[JobStatus]bool{
		StatusPending:   {StatusRunning: true, StatusCancelled: true},
		StatusRunning:   {StatusSucceeded: true, StatusFailed: true},
		StatusFailed:    {StatusPending: true, StatusDead: true},
		StatusSucceeded: {},
		StatusCancelled: {},
		StatusDead:      {},
	}
	for _, from := range all {
		for _, to := range all {
			from, to := from, to
			name := string(from) + "_to_" + string(to)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				job := &Job{Status: from}
				err := job.TransitionTo(to)
				if legal[from][to] {
					if err != nil {
						t.Fatalf("legal transition rejected: %v", err)
					}
					if job.Status != to {
						t.Fatalf("status not applied: got %q", job.Status)
					}
				} else {
					if !errors.Is(err, ErrInvalidTransition) {
						t.Fatalf("illegal transition must return ErrInvalidTransition, got %v", err)
					}
					if job.Status != from {
						t.Fatalf("failed transition mutated state to %q", job.Status)
					}
				}
			})
		}
	}
}

// Principle: temporal invariant. Every successful transition must move
// UpdatedAt forward so API responses never serve a stale timestamp
// (the Postgres trigger only fixes the stored row, not the live object).
func TestStateMachine_UpdatedAtMonotonic(t *testing.T) {
	t.Parallel()
	job := &Job{Status: StatusPending, UpdatedAt: time.Now().UTC().Add(-time.Hour)}
	chain := []JobStatus{StatusRunning, StatusFailed, StatusPending, StatusRunning, StatusSucceeded}
	prev := job.UpdatedAt
	for _, next := range chain {
		// Ensure the clock can advance (monotonic check needs distinct ticks).
		time.Sleep(time.Millisecond)
		if err := job.TransitionTo(next); err != nil {
			t.Fatalf("transition to %q: %v", next, err)
		}
		if !job.UpdatedAt.After(prev) {
			t.Fatalf("UpdatedAt not monotonic after -> %q", next)
		}
		prev = job.UpdatedAt
	}
}

// Principle: terminal-state sink. Nothing may leave a terminal state,
// which is what makes cascade-abort and audit reasoning sound.
func TestStateMachine_TerminalStatesAreSinks(t *testing.T) {
	t.Parallel()
	for _, term := range []JobStatus{StatusSucceeded, StatusCancelled, StatusDead} {
		job := &Job{Status: term}
		for _, to := range []JobStatus{StatusPending, StatusRunning, StatusSucceeded, StatusFailed, StatusCancelled, StatusDead} {
			if err := job.TransitionTo(to); !errors.Is(err, ErrInvalidTransition) {
				t.Fatalf("terminal %q escaped to %q (err=%v)", term, to, err)
			}
		}
		if !job.IsTerminal() {
			t.Fatalf("%q must report IsTerminal", term)
		}
	}
}
