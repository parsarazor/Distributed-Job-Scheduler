package domain

import (
	"errors"
	"testing"
)

func TestJobStateMachine_ValidTransitions(t *testing.T) {
	tests := []struct {
		name string
		from JobStatus
		to   JobStatus
	}{
		{"pending to running", StatusPending, StatusRunning},
		{"pending to cancelled", StatusPending, StatusCancelled},
		{"running to succeeded", StatusRunning, StatusSucceeded},
		{"running to failed", StatusRunning, StatusFailed},
		{"failed to pending (retry)", StatusFailed, StatusPending},
		{"failed to dead (exhausted)", StatusFailed, StatusDead},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			job := &Job{Status: tc.from}
			err := job.TransitionTo(tc.to)
			if err != nil {
				t.Fatalf("expected transition from %s to %s to succeed, got: %v", tc.from, tc.to, err)
			}
			if job.Status != tc.to {
				t.Fatalf("expected job status to be %s, got: %s", tc.to, job.Status)
			}
		})
	}
}

func TestJobStateMachine_InvalidTransitions(t *testing.T) {
	tests := []struct {
		name string
		from JobStatus
		to   JobStatus
	}{
		{"pending to succeeded (must run first)", StatusPending, StatusSucceeded},
		{"pending to dead (must fail first)", StatusPending, StatusDead},
		{"running to cancelled (cannot cancel running job directly)", StatusRunning, StatusCancelled},
		{"running to pending", StatusRunning, StatusPending},
		{"succeeded to running (terminal)", StatusSucceeded, StatusRunning},
		{"succeeded to cancelled (terminal)", StatusSucceeded, StatusCancelled},
		{"cancelled to pending (terminal)", StatusCancelled, StatusPending},
		{"dead to running (terminal)", StatusDead, StatusRunning},
		{"dead to pending (terminal)", StatusDead, StatusPending},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			job := &Job{Status: tc.from}
			err := job.TransitionTo(tc.to)
			if err == nil {
				t.Fatalf("expected transition from %s to %s to fail, but it succeeded", tc.from, tc.to)
			}
			if !errors.Is(err, ErrInvalidTransition) {
				t.Fatalf("expected ErrInvalidTransition, got: %v", err)
			}
			if job.Status != tc.from {
				t.Fatalf("expected job status to remain %s, got: %s", tc.from, job.Status)
			}
		})
	}
}

func TestJob_CanCancel(t *testing.T) {
	pendingJob := &Job{Status: StatusPending}
	if !pendingJob.CanCancel() {
		t.Errorf("expected pending job to be cancellable")
	}

	runningJob := &Job{Status: StatusRunning}
	if runningJob.CanCancel() {
		t.Errorf("expected running job not to be cancellable")
	}

	succeededJob := &Job{Status: StatusSucceeded}
	if succeededJob.CanCancel() {
		t.Errorf("expected succeeded job not to be cancellable")
	}
}
