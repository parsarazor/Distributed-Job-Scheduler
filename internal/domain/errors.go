package domain

import (
	"context"
	"errors"
	"fmt"
)

var (
	// ErrInvalidInput indicates an invalid API field.
	ErrInvalidInput = errors.New("invalid input")
	// ErrIdempotencyConflict indicates that a submission key identifies different input.
	ErrIdempotencyConflict = errors.New("submission key already used for different input")
	ErrPayloadExpired      = errors.New("terminal job payload has expired; submit a new job")
	// ErrTenantLimit indicates the tenant's outstanding quota or submission rate is exceeded.
	ErrTenantLimit = errors.New("tenant quota or submission rate exceeded")
	// ErrJobNotFound indicates the requested job does not exist.
	ErrJobNotFound = errors.New("job not found")

	// ErrTenantNotFound indicates the tenant does not exist or credentials mismatch.
	ErrTenantNotFound = errors.New("tenant not found")

	// ErrInvalidTransition indicates an illegal state machine jump.
	ErrInvalidTransition = errors.New("invalid job status transition")

	// ErrFencingTokenStale rejects any token unequal to the stored execution token.
	ErrFencingTokenStale = errors.New("fencing token does not match current execution")

	// ErrJobNotClaimable indicates a duplicate, stale, or not-yet-ready announcement.
	ErrJobNotClaimable = errors.New("job is not ready to be claimed")

	// ErrLeaseLost indicates the execution no longer owns a live lease.
	ErrLeaseLost = errors.New("job lease lost")

	// ErrPermanentFailure marks a handler error that must not be retried.
	ErrPermanentFailure = errors.New("permanent handler failure")

	// ErrExecutionCanceled is the cancellation cause of an operator CancelExecution.
	ErrExecutionCanceled = fmt.Errorf("execution canceled by operator: %w", context.Canceled)

	// ErrWorkerShutdown is the cancellation cause when a worker's shutdown
	// budget expires with the handler still running.
	ErrWorkerShutdown = fmt.Errorf("worker shutting down: %w", context.Canceled)

	// ErrInvalidLease indicates an empty owner or nonpositive lease duration.
	ErrInvalidLease = errors.New("lease requires a worker ID and positive TTL")

	// ErrDependencyNotMet indicates a parent job has not successfully completed.
	ErrDependencyNotMet = errors.New("job dependency has not succeeded yet")

	// ErrJobCannotBeCancelled indicates a job cannot be cancelled because it is no longer pending.
	ErrJobCannotBeCancelled = errors.New("job can only be cancelled while in pending status")

	// ErrDependencyCycle indicates a job dependency graph would form a cycle.
	ErrDependencyCycle = errors.New("job dependency cycle detected")

	// ErrMaxAttemptsReached indicates the job has exhausted its retry attempts.
	ErrMaxAttemptsReached = errors.New("job has reached maximum retry attempts")
)
