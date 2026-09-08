package domain

import "errors"

var (
	// ErrJobNotFound indicates the requested job does not exist.
	ErrJobNotFound = errors.New("job not found")

	// ErrTenantNotFound indicates the tenant does not exist or credentials mismatch.
	ErrTenantNotFound = errors.New("tenant not found")

	// ErrInvalidTransition indicates an illegal state machine jump.
	ErrInvalidTransition = errors.New("invalid job status transition")

	// ErrFencingTokenStale is raised when a worker attempts to update a job using a lease/token
	// that has been superseded by a higher fencing token.
	ErrFencingTokenStale = errors.New("fencing token is stale: lease expired or superseded")

	// ErrDependencyNotMet indicates a parent job has not successfully completed.
	ErrDependencyNotMet = errors.New("job dependency has not succeeded yet")

	// ErrJobCannotBeCancelled indicates a job cannot be cancelled because it is no longer pending.
	ErrJobCannotBeCancelled = errors.New("job can only be cancelled while in pending status")

	// ErrDependencyCycle indicates a job dependency graph would form a cycle.
	ErrDependencyCycle = errors.New("job dependency cycle detected")

	// ErrMaxAttemptsReached indicates the job has exhausted its retry attempts.
	ErrMaxAttemptsReached = errors.New("job has reached maximum retry attempts")
)
