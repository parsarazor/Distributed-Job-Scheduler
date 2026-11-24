package worker

import (
	"errors"

	"github.com/parsarazor/distributed-job-scheduler/internal/domain"
)

// Cancellation causes of a handler's context, read with context.Cause(ctx).
// A job timeout has the cause context.DeadlineExceeded.
var (
	// ErrLeaseLost: the scheduler no longer considers this worker the job's
	// owner, so another worker may run it. Any result is discarded.
	ErrLeaseLost = domain.ErrLeaseLost
	// ErrCanceled: an operator cancelled this execution. It wraps context.Canceled.
	ErrCanceled = domain.ErrExecutionCanceled
	// ErrShutdown: Config.ShutdownTimeout expired while the handler was still
	// running. It wraps context.Canceled.
	ErrShutdown = domain.ErrWorkerShutdown
)

type permanentError struct{ err error }

func (e *permanentError) Error() string        { return e.err.Error() }
func (e *permanentError) Unwrap() error        { return e.err }
func (e *permanentError) Is(target error) bool { return target == domain.ErrPermanentFailure }

// Permanent marks err as not retryable: the job goes dead without using its
// remaining attempts. Permanent(nil) returns nil.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent reports whether err or any error it wraps came from Permanent.
func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}
