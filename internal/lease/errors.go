package lease

import "errors"

var (
	// ErrLeaseHeld means another worker owns the lease.
	ErrLeaseHeld = errors.New("job lease held by another worker")
	// ErrLeaseNotHeld means this worker does not own the lease.
	ErrLeaseNotHeld = errors.New("job lease not held by this worker")
)
