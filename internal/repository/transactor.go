package repository

import "context"

// Transactor defines transaction management boundaries.
// It allows orchestrating multi-repository operations (e.g. Job + Outbox + Event)
// within a single atomic database transaction.
type Transactor interface {
	// WithinTransaction executes fn within an atomic database transaction.
	// If fn returns an error, the transaction is rolled back; otherwise it commits.
	WithinTransaction(ctx context.Context, fn func(ctx context.Context) error) error
}
