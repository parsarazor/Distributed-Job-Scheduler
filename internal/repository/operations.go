package repository

import (
	"context"
	"time"
)

// Stats describes persisted queue health without tenant/job metric labels.
type Stats struct {
	Jobs          map[string]int64
	OutboxPending int64
	OutboxLag     time.Duration
}

// Operations supplies readiness, metrics and retention for a storage backend.
type Operations interface {
	Ping(context.Context) error
	Stats(context.Context) (Stats, error)
	Prune(context.Context, time.Duration, time.Duration) error
}
