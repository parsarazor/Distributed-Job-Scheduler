// Package sendemail is an example pkg/worker handler that simulates sending
// an email. cmd/worker registers it as "send-email" in development and demo.
package sendemail

import (
	"context"
	"time"

	"github.com/parsarazor/distributed-job-scheduler/pkg/worker"
)

// Name is the job name the handler serves.
const Name = "send-email"

// Handle pretends to send an email, honouring cancellation.
func Handle(ctx context.Context, _ *worker.Job) error {
	t := time.NewTimer(100 * time.Millisecond)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
