package app

import (
	"context"

	"github.com/parsarazor/distributed-job-scheduler/internal/service"
)

// NewWorkerPortDeps wires the admin-only worker gRPC port and the operations
// server around an SDK worker's engine. ready backs /readyz.
func NewWorkerPortDeps(w *service.Worker, ready func(context.Context) error) (*Deps, error) {
	cfg, err := loadServerConfig("postgres")
	if err != nil {
		return nil, err
	}
	// The worker port is admin-only: the worker's own scheduler credential,
	// which the scheduler may revoke, must not authorize anything there.
	cfg.noSharedWorkerToken = true
	return &Deps{Worker: w, Cleanup: func() {}, checkSchema: ready, serverConfig: &cfg}, nil
}
