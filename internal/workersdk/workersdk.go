// Package workersdk lets this repository's binaries reach the execution engine
// behind a pkg/worker.Worker, e.g. to serve the admin-only worker port.
package workersdk

import "github.com/parsarazor/distributed-job-scheduler/internal/service"

// Service returns the engine of a *worker.Worker. pkg/worker sets it at init.
var Service func(sdkWorker any) *service.Worker
