// Command worker runs the gRPC WorkerService and an execution loop with
// bounded concurrency, lease heartbeats and graceful drain on SIGTERM.
//
// Env: WORKER_ID (default worker-1), SCHEDULER_ADDR (unused in-process demo),
// PORT (default 50052), MAX_CONCURRENCY (default 10).
package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prsamohamadi/distributed-job-scheduler/internal/app"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/service"
	apiv1 "github.com/prsamohamadi/distributed-job-scheduler/pkg/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	ctx := context.Background()
	deps, err := app.NewFromEnv(ctx)
	if err != nil {
		log.Fatalf("wire backends: %v", err)
	}
	defer deps.Cleanup()

	// Demo handler: pretend to do work.
	deps.Worker.RegisterHandler("send-email", func(ctx context.Context, job *domain.Job) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
			return nil
		}
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "50052"
	}
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	apiv1.RegisterWorkerServiceServer(grpcServer, service.NewWorkerServer(deps.Worker))
	reflection.Register(grpcServer)

	stop := make(chan struct{})
	go func() {
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				n, _ := deps.Worker.RunOnce(context.Background(), 10)
				if n > 0 {
					log.Printf("worker: executed %d jobs", n)
				}
			}
		}
	}()

	go func() {
		log.Printf("worker %s listening on :%s", deps.Worker.ID(), port)
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("grpc serve: %v", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("draining worker...")
	deps.Worker.Drain()
	close(stop)
	grpcServer.GracefulStop()
	log.Println("worker stopped")
}
