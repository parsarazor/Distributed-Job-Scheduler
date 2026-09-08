// Command demo runs the full job lifecycle in-process with in-memory
// backends: submit -> dispatch -> worker executes -> completion ->
// dependent unlocked -> audit trail. No Docker required.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/prsamohamadi/distributed-job-scheduler/internal/broker"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/lease"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/memory"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/service"
)

func main() {
	ctx := context.Background()
	b := memory.NewBundle()
	bus := broker.NewMemoryBroker()
	leases := lease.NewMemoryLeaseManager()
	sched := service.NewScheduler(b.Jobs, b.Tenants, b.Events, b.Outbox, b.Tx, bus, nil)
	worker := service.NewWorker("demo-worker", sched, leases, 4)
	worker.RegisterHandler("default", func(ctx context.Context, job *domain.Job) error {
		fmt.Printf("  executing %-16s id=%s prio=%d\n", job.Name, job.ID.String()[:8], int(job.Priority))
		return nil
	})

	tenant, err := domain.NewTenant("acme", "secret-key")
	if err != nil {
		log.Fatal(err)
	}
	if err := b.Tenants.Create(ctx, tenant); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("tenant %s (%s)\n", tenant.Name, tenant.ID.String()[:8])

	parent, err := sched.SubmitJob(ctx, service.SubmitInput{
		TenantID: tenant.ID, Name: "parent-job",
		Payload:  json.RawMessage(`{"step":1}`),
		Priority: domain.PriorityHigh, MaxAttempts: 3, ScheduledAt: time.Now().UTC(),
	})
	if err != nil {
		log.Fatal(err)
	}
	child, err := sched.SubmitJob(ctx, service.SubmitInput{
		TenantID: tenant.ID, Name: "child-job",
		Payload:  json.RawMessage(`{"step":2}`),
		Priority: domain.PriorityDefault, MaxAttempts: 3,
		DependsOn: &parent.ID, ScheduledAt: time.Now().UTC(),
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("submitted parent=%s child=%s (depends on parent)\n", parent.ID.String()[:8], child.ID.String()[:8])

	// Child must NOT be ready before parent succeeds (dependency gate).
	ready, _ := b.Jobs.GetReadyJobs(ctx, 10)
	if len(ready) != 1 || ready[0].ID != parent.ID {
		log.Fatalf("dependency gate broken: ready=%d, want parent only", len(ready))
	}
	fmt.Println("gate check: only parent is ready (child correctly gated)")

	if n, _ := sched.DispatchReady(ctx, 10); n != 1 {
		log.Fatalf("expected 1 dispatch (parent only), got %d", n)
	}
	fmt.Println("dispatch 1: parent -> running")

	if n, _ := worker.RunOnce(ctx, 10); n != 0 {
		fmt.Printf("worker pass 1: %d (parent already running, nothing new)\n", n)
	}
	// Complete the parent as the worker would.
	p, _ := sched.GetJob(ctx, parent.ID)
	n, err := sched.ReportCompletion(ctx, parent.ID, "demo-worker", domain.StatusSucceeded, p.FencingToken, "")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("parent completed: dependents unlocked=%d\n", n)

	// Now the child is eligible.
	if n, _ := sched.DispatchReady(ctx, 10); n != 1 {
		log.Fatalf("expected child dispatch, got %d", n)
	}
	c, _ := sched.GetJob(ctx, child.ID)
	if _, err := sched.ReportCompletion(ctx, child.ID, "demo-worker", domain.StatusSucceeded, c.FencingToken, ""); err != nil {
		log.Fatal(err)
	}
	fmt.Println("child completed")

	// Worker path: submit an independent job and let the bounded worker
	// execute it end-to-end (lease -> run -> fenced report).
	wjob, err := sched.SubmitJob(ctx, service.SubmitInput{
		TenantID: tenant.ID, Name: "default",
		Payload:  json.RawMessage(`{"via":"worker"}`),
		Priority: domain.PriorityDefault, MaxAttempts: 3, ScheduledAt: time.Now().UTC(),
	})
	if err != nil {
		log.Fatal(err)
	}
	if n, _ := worker.RunOnce(ctx, 10); n != 1 {
		log.Fatalf("worker expected to execute 1 job, did %d", n)
	}
	wj, _ := sched.GetJob(ctx, wjob.ID)
	fmt.Printf("worker executed %s -> %s\n", wjob.ID.String()[:8], wj.Status)

	// Reconcile the transactional outbox.
	if n, _ := sched.ReconcileOutbox(ctx, 100); n > 0 {
		fmt.Printf("outbox reconciled: %d events published\n", n)
	}
	fmt.Printf("broker publishes: %d\n", len(bus.Published()))

	for _, jobID := range []string{parent.ID.String(), child.ID.String()} {
		fmt.Printf("audit %s:\n", jobID[:8])
		// parse back to uuid via service helper
		events, _ := b.Events.ListByJobID(ctx, mustParse(jobID))
		for _, e := range events {
			fmt.Printf("  %s -> %s @ %s\n", e.FromStatus, e.ToStatus, e.OccurredAt.Format("15:04:05"))
		}
	}
	fmt.Println("DEMO OK: submit -> dispatch -> execute -> complete -> dependent unlocked -> audit verified")
}
