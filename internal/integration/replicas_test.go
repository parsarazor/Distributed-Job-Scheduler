package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/postgres"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/service"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/testinfra"
)

// Two replicas with separate pools run every background loop at once while
// workers claim through one and complete through the other.
func TestSchedulerReplicasShareOneDatabase(t *testing.T) {
	server := testinfra.StartPostgres(t)
	f := newFixture(t, server)
	pool, err := postgres.New(f.ctx, f.dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	other := postgres.NewRepositories(pool)
	busB := &recordingPublisher{}
	replicas := []*service.Scheduler{
		f.sched,
		service.NewScheduler(other.Jobs, other.Tenants, other.Events, other.Outbox, other.Tx, busB, &domain.ConstantBackoff{Delay: time.Hour}),
	}
	buses := []*recordingPublisher{f.bus, busB}

	const jobs = 60
	for i := range jobs {
		f.submit(t, fmt.Sprintf("replica-%d", i), 3, nil, time.Time{})
	}

	ctx, cancel := context.WithTimeout(f.ctx, 30*time.Second)
	defer cancel()
	var running, maxRunning, claims, completed atomic.Int32
	claimed := sync.Map{}
	var handled sync.Map
	var workers sync.WaitGroup
	var loops sync.WaitGroup
	for i, replica := range replicas {
		loops.Add(1)
		go func() {
			defer loops.Done()
			for ctx.Err() == nil {
				if _, err := replica.DispatchReady(ctx, 100); err != nil && ctx.Err() == nil {
					t.Errorf("replica %d dispatch: %v", i, err)
				}
				if _, err := replica.ReconcileOutbox(ctx, 100); err != nil && ctx.Err() == nil {
					t.Errorf("replica %d outbox: %v", i, err)
				}
				if _, err := replica.ReapExpiredLeases(ctx, 100); err != nil && ctx.Err() == nil {
					t.Errorf("replica %d reaper: %v", i, err)
				}
				for _, message := range buses[i].published() {
					if _, seen := handled.LoadOrStore(message.MessageID, true); seen {
						continue
					}
					var announcement domain.Job
					if err := json.Unmarshal(message.Payload, &announcement); err != nil {
						t.Errorf("announcement: %v", err)
						continue
					}
					workers.Add(1)
					go func() {
						defer workers.Done()
						claimVia, completeVia := replica, replicas[(i+1)%len(replicas)]
						job, token, err := claimVia.ClaimExecution(ctx, &announcement, "w", time.Minute)
						if errors.Is(err, domain.ErrJobNotClaimable) {
							return
						}
						if err != nil {
							t.Errorf("claim: %v", err)
							return
						}
						claims.Add(1)
						if _, dup := claimed.LoadOrStore(job.ID, true); dup {
							t.Errorf("job %s claimed twice", job.ID)
						}
						n := running.Add(1)
						for m := maxRunning.Load(); n > m && !maxRunning.CompareAndSwap(m, n); m = maxRunning.Load() {
						}
						time.Sleep(20 * time.Millisecond)
						running.Add(-1)
						if _, err := completeVia.ReportCompletion(ctx, job.ID, "w", domain.StatusSucceeded, token, ""); err != nil {
							t.Errorf("complete via other replica: %v", err)
							return
						}
						if completed.Add(1) == jobs {
							cancel()
						}
					}()
				}
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}
	loops.Wait()
	workers.Wait()

	if completed.Load() != jobs || claims.Load() != jobs {
		t.Fatalf("completed=%d claims=%d, want %d each", completed.Load(), claims.Load(), jobs)
	}
	if m := maxRunning.Load(); m > domain.DefaultTenantMaxInFlight {
		t.Fatalf("tenant ran %d jobs at once across replicas, limit %d", m, domain.DefaultTenantMaxInFlight)
	}
	counts := map[string]int{}
	for _, bus := range buses {
		for _, message := range bus.published() {
			counts[message.MessageID]++
		}
	}
	for id, n := range counts {
		if n != 1 {
			t.Fatalf("announcement %s published %d times across replicas", id, n)
		}
	}
}
