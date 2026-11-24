package service

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/parsarazor/distributed-job-scheduler/internal/broker"
	"github.com/parsarazor/distributed-job-scheduler/internal/domain"
	"github.com/parsarazor/distributed-job-scheduler/internal/repository"
)

func TestDispatchStagesReadyAttemptsOnceAcrossReplicas(t *testing.T) {
	s, b := newTestScheduler()
	ctx := t.Context()
	tenant := seedTenant(t, s, b)
	parent, _ := s.SubmitJob(ctx, SubmitInput{TenantID: tenant.ID, Name: "parent"})
	_, _ = s.SubmitJob(ctx, SubmitInput{TenantID: tenant.ID, Name: "child", DependsOn: &parent.ID})
	_, _ = s.SubmitJob(ctx, SubmitInput{TenantID: tenant.ID, Name: "future", ScheduledAt: time.Now().Add(time.Hour)})
	second := NewScheduler(b.Jobs, b.Tenants, b.Events, b.Outbox, b.Tx, s.bus, s.backoff)
	var wg sync.WaitGroup
	for _, replica := range []*Scheduler{s, second} {
		wg.Go(func() {
			if _, err := replica.DispatchReady(ctx, 100); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	bus := s.bus.(*broker.MemoryBroker)
	if len(bus.Published()) != 0 {
		t.Fatal("dispatch must only stage outbox rows")
	}
	rows, err := b.Outbox.FetchUnpublished(ctx, 100, 0)
	if err != nil || len(rows) != 1 || rows[0].JobID != parent.ID {
		t.Fatalf("rows=%+v err=%v", rows, err)
	}
	for _, replica := range []*Scheduler{s, second} {
		wg.Go(func() {
			if _, err := replica.ReconcileOutbox(ctx, 100); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if got := len(bus.Published()); got != 1 {
		t.Fatalf("concurrent publication count=%d", got)
	}
	token := claimTestJob(t, s, parent.ID, "w")
	if _, err := s.ReportCompletion(ctx, parent.ID, "w", domain.StatusFailed, token, "retry"); err != nil {
		t.Fatal(err)
	}
	stored, _ := s.GetJob(ctx, parent.ID)
	if wait := time.Until(*stored.NextRetryAt); wait > 0 {
		time.Sleep(wait)
	}
	if n, err := s.DispatchReady(ctx, 100); err != nil || n != 1 {
		t.Fatalf("retry staging=%d %v", n, err)
	}
	if n, err := s.ReconcileOutbox(ctx, 100); err != nil || n != 1 {
		t.Fatalf("retry publish=%d %v", n, err)
	}
	messages := bus.Published()
	if len(messages) != 2 || messages[0].MessageID == messages[1].MessageID {
		t.Fatal("each attempt needs its own message identity")
	}
	var announced domain.Job
	if err := json.Unmarshal(messages[1].Payload, &announced); err != nil || announced.AttemptCount != 1 {
		t.Fatalf("retry announcement=%+v err=%v", announced, err)
	}
}

func TestSubmitRejectsTerminalAndForeignParents(t *testing.T) {
	for _, terminal := range []bool{false, true} {
		s, b := newTestScheduler()
		ctx := context.Background()
		tenant := seedTenant(t, s, b)
		parent, _ := s.SubmitJob(ctx, SubmitInput{TenantID: tenant.ID, Name: "parent"})
		if terminal {
			if _, err := s.CancelJob(ctx, parent.ID, "cancel"); err != nil {
				t.Fatal(err)
			}
		} else {
			other, _ := domain.NewTenant("other", "other-key")
			if err := b.Tenants.Create(ctx, other); err != nil {
				t.Fatal(err)
			}
			tenant = other
		}
		if _, err := s.SubmitJob(ctx, SubmitInput{TenantID: tenant.ID, Name: "child", DependsOn: &parent.ID}); err != domain.ErrDependencyNotMet {
			t.Fatalf("invalid parent accepted: %v", err)
		}
	}
}

func TestDispatchReady_HonorsTenantMaxInFlight(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, _, _ := newWiredPair()
	small, _ := domain.NewTenant("small", "small-key")
	small.MaxInFlight = 3
	large, _ := domain.NewTenant("large", "large-key")
	large.MaxInFlight = 40
	counts := map[uuid.UUID]int{}
	for _, tn := range []*domain.Tenant{small, large} {
		if err := s.tenants.Create(ctx, tn); err != nil {
			t.Fatal(err)
		}
		for range 50 {
			if _, err := s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "x", MaxAttempts: 1}); err != nil {
				t.Fatal(err)
			}
		}
	}
	select {
	case <-s.DispatchWake():
	default:
		t.Fatal("submission did not wake the dispatcher")
	}
	if n, err := s.DispatchReady(ctx, 1000); err != nil || n != 43 {
		t.Fatalf("dispatched %d (err=%v), want 3+40", n, err)
	}
	select {
	case <-s.PublishWake():
	default:
		t.Fatal("dispatch did not wake the publisher")
	}
	for _, tn := range []*domain.Tenant{small, large} {
		jobs, _, err := s.ListJobs(ctx, tn.ID, repository.JobListFilter{Limit: 100})
		if err != nil {
			t.Fatal(err)
		}
		for _, j := range jobs {
			if j.DispatchedAt != nil {
				counts[tn.ID]++
			}
		}
	}
	if counts[small.ID] != 3 || counts[large.ID] != 40 {
		t.Fatalf("dispatched per tenant small=%d large=%d, want 3 and 40", counts[small.ID], counts[large.ID])
	}
}
