package service

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/prsamohamadi/distributed-job-scheduler/internal/broker"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
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
