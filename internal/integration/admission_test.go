package integration_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/service"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/testinfra"
)

func TestPostgresTenantAdmissionAndFairness(t *testing.T) {
	server := testinfra.StartPostgres(t)
	f := newFixture(t, server)
	f.sched.TenantQuota = 2
	input := service.SubmitInput{TenantID: f.tenant.ID, Name: "idempotent", IdempotencyKey: "same-key", Timeout: time.Second}
	var wg sync.WaitGroup
	ids := make(chan uuid.UUID, 12)
	for range 12 {
		wg.Go(func() {
			j, err := f.sched.SubmitJob(f.ctx, input)
			if err != nil {
				t.Error(err)
				return
			}
			ids <- j.ID
		})
	}
	wg.Wait()
	close(ids)
	var first uuid.UUID
	for id := range ids {
		if first == uuid.Nil {
			first = id
		}
		if id != first {
			t.Fatal("concurrent idempotent submissions created duplicates")
		}
	}
	audit, err := f.repos.Events.ListByJobID(f.ctx, first)
	if err != nil || len(audit) != 1 {
		t.Fatalf("submission audit count=%d err=%v", len(audit), err)
	}
	f.submit(t, "second", 3, nil, time.Time{})
	if _, err := f.sched.SubmitJob(f.ctx, service.SubmitInput{TenantID: f.tenant.ID, Name: "excess"}); !errors.Is(err, domain.ErrTenantLimit) {
		t.Fatalf("quota bypass: %v", err)
	}
	if _, err := f.sched.SubmitJob(f.ctx, input); err != nil {
		t.Fatalf("idempotent retry at quota: %v", err)
	}
	other, err := domain.NewTenant("other", uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	if err := f.repos.Tenants.Create(f.ctx, other); err != nil {
		t.Fatal(err)
	}
	input.TenantID = other.ID
	j, err := f.sched.SubmitJob(f.ctx, input)
	if err != nil || j.ID == first {
		t.Fatalf("tenant-scoped key: %+v %v", j, err)
	}
	for range 2 {
		if n, err := f.sched.DispatchReady(f.ctx, 1); n != 1 || err != nil {
			t.Fatalf("dispatch n=%d err=%v", n, err)
		}
	}
	owners := map[uuid.UUID]bool{}
	for _, row := range unpublished(t, f) {
		j, err := f.sched.GetJob(f.ctx, row.JobID)
		if err != nil {
			t.Fatal(err)
		}
		owners[j.TenantID] = true
	}
	if len(owners) != 2 {
		t.Fatalf("persistent turns must rotate eligible tenants: %v", owners)
	}
}
