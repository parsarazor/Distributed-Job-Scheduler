package postgres

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/testinfra"
)

// Principle: live-contract suite. These run the same guarantees as the memory
// contract suite against real Postgres. INTEGRATION=1 starts and migrates a
// disposable testcontainer or explicit TEST_POSTGRES_DSN. Unit runs never
// inherit an application DATABASE_URL or contact Docker/Postgres.
//
//	INTEGRATION=1 go test -race ./internal/postgres/ -count=1 -v
func openTestPool(t *testing.T) *Pool {
	t.Helper()
	dsn := testinfra.StartPostgres(t).DSN
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	p, err := New(ctx, dsn)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(p.Close)
	if err := p.Ping(ctx); err != nil {
		t.Fatalf("postgres unreachable: %v", err)
	}
	return p
}

func liveTenant(t *testing.T, r *Repositories) *domain.Tenant {
	t.Helper()
	tn, err := domain.NewTenant("live-acme", uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Tenants.Create(t.Context(), tn); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := r.Tx.pool.Exec(ctx, `DELETE FROM tenants WHERE id=$1`, tn.ID); err != nil {
			t.Errorf("clean up test tenant: %v", err)
		}
	})
	return tn
}

func TestLive_TenantRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := openTestPool(t)
	r := NewRepositories(p)
	tn := liveTenant(t, r)
	got, err := r.Tenants.GetByAPIKeyHash(ctx, tn.APIKeyHash)
	if err != nil || got.ID != tn.ID {
		t.Fatalf("hash lookup: %+v %v", got, err)
	}
}

func TestLive_JobFencingRoundTrip(t *testing.T) {
	p := openTestPool(t)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	r := NewRepositories(p)
	tn := liveTenant(t, r)
	job, _ := domain.NewJob(tn.ID, "live-job", nil, domain.PriorityHigh, 3, nil, time.Now().UTC())
	if err := r.Jobs.Create(ctx, job); err != nil {
		t.Fatalf("create job: %v", err)
	}
	var mu sync.Mutex
	var tokens []int64
	var owners []string
	var claimErrors []error
	var wg sync.WaitGroup
	const competitors = 16
	start := make(chan struct{})
	for i := 0; i < competitors; i++ {
		workerID := uuid.NewString()
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			token, err := r.Jobs.ClaimJob(ctx, job.ID, workerID, time.Minute)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				claimErrors = append(claimErrors, err)
				return
			}
			tokens = append(tokens, token)
			owners = append(owners, workerID)
		}()
	}
	close(start)
	wg.Wait()
	if len(tokens) != 1 || len(claimErrors) != competitors-1 || tokens[0] != 1 {
		t.Fatalf("one atomic claim must win with token 1, tokens=%v errors=%v", tokens, claimErrors)
	}
	for _, err := range claimErrors {
		if err != domain.ErrJobNotClaimable {
			t.Fatalf("losing claim returned unexpected error: %v", err)
		}
	}
	claimed, err := r.Jobs.GetByID(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if claimed.Status != domain.StatusRunning || claimed.FencingToken != 1 || claimed.LeaseOwner == nil || *claimed.LeaseOwner != owners[0] || claimed.LeaseExpiresAt == nil {
		t.Fatalf("winning claim must atomically persist its token, owner and lease: %+v", claimed)
	}
	for _, token := range []int64{tokens[0] - 1, tokens[0] + 1} {
		if _, err := r.Jobs.CompleteLease(ctx, job.ID, owners[0], token, domain.StatusSucceeded); err != domain.ErrFencingTokenStale {
			t.Fatalf("completion with token %d must fail, got %v", token, err)
		}
		if err := r.Jobs.TransitionStatus(ctx, job.ID, domain.StatusRunning, domain.StatusSucceeded, token); err != domain.ErrFencingTokenStale {
			t.Fatalf("transition with token %d must fail, got %v", token, err)
		}
		staleCopy := *claimed
		staleCopy.FencingToken = token
		if err := r.Jobs.UpdateWithFencingToken(ctx, &staleCopy, token); err != domain.ErrFencingTokenStale {
			t.Fatalf("attempt update with token %d must fail, got %v", token, err)
		}
		if err := r.Jobs.RenewLease(ctx, job.ID, owners[0], token, time.Minute); err != domain.ErrLeaseLost {
			t.Fatalf("renewal with token %d must fail, got %v", token, err)
		}
	}
	if err := r.Jobs.RenewLease(ctx, job.ID, "not-owner", tokens[0], time.Minute); err != domain.ErrLeaseLost {
		t.Fatalf("wrong-owner renewal must fail, got %v", err)
	}
	if err := r.Jobs.RenewLease(ctx, job.ID, owners[0], tokens[0], 2*time.Minute); err != nil {
		t.Fatalf("lease renewal: %v", err)
	}
	renewed, err := r.Jobs.GetByID(ctx, job.ID)
	if err != nil || renewed.FencingToken != tokens[0] || renewed.LeaseExpiresAt == nil || !renewed.LeaseExpiresAt.After(*claimed.LeaseExpiresAt) {
		t.Fatalf("renewal must extend expiry without changing token: job=%+v err=%v", renewed, err)
	}
	if _, err := r.Jobs.CompleteLease(ctx, job.ID, owners[0], tokens[0], domain.StatusSucceeded); err != nil {
		t.Fatalf("fresh completion: %v", err)
	}
	completed, err := r.Jobs.GetByID(ctx, job.ID)
	if err != nil || completed.Status != domain.StatusSucceeded || completed.LeaseOwner != nil || completed.LeaseExpiresAt != nil {
		t.Fatalf("completion must clear lease: job=%+v err=%v", completed, err)
	}
}

func TestLive_ExpiredLeaseSkipLockedAndReap(t *testing.T) {
	p := openTestPool(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	r := NewRepositories(p)
	tn := liveTenant(t, r)
	job, _ := domain.NewJob(tn.ID, "expired-live-job", nil, domain.PriorityHigh, 3, nil, time.Now().UTC())
	if err := r.Jobs.Create(ctx, job); err != nil {
		t.Fatal(err)
	}
	token, err := r.Jobs.ClaimJob(ctx, job.ID, "expired-worker", 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.pool.Exec(ctx, `SELECT pg_sleep(0.03)`); err != nil {
		t.Fatal(err)
	}

	if err := r.Jobs.RenewLease(ctx, job.ID, "expired-worker", token, time.Minute); err != domain.ErrLeaseLost {
		t.Fatalf("expired lease cannot be renewed, got %v", err)
	}
	heldTx, err := p.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = heldTx.Rollback(context.Background()) }()
	lockedJobs, err := r.Jobs.GetExpiredLeasesForUpdate(context.WithValue(ctx, txKey{}, heldTx), 100000)
	if err != nil {
		t.Fatal(err)
	}
	lockedIDs := make(map[uuid.UUID]bool)
	for _, expired := range lockedJobs {
		lockedIDs[expired.ID] = true
	}
	if !lockedIDs[job.ID] {
		t.Fatalf("target job %s not in locked expired jobs", job.ID)
	}
	skipCtx, stopSkip := context.WithTimeout(ctx, time.Second)
	skipErr := r.Tx.WithinTransaction(skipCtx, func(txctx context.Context) error {
		jobs, err := r.Jobs.GetExpiredLeasesForUpdate(txctx, 100000)
		for _, expired := range jobs {
			if lockedIDs[expired.ID] {
				return fmt.Errorf("second transaction selected locked job %s", expired.ID)
			}
		}
		return err
	})
	stopSkip()
	if skipErr != nil {
		t.Fatalf("SKIP LOCKED query failed: %v", skipErr)
	}
	if err := heldTx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	if err := r.Tx.WithinTransaction(ctx, func(txctx context.Context) error {
		jobs, err := r.Jobs.GetExpiredLeasesForUpdate(txctx, 100000)
		if err != nil {
			return err
		}
		found := false
		for _, expired := range jobs {
			found = found || expired.ID == job.ID
		}
		if !found {
			return fmt.Errorf("target job %s not returned after releasing lock", job.ID)
		}
		_, err = r.Jobs.ExpireLease(txctx, job.ID, token)
		return err
	}); err != nil {
		t.Fatalf("expire lease: %v", err)
	}
	stored, err := r.Jobs.GetByID(ctx, job.ID)
	if err != nil || stored.Status != domain.StatusFailed || stored.FencingToken != token+1 || stored.AttemptCount != 1 || stored.LeaseOwner != nil || stored.LeaseExpiresAt != nil {
		t.Fatalf("expired lease must be invalidated: job=%+v err=%v", stored, err)
	}
	if err := r.Jobs.TransitionStatus(ctx, job.ID, domain.StatusFailed, domain.StatusPending, token); err != domain.ErrFencingTokenStale {
		t.Fatalf("expired holder's token must be stale, got %v", err)
	}
}

func TestLive_OutboxSkipLocked(t *testing.T) {
	p := openTestPool(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	r := NewRepositories(p)
	tn := liveTenant(t, r)
	job, _ := domain.NewJob(tn.ID, "live-job", nil, domain.PriorityDefault, 3, nil, time.Now().UTC().Add(-time.Minute))
	if err := r.Jobs.Create(ctx, job); err != nil {
		t.Fatal(err)
	}
	event := domain.NewOutboxEvent(job.ID, "jobs.execute", nil)
	if err := r.Outbox.Store(ctx, event); err != nil {
		t.Fatal(err)
	}
	held, err := p.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = held.Rollback(context.Background()) }()
	got, err := r.Outbox.FetchUnpublished(context.WithValue(ctx, txKey{}, held), 100000, 0)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	found := false
	for _, e := range got {
		if e.JobID == job.ID {
			found = true
		}
	}
	if !found {
		t.Fatal("stored outbox row not returned by SKIP LOCKED fetch")
	}
	locked := make(map[uuid.UUID]bool, len(got))
	for _, e := range got {
		locked[e.ID] = true
	}
	skipCtx, stop := context.WithTimeout(ctx, time.Second)
	err = p.WithinTransaction(skipCtx, func(txctx context.Context) error {
		rows, err := r.Outbox.FetchUnpublished(txctx, 100000, 0)
		if err != nil {
			return err
		}
		for _, e := range rows {
			if locked[e.ID] {
				return fmt.Errorf("competing publisher selected locked event %s", e.ID)
			}
		}
		return nil
	})
	stop()
	if err != nil {
		t.Fatalf("competing fetch must skip held outbox locks: %v", err)
	}
	if err := held.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := p.WithinTransaction(ctx, func(txctx context.Context) error {
		rows, err := r.Outbox.FetchUnpublished(txctx, 100000, 0)
		if err != nil {
			return err
		}
		for _, e := range rows {
			if e.ID == event.ID {
				return r.Outbox.MarkPublished(txctx, e.ID)
			}
		}
		return fmt.Errorf("released event %s was not available", event.ID)
	}); err != nil {
		t.Fatal(err)
	}
}

func TestLive_TransactionAtomicity(t *testing.T) {
	ctx := context.Background()
	p := openTestPool(t)
	r := NewRepositories(p)
	tn := liveTenant(t, r)
	job, _ := domain.NewJob(tn.ID, "live-job", nil, domain.PriorityDefault, 3, nil, time.Now().UTC())
	sentinel := context.DeadlineExceeded
	err := r.Tx.WithinTransaction(ctx, func(ctx context.Context) error {
		if err := r.Jobs.Create(ctx, job); err != nil {
			return err
		}
		return sentinel // force rollback
	})
	if err != sentinel {
		t.Fatalf("want sentinel, got %v", err)
	}
	if _, err := r.Jobs.GetByID(ctx, job.ID); err != domain.ErrJobNotFound {
		t.Fatalf("rolled-back job must not exist, got %v", err)
	}
}
