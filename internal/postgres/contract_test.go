package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/parsarazor/distributed-job-scheduler/internal/broker"
	"github.com/parsarazor/distributed-job-scheduler/internal/domain"
	"github.com/parsarazor/distributed-job-scheduler/internal/service"
	"github.com/parsarazor/distributed-job-scheduler/internal/testinfra"
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
	if claimed.LastLeaseOwner == nil || *claimed.LastLeaseOwner != owners[0] {
		t.Fatalf("claim must record the last lease owner: %+v", claimed.LastLeaseOwner)
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
	if completed.LastLeaseOwner == nil || *completed.LastLeaseOwner != owners[0] {
		t.Fatalf("completion must keep the last lease owner for idempotent confirmation: %+v", completed.LastLeaseOwner)
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

func TestLive_OutboxClaimSemantics(t *testing.T) {
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
	// Claim: publishing_since is stamped and committed.
	var claimed *domain.OutboxEvent
	if err := p.WithinTransaction(ctx, func(txctx context.Context) error {
		var err error
		claimed, err = r.Outbox.FetchPublishable(txctx, 0, nil)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if claimed == nil || claimed.ID != event.ID {
		t.Fatalf("due event must be claimable, got %+v", claimed)
	}
	// A live claim is not handed to a second publisher.
	second, err := r.Outbox.FetchPublishable(ctx, 0, nil)
	if err != nil || second != nil {
		t.Fatalf("live claim must not be re-claimable, got %v / %v", second, err)
	}
	// Age the claim past the publishing lease, then reclaim.
	if _, err := p.pool.Exec(ctx, `UPDATE job_outbox SET publishing_since = clock_timestamp() - make_interval(secs => $1) WHERE id=$2`,
		int64(domain.PublishingLease/time.Second)+1, event.ID); err != nil {
		t.Fatal(err)
	}
	if n, err := r.Outbox.ReclaimStalePublishing(ctx); err != nil || n != 1 {
		t.Fatalf("stale claim must be reclaimed: n=%d err=%v", n, err)
	}
	reclaimed, err := r.Outbox.FetchPublishable(ctx, 0, nil)
	if err != nil || reclaimed == nil || reclaimed.ID != event.ID {
		t.Fatalf("reclaimed event must be claimable again, got %v / %v", reclaimed, err)
	}
	if err := r.Outbox.MarkPublished(ctx, event.ID); err != nil {
		t.Fatal(err)
	}
	gone, err := r.Outbox.FetchPublishable(ctx, 0, nil)
	if err != nil || gone != nil {
		t.Fatalf("published event must not be claimable, got %v / %v", gone, err)
	}

	// Dead-letter: after the retry ceiling the row stops being claimable.
	event2 := domain.NewOutboxEvent(job.ID, "jobs.execute", nil)
	if err := r.Outbox.Store(ctx, event2); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= domain.MaxOutboxRetries; i++ {
		if err := r.Outbox.MarkFailed(ctx, event2.ID, "broker down"); err != nil {
			t.Fatal(err)
		}
	}
	rows2, err := r.Outbox.FetchUnpublished(ctx, 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	dead := map[uuid.UUID]domain.OutboxEvent{}
	for _, e := range rows2 {
		dead[e.ID] = *e
	}
	dl, ok := dead[event2.ID]
	if !ok || !dl.DeadLettered || dl.RetryCount != domain.MaxOutboxRetries {
		t.Fatalf("row must be dead-lettered at ceiling, got %+v", dl)
	}
	again, err := r.Outbox.FetchPublishable(ctx, 0, nil)
	if err != nil || again != nil {
		t.Fatalf("dead-lettered row must not be claimable, got %v / %v", again, err)
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

// --- Live dead-letter recovery and retention coverage ---

type liveOutboxRow struct {
	Published    bool
	DeadLettered bool
	RetryCount   int
	CreatedAt    time.Time
}

func liveCreateJob(t *testing.T, ctx context.Context, r *Repositories, tenantID uuid.UUID, name string) *domain.Job {
	t.Helper()
	job, err := domain.NewJob(tenantID, name, json.RawMessage(`{"work":1}`), domain.PriorityDefault, 3, nil, time.Now().UTC().Add(-time.Minute))
	if err != nil {
		t.Fatalf("new job: %v", err)
	}
	if err := r.Jobs.Create(ctx, job); err != nil {
		t.Fatalf("create job: %v", err)
	}
	return job
}

func liveBackdateDispatch(t *testing.T, ctx context.Context, r *Repositories, p *Pool, job *domain.Job, age time.Duration) {
	t.Helper()
	if err := r.Jobs.MarkDispatched(ctx, job.ID, job.FencingToken); err != nil {
		t.Fatalf("mark dispatched: %v", err)
	}
	if _, err := p.pool.Exec(ctx, `UPDATE jobs SET dispatched_at=clock_timestamp()-make_interval(secs => $1) WHERE id=$2`, age.Seconds(), job.ID); err != nil {
		t.Fatalf("backdate dispatch: %v", err)
	}
}

func liveDeadLetter(t *testing.T, ctx context.Context, r *Repositories, jobID uuid.UUID, createdAt time.Time) *domain.OutboxEvent {
	t.Helper()
	e := domain.NewOutboxEvent(jobID, broker.JobsRoutingKey, json.RawMessage(`{"attempt":0}`))
	e.DeadLettered = true
	e.RetryCount = domain.MaxOutboxRetries
	e.CreatedAt = createdAt
	if err := r.Outbox.Store(ctx, e); err != nil {
		t.Fatalf("store dead-lettered event: %v", err)
	}
	return e
}

func liveOutboxRows(t *testing.T, ctx context.Context, p *Pool, jobID uuid.UUID) map[uuid.UUID]liveOutboxRow {
	t.Helper()
	rows, err := p.pool.Query(ctx, `SELECT id, published, dead_lettered, retry_count, created_at FROM job_outbox WHERE job_id=$1`, jobID)
	if err != nil {
		t.Fatalf("query outbox: %v", err)
	}
	defer rows.Close()
	out := make(map[uuid.UUID]liveOutboxRow)
	for rows.Next() {
		var id uuid.UUID
		var row liveOutboxRow
		if err := rows.Scan(&id, &row.Published, &row.DeadLettered, &row.RetryCount, &row.CreatedAt); err != nil {
			t.Fatalf("scan outbox: %v", err)
		}
		out[id] = row
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate outbox: %v", err)
	}
	return out
}

func liveGetJob(t *testing.T, ctx context.Context, r *Repositories, id uuid.UUID) *domain.Job {
	t.Helper()
	job, err := r.Jobs.GetByID(ctx, id)
	if err != nil {
		t.Fatalf("get job %s: %v", id, err)
	}
	return job
}

func TestLive_RecoverStaleDispatchPastDeadLetteredAnnouncement(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	p := openTestPool(t)
	r := NewRepositories(p)
	tn := liveTenant(t, r)

	job := liveCreateJob(t, ctx, r, tn.ID, "dead-letter-recovery")
	liveBackdateDispatch(t, ctx, r, p, job, time.Hour)
	dead := domain.NewOutboxEvent(job.ID, broker.JobsRoutingKey, json.RawMessage(`{"attempt":0}`))
	dead.ID = domain.DispatchID(job.ID, job.AttemptCount, job.FencingToken)
	dead.DeadLettered = true
	dead.RetryCount = domain.MaxOutboxRetries
	dead.CreatedAt = time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Microsecond)
	if err := r.Outbox.Store(ctx, dead); err != nil {
		t.Fatalf("store dead-lettered event: %v", err)
	}

	if err := r.Jobs.RecoverStaleDispatches(ctx, 10, time.Minute); err != nil {
		t.Fatalf("recover stale dispatch: %v", err)
	}
	recovered := liveGetJob(t, ctx, r, job.ID)
	if recovered.DispatchedAt != nil {
		t.Fatalf("stale dispatch must be released despite the dead-lettered row, got dispatched_at=%v", recovered.DispatchedAt)
	}
	if want := job.FencingToken + 1; recovered.FencingToken != want {
		t.Fatalf("fencing token = %d, want %d", recovered.FencingToken, want)
	}

	claimed, err := r.Outbox.FetchPublishable(ctx, 0, nil)
	if err != nil {
		t.Fatalf("fetch publishable before re-dispatch: %v", err)
	}
	if claimed != nil {
		t.Fatalf("dead-lettered row must not be claimed, got %s", claimed.ID)
	}

	s := service.NewScheduler(r.Jobs, r.Tenants, r.Events, r.Outbox, r.Tx, nil, nil)
	s.RedispatchAfter = time.Minute
	n, err := s.DispatchReady(ctx, 10)
	if err != nil {
		t.Fatalf("dispatch ready: %v", err)
	}
	if n != 1 {
		t.Fatalf("re-dispatch count = %d, want 1", n)
	}

	redriven := liveGetJob(t, ctx, r, job.ID)
	if redriven.DispatchedAt == nil {
		t.Fatal("job must be re-staged with a fresh dispatched_at")
	}
	if redriven.FencingToken != recovered.FencingToken {
		t.Fatalf("re-dispatch must keep the recovery fencing token %d, got %d", recovered.FencingToken, redriven.FencingToken)
	}

	rows := liveOutboxRows(t, ctx, p, job.ID)
	if len(rows) != 2 {
		t.Fatalf("outbox rows = %d, want the dead letter plus one fresh announcement", len(rows))
	}
	kept, ok := rows[dead.ID]
	if !ok {
		t.Fatal("old dead-lettered row must remain untouched")
	}
	if kept.Published || !kept.DeadLettered || kept.RetryCount != domain.MaxOutboxRetries {
		t.Fatalf("dead-lettered row was mutated: %+v", kept)
	}
	if !kept.CreatedAt.Equal(dead.CreatedAt) {
		t.Fatalf("dead-lettered created_at = %v, want %v", kept.CreatedAt, dead.CreatedAt)
	}

	wantStaged := domain.DispatchID(job.ID, redriven.AttemptCount, redriven.FencingToken)
	if _, ok := rows[wantStaged]; !ok {
		t.Fatalf("missing fresh publishable outbox row %s in %v", wantStaged, rows)
	}
	claimed, err = r.Outbox.FetchPublishable(ctx, 0, nil)
	if err != nil {
		t.Fatalf("fetch publishable after re-dispatch: %v", err)
	}
	if claimed == nil || claimed.ID != wantStaged {
		t.Fatalf("FetchPublishable = %v, want fresh row %s", claimed, wantStaged)
	}
}

func TestLive_TenantCreditsRecoverFromDeadLetteredAnnouncements(t *testing.T) {
	const jobs = 10
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	p := openTestPool(t)
	r := NewRepositories(p)
	tn := liveTenant(t, r)

	ids := make([]uuid.UUID, 0, jobs)
	for i := 0; i < jobs; i++ {
		job := liveCreateJob(t, ctx, r, tn.ID, fmt.Sprintf("starved-%02d", i))
		liveBackdateDispatch(t, ctx, r, p, job, time.Hour)
		liveDeadLetter(t, ctx, r, job.ID, time.Now().UTC().Add(-2*time.Hour))
		ids = append(ids, job.ID)
	}

	before, err := r.Jobs.GetUndispatchedJobsForUpdate(ctx, jobs)
	if err != nil {
		t.Fatalf("list ready before recovery: %v", err)
	}
	if len(before) != 0 {
		t.Fatalf("dispatched jobs must not be ready before recovery, got %d", len(before))
	}

	s := service.NewScheduler(r.Jobs, r.Tenants, r.Events, r.Outbox, r.Tx, nil, nil)
	s.RedispatchAfter = time.Minute
	n, err := s.DispatchReady(ctx, jobs)
	if err != nil {
		t.Fatalf("dispatch ready: %v", err)
	}
	if n != jobs {
		t.Fatalf("re-dispatched = %d, want all %d jobs; tenant stayed starved", n, jobs)
	}
	rows := liveOutboxRows(t, ctx, p, ids[0])
	if len(rows) != 2 {
		t.Fatalf("outbox rows for first job = %d, want dead letter plus fresh announcement", len(rows))
	}

	for _, id := range ids {
		got := liveGetJob(t, ctx, r, id)
		if got.DispatchedAt == nil {
			t.Fatalf("job %s was not re-staged", id)
		}
		if got.FencingToken != 1 {
			t.Fatalf("job %s fencing token = %d, want 1", id, got.FencingToken)
		}
	}

	// The ten recovered attempts legitimately consume the tenant's ten dispatch
	// credits. Settle them the way a worker and broker would, then a brand-new
	// job must still be dispatchable.
	if _, err := p.pool.Exec(ctx, `UPDATE job_outbox o SET published=TRUE, published_at=clock_timestamp(), delivered_at=clock_timestamp()
		WHERE NOT o.dead_lettered AND o.job_id IN (SELECT id FROM jobs WHERE tenant_id=$1)`, tn.ID); err != nil {
		t.Fatalf("settle outbox rows: %v", err)
	}
	if _, err := p.pool.Exec(ctx, `UPDATE jobs SET status='succeeded', dispatched_at=NULL WHERE tenant_id=$1`, tn.ID); err != nil {
		t.Fatalf("settle jobs: %v", err)
	}

	fresh := liveCreateJob(t, ctx, r, tn.ID, "after-recovery")
	if n, err := s.DispatchReady(ctx, jobs); err != nil {
		t.Fatalf("dispatch fresh job: %v", err)
	} else if n != 1 {
		t.Fatalf("fresh job dispatch count = %d, want 1", n)
	}
	if got := liveGetJob(t, ctx, r, fresh.ID); got.DispatchedAt == nil {
		t.Fatal("fresh job must be dispatched after the tenant's credits are released")
	}
}

func TestLive_DispatchHonorsTenantMaxInFlight(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	p := openTestPool(t)
	r := NewRepositories(p)
	tn := liveTenant(t, r)
	if _, err := p.pool.Exec(ctx, `UPDATE tenants SET max_in_flight=25 WHERE id=$1`, tn.ID); err != nil {
		t.Fatal(err)
	}
	if got, err := r.Tenants.GetByID(ctx, tn.ID); err != nil || got.MaxInFlight != 25 {
		t.Fatalf("tenant limit round trip: %+v %v", got, err)
	}
	for i := range 30 {
		liveCreateJob(t, ctx, r, tn.ID, fmt.Sprintf("burst-%02d", i))
	}
	s := service.NewScheduler(r.Jobs, r.Tenants, r.Events, r.Outbox, r.Tx, nil, nil)
	if n, err := s.DispatchReady(ctx, 100); err != nil || n != 25 {
		t.Fatalf("first dispatch = %d (err=%v), want the tenant's 25 slots", n, err)
	}
	if n, err := s.DispatchReady(ctx, 100); err != nil || n != 0 {
		t.Fatalf("second dispatch = %d (err=%v), want 0 while every slot is held", n, err)
	}
	if _, err := p.pool.Exec(ctx, `UPDATE tenants SET max_in_flight=0 WHERE id=$1`, tn.ID); err == nil {
		t.Fatal("max_in_flight=0 must violate the check constraint")
	}
}

func TestLive_TerminalizePoisonPillAtDeadLetterCap(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	p := openTestPool(t)
	r := NewRepositories(p)
	tn := liveTenant(t, r)

	job := liveCreateJob(t, ctx, r, tn.ID, "poison-pill")
	liveBackdateDispatch(t, ctx, r, p, job, time.Hour)
	for i := 0; i < domain.MaxDeadLetteredDispatches; i++ {
		liveDeadLetter(t, ctx, r, job.ID, time.Now().UTC().Add(-2*time.Hour))
	}

	s := service.NewScheduler(r.Jobs, r.Tenants, r.Events, r.Outbox, r.Tx, nil, nil)
	s.RedispatchAfter = time.Minute
	if n, err := s.DispatchReady(ctx, 10); err != nil {
		t.Fatalf("dispatch ready: %v", err)
	} else if n != 0 {
		t.Fatalf("poison pill must not be re-dispatched, got %d", n)
	}

	got := liveGetJob(t, ctx, r, job.ID)
	if got.Status != domain.StatusDead {
		t.Fatalf("poison pill status = %q, want %q", got.Status, domain.StatusDead)
	}
	if got.DispatchedAt != nil {
		t.Fatalf("terminal job must release its dispatch credit, got dispatched_at=%v", got.DispatchedAt)
	}

	events, err := r.Events.ListByJobID(ctx, job.ID)
	if err != nil {
		t.Fatalf("list job events: %v", err)
	}
	var terminal *domain.JobEvent
	for _, e := range events {
		if e.ToStatus == domain.StatusDead {
			terminal = e
		}
	}
	if terminal == nil {
		t.Fatalf("missing terminal job event, got %d events", len(events))
	}
	if terminal.FromStatus != domain.StatusPending {
		t.Fatalf("terminal event from_status = %q, want %q", terminal.FromStatus, domain.StatusPending)
	}
	if terminal.ErrorMessage == nil || !strings.Contains(*terminal.ErrorMessage, "outbox publish exhausted") {
		t.Fatalf("terminal event error_message = %v", terminal.ErrorMessage)
	}
	if terminal.OccurredAt.IsZero() {
		t.Fatal("terminal event must record occurred_at")
	}
	if terminal.WorkerID != nil && *terminal.WorkerID != "" {
		t.Fatalf("terminal event worker_id = %q, want empty audit field", *terminal.WorkerID)
	}

	rows := liveOutboxRows(t, ctx, p, job.ID)
	if len(rows) != domain.MaxDeadLetteredDispatches {
		t.Fatalf("outbox rows = %d, want only the dead letters", len(rows))
	}
	for id, row := range rows {
		if row.Published || !row.DeadLettered {
			t.Fatalf("unexpected live announcement %s after terminalization: %+v", id, row)
		}
	}

	if n, err := s.DispatchReady(ctx, 10); err != nil {
		t.Fatalf("second dispatch ready: %v", err)
	} else if n != 0 {
		t.Fatalf("terminal job was re-dispatched, count = %d", n)
	}
	if got := liveGetJob(t, ctx, r, job.ID); got.Status != domain.StatusDead || got.DispatchedAt != nil {
		t.Fatalf("terminal job mutated by later pass: status=%q dispatched_at=%v", got.Status, got.DispatchedAt)
	}
	if rows := liveOutboxRows(t, ctx, p, job.ID); len(rows) != domain.MaxDeadLetteredDispatches {
		t.Fatalf("later pass staged an announcement for a terminal job: %d rows", len(rows))
	}
	events, err = r.Events.ListByJobID(ctx, job.ID)
	if err != nil {
		t.Fatalf("list job events after second pass: %v", err)
	}
	deadEvents := 0
	for _, e := range events {
		if e.ToStatus == domain.StatusDead {
			deadEvents++
		}
	}
	if deadEvents != 1 {
		t.Fatalf("terminal event recorded %d times, want once", deadEvents)
	}
}

func TestLive_PruneDeadLetterRetention(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	p := openTestPool(t)
	r := NewRepositories(p)
	tn := liveTenant(t, r)
	job := liveCreateJob(t, ctx, r, tn.ID, "retention")

	now := time.Now().UTC()
	oldPublished := domain.NewOutboxEvent(job.ID, broker.JobsRoutingKey, json.RawMessage(`{}`))
	oldPublished.Published = true
	oldPublished.CreatedAt = now.Add(-2 * time.Hour)
	publishedAt := now.Add(-2 * time.Hour)
	oldPublished.PublishedAt = &publishedAt
	if err := r.Outbox.Store(ctx, oldPublished); err != nil {
		t.Fatalf("store old published row: %v", err)
	}

	recentDead := domain.NewOutboxEvent(job.ID, broker.JobsRoutingKey, json.RawMessage(`{}`))
	recentDead.DeadLettered = true
	recentDead.RetryCount = domain.MaxOutboxRetries
	recentDead.CreatedAt = now.Add(-time.Hour)
	if err := r.Outbox.Store(ctx, recentDead); err != nil {
		t.Fatalf("store recent dead letter: %v", err)
	}

	oldDead := domain.NewOutboxEvent(job.ID, broker.JobsRoutingKey, json.RawMessage(`{}`))
	oldDead.DeadLettered = true
	oldDead.RetryCount = domain.MaxOutboxRetries
	oldDead.CreatedAt = now.Add(-domain.DeadLetterRetention - time.Hour)
	if err := r.Outbox.Store(ctx, oldDead); err != nil {
		t.Fatalf("store old dead letter: %v", err)
	}

	if err := p.Prune(ctx, time.Hour, time.Hour); err != nil {
		t.Fatalf("prune: %v", err)
	}

	rows := liveOutboxRows(t, ctx, p, job.ID)
	if len(rows) != 1 {
		t.Fatalf("outbox rows after prune = %d, want only the in-window dead letter", len(rows))
	}
	if _, ok := rows[recentDead.ID]; !ok {
		t.Fatalf("dead letter inside the retention window was deleted")
	}
	if _, ok := rows[oldPublished.ID]; ok {
		t.Fatalf("published row past the outbox age was not deleted")
	}
	if _, ok := rows[oldDead.ID]; ok {
		t.Fatalf("dead letter past the retention window was not deleted")
	}
}

func TestLive_CheckSchemaAtCurrentVersion(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	p := openTestPool(t)
	if err := p.CheckSchema(ctx); err != nil {
		t.Fatalf("CheckSchema at version %d: %v", SchemaVersion, err)
	}
}

func TestLive_ExtendRunningLeasesRevivesLapsedLease(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	p := openTestPool(t)
	r := NewRepositories(p)
	tn := liveTenant(t, r)
	j := liveCreateJob(t, ctx, r, tn.ID, "lapsed")
	token, err := r.Jobs.ClaimJob(ctx, j.ID, "owner", time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	if n, err := r.Jobs.ExtendRunningLeases(ctx, time.Minute); err != nil || n < 1 {
		t.Fatalf("extend: n=%d err=%v", n, err)
	}
	if err := r.Jobs.RenewLease(ctx, j.ID, "owner", token, time.Minute); err != nil {
		t.Fatalf("owner must renew after grace: %v", err)
	}
	if done, err := r.Jobs.CompleteLease(ctx, j.ID, "owner", token, domain.StatusSucceeded); err != nil || done.Status != domain.StatusSucceeded {
		t.Fatalf("owner must complete after grace: %+v %v", done, err)
	}
}

func TestLive_TenantUpdateAndList(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	p := openTestPool(t)
	r := NewRepositories(p)
	tn := liveTenant(t, r)
	now := time.Now().UTC().Truncate(time.Microsecond)
	tn.APIKeyHash = domain.HashAPIKey("rotated-" + tn.ID.String())
	tn.MaxInFlight = 77
	tn.DisabledAt = &now
	if err := r.Tenants.Update(ctx, tn); err != nil {
		t.Fatal(err)
	}
	got, err := r.Tenants.GetByAPIKeyHash(ctx, tn.APIKeyHash)
	if err != nil || got.MaxInFlight != 77 || got.DisabledAt == nil || !got.DisabledAt.Equal(now) {
		t.Fatalf("update round trip: %+v %v", got, err)
	}
	list, err := r.Tenants.List(ctx, 1000, 0)
	found := false
	for _, x := range list {
		found = found || x.ID == tn.ID
	}
	if err != nil || !found {
		t.Fatalf("list: found=%v err=%v", found, err)
	}
	missing := *tn
	missing.ID = uuid.New()
	if err := r.Tenants.Update(ctx, &missing); err != domain.ErrTenantNotFound {
		t.Fatalf("unknown tenant: %v", err)
	}
}

func TestLive_WorkerCredentials(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	r := NewRepositories(openTestPool(t))
	cred := &domain.WorkerCredential{ID: uuid.New(), WorkerID: "edge-" + uuid.NewString(), TokenHash: domain.HashAPIKey(uuid.NewString()), CreatedAt: time.Now().UTC()}
	if err := r.WorkerCredentials.Create(ctx, cred); err != nil {
		t.Fatal(err)
	}
	got, err := r.WorkerCredentials.GetByTokenHash(ctx, cred.TokenHash)
	if err != nil || got.WorkerID != cred.WorkerID || got.RevokedAt != nil {
		t.Fatalf("get: %+v %v", got, err)
	}
	first := time.Now().UTC().Truncate(time.Microsecond)
	if got, err = r.WorkerCredentials.Revoke(ctx, cred.ID, first); err != nil || got.RevokedAt == nil || !got.RevokedAt.Equal(first) {
		t.Fatalf("revoke: %+v %v", got, err)
	}
	if got, err = r.WorkerCredentials.Revoke(ctx, cred.ID, first.Add(time.Hour)); err != nil || !got.RevokedAt.Equal(first) {
		t.Fatalf("second revoke must keep the first timestamp: %+v %v", got, err)
	}
	list, err := r.WorkerCredentials.List(ctx, 1000, 0)
	found := false
	for _, c := range list {
		found = found || c.ID == cred.ID
	}
	if err != nil || !found {
		t.Fatalf("list: found=%v err=%v", found, err)
	}
	if _, err := r.WorkerCredentials.GetByTokenHash(ctx, "missing"); err != domain.ErrWorkerCredentialNotFound {
		t.Fatalf("missing: %v", err)
	}
	if _, err := r.WorkerCredentials.Revoke(ctx, uuid.New(), first); err != domain.ErrWorkerCredentialNotFound {
		t.Fatalf("revoke missing: %v", err)
	}
}

func TestLive_RequeueDeadResetsOutboxDeadLetterBudget(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	p := openTestPool(t)
	r := NewRepositories(p)
	tn := liveTenant(t, r)
	job := liveCreateJob(t, ctx, r, tn.ID, "requeue-after-outbox-exhaustion")
	for i := 0; i < domain.MaxDeadLetteredDispatches; i++ {
		liveDeadLetter(t, ctx, r, job.ID, time.Now().UTC())
	}
	if ids, err := r.Jobs.TerminalizeOutboxExhausted(ctx, 10, domain.MaxDeadLetteredDispatches); err != nil || !slices.Contains(ids, job.ID) {
		t.Fatalf("terminalize=%v err=%v", ids, err)
	}
	if _, err := r.Jobs.RequeueDead(ctx, job.ID); err != nil {
		t.Fatalf("requeue: %v", err)
	}
	requeuedAt, err := r.Jobs.Now(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Events.Record(ctx, &domain.JobEvent{ID: uuid.New(), JobID: job.ID, FromStatus: domain.StatusDead, ToStatus: domain.StatusPending, OccurredAt: requeuedAt}); err != nil {
		t.Fatalf("record requeue: %v", err)
	}
	if ids, err := r.Jobs.TerminalizeOutboxExhausted(ctx, 10, domain.MaxDeadLetteredDispatches); err != nil || slices.Contains(ids, job.ID) {
		t.Fatalf("requeued job terminalized again: ids=%v err=%v", ids, err)
	}
	if got := liveGetJob(t, ctx, r, job.ID); got.Status != domain.StatusPending {
		t.Fatalf("status=%q, want pending", got.Status)
	}
	if rows := liveOutboxRows(t, ctx, p, job.ID); len(rows) != domain.MaxDeadLetteredDispatches {
		t.Fatalf("dead-letter history must be retained, got %d rows", len(rows))
	}
	for i := 0; i < domain.MaxDeadLetteredDispatches; i++ {
		liveDeadLetter(t, ctx, r, job.ID, time.Now().UTC().Add(time.Second))
	}
	if ids, err := r.Jobs.TerminalizeOutboxExhausted(ctx, 10, domain.MaxDeadLetteredDispatches); err != nil || !slices.Contains(ids, job.ID) {
		t.Fatalf("new dead letters after requeue must still count: ids=%v err=%v", ids, err)
	}
}

type liveUnroutableBroker struct{}

func (liveUnroutableBroker) Publish(context.Context, string, []byte, string) error {
	return fmt.Errorf("%w: 312 NO_ROUTE", broker.ErrUnroutable)
}
func (liveUnroutableBroker) Close() error { return nil }

func TestLive_NoWorkerDefersThenDies(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	p := openTestPool(t)
	r := NewRepositories(p)
	tn := liveTenant(t, r)
	s := service.NewScheduler(r.Jobs, r.Tenants, r.Events, r.Outbox, r.Tx, liveUnroutableBroker{}, nil)
	s.ReconcileThreshold = 0
	s.NoWorkerRetry = 200 * time.Millisecond
	s.NoWorkerTimeout = time.Hour
	// Scheduled long ago: only time since the first refusal may count.
	job, err := s.SubmitJob(ctx, service.SubmitInput{TenantID: tn.ID, Name: "no-such-worker", Payload: json.RawMessage(`{}`), MaxAttempts: 3, ScheduledAt: time.Now().Add(-48 * time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	cycle := func() {
		t.Helper()
		if _, err := s.DispatchReady(ctx, 10); err != nil {
			t.Fatal(err)
		}
		if _, err := s.ReconcileOutbox(ctx, 10); err != nil {
			t.Fatal(err)
		}
	}
	cycle()
	deferred := liveGetJob(t, ctx, r, job.ID)
	if deferred.Status != domain.StatusPending || deferred.DispatchedAt != nil || deferred.NextRetryAt == nil {
		t.Fatalf("unroutable job not deferred: %+v", deferred)
	}
	for id, row := range liveOutboxRows(t, ctx, p, job.ID) {
		if !row.Published || row.DeadLettered || row.RetryCount != 0 {
			t.Fatalf("unroutable row %s used retry budget: %+v", id, row)
		}
	}
	if n, _ := s.DispatchReady(ctx, 10); n != 0 {
		t.Fatal("deferred job dispatched before NoWorkerRetry")
	}
	time.Sleep(300 * time.Millisecond)
	s.NoWorkerTimeout = time.Millisecond
	cycle()
	if dead := liveGetJob(t, ctx, r, job.ID); dead.Status != domain.StatusDead || dead.DispatchedAt != nil {
		t.Fatalf("job not dead after NoWorkerTimeout: %+v", dead)
	}
	if rows := liveOutboxRows(t, ctx, p, job.ID); len(rows) != 2 {
		t.Fatalf("outbox rows = %d, want 2 settled announcements", len(rows))
	}
}
