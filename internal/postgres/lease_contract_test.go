package postgres

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
)

func createLeaseJob(t *testing.T, r *Repositories, tenantID uuid.UUID) *domain.Job {
	t.Helper()
	job, err := domain.NewJob(tenantID, "lease-contract", nil, domain.PriorityHigh, 3, nil, time.Now().Add(-time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Jobs.Create(t.Context(), job); err != nil {
		t.Fatal(err)
	}
	return job
}

func TestLive_CompleteLeaseOwnership(t *testing.T) {
	p := openTestPool(t)
	r := NewRepositories(p)
	tenant := liveTenant(t, r)
	for _, final := range []domain.JobStatus{domain.StatusSucceeded, domain.StatusFailed} {
		t.Run(string(final), func(t *testing.T) {
			ctx := t.Context()
			job := createLeaseJob(t, r, tenant.ID)
			token, err := r.Jobs.ClaimJob(ctx, job.ID, "owner", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			// A lease claimed before migration 22 has no last_lease_owner.
			if _, err := p.pool.Exec(ctx, `UPDATE jobs SET last_lease_owner=NULL WHERE id=$1`, job.ID); err != nil {
				t.Fatal(err)
			}
			before, err := r.Jobs.GetByID(ctx, job.ID)
			if err != nil {
				t.Fatal(err)
			}
			for _, owner := range []string{"", "other-owner"} {
				if _, err := r.Jobs.CompleteLease(ctx, job.ID, owner, token, final); !errors.Is(err, domain.ErrLeaseLost) {
					t.Fatalf("owner %q must not complete another worker's lease: %v", owner, err)
				}
			}
			for _, invalid := range []domain.JobStatus{domain.StatusPending, domain.StatusRunning, domain.StatusDead, domain.StatusCancelled} {
				if _, err := r.Jobs.CompleteLease(ctx, job.ID, "owner", token, invalid); !errors.Is(err, domain.ErrInvalidTransition) {
					t.Fatalf("completion status %q must fail: %v", invalid, err)
				}
			}
			after, err := r.Jobs.GetByID(ctx, job.ID)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("rejected completions changed the job: before=%+v after=%+v err=%v", before, after, err)
			}
			completed, err := r.Jobs.CompleteLease(ctx, job.ID, "owner", token, final)
			if err != nil {
				t.Fatal(err)
			}
			wantAttempts := 0
			if final == domain.StatusFailed {
				wantAttempts = 1
			}
			if completed.Status != final || completed.AttemptCount != wantAttempts || completed.FencingToken != token || completed.LeaseOwner != nil || completed.LeaseExpiresAt != nil {
				t.Fatalf("completion must atomically clear the lease and count failures once: %+v", completed)
			}
			if completed.LastLeaseOwner == nil || *completed.LastLeaseOwner != "owner" {
				t.Fatalf("completion must record who held the lease: %v", completed.LastLeaseOwner)
			}
			if _, err := r.Jobs.CompleteLease(ctx, job.ID, "owner", token, final); !errors.Is(err, domain.ErrInvalidTransition) {
				t.Fatalf("repeated completion must be rejected: %v", err)
			}
			stored, err := r.Jobs.GetByID(ctx, job.ID)
			if err != nil || !reflect.DeepEqual(completed, stored) {
				t.Fatalf("repeated completion changed stored result: %+v err=%v", stored, err)
			}
		})
	}
}

// NOW() is fixed at transaction start. These transactions deliberately begin
// before expiry and perform the guarded write after expiry, so NOW()-based
// implementations would incorrectly revive or complete an expired lease.
func TestLive_LeaseExpiryUsesDatabaseClockInOldTransaction(t *testing.T) {
	p := openTestPool(t)
	r := NewRepositories(p)
	tenant := liveTenant(t, r)
	for _, operation := range []string{"renew", "complete success", "complete failure"} {
		t.Run(operation, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			job := createLeaseJob(t, r, tenant.ID)
			var before *domain.Job
			err := p.WithinTransaction(ctx, func(txctx context.Context) error {
				// Establish the transaction before claiming on another connection,
				// guaranteeing its timestamp predates expiry even on a slow runner.
				var transactionTime time.Time
				if err := dbFromCtx(txctx, p).QueryRow(txctx, `SELECT transaction_timestamp()`).Scan(&transactionTime); err != nil {
					return err
				}
				token, err := r.Jobs.ClaimJob(ctx, job.ID, "owner", time.Second)
				if err != nil {
					return err
				}
				before, err = r.Jobs.GetByID(txctx, job.ID)
				if err != nil {
					return err
				}
				var beganBeforeExpiry, expiredNow bool
				if err := dbFromCtx(txctx, p).QueryRow(txctx, `
					SELECT transaction_timestamp() < $1, clock_timestamp() >= $1
					FROM pg_sleep(GREATEST(0, EXTRACT(EPOCH FROM ($1::timestamptz - clock_timestamp())))::double precision + 0.05)`,
					before.LeaseExpiresAt).Scan(&beganBeforeExpiry, &expiredNow); err != nil {
					return err
				}
				if !beganBeforeExpiry || !expiredNow {
					return fmt.Errorf("test requires transaction start < expiry <= DB clock: before=%v expired=%v", beganBeforeExpiry, expiredNow)
				}
				if operation == "renew" {
					err = r.Jobs.RenewLease(txctx, job.ID, "owner", token, time.Minute)
				} else {
					final := domain.StatusSucceeded
					if operation == "complete failure" {
						final = domain.StatusFailed
					}
					_, err = r.Jobs.CompleteLease(txctx, job.ID, "owner", token, final)
				}
				if !errors.Is(err, domain.ErrLeaseLost) {
					return fmt.Errorf("%s must reject expired lease with ErrLeaseLost, got %v", operation, err)
				}
				return nil // Commit the rejected operation so rollback cannot hide writes.
			})
			if err != nil {
				t.Fatal(err)
			}
			after, err := r.Jobs.GetByID(ctx, job.ID)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("expired %s changed the job: before=%+v after=%+v err=%v", operation, before, after, err)
			}
		})
	}
}

func TestLive_UndispatchedJobsSkipLocked(t *testing.T) {
	p := openTestPool(t)
	r := NewRepositories(p)
	tenant := liveTenant(t, r)
	first := createLeaseJob(t, r, tenant.ID)
	second := createLeaseJob(t, r, tenant.ID)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	held, err := p.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = held.Rollback(context.Background()) }()
	if _, err := r.Jobs.GetByIDForUpdate(context.WithValue(ctx, txKey{}, held), first.ID); err != nil {
		t.Fatalf("lock first ready job: %v", err)
	}
	skipCtx, stop := context.WithTimeout(ctx, time.Second)
	err = p.WithinTransaction(skipCtx, func(txctx context.Context) error {
		other, err := r.Jobs.GetUndispatchedJobsForUpdate(txctx, 100000)
		if err != nil {
			return err
		}
		foundSecond := false
		for _, job := range other {
			if job.ID == first.ID {
				return fmt.Errorf("competing dispatcher selected locked row %s", first.ID)
			}
			foundSecond = foundSecond || job.ID == second.ID
		}
		if !foundSecond {
			return fmt.Errorf("competing dispatcher did not select unlocked row %s", second.ID)
		}
		return nil
	})
	stop()
	if err != nil {
		t.Fatalf("dispatch query must skip held lock without blocking: %v", err)
	}
	if err := held.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := r.Jobs.MarkDispatched(ctx, first.ID, first.FencingToken+1); !errors.Is(err, domain.ErrJobNotClaimable) {
		t.Fatalf("wrong-token dispatch marker must fail: %v", err)
	}
	if err := r.Jobs.MarkDispatched(ctx, first.ID, first.FencingToken); err != nil {
		t.Fatal(err)
	}
	if err := r.Jobs.MarkDispatched(ctx, first.ID, first.FencingToken); !errors.Is(err, domain.ErrJobNotClaimable) {
		t.Fatalf("same attempt must not be marked twice: %v", err)
	}
	if err := p.WithinTransaction(ctx, func(txctx context.Context) error {
		ready, err := r.Jobs.GetUndispatchedJobsForUpdate(txctx, 100000)
		for _, j := range ready {
			if j.ID == first.ID {
				return fmt.Errorf("marked job %s was selected again", j.ID)
			}
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestLive_LeaseExpiryAfterRowLockWait(t *testing.T) {
	p := openTestPool(t)
	r := NewRepositories(p)
	tenant := liveTenant(t, r)
	for _, operation := range []string{"renew", "complete", "claim"} {
		t.Run(operation, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			job := createLeaseJob(t, r, tenant.ID)
			var token int64
			var expires time.Time
			if operation != "claim" {
				var err error
				token, err = r.Jobs.ClaimJob(ctx, job.ID, "owner", 400*time.Millisecond)
				if err != nil {
					t.Fatal(err)
				}
				stored, err := r.Jobs.GetByID(ctx, job.ID)
				if err != nil {
					t.Fatal(err)
				}
				expires = *stored.LeaseExpiresAt
			} else {
				expires = time.Now().Add(200 * time.Millisecond)
			}
			held, err := p.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = held.Rollback(context.Background()) }()
			if _, err := held.Exec(ctx, `SELECT id FROM jobs WHERE id=$1 FOR UPDATE`, job.ID); err != nil {
				t.Fatal(err)
			}
			// An explicit operation transaction gives us a backend PID, so this
			// test proves the operation reached a DB lock before expiry.
			op, err := p.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = op.Rollback(context.Background()) }()
			var pid int
			if err := op.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
				t.Fatal(err)
			}
			result := make(chan error, 1)
			go func() {
				opCtx := context.WithValue(ctx, txKey{}, op)
				var err error
				switch operation {
				case "renew":
					err = r.Jobs.RenewLease(opCtx, job.ID, "owner", token, time.Minute)
				case "complete":
					_, err = r.Jobs.CompleteLease(opCtx, job.ID, "owner", token, domain.StatusSucceeded)
				case "claim":
					_, err = r.Jobs.ClaimJob(opCtx, job.ID, "owner", 200*time.Millisecond)
				}
				result <- err
			}()
			tick := time.NewTicker(5 * time.Millisecond)
			defer tick.Stop()
			for {
				var blocked bool
				if err := p.pool.QueryRow(ctx, `SELECT COALESCE(wait_event_type='Lock', false) FROM pg_stat_activity WHERE pid=$1`, pid).Scan(&blocked); err != nil {
					t.Fatal(err)
				}
				if blocked {
					break
				}
				select {
				case err := <-result:
					t.Fatalf("operation escaped row lock: %v", err)
				case <-tick.C:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			if _, err := p.pool.Exec(ctx, `SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM ($1::timestamptz-clock_timestamp())))::double precision + 0.05)`, expires); err != nil {
				t.Fatal(err)
			}
			if err := held.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			err = <-result
			if operation != "claim" && !errors.Is(err, domain.ErrLeaseLost) {
				t.Fatalf("expired %s was accepted: %v", operation, err)
			}
			if operation == "claim" && err != nil {
				t.Fatal(err)
			}
			if err := op.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			stored, err := r.Jobs.GetByID(ctx, job.ID)
			if err != nil {
				t.Fatal(err)
			}
			if operation == "claim" {
				if stored.LeaseExpiresAt == nil || time.Until(*stored.LeaseExpiresAt) < 100*time.Millisecond {
					t.Fatalf("claim minted a lease before its lock wait: %+v", stored)
				}
			} else if stored.LeaseExpiresAt == nil || !stored.LeaseExpiresAt.Equal(expires) || stored.Status != domain.StatusRunning {
				t.Fatalf("expired operation mutated job: %+v", stored)
			}
		})
	}
}
