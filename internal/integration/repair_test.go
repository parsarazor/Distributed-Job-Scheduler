package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/service"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/testinfra"
)

func repairSQL(t *testing.T, f *fixture) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(f.ctx, f.dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func TestPostgresRepairContracts(t *testing.T) {
	server := testinfra.StartPostgres(t)
	t.Run("terminal payload retention preserves submission identity", func(t *testing.T) {
		f := newFixture(t, server)
		input := service.SubmitInput{TenantID: f.tenant.ID, Name: "expire", MaxAttempts: 1, Payload: json.RawMessage(`{"secret":"old-input"}`), IdempotencyKey: "retained-key"}
		job, err := f.sched.SubmitJob(f.ctx, input)
		if err != nil {
			t.Fatal(err)
		}
		token, err := f.repos.Jobs.ClaimJob(f.ctx, job.ID, "retention", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.sched.ReportCompletion(f.ctx, job.ID, "retention", domain.StatusFailed, token, "private-detail"); err != nil {
			t.Fatal(err)
		}
		conn := repairSQL(t, f)
		if _, err := conn.Exec(f.ctx, `ALTER TABLE jobs DISABLE TRIGGER trigger_jobs_updated_at; UPDATE jobs SET updated_at=clock_timestamp()-interval '100 days'; ALTER TABLE jobs ENABLE TRIGGER trigger_jobs_updated_at;`); err != nil {
			t.Fatal(err)
		}
		if err := f.repos.Tx.Prune(f.ctx, 7*24*time.Hour, 90*24*time.Hour); err != nil {
			t.Fatal(err)
		}
		stored, err := f.sched.GetJob(f.ctx, job.ID)
		if err != nil || !stored.PayloadExpired || string(stored.Payload) != "{}" || stored.SubmissionKey != input.IdempotencyKey || stored.SubmissionHash == "" {
			t.Fatalf("retention=%+v err=%v", stored, err)
		}
		if _, err := f.sched.RequeueDead(f.ctx, job.ID); !errors.Is(err, domain.ErrPayloadExpired) {
			t.Fatalf("expired input requeued: %v", err)
		}
		replay, err := f.sched.SubmitJob(f.ctx, input)
		if err != nil || replay.ID != job.ID {
			t.Fatalf("expired job lost deduplication: %+v %v", replay, err)
		}
	})
	t.Run("distinct concurrent quota and rate admission", func(t *testing.T) {
		for _, rateLimited := range []bool{false, true} {
			t.Run(fmt.Sprintf("rate=%t", rateLimited), func(t *testing.T) {
				f := newFixture(t, server)
				f.sched.TenantQuota, f.sched.TenantRate = 3, 100
				if rateLimited {
					f.sched.TenantQuota, f.sched.TenantRate = 100, 3
				}
				type outcome struct{ err error }
				results := make(chan outcome, 16)
				start := make(chan struct{})
				var wg sync.WaitGroup
				for i := range cap(results) {
					wg.Go(func() {
						<-start
						job, err := f.sched.SubmitJob(f.ctx, service.SubmitInput{TenantID: f.tenant.ID, Name: fmt.Sprintf("distinct-%d", i)})
						if rateLimited && err == nil {
							// Terminal history still counts against the rolling rate.
							_, err = f.sched.CancelJob(f.ctx, job.ID, "fixture complete")
						}
						results <- outcome{err}
					})
				}
				close(start)
				wg.Wait()
				close(results)
				accepted, rejected := 0, 0
				for result := range results {
					switch {
					case result.err == nil:
						accepted++
					case errors.Is(result.err, domain.ErrTenantLimit):
						rejected++
					default:
						t.Fatalf("unexpected admission failure: %v", result.err)
					}
				}
				var jobs, submissions int
				err := repairSQL(t, f).QueryRow(f.ctx, `SELECT (SELECT count(*) FROM jobs), (SELECT count(*) FROM job_events WHERE from_status='pending' AND to_status='pending')`).Scan(&jobs, &submissions)
				if err != nil || accepted != 3 || rejected != 13 || jobs != 3 || submissions != 3 {
					t.Fatalf("admission accepted=%d rejected=%d jobs=%d audits=%d err=%v", accepted, rejected, jobs, submissions, err)
				}
			})
		}
	})
	t.Run("confirmed prefix survives a later pass timeout", func(t *testing.T) {
		f := newFixture(t, server)
		for range 8 {
			f.submit(t, "slow", 3, nil, time.Time{})
		}
		if n, err := f.sched.DispatchReady(f.ctx, 8); err != nil || n != 8 {
			t.Fatalf("dispatch=%d err=%v", n, err)
		}
		slow := &recordingPublisher{hook: func(ctx context.Context) error {
			timer := time.NewTimer(100 * time.Millisecond)
			defer timer.Stop()
			select {
			case <-timer.C:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}}
		ctx, cancel := context.WithTimeout(f.ctx, 350*time.Millisecond)
		defer cancel()
		n, err := f.scheduler(f.repos.Events, f.repos.Outbox, slow).ReconcileOutbox(ctx, 8)
		if !errors.Is(err, context.DeadlineExceeded) || n < 1 || n >= 8 {
			t.Fatalf("slow pass progress=%d err=%v", n, err)
		}
		if left := unpublished(t, f); len(left) != 8-n {
			t.Fatalf("confirmed progress rolled back: confirmed=%d left=%d", n, len(left))
		}
		if recovered, err := f.sched.ReconcileOutbox(f.ctx, 8); err != nil || recovered != 8-n {
			t.Fatalf("recovery progress=%d err=%v", recovered, err)
		}
		confirmed := map[string]bool{}
		for _, m := range slow.published()[:n] {
			confirmed[m.MessageID] = true
		}
		for _, m := range f.bus.published() {
			if confirmed[m.MessageID] {
				t.Fatal("recovery republished a committed confirmation")
			}
		}
	})
	t.Run("cancel retires unsent work and retains published credits", func(t *testing.T) {
		f := newFixture(t, server)
		unsent := f.submit(t, "unsent", 3, nil, time.Time{})
		_, _ = f.sched.DispatchReady(f.ctx, 1)
		if _, err := f.sched.CancelJob(f.ctx, unsent.ID, "cancel"); err != nil {
			t.Fatal(err)
		}
		if n, err := f.sched.ReconcileOutbox(f.ctx, 10); err != nil || n != 0 {
			t.Fatalf("canceled announcement was published: %d %v", n, err)
		}
		var deliveries [][]byte
		for range 10 {
			job := f.submit(t, "queued", 3, nil, time.Time{})
			if n, err := f.sched.DispatchReady(f.ctx, 1); err != nil || n != 1 {
				t.Fatalf("dispatch=%d err=%v", n, err)
			}
			if n, err := f.sched.ReconcileOutbox(f.ctx, 1); err != nil || n != 1 {
				t.Fatalf("publish=%d err=%v", n, err)
			}
			deliveries = append(deliveries, f.bus.published()[len(deliveries)].Payload)
			if _, err := f.sched.CancelJob(f.ctx, job.ID, "cancel published"); err != nil {
				t.Fatal(err)
			}
		}
		f.submit(t, "after-cap", 3, nil, time.Time{})
		if n, err := f.sched.DispatchReady(f.ctx, 10); err != nil || n != 0 {
			t.Fatalf("cancellation bypassed delivery cap: %d %v", n, err)
		}
		worker := service.NewWorker("discard-obsolete", f.sched, 1, time.Minute)
		if err := worker.HandleMessage(f.ctx, deliveries[0]); err != nil {
			t.Fatal(err)
		}
		if n, err := f.sched.DispatchReady(f.ctx, 1); err != nil || n != 1 {
			t.Fatalf("receipt did not release credit: %d %v", n, err)
		}
	})
	t.Run("stale confirmed dispatch is replaced and old identity is fenced", func(t *testing.T) {
		f := newFixture(t, server)
		job := f.submit(t, "recover", 3, nil, time.Time{})
		_, _ = f.sched.DispatchReady(f.ctx, 1)
		first := unpublished(t, f)[0]
		if n, err := f.sched.ReconcileOutbox(f.ctx, 1); n != 1 || err != nil {
			t.Fatalf("publish=%d err=%v", n, err)
		}
		f.sched.RedispatchAfter = time.Nanosecond
		if n, err := f.sched.DispatchReady(f.ctx, 1); n != 1 || err != nil {
			t.Fatalf("recover=%d err=%v", n, err)
		}
		fresh := unpublished(t, f)[0]
		if fresh.ID == first.ID || fresh.JobID != job.ID || len(fresh.Payload) > 512 {
			t.Fatalf("replacement identity or size invalid: %+v", fresh)
		}
		worker := service.NewWorker("recovered", f.sched, 1, time.Minute)
		runs := 0
		worker.RegisterHandler(job.Name, func(context.Context, *domain.Job) error { runs++; return nil })
		if err := worker.HandleMessage(f.ctx, first.Payload); err != nil || runs != 0 {
			t.Fatalf("stale announcement executed: runs=%d err=%v", runs, err)
		}
		if err := worker.HandleMessage(f.ctx, fresh.Payload); err != nil || runs != 1 {
			t.Fatalf("replacement failed: runs=%d err=%v", runs, err)
		}
	})
	t.Run("legacy missing retry and exhausted attempts recover", func(t *testing.T) {
		f := newFixture(t, server)
		pending := f.submit(t, "legacy-retry", 3, nil, time.Time{})
		dead := f.submit(t, "legacy-exhausted", 1, nil, time.Time{})
		child := f.submit(t, "blocked-child", 3, &dead.ID, time.Time{})
		for _, job := range []*domain.Job{pending, dead} {
			token, err := f.repos.Jobs.ClaimJob(f.ctx, job.ID, "legacy-worker", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.repos.Jobs.CompleteLease(f.ctx, job.ID, "legacy-worker", token, domain.StatusFailed); err != nil {
				t.Fatal(err)
			}
		}
		if n, err := f.sched.RetryDue(f.ctx, 10); n != 2 || err != nil {
			t.Fatalf("recovered=%d err=%v", n, err)
		}
		for id, want := range map[uuid.UUID]domain.JobStatus{pending.ID: domain.StatusPending, dead.ID: domain.StatusDead, child.ID: domain.StatusCancelled} {
			got, err := f.sched.GetJob(f.ctx, id)
			if err != nil || got.Status != want {
				t.Fatalf("status=%+v want=%s err=%v", got, want, err)
			}
		}
	})
	t.Run("priority selects job identity and announcements omit payloads", func(t *testing.T) {
		f := newFixture(t, server)
		ids := map[domain.JobPriority]uuid.UUID{}
		for _, priority := range []domain.JobPriority{domain.PriorityLow, domain.PriorityDefault, domain.PriorityCritical} {
			job, err := f.sched.SubmitJob(f.ctx, service.SubmitInput{TenantID: f.tenant.ID, Name: "priority", Priority: priority, Payload: json.RawMessage(`{"large_number":1e100000}`)})
			if err != nil {
				t.Fatal(err)
			}
			ids[priority] = job.ID
		}
		for _, priority := range []domain.JobPriority{domain.PriorityCritical, domain.PriorityDefault, domain.PriorityLow} {
			if n, err := f.sched.DispatchReady(f.ctx, 1); n != 1 || err != nil {
				t.Fatalf("dispatch=%d err=%v", n, err)
			}
			rows := unpublished(t, f)
			if row := rows[len(rows)-1]; row.JobID != ids[priority] || len(row.Payload) > 512 {
				t.Fatalf("priority=%d row=%+v", priority, row)
			}
		}
	})
}
