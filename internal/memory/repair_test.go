package memory

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
)

func TestTerminalPayloadRetentionPreservesDeduplicationMetadata(t *testing.T) {
	b := NewBundle()
	job := testJob(uuid.New(), "retained", domain.PriorityDefault, time.Now().Add(-time.Hour))
	job.Status = domain.StatusDead
	job.Payload = json.RawMessage(`{"secret":"old-input"}`)
	job.TraceParent = "old-trace"
	job.SubmissionKey, job.SubmissionHash = "keep-key", "keep-hash"
	job.UpdatedAt = time.Now().Add(-time.Hour)
	if err := b.Jobs.Create(t.Context(), job); err != nil {
		t.Fatal(err)
	}
	if err := b.Tx.Prune(t.Context(), time.Minute, time.Minute); err != nil {
		t.Fatal(err)
	}
	got, err := b.Jobs.GetByID(t.Context(), job.ID)
	if err != nil || !got.PayloadExpired || string(got.Payload) != "{}" || got.TraceParent != "" || got.SubmissionKey != "keep-key" || got.SubmissionHash != "keep-hash" {
		t.Fatalf("retention=%+v err=%v", got, err)
	}
	if _, err := b.Jobs.RequeueDead(t.Context(), job.ID); !errors.Is(err, domain.ErrPayloadExpired) {
		t.Fatalf("expired input requeued: %v", err)
	}
}

func TestRepositoryReadDeadlineWhileTransactionHoldsLock(t *testing.T) {
	b := NewBundle()
	job := testJob(uuid.New(), "locked", domain.PriorityDefault, time.Now())
	if err := b.Jobs.Create(t.Context(), job); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- b.Tx.WithinTransaction(t.Context(), func(context.Context) error { close(entered); <-release; return nil })
	}()
	<-entered
	defer func() {
		close(release)
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	read := make(chan error, 1)
	go func() { _, err := b.Jobs.GetByID(ctx, job.ID); read <- err }()
	select {
	case err := <-read:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("read=%v", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("repository read ignored deadline behind transaction")
	}
}
