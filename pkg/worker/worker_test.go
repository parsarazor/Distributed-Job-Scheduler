package worker

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/parsarazor/distributed-job-scheduler/internal/domain"
)

func TestPermanent(t *testing.T) {
	if Permanent(nil) != nil {
		t.Fatal("Permanent(nil) must be nil")
	}
	cause := errors.New("bad card")
	err := fmt.Errorf("charge: %w", Permanent(cause))
	if !IsPermanent(err) || !errors.Is(err, cause) || !errors.Is(err, domain.ErrPermanentFailure) {
		t.Fatalf("wrapping lost the permanent marker: %v", err)
	}
	if errors.Unwrap(Permanent(cause)) != cause || Permanent(cause).Error() != "bad card" {
		t.Fatal("Permanent must unwrap to and print its cause")
	}
	if IsPermanent(cause) || IsPermanent(nil) {
		t.Fatal("plain errors are retryable")
	}
}

func TestCancellationCausesWrapContextCanceled(t *testing.T) {
	if !errors.Is(ErrCanceled, context.Canceled) || !errors.Is(ErrShutdown, context.Canceled) || errors.Is(ErrLeaseLost, context.Canceled) {
		t.Fatal("unexpected cause chains")
	}
}

func mustPanic(t *testing.T, name string, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatalf("%s did not panic", name)
		}
	}()
	fn()
}

func TestHandle_RejectsBadRegistrations(t *testing.T) {
	w, err := New(validConfig())
	if err != nil {
		t.Fatal(err)
	}
	ok := func(context.Context, *Job) error { return nil }
	w.HandleFunc("email", ok)
	mustPanic(t, "empty name", func() { w.HandleFunc("", ok) })
	mustPanic(t, "nil Handler", func() { w.Handle("a", nil) })
	mustPanic(t, "nil HandlerFunc", func() { w.Handle("b", HandlerFunc(nil)) })
	mustPanic(t, "nil func", func() { w.HandleFunc("c", nil) })
	mustPanic(t, "duplicate", func() { w.HandleFunc("email", ok) })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := w.Run(ctx); err == nil {
		t.Fatal("Run with a cancelled context must fail startup")
	}
	mustPanic(t, "after Run", func() { w.HandleFunc("late", ok) })
	if err := w.Run(context.Background()); err == nil {
		t.Fatal("second Run must fail")
	}
}

func TestJobConversion(t *testing.T) {
	id, tenant := uuid.New(), uuid.New()
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	src := &domain.Job{ID: id, TenantID: tenant, Name: "invoice", Payload: []byte(`{"order":7}`), AttemptCount: 2, MaxAttempts: 5, ScheduledAt: at}
	j := toJob(src)
	if j.ID != id.String() || j.TenantID != tenant.String() || j.Name != "invoice" || j.Attempt != 3 || j.MaxAttempts != 5 || !j.ScheduledAt.Equal(at) {
		t.Fatalf("unexpected job: %+v", j)
	}
	j.Payload[0] = 'x'
	if src.Payload[0] != '{' {
		t.Fatal("Job.Payload must be a copy")
	}
	var v struct{ Order int }
	if err := toJob(src).Decode(&v); err != nil || v.Order != 7 {
		t.Fatalf("decode: %v %+v", err, v)
	}
	if err := (&Job{}).Decode(&v); err == nil {
		t.Fatal("empty payload must fail to decode")
	}
}

// The SDK must never give a worker process a database driver or the
// scheduler's storage code.
func TestDependencyBoundary(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps", ".").CombinedOutput()
	if err != nil {
		t.Fatalf("go list: %v\n%s", err, out)
	}
	for _, dep := range strings.Fields(string(out)) {
		for _, banned := range []string{"/internal/postgres", "/internal/memory", "/internal/app", "github.com/jackc/pgx"} {
			if strings.Contains(dep, banned) {
				t.Errorf("pkg/worker depends on %s", dep)
			}
		}
	}
}
