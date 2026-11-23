package memory

import (
	"context"
	"encoding/json"
	"time"

	"github.com/prsamohamadi/distributed-job-scheduler/internal/repository"
)

func (t *Transactor) Ping(ctx context.Context) error { return ctx.Err() }

func (t *Transactor) Stats(ctx context.Context) (repository.Stats, error) {
	s := repository.Stats{Jobs: make(map[string]int64)}
	if err := lockContext(ctx, &t.s.mu); err != nil {
		return s, err
	}
	defer t.s.mu.Unlock()
	for _, j := range t.s.jobs {
		s.Jobs[string(j.Status)]++
	}
	for _, e := range t.s.outbox {
		if !e.Published {
			s.OutboxPending++
			s.OutboxLag = max(s.OutboxLag, time.Since(e.CreatedAt))
		}
	}
	return s, nil
}

func (t *Transactor) Prune(ctx context.Context, outboxAge, auditAge time.Duration) error {
	if err := lockContext(ctx, &t.s.mu); err != nil {
		return err
	}
	defer t.s.mu.Unlock()
	for id, e := range t.s.outbox {
		if e.PublishedAt != nil && time.Since(*e.PublishedAt) > outboxAge {
			delete(t.s.outbox, id)
		}
	}
	for id, events := range t.s.events {
		if j := t.s.jobs[id]; j != nil && j.IsTerminal() {
			kept := events[:0]
			for _, e := range events {
				if time.Since(e.OccurredAt) <= auditAge {
					kept = append(kept, e)
				}
			}
			t.s.events[id] = kept
		}
	}
	pruned := 0
	for _, j := range t.s.jobs {
		if pruned >= 1000 {
			break
		}
		if j.IsTerminal() && !j.PayloadExpired && time.Since(j.UpdatedAt) > auditAge {
			j.Payload = json.RawMessage(`{}`)
			j.PayloadExpired = true
			j.TraceParent, j.TraceState = "", ""
			pruned++
		}
	}
	return nil
}
