package memory

import (
	"context"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/broker"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
)

func (r *JobRepo) GetDependency(ctx context.Context, tenantID, id uuid.UUID, _ bool) (*domain.Job, error) {
	s := stateFromCtx(ctx, r.s)
	if err := readLockContext(ctx, &s.mu); err != nil {
		return nil, err
	}
	defer s.mu.RUnlock()
	parent := s.jobs[id]
	if parent == nil || parent.TenantID != tenantID {
		return nil, domain.ErrDependencyNotMet
	}
	return projectJob(parent, true), nil
}

func (r *JobRepo) CountWaitingDependents(ctx context.Context, parentID uuid.UUID) (int, error) {
	s := stateFromCtx(ctx, r.s)
	if err := readLockContext(ctx, &s.mu); err != nil {
		return 0, err
	}
	defer s.mu.RUnlock()
	parent := s.jobs[parentID]
	if parent == nil {
		return 0, nil
	}
	n := 0
	for _, j := range s.jobs {
		if j.DependsOn != nil && *j.DependsOn == parentID && j.TenantID == parent.TenantID &&
			j.Status == domain.StatusPending && j.AttemptCount < j.MaxAttempts {
			n++
		}
	}
	return n, nil
}

func (r *JobRepo) CancelDependents(ctx context.Context, parentID uuid.UUID) ([]*domain.Job, error) {
	s := stateFromCtx(ctx, r.s)
	if err := lockContext(ctx, &s.mu); err != nil {
		return nil, err
	}
	defer s.mu.Unlock()
	parent := s.jobs[parentID]
	if parent == nil {
		return nil, domain.ErrJobNotFound
	}
	children := make(map[uuid.UUID][]*domain.Job)
	for _, j := range s.jobs {
		if j.DependsOn != nil && j.TenantID == parent.TenantID {
			children[*j.DependsOn] = append(children[*j.DependsOn], j)
		}
	}
	queue := []uuid.UUID{parentID}
	seen := map[uuid.UUID]bool{parentID: true}
	var cancelled []*domain.Job
	now := time.Now().UTC()
	for len(queue) != 0 {
		branch := children[queue[0]]
		queue = queue[1:]
		sort.Slice(branch, func(i, j int) bool { return uuidLess(branch[i].ID, branch[j].ID) })
		for _, child := range branch {
			if seen[child.ID] {
				continue
			}
			seen[child.ID] = true
			queue = append(queue, child.ID)
			if child.Status == domain.StatusPending || child.Status == domain.StatusFailed {
				cancelled = append(cancelled, &domain.Job{ID: child.ID, Status: child.Status})
				child.Status = domain.StatusCancelled
				child.NextRetryAt, child.DispatchedAt = nil, nil
				child.LeaseOwner, child.LeaseExpiresAt = nil, nil
				child.UpdatedAt = now
			}
		}
	}
	return cancelled, nil
}

func (r *JobRepo) RecoverStaleDispatches(ctx context.Context, limit int, age time.Duration) error {
	if limit <= 0 || age <= 0 {
		return domain.ErrInvalidInput
	}
	s := stateFromCtx(ctx, r.s)
	if err := lockContext(ctx, &s.mu); err != nil {
		return err
	}
	defer s.mu.Unlock()
	now := time.Now().UTC()
	cutoff := now.Add(-age)
	protected := make(map[uuid.UUID]bool)
	for _, e := range s.outbox {
		if j := s.jobs[e.JobID]; j != nil && j.IsTerminal() && e.PublishedAt != nil &&
			e.DeliveredAt == nil && !e.PublishedAt.After(now.Add(-broker.ExecutionMessageTTL)) {
			delivered := now
			e.DeliveredAt = &delivered
		}
		if !e.Published || (e.PublishedAt != nil && e.PublishedAt.After(cutoff)) {
			protected[e.JobID] = true
		}
	}
	var stale []*domain.Job
	for _, j := range s.jobs {
		if j.Status == domain.StatusPending && j.DispatchedAt != nil && !j.DispatchedAt.After(cutoff) && !protected[j.ID] {
			stale = append(stale, j)
		}
	}
	sort.Slice(stale, func(i, j int) bool {
		if !stale[i].DispatchedAt.Equal(*stale[j].DispatchedAt) {
			return stale[i].DispatchedAt.Before(*stale[j].DispatchedAt)
		}
		return uuidLess(stale[i].ID, stale[j].ID)
	})
	for _, j := range stale[:min(len(stale), limit)] {
		j.DispatchedAt = nil
		j.FencingToken++
		j.UpdatedAt = now
	}
	return nil
}
