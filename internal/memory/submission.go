package memory

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
)

func (r *TenantRepo) Lock(ctx context.Context, id uuid.UUID) error {
	_, err := r.GetByID(ctx, id)
	return err
}

func (r *JobRepo) GetBySubmissionKey(ctx context.Context, tenant uuid.UUID, key string) (*domain.Job, error) {
	s := stateFromCtx(ctx, r.s)

	if err := readLockContext(ctx, &s.mu); err != nil {
		return nil, err
	}
	defer s.mu.RUnlock()
	for _, j := range s.jobs {
		if j.TenantID == tenant && j.SubmissionKey == key {
			return cloneJob(j), nil
		}
	}
	return nil, domain.ErrJobNotFound
}

func (r *JobRepo) CheckAdmission(ctx context.Context, tenant uuid.UUID, maxOutstanding, perMinute int) error {
	s := stateFromCtx(ctx, r.s)

	if err := readLockContext(ctx, &s.mu); err != nil {
		return err
	}
	defer s.mu.RUnlock()
	outstanding, recent := 0, 0
	cutoff := time.Now().Add(-time.Minute)
	for _, j := range s.jobs {
		if j.TenantID != tenant {
			continue
		}
		if !j.IsTerminal() {
			outstanding++
		}
		if j.CreatedAt.After(cutoff) {
			recent++
		}
	}
	if outstanding >= maxOutstanding || recent >= perMinute {
		return domain.ErrTenantLimit
	}
	return nil
}

func (r *JobRepo) Now(ctx context.Context) (time.Time, error) {
	return time.Now().UTC(), ctx.Err()
}

func (r *JobRepo) RequeueDead(ctx context.Context, id uuid.UUID) (*domain.Job, error) {
	s := stateFromCtx(ctx, r.s)

	if err := lockContext(ctx, &s.mu); err != nil {
		return nil, err
	}
	defer s.mu.Unlock()
	j := s.jobs[id]
	if j == nil {
		return nil, domain.ErrJobNotFound
	}
	if j.Status != domain.StatusDead {
		return nil, domain.ErrInvalidTransition
	}
	if j.PayloadExpired {
		return nil, domain.ErrPayloadExpired
	}
	j.Status = domain.StatusPending
	j.FencingToken++
	j.MaxAttempts = j.AttemptCount + 3
	j.NextRetryAt = nil
	j.DispatchedAt = nil
	j.LeaseOwner = nil
	j.LeaseExpiresAt = nil
	j.UpdatedAt = time.Now().UTC()
	for eid, e := range s.outbox {
		if e.JobID == id && e.DeadLettered {
			delete(s.outbox, eid)
		}
	}
	return cloneJob(j), nil
}
