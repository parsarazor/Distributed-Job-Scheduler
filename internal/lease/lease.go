package lease

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// LeaseManager enforces single-worker execution (ADR 002). Acquire returns a
// new fencing token; Renew extends ownership; Release frees the lease.
type LeaseManager interface {
	Acquire(ctx context.Context, jobID uuid.UUID, workerID string, ttl time.Duration) (int64, error)
	Renew(ctx context.Context, jobID uuid.UUID, workerID string, fencingToken int64, ttl time.Duration) error
	Release(ctx context.Context, jobID uuid.UUID, workerID string) error
}

type entry struct {
	workerID string
	token    int64
	expires  time.Time
}

// MemoryLeaseManager is the default for local runs.
type MemoryLeaseManager struct {
	mu     sync.Mutex
	leases map[uuid.UUID]*entry
	tokens map[uuid.UUID]int64
}

// NewMemoryLeaseManager creates an empty lease manager.
func NewMemoryLeaseManager() *MemoryLeaseManager {
	return &MemoryLeaseManager{leases: make(map[uuid.UUID]*entry), tokens: make(map[uuid.UUID]int64)}
}

func (m *MemoryLeaseManager) Acquire(_ context.Context, jobID uuid.UUID, workerID string, ttl time.Duration) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok := m.leases[jobID]; ok && time.Now().Before(e.expires) {
		// A second acquire from the same worker is also contention: allowing it
		// would run the same job twice in one process and invalidate its token.
		return 0, ErrLeaseHeld
	}
	m.tokens[jobID]++
	tok := m.tokens[jobID]
	m.leases[jobID] = &entry{workerID: workerID, token: tok, expires: time.Now().Add(ttl)}
	return tok, nil
}

func (m *MemoryLeaseManager) Renew(_ context.Context, jobID uuid.UUID, workerID string, fencingToken int64, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.leases[jobID]
	if !ok || e.workerID != workerID || e.token != fencingToken {
		return ErrLeaseNotHeld
	}
	e.expires = time.Now().Add(ttl)
	return nil
}

func (m *MemoryLeaseManager) Release(_ context.Context, jobID uuid.UUID, workerID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok := m.leases[jobID]; ok && e.workerID == workerID {
		delete(m.leases, jobID)
	}
	return nil
}

// RedisLeaseManager implements leases via SET NX EX + Lua-guarded renew.
type RedisLeaseManager struct {
	rdb *redis.Client
}

// NewRedisLeaseManager dials Redis.
func NewRedisLeaseManager(addr string) *RedisLeaseManager {
	return &RedisLeaseManager{rdb: redis.NewClient(&redis.Options{Addr: addr})}
}

func leaseKey(jobID uuid.UUID) string { return "lock:job:" + jobID.String() }

func (r *RedisLeaseManager) Acquire(ctx context.Context, jobID uuid.UUID, workerID string, ttl time.Duration) (int64, error) {
	ok, err := r.rdb.SetNX(ctx, leaseKey(jobID), workerID, ttl).Result()
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, ErrLeaseHeld
	}
	// Monotonic fencing token per job (ADR 002/009).
	tok, err := r.rdb.Incr(ctx, "fencing:job:"+jobID.String()).Result()
	if err != nil {
		return 0, err
	}
	return tok, nil
}

var renewScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], ARGV[2])
else
  return 0
end`)

func (r *RedisLeaseManager) Renew(ctx context.Context, jobID uuid.UUID, workerID string, _ int64, ttl time.Duration) error {
	n, err := renewScript.Run(ctx, r.rdb, []string{leaseKey(jobID)}, workerID, int64(ttl/time.Millisecond)).Int()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrLeaseNotHeld
	}
	return nil
}

func (r *RedisLeaseManager) Release(ctx context.Context, jobID uuid.UUID, workerID string) error {
	script := redis.NewScript(`if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("DEL", KEYS[1]) else return 0 end`)
	return script.Run(ctx, r.rdb, []string{leaseKey(jobID)}, workerID).Err()
}
