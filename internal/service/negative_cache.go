package service

import (
	"sync"
	"time"
)

// negativeKeyCache is a strictly bounded, short-TTL cache of API key digests
// that were just rejected. It keeps a flood of bad credentials from converting
// 1:1 into database load: repeated invalid keys are answered from memory until
// the TTL expires, after which the next attempt goes back to the database (so a
// key created between attempts is honored within the TTL window, never rejected
// stale for longer).
type negativeKeyCache struct {
	mu       sync.Mutex
	entries  map[string]time.Time
	capacity int
	ttl      time.Duration
}

func newNegativeKeyCache(capacity int, ttl time.Duration) *negativeKeyCache {
	return &negativeKeyCache{
		entries:  make(map[string]time.Time, capacity),
		capacity: capacity,
		ttl:      ttl,
	}
}

// recent reports whether the digest was rejected within the TTL.
func (c *negativeKeyCache) recent(digest string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	at, ok := c.entries[digest]
	if !ok {
		return false
	}
	if time.Since(at) > c.ttl {
		delete(c.entries, digest)
		return false
	}
	return true
}

// remember records a rejection, evicting the oldest entry when full.
func (c *negativeKeyCache) remember(digest string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.entries[digest]; !exists && len(c.entries) >= c.capacity {
		var oldestKey string
		var oldestAt time.Time
		first := true
		for k, at := range c.entries {
			if first || at.Before(oldestAt) {
				oldestKey, oldestAt, first = k, at, false
			}
		}
		delete(c.entries, oldestKey)
	}
	c.entries[digest] = time.Now()
}
