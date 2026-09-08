package domain

import (
	"testing"
	"time"
)

// Principle: property test over the jitter formula.
// For ANY attempt, delay must satisfy 0 <= d <= min(max, base*2^(n-1)).
// Sampling many attempts x many draws covers the RNG surface that a single
// golden value would miss.
func TestBackoff_PropertyBoundsAcrossAttempts(t *testing.T) {
	t.Parallel()
	s := &ExponentialBackoffWithJitter{BaseDelay: 100 * time.Millisecond, MaxDelay: 2 * time.Second, Factor: 2.0}
	ceilings := map[int]time.Duration{
		1:  100 * time.Millisecond,
		2:  200 * time.Millisecond,
		5:  1600 * time.Millisecond,
		10: 2 * time.Second, // capped
		50: 2 * time.Second, // capped, no overflow
	}
	for attempt, ceiling := range ceilings {
		for i := 0; i < 100; i++ {
			if d := s.NextDelay(attempt); d < 0 || d > ceiling {
				t.Fatalf("attempt %d: delay %v outside [0, %v]", attempt, d, ceiling)
			}
		}
	}
}

// Principle: degenerate-input robustness. Non-positive attempts, factor <= 1,
// and huge attempts must never panic, go negative, or exceed the cap.
func TestBackoff_DegenerateInputs(t *testing.T) {
	t.Parallel()
	s := &ExponentialBackoffWithJitter{BaseDelay: 100 * time.Millisecond, MaxDelay: time.Second, Factor: 0.5}
	for _, attempt := range []int{-100, -1, 0, 1, 1 << 20} {
		d := s.NextDelay(attempt)
		if d < 0 || d > time.Second {
			t.Fatalf("attempt %d: delay %v outside [0, max]", attempt, d)
		}
	}
	if got := s.NextDelay(0); got != s.BaseDelay {
		t.Fatalf("attempt<=0 must return BaseDelay, got %v", got)
	}
}

// Principle: jitter actually jitters. Over many draws at a mid-range attempt
// we must observe variance (a constant output means thundering-herd
// protection silently died).
func TestBackoff_JitterProducesVariance(t *testing.T) {
	t.Parallel()
	s := &ExponentialBackoffWithJitter{BaseDelay: 500 * time.Millisecond, MaxDelay: 10 * time.Second, Factor: 2.0}
	seen := map[time.Duration]bool{}
	for i := 0; i < 50; i++ {
		seen[s.NextDelay(4)] = true // ceiling 4s: plenty of room to vary
	}
	if len(seen) < 5 {
		t.Fatalf("expected spread of jittered delays, got %d distinct values", len(seen))
	}
}

// Principle: fuzz the strategy. Invariant under adversarial inputs:
// delay in [0, MaxDelay], never negative, never panics.
func FuzzExponentialBackoffBounds(f *testing.F) {
	f.Add(100_000_000, 2_000_000_000, 2.0, 3)
	f.Add(1_000_000_000, 3_600_000_000_000, 2.0, 100)
	f.Add(1, 1, -5.0, -1)
	f.Fuzz(func(t *testing.T, baseNs, maxNs int, factor float64, attempt int) {
		if baseNs < 0 {
			baseNs = -baseNs
		}
		if maxNs <= 0 {
			maxNs = 1_000_000_000
		}
		if baseNs > maxNs {
			baseNs, maxNs = maxNs, baseNs
		}
		s := &ExponentialBackoffWithJitter{BaseDelay: time.Duration(baseNs), MaxDelay: time.Duration(maxNs), Factor: factor}
		d := s.NextDelay(attempt)
		if d < 0 || d > s.MaxDelay {
			t.Fatalf("delay %v outside [0, %v] (base=%v factor=%v attempt=%d)", d, s.MaxDelay, s.BaseDelay, factor, attempt)
		}
	})
}
