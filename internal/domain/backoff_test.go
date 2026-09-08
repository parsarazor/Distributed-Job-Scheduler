package domain

import (
	"testing"
	"time"
)

func TestExponentialBackoffWithJitter_Bounds(t *testing.T) {
	strategy := &ExponentialBackoffWithJitter{
		BaseDelay: 100 * time.Millisecond,
		MaxDelay:  2 * time.Second,
		Factor:    2.0,
	}

	// For attempt 1, max ceiling is 100ms * 2^0 = 100ms
	for i := 0; i < 50; i++ {
		delay := strategy.NextDelay(1)
		if delay < 0 || delay > 100*time.Millisecond {
			t.Fatalf("attempt 1 delay out of range [0, 100ms]: got %v", delay)
		}
	}

	// For attempt 5, max ceiling is 100ms * 2^4 = 1600ms <= MaxDelay (2000ms)
	for i := 0; i < 50; i++ {
		delay := strategy.NextDelay(5)
		if delay < 0 || delay > 1600*time.Millisecond {
			t.Fatalf("attempt 5 delay out of range [0, 1600ms]: got %v", delay)
		}
	}

	// For attempt 10, max ceiling is capped at MaxDelay (2000ms)
	for i := 0; i < 50; i++ {
		delay := strategy.NextDelay(10)
		if delay < 0 || delay > 2*time.Second {
			t.Fatalf("attempt 10 delay exceeded MaxDelay [0, 2s]: got %v", delay)
		}
	}
}

func TestConstantBackoff(t *testing.T) {
	expected := 5 * time.Second
	strategy := &ConstantBackoff{Delay: expected}

	for attempt := 1; attempt <= 5; attempt++ {
		if delay := strategy.NextDelay(attempt); delay != expected {
			t.Fatalf("expected delay %v, got %v for attempt %d", expected, delay, attempt)
		}
	}
}
