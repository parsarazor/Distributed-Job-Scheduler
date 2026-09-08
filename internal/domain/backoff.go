package domain

import (
	"math"
	"math/rand"
	"time"
)

// BackoffStrategy defines the interface for calculating retry delays (Strategy Pattern).
type BackoffStrategy interface {
	// NextDelay calculates the duration to wait before attempt `attempt` (1-indexed).
	NextDelay(attempt int) time.Duration
}

// ExponentialBackoffWithJitter implements BackoffStrategy using Full Jitter.
// Reference: AWS Architecture Blog "Exponential Backoff And Jitter"
// Formula: Sleep = rand(0, min(maxBackoff, baseBackoff * factor^(attempt-1)))
type ExponentialBackoffWithJitter struct {
	BaseDelay time.Duration
	MaxDelay  time.Duration
	Factor    float64
}

// NewDefaultBackoffStrategy creates an exponential backoff strategy with standard distributed defaults:
// 1s base delay, 1 hour max delay, exponential factor of 2.0.
func NewDefaultBackoffStrategy() *ExponentialBackoffWithJitter {
	return &ExponentialBackoffWithJitter{
		BaseDelay: 1 * time.Second,
		MaxDelay:  1 * time.Hour,
		Factor:    2.0,
	}
}

// NextDelay calculates the jittered exponential delay for the given attempt.
func (e *ExponentialBackoffWithJitter) NextDelay(attempt int) time.Duration {
	if attempt <= 0 {
		return e.BaseDelay
	}

	factor := e.Factor
	if factor <= 1.0 {
		factor = 2.0
	}

	multiplier := math.Pow(factor, float64(attempt-1))
	temp := float64(e.BaseDelay) * multiplier
	if temp > float64(e.MaxDelay) {
		temp = float64(e.MaxDelay)
	}

	// Full Jitter: pick uniformly between 0 and temp to smooth thundering herds
	jittered := rand.Int63n(int64(temp) + 1)
	return time.Duration(jittered)
}

// ConstantBackoff provides a fixed delay strategy (useful in testing or predictable workflows).
type ConstantBackoff struct {
	Delay time.Duration
}

// NextDelay returns the constant delay.
func (c *ConstantBackoff) NextDelay(attempt int) time.Duration {
	return c.Delay
}
