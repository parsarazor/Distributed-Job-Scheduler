package domain

import (
	"testing"
	"time"
)

// Principle: benchmarks guard the hot path. Dispatch touches ValidateTransition
// and NextDelay per job; a 10x regression here is a production incident.
func BenchmarkValidateTransition(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = ValidateTransition(StatusPending, StatusRunning)
	}
}

func BenchmarkTransitionTo(b *testing.B) {
	for i := 0; i < b.N; i++ {
		j := &Job{Status: StatusPending}
		_ = j.TransitionTo(StatusRunning)
	}
}

func BenchmarkNextDelay(b *testing.B) {
	s := NewDefaultBackoffStrategy()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.NextDelay(3)
	}
}

func BenchmarkHashAPIKey(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = HashAPIKey("bench-key")
	}
}

var _ = time.Now
