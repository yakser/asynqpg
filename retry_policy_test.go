package asynqpg

import (
	"testing"
	"time"
)

func TestDefaultRetryPolicy_NextRetry(t *testing.T) {
	policy := &DefaultRetryPolicy{}

	tests := []struct {
		attempt     int
		minExpected time.Duration
		maxExpected time.Duration
	}{
		{1, 900 * time.Millisecond, 1100 * time.Millisecond},    // 1^4 = 1s ± 10%
		{2, 14400 * time.Millisecond, 17600 * time.Millisecond}, // 2^4 = 16s ± 10%
		{3, 72900 * time.Millisecond, 89100 * time.Millisecond}, // 3^4 = 81s ± 10%
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			delay := policy.NextRetry(tt.attempt)
			if delay < tt.minExpected || delay > tt.maxExpected {
				t.Errorf("attempt %d: got %v, want between %v and %v",
					tt.attempt, delay, tt.minExpected, tt.maxExpected)
			}
		})
	}
}

func TestDefaultRetryPolicy_MaxDelay(t *testing.T) {
	policy := &DefaultRetryPolicy{
		MaxRetryDelay: 1 * time.Hour,
	}

	// Very high attempt should be capped at max
	delay := policy.NextRetry(100)
	if delay > 1*time.Hour {
		t.Errorf("expected delay <= 1h, got %v", delay)
	}
}

func TestConstantRetryPolicy(t *testing.T) {
	policy := &ConstantRetryPolicy{
		Delay: 5 * time.Second,
	}

	for i := 1; i <= 10; i++ {
		delay := policy.NextRetry(i)
		if delay != 5*time.Second {
			t.Errorf("attempt %d: got %v, want 5s", i, delay)
		}
	}
}
