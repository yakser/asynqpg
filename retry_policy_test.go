package asynqpg

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDefaultRetryPolicy_NextRetry(t *testing.T) {
	t.Parallel()

	policy := &DefaultRetryPolicy{}

	tests := []struct {
		attempt int
		wantMin time.Duration
		wantMax time.Duration
	}{
		{1, 900 * time.Millisecond, 1100 * time.Millisecond},    // 1^4 = 1s ± 10%
		{2, 14400 * time.Millisecond, 17600 * time.Millisecond}, // 2^4 = 16s ± 10%
		{3, 72900 * time.Millisecond, 89100 * time.Millisecond}, // 3^4 = 81s ± 10%
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("attempt_%d", tt.attempt), func(t *testing.T) {
			t.Parallel()

			delay := policy.NextRetry(tt.attempt)
			assert.GreaterOrEqual(t, delay, tt.wantMin, "attempt %d", tt.attempt)
			assert.LessOrEqual(t, delay, tt.wantMax, "attempt %d", tt.attempt)
		})
	}
}

func TestDefaultRetryPolicy_MaxDelay(t *testing.T) {
	t.Parallel()

	policy := &DefaultRetryPolicy{
		MaxRetryDelay: 1 * time.Hour,
	}

	// Very high attempt should be capped at max
	delay := policy.NextRetry(100)
	assert.LessOrEqual(t, delay, 1*time.Hour)
}

func TestConstantRetryPolicy(t *testing.T) {
	t.Parallel()

	policy := &ConstantRetryPolicy{
		Delay: 5 * time.Second,
	}

	for i := 1; i <= 10; i++ {
		delay := policy.NextRetry(i)
		assert.Equal(t, 5*time.Second, delay, "attempt %d", i)
	}
}
