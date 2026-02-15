package asynqpg

import (
	"math"
	"math/rand/v2"
	"time"
)

// RetryPolicy determines when a failed task should be retried.
type RetryPolicy interface {
	// NextRetry returns the duration to wait before the next retry attempt.
	// attempt is the number of the upcoming attempt (1-indexed).
	NextRetry(attempt int) time.Duration
}

// DefaultRetryPolicy implements exponential backoff with jitter.
// Formula: attempt^4 seconds with ±10% jitter.
// Examples: 1s, 16s, 81s, 256s, 625s, ...
type DefaultRetryPolicy struct {
	// MaxRetryDelay caps the maximum delay between retries.
	// Default: 24 hours.
	MaxRetryDelay time.Duration
}

// NextRetry calculates the next retry delay using exponential backoff.
func (p *DefaultRetryPolicy) NextRetry(attempt int) time.Duration {
	maxDelay := p.MaxRetryDelay
	if maxDelay == 0 {
		maxDelay = 24 * time.Hour
	}

	// Base delay: attempt^4 seconds
	baseSeconds := math.Pow(float64(attempt), 4)

	// Cap at max delay
	if baseSeconds > maxDelay.Seconds() {
		return maxDelay
	}

	// Add jitter: ±10%
	jitter := baseSeconds * (rand.Float64()*0.2 - 0.1) //nolint:gosec // jitter does not require cryptographic randomness
	finalSeconds := baseSeconds + jitter

	// Ensure positive and within bounds
	if finalSeconds < 1 {
		finalSeconds = 1
	}
	if finalSeconds > maxDelay.Seconds() {
		finalSeconds = maxDelay.Seconds()
	}

	return time.Duration(finalSeconds * float64(time.Second))
}

// ConstantRetryPolicy always returns the same delay.
// Useful for testing or specific use cases.
type ConstantRetryPolicy struct {
	Delay time.Duration
}

// NextRetry returns the constant delay.
func (p *ConstantRetryPolicy) NextRetry(_ int) time.Duration {
	return p.Delay
}
