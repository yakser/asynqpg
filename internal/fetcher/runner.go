// Package fetcher combines readiness hints, worker capacity, and safety polling.
package fetcher

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"
)

// Reason identifies the signal that requested a fetch, with bounded metric values.
type Reason string

const (
	ReasonStartup   Reason = "startup"
	ReasonNotify    Reason = "notify"
	ReasonReconnect Reason = "reconnect"
	ReasonPoll      Reason = "poll"
	ReasonCapacity  Reason = "capacity"
)

// Result reports the capacity observed before a claim and the number claimed.
// Limit zero means no database query was made.
type Result struct{ Fetched, Limit int }

type Config struct {
	PollInterval, Cooldown time.Duration
	Fetch                  func(context.Context, Reason) (Result, error)
	CapacityAvailable      <-chan struct{}
	Logger                 *slog.Logger
}

// Runner serializes fetches and coalesces hints without losing pending demand.
type Runner struct {
	config           Config
	wake             chan Reason
	done             chan struct{}
	mu               sync.Mutex
	started, stopped bool
	cancel           context.CancelFunc
}

func New(config Config) (*Runner, error) {
	if config.PollInterval <= 0 || config.Cooldown <= 0 || config.Cooldown > config.PollInterval {
		return nil, fmt.Errorf("fetch intervals must be positive and cooldown must not exceed poll interval")
	}
	if config.Fetch == nil {
		return nil, fmt.Errorf("fetch callback is required")
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	return &Runner{config: config, wake: make(chan Reason, 1), done: make(chan struct{})}, nil
}
func (r *Runner) Start(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started || r.stopped {
		return fmt.Errorf("fetcher already started or stopped")
	}
	ctx, r.cancel = context.WithCancel(ctx)
	r.started = true
	// Initial reconciliation covers signals delivered before Start.
	select {
	case <-r.wake:
	default:
	}
	go r.run(ctx)
	return nil
}
func (r *Runner) Wake(reason Reason) {
	select {
	case r.wake <- reason:
	default:
	}
}
func (r *Runner) Stop() {
	r.mu.Lock()
	r.stopped = true
	if !r.started {
		r.mu.Unlock()
		return
	}
	r.cancel()
	r.mu.Unlock()
	<-r.done
}
func (r *Runner) pollDelay() time.Duration {
	jitter := min(r.config.PollInterval/10, 100*time.Millisecond)
	if jitter <= 0 {
		return r.config.PollInterval
	}
	return r.config.PollInterval + time.Duration(rand.Int64N(int64(jitter))) //nolint:gosec // Scheduling jitter is not security-sensitive.
}
func (r *Runner) run(ctx context.Context) {
	defer close(r.done)
	poll := time.NewTimer(r.pollDelay())
	defer poll.Stop()
	attempt := time.NewTimer(time.Hour)
	attempt.Stop()
	defer attempt.Stop()
	pending, waitingCapacity := true, false
	reason := ReasonStartup
	var nextAllowed time.Time
	for {
		if ctx.Err() != nil {
			return
		}
		if pending && !waitingCapacity && !time.Now().Before(nextAllowed) {
			attemptStarted := time.Now()
			result, err := r.config.Fetch(ctx, reason)
			if ctx.Err() != nil {
				return
			}
			if err != nil {
				r.config.Logger.ErrorContext(ctx, "fetch failed", "error", err)
				nextAllowed = time.Now().Add(r.config.PollInterval)
				pending = true
			} else if result.Limit == 0 {
				waitingCapacity = true
			} else {
				nextAllowed = attemptStarted.Add(r.config.Cooldown)
				pending = result.Fetched >= result.Limit
			}
		}
		attempt.Stop()
		var ready <-chan time.Time
		if pending && !waitingCapacity {
			attempt.Reset(max(time.Until(nextAllowed), 0))
			ready = attempt.C
		}
		select {
		case <-ctx.Done():
			return
		case reason = <-r.wake:
			pending = true
		case <-r.config.CapacityAvailable:
			if pending {
				waitingCapacity = false
				reason = ReasonCapacity
			}
		case <-poll.C:
			pending = true
			waitingCapacity = false
			reason = ReasonPoll
			poll.Reset(r.pollDelay())
		case <-ready:
		}
	}
}
