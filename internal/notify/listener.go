package notify

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/yakser/asynqpg"
)

const operationTimeout = 5 * time.Second

type Config struct {
	Pool    asynqpg.NotificationPool
	Logger  *slog.Logger
	Metrics *asynqpg.Metrics
	// Callbacks must be nonblocking. OnType reports whether the type is registered.
	OnType  func(string) bool
	OnReady func()
}

// Listener owns exactly one connection at a time and never exposes connection errors containing a DSN.
type Listener struct {
	config             Config
	dial               func(context.Context) (connection, error)
	resolve            func(context.Context) (Target, error)
	mu                 sync.Mutex
	attempted, stopped bool
	cancel             context.CancelFunc
	done               chan struct{}
	channel            string // Owned by Start, then by the run goroutine.
}

func New(config Config) (*Listener, error) {
	if config.Pool == nil || config.OnType == nil || config.OnReady == nil {
		return nil, fmt.Errorf("notification pool and callbacks are required")
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	if config.Metrics == nil {
		var err error
		config.Metrics, err = asynqpg.NewMetrics(nil)
		if err != nil {
			return nil, fmt.Errorf("create notification metrics: %w", err)
		}
	}
	l := &Listener{config: config, done: make(chan struct{})}
	l.dial = config.Pool.OpenNotificationConnection
	l.resolve = func(ctx context.Context) (Target, error) { return ResolveTarget(ctx, config.Pool) }
	return l, nil
}
func (l *Listener) Start(ctx context.Context) error {
	l.mu.Lock()
	if l.attempted || l.stopped {
		l.mu.Unlock()
		return fmt.Errorf("notification listener already started or stopped")
	}
	l.attempted = true
	ctx, l.cancel = context.WithCancel(ctx)
	l.mu.Unlock()
	conn, err := l.connect(ctx)
	if err != nil {
		l.cancel()
		close(l.done)
		return fmt.Errorf("start notification listener: %w", err)
	}
	l.config.Metrics.NotificationConnections.Add(ctx, 1)
	l.config.OnReady()
	go l.run(ctx, conn)
	return nil
}
func (l *Listener) Stop() {
	l.mu.Lock()
	l.stopped = true
	if !l.attempted {
		l.mu.Unlock()
		return
	}
	l.cancel()
	l.mu.Unlock()
	<-l.done
}
func (l *Listener) connect(parent context.Context) (connection, error) {
	ctx, cancel := context.WithTimeout(parent, operationTimeout)
	defer cancel()
	target, err := l.resolve(ctx)
	if err != nil {
		if parent.Err() != nil {
			return nil, parent.Err()
		}
		return nil, err
	}
	conn, err := l.dial(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, operationError("acquire", err, ErrUnavailable)
	}
	if conn == nil {
		return nil, fmt.Errorf("acquire: %w", ErrUnavailable)
	}
	success := false
	defer func() {
		if !success {
			cleanup, cancel := context.WithTimeout(context.Background(), operationTimeout)
			defer cancel()
			_ = conn.Close(cleanup)
		}
	}()
	if err := verifyRoute(ctx, l.config.Pool, conn); err != nil {
		if parent.Err() != nil {
			return nil, parent.Err()
		}
		return nil, err
	}
	if err := conn.Listen(ctx, target.Channel); err != nil {
		if parent.Err() != nil {
			return nil, parent.Err()
		}
		return nil, operationError("subscribe", err, ErrUnavailable)
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	success = true
	l.channel = target.Channel
	return conn, nil
}
func (l *Listener) closeConnection(conn connection) {
	ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
	defer cancel()
	_ = conn.Close(ctx)
	l.config.Metrics.NotificationConnections.Add(ctx, -1)
}
func (l *Listener) read(ctx context.Context, conn connection) error {
	for {
		waitCtx, cancel := context.WithTimeout(ctx, operationTimeout)
		n, err := conn.WaitForNotification(waitCtx)
		cancel()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, context.DeadlineExceeded) {
			pingCtx, pingCancel := context.WithTimeout(ctx, operationTimeout)
			err = conn.Ping(pingCtx)
			pingCancel()
			if err != nil {
				return operationError("ping", err, ErrUnavailable)
			}
			continue
		}
		if err != nil {
			return operationError("receive", err, ErrUnavailable)
		}
		l.config.Metrics.NotificationsReceived.Add(ctx, 1)
		if n.Channel != l.channel || !l.config.OnType(n.Payload) {
			l.config.Metrics.NotificationsIgnored.Add(ctx, 1)
		}
	}
}
func (l *Listener) run(ctx context.Context, conn connection) {
	defer close(l.done)
	for {
		l.config.Logger.DebugContext(ctx, "notification listener connected")
		readErr := l.read(ctx, conn)
		l.closeConnection(conn)
		if ctx.Err() != nil {
			return
		}
		l.config.Logger.WarnContext(ctx, "notification listener disconnected; polling continues", "error", readErr)
		for attempt := 0; ; attempt++ {
			if !waitBackoff(ctx, reconnectDelay(attempt)) {
				return
			}
			var err error
			conn, err = l.connect(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				if attempt == 0 || attempt%12 == 0 {
					l.config.Logger.WarnContext(ctx, "notification reconnect failed; polling continues", "error", err, "attempt", attempt+1)
				}
				continue
			}
			l.config.Metrics.NotificationConnections.Add(ctx, 1)
			l.config.Metrics.NotificationReconnects.Add(ctx, 1)
			l.config.OnReady()
			break
		}
	}
}
func reconnectDelay(attempt int) time.Duration {
	base := min(100*time.Millisecond*time.Duration(1<<min(max(attempt, 0), 6)), 5*time.Second)
	return base + time.Duration(rand.Int64N(int64(base/5))) //nolint:gosec // Reconnect jitter is not security-sensitive.
}
func waitBackoff(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return ctx.Err() == nil
	}
}
