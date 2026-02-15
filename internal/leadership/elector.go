package leadership

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

const (
	defaultElectInterval = 5 * time.Second
	defaultTTL           = 15 * time.Second
	defaultName          = "default"
)

// ElectorConfig configures the leader elector.
type ElectorConfig struct {
	// ClientID is a unique identifier for this consumer instance.
	// If empty, a UUID will be generated.
	ClientID string

	// Name is the leadership group name.
	// Multiple groups can exist independently.
	// Default: "default".
	Name string

	// ElectInterval is how often to attempt election/reelection.
	// Default: 5 seconds.
	ElectInterval time.Duration

	// TTL is the leadership expiration time.
	// Should be greater than ElectInterval.
	// Default: 15 seconds.
	TTL time.Duration

	// Logger for the elector.
	Logger *slog.Logger
}

func (c *ElectorConfig) setDefaults() {
	if c.ClientID == "" {
		c.ClientID = uuid.New().String()
	}
	if c.Name == "" {
		c.Name = defaultName
	}
	if c.ElectInterval <= 0 {
		c.ElectInterval = defaultElectInterval
	}
	if c.TTL <= 0 {
		c.TTL = defaultTTL
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

// dbExecer is the minimal interface the elector needs from a database pool.
type dbExecer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// Elector implements distributed leader election using PostgreSQL.
// Only one consumer in the cluster will be the leader at any time.
type Elector struct {
	config *ElectorConfig
	db     dbExecer

	isLeader atomic.Bool

	mu          sync.Mutex
	started     bool
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	subscribers []chan<- bool
}

// NewElector creates a new Elector.
func NewElector(db dbExecer, config ElectorConfig) *Elector {
	config.setDefaults()
	return &Elector{
		config: &config,
		db:     db,
	}
}

// Start starts the election loop.
func (e *Elector) Start(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.started {
		return nil
	}

	ctx, e.cancel = context.WithCancel(ctx)
	e.started = true

	e.wg.Add(1)
	go e.electionLoop(ctx)

	e.config.Logger.Info("elector started",
		"client_id", e.config.ClientID,
		"name", e.config.Name,
		"elect_interval", e.config.ElectInterval,
		"ttl", e.config.TTL,
	)
	return nil
}

// Stop stops the elector and resigns leadership if held.
func (e *Elector) Stop() {
	e.mu.Lock()
	if !e.started {
		e.mu.Unlock()
		return
	}
	e.cancel()
	e.started = false
	e.mu.Unlock()

	e.wg.Wait()

	// Resign leadership on stop
	if e.isLeader.Load() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := e.resign(ctx); err != nil {
			e.config.Logger.Error("resign leadership on stop", "error", err)
		}
	}

	e.config.Logger.Info("elector stopped", "client_id", e.config.ClientID)
}

// IsLeader returns whether this instance is currently the leader.
func (e *Elector) IsLeader() bool {
	return e.isLeader.Load()
}

// Subscribe returns a channel that receives leadership change notifications.
// The channel receives true when leadership is gained, false when lost.
// The initial state is sent immediately upon subscription.
func (e *Elector) Subscribe() <-chan bool {
	ch := make(chan bool, 1)

	e.mu.Lock()
	e.subscribers = append(e.subscribers, ch)
	// Send initial state
	ch <- e.isLeader.Load()
	e.mu.Unlock()

	return ch
}

func (e *Elector) electionLoop(ctx context.Context) {
	defer e.wg.Done()

	ticker := time.NewTicker(e.config.ElectInterval)
	defer ticker.Stop()

	// Try to elect immediately on start
	e.tryElect(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.tryElect(ctx)
		}
	}
}

func (e *Elector) tryElect(ctx context.Context) {
	wasLeader := e.isLeader.Load()

	elected, err := e.attemptElect(ctx)
	if err != nil {
		e.config.Logger.Error("election attempt failed",
			"client_id", e.config.ClientID,
			"error", err,
		)
		// On error, assume we lost leadership for safety
		if wasLeader {
			e.setLeader(false)
		}
		return
	}

	if elected != wasLeader {
		e.setLeader(elected)
		if elected {
			e.config.Logger.Info("gained leadership", "client_id", e.config.ClientID)
		} else {
			e.config.Logger.Info("lost leadership", "client_id", e.config.ClientID)
		}
	}
}

func (e *Elector) setLeader(isLeader bool) {
	e.isLeader.Store(isLeader)

	e.mu.Lock()
	subscribers := e.subscribers
	e.mu.Unlock()

	for _, ch := range subscribers {
		select {
		case ch <- isLeader:
		default:
			// Channel full, skip (subscriber should be reading)
		}
	}
}

func (e *Elector) attemptElect(ctx context.Context) (bool, error) {
	now := time.Now()
	expiresAt := now.Add(e.config.TTL)

	// First, delete expired leaders
	_, err := e.db.ExecContext(ctx, `
		DELETE FROM asynqpg_leader
		WHERE name = $1 AND expires_at < $2
	`, e.config.Name, now)
	if err != nil {
		return false, fmt.Errorf("delete expired leader: %w", err)
	}

	// Try to become leader or refresh leadership
	result, err := e.db.ExecContext(ctx, `
		INSERT INTO asynqpg_leader (name, leader_id, elected_at, expires_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (name) DO UPDATE SET
			expires_at = EXCLUDED.expires_at
		WHERE asynqpg_leader.leader_id = EXCLUDED.leader_id
	`, e.config.Name, e.config.ClientID, now, expiresAt)
	if err != nil {
		return false, fmt.Errorf("attempt election: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("get rows affected: %w", err)
	}

	// If we inserted/updated a row, we're the leader
	return rowsAffected > 0, nil
}

func (e *Elector) resign(ctx context.Context) error {
	_, err := e.db.ExecContext(ctx, `
		DELETE FROM asynqpg_leader
		WHERE name = $1 AND leader_id = $2
	`, e.config.Name, e.config.ClientID)
	if err != nil {
		return fmt.Errorf("resign: %w", err)
	}

	e.setLeader(false)
	e.config.Logger.Info("resigned leadership", "client_id", e.config.ClientID)
	return nil
}
