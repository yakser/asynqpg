// Package publisher builds atomic readiness publications without listener dependencies.
package publisher

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/lib/pq"

	"github.com/yakser/asynqpg"
)

const Interval = 10 * time.Millisecond
const MaxTypes = 4096

// Limiter is local to one repository. Reservations deliberately precede commit:
// rollback or a failed write can suppress a hint until fallback polling.
// When full, new types are suppressed until entries expire, never evicted early.
type Limiter struct {
	mu      sync.Mutex
	next    map[string]time.Time
	cleanup time.Time
}

func (l *Limiter) Reserve(types []string) []string { return l.reserve(types, time.Now()) }
func (l *Limiter) reserve(types []string, now time.Time) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.next == nil {
		l.next = make(map[string]time.Time)
	}
	if !now.Before(l.cleanup) {
		for typ, until := range l.next {
			if !now.Before(until) {
				delete(l.next, typ)
			}
		}
		l.cleanup = now.Add(Interval)
	}
	allowed := make([]string, 0, len(types))
	for _, typ := range types {
		until, exists := l.next[typ]
		if now.Before(until) || (!exists && len(l.next) >= MaxTypes) {
			continue
		}
		l.next[typ] = now.Add(Interval)
		allowed = append(allowed, typ)
	}
	return allowed
}

// ForIDs resolves candidate types on the supplied querier (including caller Tx).
// The mutation CTE still filters actual changed rows, so missing/no-op rows never publish.
func (l *Limiter) ForIDs(ctx context.Context, q asynqpg.Querier, ids []int64) ([]string, error) {
	var types []string
	if err := q.SelectContext(ctx, &types, `SELECT DISTINCT type FROM asynqpg_tasks WHERE id = ANY($1)`, pq.Array(ids)); err != nil {
		return nil, fmt.Errorf("resolve publication types: %w", err)
	}
	return l.Reserve(types), nil
}

// CTE consumes only actual mutation results and deduplicates per statement.
// The aggregate reference forces publication even when the final projection is empty.
func CTE(source, predicate string, parameter int) string {
	return fmt.Sprintf(`, readiness_hints AS MATERIALIZED (
 SELECT pg_catalog.pg_notify('asynqpg_ready_v1_' || 'asynqpg_tasks'::regclass::oid::text, type)
 FROM (SELECT DISTINCT type FROM %s WHERE %s AND type = ANY($%d::text[])) ready_types
 ) `, source, predicate, parameter)
}
