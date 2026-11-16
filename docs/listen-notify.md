# Consumer readiness notifications

PostgreSQL rows and SQL claims remain authoritative. Optional notifications carry only
raw task types on `asynqpg_ready_v1_<table OID>` channels; they are transient hints,
not ownership or a durable event stream.

## Enable listening

No trigger installation or second DSN is required. Wrap the caller-owned native pool:

Use `github.com/jackc/pgx/v5/pgxpool`,
`github.com/yakser/asynqpg/adapter/pgxadapter` and
`github.com/yakser/asynqpg/consumer` in the application's startup code:

```go
native, err := pgxpool.New(ctx, databaseURL)
if err != nil { return err }
defer native.Close()
db, err := pgxadapter.New(native)
if err != nil { return err }
defer db.Close()
c, err := consumer.New(consumer.Config{
    Pool: db,
    FetchInterval: time.Second,
    FetchCooldown: 10 * time.Millisecond,
    Notifications: &consumer.NotificationConfig{},
})
if err != nil { return err }
// Register handlers, call c.Start(), and shut down c before closing db/native.
```

Register handlers, Start, then stop the consumer before closing the adapter and native
pool. `NotificationConfig` is an empty enable marker; nil disables listening only.
`consumer.Config.FetchCooldown` applies to both modes. Its default is 10ms, capped at
the effective FetchInterval for shorter polling intervals. Explicit values must be
at least 1ms and no greater than FetchInterval. FetchInterval still defaults to 100ms.
Both modes use the same capacity-aware runner: startup reconciles, full batches keep
fetching, and worker release resumes pending work. Notifications add a wake source.
Fetch errors wait for the fallback interval even if hints arrive continuously.

Pool, Querier and Tx retain their SQL/sqlx interfaces. Producers do not need a
listener or a pgx pool. Query-only pools can publish and use polling consumers;
enabling listening requires NotificationPool or returns ErrNotificationsUnsupported.
Use adapter.BeginTxx for transactions shared with business writes; native pgx.Tx does
not implement asynqpg.Tx. The SQL bridge uses the same native pool and database.
Pass the transaction returned by `db.BeginTxx(ctx, nil)` to `EnqueueTx` or
`EnqueueManyTx`; commit or roll it back together with the application's business writes.
For a complete setup example, see [the consumer examples](../consumer/example_test.go).

Each enabled consumer owns one detached PostgreSQL session beyond reusable MaxConns.
Use a direct or session-pooled endpoint for LISTEN, not transaction pooling. The
adapter closes its SQL bridge, never the native pool. Caller BeforeClose hooks run
on detached sessions and must not block indefinitely.

## Atomic publication and throttling

Standard producer, worker and client repositories publish by default. Enqueue and
batch enqueue, manual retry (including UI bulk retry), and worker retry/snooze publish
in the same SQL data-modifying CTE as the mutation. Tx APIs execute that CTE on the
provided transaction: commit releases hints, rollback releases none. There are no
postcommit background sends or separate pool queries inside Tx APIs. Worker/manual
retry resolves candidate types on the same querier before the atomic mutation; only
actual changed rows can publish. Batch types are deduplicated. Duplicate idempotent
inserts, unchanged retries, claims and terminal outcomes do not publish.

Each repository has a concurrent per-type 10ms publication limiter, independent of
consumer listening configuration. Polling-only consumers do not disable hints for
other listeners. Reservations can precede commit (and even fail or roll back), so a
rollback can suppress a later hint until the next fallback poll. Suppression never
suppresses the task mutation. State is bounded to 4096 types with expiry cleanup;
while full, new types are suppressed until expiry, rather than evicting active limits.
No durable/global throttle table exists. Independent instances can each publish.
When no enqueue hint is allowed, the operation uses plain INSERT SQL, skipping the
publication CTE. When publication is allowed, its failure rolls back the same statement.

Direct external SQL does **not** automatically publish. Fallback polling discovers
those writes, delayed/retry/snoozed work, expired leases, lost hints and suppressed
publications. A longer FetchInterval increases their discovery latency. Time passing
does not generate a notification. Notification queue errors fail the enclosing
statement/transaction; applications must handle these as ordinary write failures.

## When notifications help

Use notifications for prompt pickup after an idle period, sparse arrivals, or bursts.
With a continuously nonempty queue, both modes already fetch again as workers become
free; enabling notifications does not by itself raise the processing ceiling. At the
same fallback interval, an idle notification consumer still performs polling and pings.

Tune `FetchInterval` for the acceptable fallback delay and `FetchCooldown` for query
frequency under demand. Longer polling intervals trade lower idle query load for slower
discovery of time-based work and missed hints. Processing is at-least-once; neither
mode guarantees exactly-once side effects or a fixed delivery latency.

## Upgrade from optional triggers

Existing deployments that installed the old optional migration must remove its
triggers before relying on the library limiter, otherwise old triggers still publish
extra hints and incur transition-table costs. Run in each queue table's schema:

```sh
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -1 -f migrations/003_remove_readiness_triggers.sql
# Local demo:
make remove-legacy-notifications
```

Use the queue schema first in search_path. Removal is idempotent, preserves task rows,
and needs no new installation. The old optional up script is retained only as a legacy
upgrade fixture; do not install it on new deployments. Deploying old trigger-dependent
binaries after removal leaves their fallback polling operational.

## Hooks, diagnostics and recovery

The pgx adapter rejects a statically configured ConnConfig.OnNotification. Ordinary
BeforeConnect hooks (for example credential refresh) remain supported. If a hook installs
an incompatible notification callback on an actual connection, the startup/reconnect
probe reports ErrNotificationsUnsupported at the receive stage; it is not misreported
as a routing mismatch. Caller hooks are never overwritten.

Startup verifies the task table and performs a random-channel round trip through the
query pool. After subscribing to the task channel it reconciles ready rows. Reconnects
repeat that sequence; failed runtime connections leave safety polling active. The
probe proves current delivery, not all future proxy routes. Provider errors include
safe operation stages (acquire, subscribe, probe receive, ping) without raw connection
strings. Consumer shutdown can cancel startup and applies its deadline while waiting
for cleanup; user-provided hooks must themselves return promptly.

When upgrading ownership protection as well, stop old consumers and maintenance
processes, apply `002_claim_generation.sql`, remove old triggers with
`003_remove_readiness_triggers.sql`, and restart upgraded binaries. Mixed old/new
workers are not safe: old ID-only outcome writes cannot be fenced by the new code.
Generation checks protect database state; handlers still need idempotent side effects.
