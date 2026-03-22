# asynqpg

Distributed task queue for Go, backed by PostgreSQL.

[![Tests](https://github.com/yakser/asynqpg/actions/workflows/tests.yml/badge.svg)](https://github.com/yakser/asynqpg/actions/workflows/tests.yml)
![Coverage](https://img.shields.io/badge/coverage-74.7%25-yellowgreen)
[![Go Reference](https://pkg.go.dev/badge/github.com/yakser/asynqpg.svg)](https://pkg.go.dev/github.com/yakser/asynqpg)

## Contents

- [Features](#features)
- [Installation](#installation)
- [Database Setup](#database-setup)
- [Quick Start](#quick-start)
- [Architecture](#architecture)
- [Producer](#producer)
- [Consumer](#consumer)
- [Client](#client)
- [Web UI](#web-ui)
- [Observability](#observability)
- [Demo](#demo)
- [Performance](#performance)
- [Testing](#testing)
- [Contributing](#contributing)
- [Support](#support)
- [Project Status](#project-status)
- [License](#license)

## Features

- **PostgreSQL-native** – no Redis or external broker required
- **Safe concurrent processing** – multiple consumers with no duplicate work
- **Flexible retries** – exponential or constant backoff, skip retry, reschedule for later
- **Delayed & scheduled tasks** – run tasks at a future time
- **Batch & transactional enqueue** – bulk insert with deduplication, enqueue within your DB transaction
- **Per-type worker pools** – independent concurrency and timeout settings per task type
- **Automatic maintenance** – leader election, stuck task rescue, old task cleanup
- **Web dashboard** – Web UI with task inspection, filtering, retry/cancel/delete, and auth (Basic/OAuth)
- **OpenTelemetry** – built-in metrics and distributed tracing

## Installation
```bash
go get github.com/yakser/asynqpg
```

Requires Go 1.25+ and PostgreSQL 14+.

## Database Setup

Apply migrations to create the `asynqpg_tasks` table:

```bash
make up         # start PostgreSQL in Docker
make migrate    # apply migrations
```

Or use `testcontainers-go` in tests – no manual setup needed.

## Quick Start

### Producer

```go
package main

import (
    "context"
    "encoding/json"
    "log"
    "time"

    "github.com/jmoiron/sqlx"
    _ "github.com/lib/pq"
    "github.com/yakser/asynqpg"
    "github.com/yakser/asynqpg/producer"
)

func main() {
    db, err := sqlx.Connect("postgres", "postgres://postgres:password@localhost:5432/asynqpg?sslmode=disable")
    if err != nil {
        log.Fatal(err)
    }

    p, err := producer.New(producer.Config{Pool: db})
    if err != nil {
        log.Fatal(err)
    }

    payload, _ := json.Marshal(map[string]string{"to": "user@example.com", "subject": "Hello"})

    _, err = p.Enqueue(context.Background(), asynqpg.NewTask("email:send", payload,
        asynqpg.WithMaxRetry(5),
        asynqpg.WithDelay(10*time.Second),
    ))
    if err != nil {
        log.Fatal(err)
    }
}
```

### Consumer

```go
package main

import (
    "context"
    "fmt"
    "log"
    "os"
    "os/signal"
    "syscall"
    "time"

    "github.com/jmoiron/sqlx"
    _ "github.com/lib/pq"
    "github.com/yakser/asynqpg"
    "github.com/yakser/asynqpg/consumer"
)

func main() {
    db, err := sqlx.Connect("postgres", "postgres://postgres:password@localhost:5432/asynqpg?sslmode=disable")
    if err != nil {
        log.Fatal(err)
    }

    c, err := consumer.New(consumer.Config{Pool: db})
    if err != nil {
        log.Fatal(err)
    }

    if err := c.RegisterTaskHandler("email:send",
        consumer.TaskHandlerFunc(func(ctx context.Context, task *asynqpg.TaskInfo) error {
            fmt.Printf("Processing task %d: %s\n", task.ID, task.Type)
            // process task...
            return nil
        }),
        consumer.WithWorkersCount(5),
        consumer.WithTimeout(30*time.Second),
    ); err != nil {
        log.Fatal(err)
    }

    if err := c.Start(); err != nil {
        log.Fatal(err)
    }

    sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
    <-sigCh

    if err := c.Stop(); err != nil {
        log.Printf("shutdown error: %v", err)
    }
}
```

## Architecture

### Task Lifecycle

```mermaid
flowchart LR
    %% Styles
    classDef pending   fill:#f59e0b,stroke:#92400e,color:#000
    classDef running   fill:#3b82f6,stroke:#1e40af,color:#fff
    classDef completed fill:#22c55e,stroke:#166534,color:#000
    classDef failed    fill:#ef4444,stroke:#991b1b,color:#fff
    classDef cancelled fill:#6b7280,stroke:#374151,color:#fff
    classDef edge      fill:#f3f4f6,stroke:#d1d5db,color:#374151

    %% States
    P["Pending"]:::pending
    T["Running"]:::running

    subgraph Final
        C["Completed"]:::completed
        F["Failed"]:::failed
        Ca["Cancelled"]:::cancelled
    end

    %% Entry points
    Enqueue(["Enqueue"]):::edge --> P
    EnqueueDelayed(["Enqueue (with delay)"]):::edge --> P

    %% Normal processing
    P -- "fetched by consumer" --> T
    T -- "handler success" --> C
    T -- "handler error, attempts_left > 0" --> P
    T -- "handler error, attempts_left = 0" --> F

    %% Rescuer (stuck tasks)
    T -- "stuck, attempts_left > 0 (Rescuer)" --> P
    T -- "stuck, attempts_left = 0 (Rescuer)" --> F

    %% Manual actions via client API
    F  -- "manual retry" --> P
    Ca -- "manual retry" --> P
    P  -- "manual cancel" --> Ca
    F  -- "manual cancel" --> Ca

    %% Cleaner (maintenance, leader-only)
    C  -. "auto-delete (Cleaner)" .-> Deleted(["Deleted"]):::edge
    F  -. "auto-delete (Cleaner)" .-> Deleted
    Ca -. "auto-delete (Cleaner)" .-> Deleted

    %% Manual delete via client API
    P  -- "manual delete" --> Deleted
    C  -- "manual delete" --> Deleted
    F  -- "manual delete" --> Deleted
    Ca -- "manual delete" --> Deleted
```

Tasks are fetched using `SELECT ... FOR NO KEY UPDATE SKIP LOCKED`, enabling safe concurrent processing across multiple consumers. The `blocked_till` column serves as both a delay mechanism (delayed enqueue, retry backoff) and a distributed lock expiry – a task re-appears for fetching only after `blocked_till` passes.

### Packages

| Package | Purpose |
|---|---|
| `producer/` | Enqueue tasks: `Enqueue`, `EnqueueTx`, `EnqueueMany`, `EnqueueManyTx` |
| `consumer/` | Fetch and process tasks with configurable worker pools |
| `client/` | Task inspection and management (get, list, cancel, retry, delete) |
| `ui/` | HTTP handler serving REST API + embedded React SPA |

## Producer

Create a producer with `producer.New`:

```go
p, err := producer.New(producer.Config{
    Pool:            db,              // required: *sqlx.DB
    Logger:          slog.Default(),  // optional
    DefaultMaxRetry: 3,               // optional, default: 3
    MeterProvider:   mp,              // optional: OTel metrics
    TracerProvider:  tp,              // optional: OTel traces
})
```

### Enqueue Methods

```go
// Single task
id, err := p.Enqueue(ctx, task)

// Within an existing transaction (atomic with your business logic)
id, err = p.EnqueueTx(ctx, tx, task)

//fixme auto-chunks
// Batch enqueue (auto-chunks at 5000, skips duplicates by idempotency token)
ids, err := p.EnqueueMany(ctx, tasks)

// Batch within a transaction
ids, err := p.EnqueueManyTx(ctx, tx, tasks)
```

### Task Options

```go
asynqpg.NewTask("type", payload,
    asynqpg.WithMaxRetry(5),                       // max retry attempts
    asynqpg.WithDelay(10*time.Second),              // delay before first processing
    asynqpg.WithIdempotencyToken("unique-token"),   // deduplicate enqueues
)

// Schedule a task for a specific time
task := asynqpg.NewTask("report:generate", payload)
task.ProcessAt = time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)
```

## Consumer

Create a consumer with `consumer.New`:

```go
c, err := consumer.New(consumer.Config{
    Pool:              db,               // required
    ClientID:          "worker-1",       // optional, for leader election
    RetryPolicy:       retryPolicy,      // optional, default: exponential backoff
    FetchInterval:     100*time.Millisecond,
    ShutdownTimeout:   30*time.Second,
    // Retention for completed/failed/cancelled tasks
    CompletedRetention: 24*time.Hour,
    FailedRetention:    7*24*time.Hour,
    CancelledRetention: 24*time.Hour,

    // DisableMaintenance: true,    // set to disable rescuer + cleaner
    // DisableBatchCompleter: true, // set to disable batch completions
})
```

### Handler Registration

```go
err := c.RegisterTaskHandler("email:send", handler,
    consumer.WithWorkersCount(10),          // goroutines for this task type
    consumer.WithMaxAttempts(5),            // override default
    consumer.WithTimeout(30*time.Second),   // per-task execution timeout
)
```

Implement `consumer.TaskHandler` or use the `TaskHandlerFunc` adapter:

```go
type TaskHandler interface {
    Handle(ctx context.Context, task *asynqpg.TaskInfo) error
}
```

### Retry Policies

**Exponential backoff** (default) – `attempt^4` seconds with 10% jitter, capped at 24h:

```go
&asynqpg.DefaultRetryPolicy{MaxRetryDelay: 24 * time.Hour}
```

**Constant delay:**

```go
&asynqpg.ConstantRetryPolicy{Delay: 5 * time.Second}
```

### SkipRetry

If a handler encounters a permanent error that should not be retried (e.g., invalid payload, business logic rejection), it can return `asynqpg.ErrSkipRetry` to immediately fail the task, skipping all remaining retry attempts:

```go
func (h *EmailHandler) Handle(ctx context.Context, task *asynqpg.TaskInfo) error {
    var payload EmailPayload
    if err := json.Unmarshal(task.Payload, &payload); err != nil {
        // Invalid payload – retrying won't help
        return fmt.Errorf("bad payload: %w", asynqpg.ErrSkipRetry)
    }
    // process task...
    return nil
}
```

`SkipRetry` works with `errors.Is`, so it can be wrapped with additional context via `fmt.Errorf("...: %w", asynqpg.ErrSkipRetry)`.

### Snooze

Sometimes a handler needs to defer processing without counting it as a failure. `TaskSnooze` reschedules the task after a given duration **without** counting it as an attempt – `attempts_left` and `attempts_elapsed` remain unchanged:

```go
func (h *MyHandler) Handle(ctx context.Context, task *asynqpg.TaskInfo) error {
    if !isExternalServiceReady() {
        // Try again in 30 seconds, doesn't count as a failed attempt
        return asynqpg.TaskSnooze(30 * time.Second)
    }
    // process task...
    return nil
}
```

If you want to reschedule **and** count it as a failed attempt (with error message stored), use `TaskSnoozeWithError`:

```go
func (h *MyHandler) Handle(ctx context.Context, task *asynqpg.TaskInfo) error {
    if err := callExternalAPI(); err != nil {
        // Retry in 1 minute, counts as an attempt, error message is stored
        return fmt.Errorf("api unavailable: %w", asynqpg.TaskSnoozeWithError(1 * time.Minute))
    }
    // process task...
    return nil
}
```

Key differences:

| | `TaskSnooze` | `TaskSnoozeWithError` |
|---|---|---|
| Counts as attempt | No | Yes |
| Stores error message | No | Yes |
| Respects max retries | No (unlimited snoozes) | Yes (fails when exhausted) |
| Use case | External dependency not ready | Transient error with custom delay |

Both work with `errors.As` and can be wrapped with `fmt.Errorf`. Panics on negative duration; zero duration makes the task immediately available.

### Task vs TaskInfo

The library uses two distinct structs to separate concerns:

- **`Task`** (root package) -- the input struct for enqueueing. Contains only fields you set when creating a task: `Type`, `Payload`, `Delay`, `MaxRetry`, `IdempotencyToken`.
- **`TaskInfo`** (root package) -- the runtime struct passed to handlers. Contains all database-assigned fields needed during processing:

```go
func (h *MyHandler) Handle(ctx context.Context, task *asynqpg.TaskInfo) error {
    task.ID               // database ID
    task.Type             // task type
    task.Payload          // task payload
    task.AttemptsLeft     // remaining retry attempts
    task.AttemptsElapsed  // number of attempts already made
    task.CreatedAt        // when the task was first enqueued
    task.Messages         // error messages from previous failed attempts
    task.AttemptedAt      // when the current processing attempt started
    // ...
}
```

The `client` package has its own `client.TaskInfo` -- a full database read model returned by `client.GetTask` / `client.ListTasks` for inspection and management. It includes additional fields like `Status`, `BlockedTill`, `UpdatedAt`, and `FinalizedAt`.

### Context Utilities

Task metadata is also available via context helpers, useful in middleware and utilities:

```go
func (h *MyHandler) Handle(ctx context.Context, task *asynqpg.TaskInfo) error {
    id, _        := asynqpg.GetTaskID(ctx)         // database ID
    retry, _     := asynqpg.GetRetryCount(ctx)     // attempts already elapsed
    max, _       := asynqpg.GetMaxRetry(ctx)       // total max retry count
    createdAt, _ := asynqpg.GetCreatedAt(ctx)       // task creation time

    // Or get all metadata at once:
    meta, ok := asynqpg.GetTaskMetadata(ctx)
    // meta.ID, meta.RetryCount, meta.MaxRetry, meta.CreatedAt
    // ...
}
```

For testing handlers, use `asynqpg.WithTaskMetadata` to create a context with metadata:

```go
ctx := asynqpg.WithTaskMetadata(context.Background(), asynqpg.TaskMetadata{
    ID: 42, RetryCount: 0, MaxRetry: 3, CreatedAt: time.Now(),
})
err := handler.Handle(ctx, task)
```

### Middleware

The consumer supports composable middleware for cross-cutting concerns. Middleware wraps task handlers using the `func(TaskHandler) TaskHandler` pattern, similar to `net/http` middleware.

**Global middleware** applies to all task types:

```go
c, _ := consumer.New(config)

_ = c.Use(func(next consumer.TaskHandler) consumer.TaskHandler {
    return consumer.TaskHandlerFunc(func(ctx context.Context, task *asynqpg.TaskInfo) error {
        slog.Info("processing task", "type", task.Type, "id", task.ID)
        err := next.Handle(ctx, task)
        slog.Info("task done", "type", task.Type, "id", task.ID, "error", err)
        return err
    })
})
```

**Per-task-type middleware** applies only to a specific handler:

```go
c.RegisterTaskHandler("email:send", emailHandler,
    consumer.WithMiddleware(rateLimitMiddleware),
    consumer.WithWorkersCount(5),
)
```

Execution order: global middleware (outermost, first registered runs first) wraps per-task middleware, which wraps the handler. A middleware can short-circuit by returning an error without calling `next.Handle`.

### Lifecycle

```go
c.Start()               // start processing
c.Stop()                 // graceful shutdown (uses configured ShutdownTimeout)
c.Shutdown(timeout)      // graceful shutdown with custom timeout
```

## Client

Inspect and manage tasks:

```go
cl, err := client.New(client.Config{Pool: db})

// Get a single task
info, err := cl.GetTask(ctx, taskID)

// List tasks with filtering
result, err := cl.ListTasks(ctx, client.NewListParams().
    States(asynqpg.TaskStatusFailed, asynqpg.TaskStatusPending).
    Types("email:send").
    Limit(50).
    OrderBy(client.OrderByCreatedAt, client.SortDesc),
)
// result.Tasks, result.Total

// Manage tasks
_, err = cl.CancelTask(ctx, id)   // pending/failed → cancelled
_, err = cl.RetryTask(ctx, id)    // failed/cancelled → pending
_, err = cl.DeleteTask(ctx, id)   // remove from database
```

All methods have `*Tx` variants for transactional use.

## Web UI

Mount the dashboard as an HTTP handler:

```go
handler, err := ui.NewHandler(ui.HandlerOpts{
    Pool:   db,
    Prefix: "/asynqpg",
})
http.Handle("/asynqpg/", handler)
```

![Dashboard](docs/images/main-dash.png)

![Tasks list](docs/images/tasks-list.png)

![Task detail](docs/images/task-detail.png)

![Dead Letter Queue](docs/images/dlq.png)

### Authentication

**Basic Auth:**

```go
ui.HandlerOpts{
    BasicAuth: &ui.BasicAuth{Username: "admin", Password: "secret"},
}
```

**OAuth providers** (e.g., GitHub):

```go
ui.HandlerOpts{
    AuthProviders: []ui.AuthProvider{githubProvider},
    SecureCookies: true,    // for HTTPS
    SessionMaxAge: 24*time.Hour,
}
```

Implement the `ui.AuthProvider` interface to add any OAuth/SSO provider. See [`examples/demo/github_provider.go`](https://github.com/yakser/asynqpg/blob/master/examples/demo/github_provider.go) for a complete GitHub OAuth implementation.

![GitHub OAuth login](docs/images/github-oauth.png)

![User profile](docs/images/profile.png)

## Observability

All public components accept optional `MeterProvider` and `TracerProvider`. When nil, the global OpenTelemetry provider is used.

### Metrics

| Metric | Type | Description |
|---|---|---|
| `asynqpg.tasks.enqueued` | Counter | Tasks enqueued |
| `asynqpg.tasks.processed` | Counter | Tasks finished processing |
| `asynqpg.tasks.errors` | Counter | Processing or enqueue errors |
| `asynqpg.task.duration` | Histogram | Handler execution duration (seconds) |
| `asynqpg.task.enqueue_duration` | Histogram | Enqueue latency (seconds) |
| `asynqpg.tasks.in_flight` | UpDownCounter | Currently executing tasks |

All metrics are tagged with `task_type` and `status` attributes.

### Tracing

Spans are created for all enqueue and processing operations with proper `SpanKind` (Producer/Consumer/Client).

### Grafana Dashboard

A pre-built Grafana dashboard is included at [`deploy/grafana/dashboards/asynqpg-overview.json`](deploy/grafana/dashboards/asynqpg-overview.json). It is provisioned automatically when you run `make demo-up`.

The dashboard refreshes every 10 seconds and includes:

- **Stat panels** – total tasks enqueued, processed, errors, and currently in-flight
- **Enqueue Rate** – tasks enqueued per second over time
- **Processing Rate by Status** – throughput broken down by `completed` / `failed` / `cancelled`
- **Task Duration Quantiles** – p50/p95/p99 handler execution latency
- **Enqueue Duration Quantiles** – p50/p95/p99 enqueue latency
- **Error Rate by Type** – error count per task type over time
- **Error Ratio** – fraction of processed tasks that errored
- **In-Flight Over Time** – concurrently executing tasks over time

All panels support a **Task Type** variable filter to drill down into a specific task type.

### Observability Stack

The demo includes a full observability stack (Jaeger, Prometheus, Grafana, OTel Collector) in `deploy/`:

```bash
make demo-up   # start PostgreSQL + observability stack
```

- **Jaeger** – `http://localhost:16686`
- **Prometheus** – `http://localhost:9090`
- **Grafana** – `http://localhost:3000`

![Grafana dashboard](docs/images/grafana.png)

![Jaeger traces](docs/images/jaeger-ui-all-traces.png)

![Jaeger error trace](docs/images/jaeger-ui-error-trace.png)

## Demo

Run the full demo with producers, consumers, web UI, and observability:

```bash
make demo       # start everything
make demo-down  # stop all services
```

## Performance

The library includes Go benchmarks covering SQL-level operations, batch completion, producer throughput, and consumer processing.

```bash
make bench   # Run benchmarks (uses testcontainers, requires Docker)
```

## Testing

```bash
make test              # unit tests (go test -race -count=1 ./...)
make test-integration  # integration tests (uses testcontainers, no manual DB needed)
make test-all          # both
make bench             # benchmarks (integration, requires Docker)
make lint              # golangci-lint
```

## Contributing

Contributions are welcome, including bug reports, feature requests,
documentation improvements, and code changes. See [`CONTRIBUTING.md`](CONTRIBUTING.md)
for local setup, contribution workflow, testing expectations, and pull request
guidelines.

## Support

- **Bug reports & feature requests**: [GitHub Issues](https://github.com/yakser/asynqpg/issues)

## TODO

- [ ] Move `testutils` to a separate module to avoid pulling `testcontainers-go` into consumer projects

## Project Status

Under active development. The API may change before v1.0. Bug reports and
feature requests are welcome.

## License

[MIT](LICENSE)
