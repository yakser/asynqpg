CREATE TYPE asynqpg_task_status AS ENUM (
    'pending', 'running', 'completed', 'failed', 'cancelled'
);

CREATE TABLE IF NOT EXISTS asynqpg_tasks (
    id BIGSERIAL PRIMARY KEY,

    attempts_left SMALLINT NOT NULL,
    attempts_elapsed SMALLINT DEFAULT 0 NOT NULL,

    blocked_till TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finalized_at TIMESTAMPTZ,
    attempted_at TIMESTAMPTZ,

    type TEXT NOT NULL,
    idempotency_token TEXT,
    status asynqpg_task_status NOT NULL DEFAULT 'pending',
    payload BYTEA NOT NULL,
    messages TEXT[] DEFAULT ARRAY[]::TEXT[] NOT NULL,

    CONSTRAINT finalized_or_finalized_at_null CHECK (
        (finalized_at IS NULL AND status NOT IN ('completed', 'failed', 'cancelled'))
        OR (finalized_at IS NOT NULL AND status IN ('completed', 'failed', 'cancelled'))
    ),
    CONSTRAINT type_length CHECK (char_length(type) > 0 AND char_length(type) < 256),
    CONSTRAINT attempts_left_non_negative CHECK (attempts_left >= 0)
) WITH (fillfactor = 90);

CREATE UNIQUE INDEX IF NOT EXISTS asynqpg_tasks_idempotency_idx
    ON asynqpg_tasks (type, idempotency_token)
    WHERE idempotency_token IS NOT NULL;

CREATE INDEX IF NOT EXISTS asynqpg_tasks_ready_idx
    ON asynqpg_tasks (type, blocked_till)
    WHERE status IN ('pending', 'running');

CREATE INDEX IF NOT EXISTS asynqpg_tasks_failed_idx
    ON asynqpg_tasks (type)
    WHERE status = 'failed';

CREATE INDEX IF NOT EXISTS asynqpg_tasks_finalized_idx
    ON asynqpg_tasks (status, finalized_at)
    WHERE status IN ('completed', 'failed', 'cancelled');

CREATE INDEX IF NOT EXISTS asynqpg_tasks_stuck_idx
    ON asynqpg_tasks (attempted_at, id)
    WHERE status = 'running';

CREATE UNLOGGED TABLE IF NOT EXISTS asynqpg_leader (
    name TEXT PRIMARY KEY DEFAULT 'default',
    leader_id TEXT NOT NULL,
    elected_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT leader_id_length CHECK (
        char_length(leader_id) > 0 AND char_length(leader_id) < 128
    )
);
