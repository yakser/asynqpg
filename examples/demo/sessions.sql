CREATE TABLE IF NOT EXISTS asynqpg_demo_sessions (
    token           TEXT        PRIMARY KEY,
    user_id         TEXT        NOT NULL,
    user_provider   TEXT        NOT NULL,
    user_name       TEXT        NOT NULL,
    user_email      TEXT        NOT NULL,
    user_avatar_url TEXT        NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL,
    expires_at      TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS asynqpg_demo_sessions_expires_at_idx
    ON asynqpg_demo_sessions (expires_at);
