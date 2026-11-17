-- Stop old workers before upgrading: their ID-only outcomes cannot be fenced.
ALTER TABLE asynqpg_tasks
    ADD COLUMN IF NOT EXISTS claim_generation BIGINT NOT NULL DEFAULT 0;
