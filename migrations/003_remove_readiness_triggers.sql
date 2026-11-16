-- Upgrade from the legacy optional notification triggers. Run in each queue schema.
-- Library mutations now publish atomically; direct external SQL uses fallback polling.
DROP TRIGGER IF EXISTS asynqpg_ready_insert_v1 ON asynqpg_tasks;
DROP TRIGGER IF EXISTS asynqpg_ready_update_v1 ON asynqpg_tasks;
DROP FUNCTION IF EXISTS asynqpg_notify_ready_insert_v1();
DROP FUNCTION IF EXISTS asynqpg_notify_ready_update_v1();
