-- LEGACY ONLY: retained for upgrade testing. Do not install on new deployments.
-- Optional: run in one transaction with the queue table's schema first in search_path.
-- Hints are committed atomically with task mutations; no task data is published.
CREATE OR REPLACE FUNCTION asynqpg_notify_ready_insert_v1() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    task_type text;
BEGIN
    FOR task_type IN SELECT DISTINCT type FROM new_tasks WHERE status = 'pending'
    LOOP
        PERFORM pg_catalog.pg_notify('asynqpg_ready_v1_' || TG_RELID::text, task_type);
    END LOOP;
    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION asynqpg_notify_ready_update_v1() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    task_type text;
BEGIN
    FOR task_type IN
        SELECT DISTINCT n.type FROM new_tasks n JOIN old_tasks o USING (id)
        WHERE n.status = 'pending'
          AND (n.status IS DISTINCT FROM o.status
            OR n.type IS DISTINCT FROM o.type
            OR n.blocked_till IS DISTINCT FROM o.blocked_till)
    LOOP
        PERFORM pg_catalog.pg_notify('asynqpg_ready_v1_' || TG_RELID::text, task_type);
    END LOOP;
    RETURN NULL;
END;
$$;

CREATE OR REPLACE TRIGGER asynqpg_ready_insert_v1
AFTER INSERT ON asynqpg_tasks
REFERENCING NEW TABLE AS new_tasks
FOR EACH STATEMENT EXECUTE FUNCTION asynqpg_notify_ready_insert_v1();

CREATE OR REPLACE TRIGGER asynqpg_ready_update_v1
AFTER UPDATE ON asynqpg_tasks
REFERENCING OLD TABLE AS old_tasks NEW TABLE AS new_tasks
FOR EACH STATEMENT EXECUTE FUNCTION asynqpg_notify_ready_update_v1();
