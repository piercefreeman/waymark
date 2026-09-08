-- The instance view, kept by the database as the events arrive: one row
-- per VM with what the observability state serves, so a read never
-- derives it from the events.
CREATE TABLE observability_vm_instances (
    vm_id uuid PRIMARY KEY,
    -- The VM's most recent event.
    last_at timestamptz NOT NULL,
    last_node_id uuid NOT NULL,
    last_node_sequence bigint NOT NULL,
    last_run_sequence bigint NOT NULL,
    last_kind text NOT NULL,
    -- The latest run: its start, and its stop once observed.
    run_node_id uuid,
    run_started_at timestamptz,
    run_start_node_sequence bigint,
    stopped_at timestamptz,
    stop_kind text,
    -- The workflow's terminal outcome, once an effect carried one.
    outcome_at timestamptz,
    outcome_kind text
);

-- The list: most recently active first, keyset-paged by (last_at, vm_id).
CREATE INDEX observability_vm_instances_last_at_idx ON observability_vm_instances (last_at, vm_id);

-- Absorb one batch of events into the instances. Every step is set-based
-- over the batch (the statement's transition table, read once per step)
-- and guarded by the order (at, node_id, node_sequence), so a batch that
-- arrives late never moves a newer value backwards.
CREATE FUNCTION observability_vm_instances_absorb() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    -- The last event: insert the VM, or advance it.
    WITH batch AS (
        SELECT
            ((payload->>'vm_id')::uuid) AS vm_id,
            node_id,
            node_sequence,
            at,
            kind,
            (payload->>'run_sequence')::bigint AS run_sequence
        FROM inserted
        WHERE payload ? 'vm_id'
          AND kind LIKE 'vm_driver.%'
    )
    INSERT INTO observability_vm_instances AS instance
        (vm_id, last_at, last_node_id, last_node_sequence, last_run_sequence, last_kind)
    SELECT DISTINCT ON (vm_id) vm_id, at, node_id, node_sequence, run_sequence, kind
    FROM batch
    ORDER BY vm_id, at DESC, node_id DESC, node_sequence DESC
    ON CONFLICT (vm_id) DO UPDATE SET
        last_at = excluded.last_at,
        last_node_id = excluded.last_node_id,
        last_node_sequence = excluded.last_node_sequence,
        last_run_sequence = excluded.last_run_sequence,
        last_kind = excluded.last_kind
    WHERE (excluded.last_at, excluded.last_node_id, excluded.last_node_sequence)
        > (instance.last_at, instance.last_node_id, instance.last_node_sequence);

    -- The latest run: a newer start replaces the run, and its stop with it.
    WITH batch AS (
        SELECT
            ((payload->>'vm_id')::uuid) AS vm_id,
            node_id,
            node_sequence,
            at,
            kind,
            (payload->>'run_sequence')::bigint AS run_sequence
        FROM inserted
        WHERE payload ? 'vm_id'
          AND kind LIKE 'vm_driver.%'
    )
    UPDATE observability_vm_instances AS instance
    SET run_node_id = start.node_id,
        run_started_at = start.at,
        run_start_node_sequence = start.node_sequence,
        stopped_at = NULL,
        stop_kind = NULL
    FROM (
        SELECT DISTINCT ON (vm_id) vm_id, at, node_id, node_sequence
        FROM batch
        WHERE kind = 'vm_driver.vm_started'
        ORDER BY vm_id, at DESC, node_id DESC, node_sequence DESC
    ) AS start
    WHERE instance.vm_id = start.vm_id
      AND (instance.run_started_at IS NULL
           OR (start.at, start.node_id, start.node_sequence)
              > (instance.run_started_at, instance.run_node_id, instance.run_start_node_sequence));

    -- The run's stop: the first stop after the run's start on the run's
    -- node, once, for a run not yet stopped.
    WITH batch AS (
        SELECT
            ((payload->>'vm_id')::uuid) AS vm_id,
            node_id,
            node_sequence,
            at,
            kind,
            (payload->>'run_sequence')::bigint AS run_sequence
        FROM inserted
        WHERE payload ? 'vm_id'
          AND kind LIKE 'vm_driver.%'
    )
    UPDATE observability_vm_instances AS instance
    SET stopped_at = first_stop.at,
        stop_kind = first_stop.kind
    FROM (
        SELECT DISTINCT ON (stop.vm_id) stop.vm_id, stop.at, stop.kind
        FROM batch AS stop
        JOIN observability_vm_instances AS current ON current.vm_id = stop.vm_id
        WHERE stop.kind LIKE 'vm_driver.vm_stopped.%'
          AND stop.node_id = current.run_node_id
          AND stop.node_sequence > current.run_start_node_sequence
          AND current.stopped_at IS NULL
        ORDER BY stop.vm_id, stop.node_sequence
    ) AS first_stop
    WHERE instance.vm_id = first_stop.vm_id;

    -- The outcome: the latest terminal effect.
    WITH batch AS (
        SELECT
            ((payload->>'vm_id')::uuid) AS vm_id,
            node_id,
            node_sequence,
            at,
            kind,
            (payload->>'run_sequence')::bigint AS run_sequence
        FROM inserted
        WHERE payload ? 'vm_id'
          AND kind LIKE 'vm_driver.%'
    )
    UPDATE observability_vm_instances AS instance
    SET outcome_at = outcome.at,
        outcome_kind = outcome.kind
    FROM (
        SELECT DISTINCT ON (vm_id) vm_id, at, node_id, node_sequence, kind
        FROM batch
        WHERE kind IN ('vm_driver.effect_emitted.complete', 'vm_driver.effect_emitted.unhandled_exception')
        ORDER BY vm_id, at DESC, node_id DESC, node_sequence DESC
    ) AS outcome
    WHERE instance.vm_id = outcome.vm_id
      AND (instance.outcome_at IS NULL OR outcome.at > instance.outcome_at);

    RETURN NULL;
END
$$;

CREATE TRIGGER observability_events_absorb_into_vm_instances
    AFTER INSERT ON observability_events
    REFERENCING NEW TABLE AS inserted
    FOR EACH STATEMENT
    EXECUTE FUNCTION observability_vm_instances_absorb();
