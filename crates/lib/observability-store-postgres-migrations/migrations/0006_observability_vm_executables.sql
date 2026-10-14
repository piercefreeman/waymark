-- The executables, kept by the database as the events arrive: one row per
-- executable the events have carried, so the instance view serves what an
-- instance runs without deriving it from the events. A run's start (the
-- VM driver's `vm_started`) carries its executable, and so does the
-- executable's registration (the bridge's `executable_registered`); the
-- row is the same whichever arrives first,
-- the executable's name and version being immutable for its id.
CREATE TABLE observability_vm_executables (
    id uuid PRIMARY KEY,
    name text NOT NULL,
    version text NOT NULL,
    -- The earliest and the latest event carrying the executable.
    first_at timestamptz NOT NULL,
    last_at timestamptz NOT NULL
);

-- The retention sweep: the executables no event has carried for a while.
CREATE INDEX observability_vm_executables_last_at_idx ON observability_vm_executables (last_at);

-- The latest run's executable, from the run's start.
ALTER TABLE observability_vm_instances ADD COLUMN executable_id uuid;

-- The retention sweep spares the executables an instance still refers to.
CREATE INDEX observability_vm_instances_executable_id_idx ON observability_vm_instances (executable_id);

-- The instances' absorb, as before but for the run step, which now
-- carries the run's executable.
CREATE OR REPLACE FUNCTION observability_vm_instances_absorb() RETURNS trigger
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
          AND starts_with(kind, 'vm_driver.')
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

    -- The latest run: a newer start replaces the run, and its stop with
    -- it — the first stop past the start among the events stored so far,
    -- which include this batch's own rows; none when it has not arrived.
    -- The start carries the run's executable.
    WITH batch AS (
        SELECT
            ((payload->>'vm_id')::uuid) AS vm_id,
            node_id,
            node_sequence,
            at,
            kind,
            (payload->>'run_sequence')::bigint AS run_sequence,
            ((payload->'observation'->>'executable_id')::uuid) AS executable_id
        FROM inserted
        WHERE payload ? 'vm_id'
          AND starts_with(kind, 'vm_driver.')
    )
    UPDATE observability_vm_instances AS instance
    SET run_node_id = start.node_id,
        run_started_at = start.at,
        run_start_node_sequence = start.node_sequence,
        executable_id = start.executable_id,
        stopped_at = stop.at,
        stop_kind = stop.kind,
        stop_node_sequence = stop.node_sequence
    FROM (
        SELECT DISTINCT ON (vm_id) vm_id, at, node_id, node_sequence, executable_id
        FROM batch
        WHERE kind = 'vm_driver.vm_started'
        ORDER BY vm_id, at DESC, node_id DESC, node_sequence DESC
    ) AS start
    LEFT JOIN LATERAL (
        SELECT event.at, event.kind, event.node_sequence
        FROM observability_events AS event
        -- `payload ? 'vm_id'` keeps every row the next line keeps: it is
        -- the stop index's own predicate, here so that the index applies.
        WHERE event.payload ? 'vm_id'
          AND ((event.payload->>'vm_id')::uuid) = start.vm_id
          AND event.node_id = start.node_id
          AND event.node_sequence > start.node_sequence
          AND starts_with(event.kind, 'vm_driver.vm_stopped.')
        ORDER BY event.node_sequence
        LIMIT 1
    ) AS stop ON true
    WHERE instance.vm_id = start.vm_id
      AND (instance.run_started_at IS NULL
           OR (start.at, start.node_id, start.node_sequence)
              > (instance.run_started_at, instance.run_node_id, instance.run_start_node_sequence));

    -- The run's stop: the batch's first stop past the run's start on the
    -- run's node, for a run not yet stopped — or stopped by a later stop,
    -- which was borrowed from a later run whose start has not arrived.
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
          AND starts_with(kind, 'vm_driver.')
    )
    UPDATE observability_vm_instances AS instance
    SET stopped_at = first_stop.at,
        stop_kind = first_stop.kind,
        stop_node_sequence = first_stop.node_sequence
    FROM (
        SELECT DISTINCT ON (stop.vm_id) stop.vm_id, stop.at, stop.kind, stop.node_sequence
        FROM batch AS stop
        JOIN observability_vm_instances AS current ON current.vm_id = stop.vm_id
        WHERE starts_with(stop.kind, 'vm_driver.vm_stopped.')
          AND stop.node_id = current.run_node_id
          AND stop.node_sequence > current.run_start_node_sequence
          AND (current.stop_node_sequence IS NULL
               OR stop.node_sequence < current.stop_node_sequence)
        ORDER BY stop.vm_id, stop.node_sequence
    ) AS first_stop
    WHERE instance.vm_id = first_stop.vm_id;

    -- The outcome: the latest terminal effect. It is the workflow's, not
    -- the run's: a newer start never clears it.
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
          AND starts_with(kind, 'vm_driver.')
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

-- Absorb one batch of events into the executables: every one the batch
-- carries, on a run's start or on a registration, each event read in its
-- source's own shape; insert it, or widen the span of events carrying it.
-- The rows are taken in id order, so that concurrent batches lock them
-- in one order. Its own trigger beside the instances': the two fire
-- within the same statement, so a batch lands in both tables or in
-- neither; neither reads the other's table, so their order is free.
CREATE FUNCTION observability_vm_executables_absorb() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    WITH batch AS (
        SELECT
            ((payload->'observation'->>'executable_id')::uuid) AS id,
            payload->'observation'->>'executable_name' AS name,
            payload->'observation'->>'executable_version' AS version,
            at
        FROM inserted
        WHERE kind = 'vm_driver.vm_started'
        UNION ALL
        SELECT
            ((payload->'observation'->>'executable_id')::uuid) AS id,
            payload->'observation'->>'executable_name' AS name,
            payload->'observation'->>'executable_version' AS version,
            at
        FROM inserted
        WHERE kind = 'bridge.executable_registered'
    )
    INSERT INTO observability_vm_executables AS executable
        (id, name, version, first_at, last_at)
    SELECT id, name, version, min(at), max(at)
    FROM batch
    GROUP BY id, name, version
    ORDER BY id
    ON CONFLICT (id) DO UPDATE SET
        first_at = least(executable.first_at, excluded.first_at),
        last_at = greatest(executable.last_at, excluded.last_at);

    RETURN NULL;
END
$$;

CREATE TRIGGER observability_events_absorb_into_vm_executables
    AFTER INSERT ON observability_events
    REFERENCING NEW TABLE AS inserted
    FOR EACH STATEMENT
    EXECUTE FUNCTION observability_vm_executables_absorb();
