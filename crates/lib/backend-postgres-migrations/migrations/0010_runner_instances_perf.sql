-- Fix gradual slowdown in update:runner_instances_state.
--
-- Root causes:
--
-- 1. GC seq-scan: The garbage-collection query filters runner_instances by
--    created_at with no index.  As completed rows accumulate the scan grows
--    linearly, slowing GC and allowing the table to bloat, which in turn
--    makes every subsequent write (and TOAST churn) more expensive.
--
-- 2. TOAST dead-tuple accumulation: runner_instances.state is a large BYTEA
--    updated on every save_graphs_once call.  PostgreSQL's default autovacuum
--    scale factor (20 %) lets dead TOAST chunks pile up before clean-up,
--    increasing write amplification over time.
--
-- 3. Missing lock_uuid index: update:runner_instances_state JOINs
--    queued_instances filtering by lock_uuid with no supporting index.

-- (1) Partial index covering only GC candidates (completed/failed instances).
--     Matches the WHERE clause in select:runner_instances_gc_candidates
--     exactly, so the planner can use an index scan instead of a seq-scan.
CREATE INDEX IF NOT EXISTS idx_runner_instances_gc_candidates
    ON runner_instances(created_at)
    WHERE result IS NOT NULL OR error IS NOT NULL;

-- (2) Tune autovacuum for runner_instances: vacuum when 1 % of rows are dead
--     (vs the default 20 %) and reduce the per-vacuum cost delay so dead
--     TOAST chunks from frequent state updates are reclaimed promptly.
ALTER TABLE runner_instances SET (
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_vacuum_cost_delay   = 2
);

-- (3) Partial index on queued_instances(lock_uuid) for locked rows only.
--     Used by the JOIN in update:runner_instances_state to verify lock
--     ownership without scanning unrelated (unlocked) rows.
CREATE INDEX IF NOT EXISTS idx_queued_instances_lock_uuid
    ON queued_instances(lock_uuid)
    WHERE lock_uuid IS NOT NULL;
