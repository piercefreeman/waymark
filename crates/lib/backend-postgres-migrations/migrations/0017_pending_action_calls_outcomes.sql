-- Record action call outcomes onto the pending-call records, so a
-- completion survives the executing process: a revived VM settles from
-- the recorded outcome instead of re-executing the action.

ALTER TABLE pending_action_calls
    ADD COLUMN result BYTEA,
    ADD COLUMN error BYTEA;

-- A call has at most one outcome: a result or an error, never both.
-- Both absent means the call is still executing.
ALTER TABLE pending_action_calls
    ADD CONSTRAINT pending_action_calls_outcome_exclusive CHECK (
        result IS NULL OR error IS NULL
    );
