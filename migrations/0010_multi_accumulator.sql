-- Migration: Support multiple accumulators per loop
-- The `accumulator` column previously stored a single JSON array.
-- We rename it to `accumulators` and change the semantics to store a JSON object
-- mapping accumulator names to their arrays, e.g.:
--   {"results": [...], "metrics": [...]}

ALTER TABLE loop_iteration_state RENAME COLUMN accumulator TO accumulators;

-- Add a comment to clarify the new semantics
COMMENT ON COLUMN loop_iteration_state.accumulators IS 'Serialized JSON object mapping accumulator variable names to their arrays';
