-- Add columns to action_logs to make it self-contained for UI display.
-- This allows us to DELETE from action_queue immediately on completion
-- while still having all the info needed to display execution history.

ALTER TABLE action_logs ADD COLUMN IF NOT EXISTS module_name TEXT;
ALTER TABLE action_logs ADD COLUMN IF NOT EXISTS action_name TEXT;
ALTER TABLE action_logs ADD COLUMN IF NOT EXISTS node_id TEXT;
ALTER TABLE action_logs ADD COLUMN IF NOT EXISTS dispatch_payload BYTEA;
