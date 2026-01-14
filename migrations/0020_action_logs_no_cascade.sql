-- Remove CASCADE delete from action_logs FK so logs survive action_queue cleanup.
--
-- This enables immediate deletion of completed actions from action_queue
-- while preserving execution history in action_logs for the webapp.
-- Logs are cleaned up independently by GC based on instance retention.

-- Drop the existing FK constraint
ALTER TABLE action_logs
DROP CONSTRAINT action_logs_action_id_fkey;

-- Re-add without CASCADE (or remove entirely - we'll just drop it)
-- The action_id column remains for tracking, but no FK enforcement.
-- This is acceptable because:
-- 1. action_logs is append-only history
-- 2. GC cleans up logs when instances are deleted
-- 3. Orphaned logs (if any) don't cause issues
