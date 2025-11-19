ALTER TABLE daemon_action_ledger
    ADD COLUMN timeout_retry_limit INTEGER NOT NULL DEFAULT 2147483647,
    ADD COLUMN retry_kind TEXT NOT NULL DEFAULT 'failure';

UPDATE daemon_action_ledger
SET retry_kind = 'failure'
WHERE retry_kind IS DISTINCT FROM 'failure';
