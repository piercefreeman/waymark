-- Add backoff policy columns for retry delay calculation
-- backoff_kind: 'none' (immediate), 'linear', 'exponential'
-- backoff_base_delay_ms: base delay in milliseconds
-- backoff_multiplier: exponential multiplier (default 2.0, only used for exponential)

ALTER TABLE daemon_action_ledger
    ADD COLUMN backoff_kind TEXT NOT NULL DEFAULT 'none',
    ADD COLUMN backoff_base_delay_ms INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN backoff_multiplier DOUBLE PRECISION NOT NULL DEFAULT 2.0;
