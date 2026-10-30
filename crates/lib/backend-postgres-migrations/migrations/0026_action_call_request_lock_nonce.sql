-- Each taken action-call request lock carries its owner's nonce for it,
-- so that a renewal or an unlock addresses exactly the lock it was issued
-- for: a lock the same owner took on the same row since carries another
-- nonce.  Renewals keep the nonce.

ALTER TABLE action_call_requests ADD COLUMN lock_nonce BIGINT;

-- Rows locked before this migration get a nonce like any other; a later
-- lock on such a row draws its own.
UPDATE action_call_requests SET lock_nonce = 0 WHERE locked_by IS NOT NULL;

-- Set and cleared together with the owner, like the expiry.
ALTER TABLE action_call_requests
    ADD CONSTRAINT action_call_requests_lock_owner_nonce_paired
    CHECK ((locked_by IS NULL) = (lock_nonce IS NULL));
