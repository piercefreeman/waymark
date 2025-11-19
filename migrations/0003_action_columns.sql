ALTER TABLE daemon_action_ledger
    ADD COLUMN module TEXT,
    ADD COLUMN function_name TEXT,
    ADD COLUMN kwargs_payload BYTEA;

UPDATE daemon_action_ledger
SET module = COALESCE(module, 'rappel.workflow_runtime'),
    function_name = COALESCE(function_name, 'workflow.execute_node'),
    kwargs_payload = COALESCE(kwargs_payload, ''::BYTEA)
WHERE module IS NULL OR function_name IS NULL OR kwargs_payload IS NULL;

ALTER TABLE daemon_action_ledger
    ALTER COLUMN module SET NOT NULL,
    ALTER COLUMN function_name SET NOT NULL,
    ALTER COLUMN kwargs_payload SET NOT NULL;

ALTER TABLE daemon_action_ledger
    DROP COLUMN payload;
