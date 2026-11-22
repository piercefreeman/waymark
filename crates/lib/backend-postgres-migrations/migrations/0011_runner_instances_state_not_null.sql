-- revision: runner-instances-state-not-null-1

-- The state is actually required to be present; make postgres data model also
-- recognize it.
ALTER TABLE runner_instances ALTER COLUMN state SET NOT NULL;
