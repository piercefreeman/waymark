-- The completion blob stores the call's execution result (Ok = the
-- outcome the action produced, Err = the execution failed to produce
-- one), so the column is named for what it holds.
ALTER TABLE action_call_completions RENAME COLUMN outcome TO execution_result;
