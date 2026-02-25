-- Rename runner action identifier to execution_id and drop stored action name.

ALTER TABLE runner_actions_done
    RENAME COLUMN node_id TO execution_id;

ALTER TABLE runner_actions_done
    DROP COLUMN action_name;
