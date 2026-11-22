-- revision: index-the-widgets-1
-- no-transaction

CREATE INDEX CONCURRENTLY widgets_label ON widgets (label);
