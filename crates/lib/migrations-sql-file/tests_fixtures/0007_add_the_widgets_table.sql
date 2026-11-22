-- revision: add-the-widgets-table-1

-- A fixture of the SQL-file migration tests: two statements in one file.

CREATE TABLE widgets (
    id INT PRIMARY KEY,
    label TEXT NOT NULL
);

INSERT INTO widgets (id, label)
VALUES (1, 'the first widget');
