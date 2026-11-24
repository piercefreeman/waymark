-- Moves an observability store that Waymark 0.30 migrated from the SQLx
-- migrator's ledger to the migrations engine's.
--
-- Run it once, before the first start of a later release, with every node,
-- bridge and app stopped, as the role that owns the tables, on the database
-- the store is in — the main database, unless the store has a URL of its
-- own:
--
--     psql "$WAYMARK_OBSERVABILITY_DATABASE_URL" --set ON_ERROR_STOP=1 --file sqlx_migrations_ledger.sql
--
-- The store's tables are in the `observability` schema, and so is everything
-- this file touches. In one transaction it checks that `_sqlx_migrations`
-- has exactly the migrations of Waymark 0.30, each successful and with the
-- checksum the SQLx migrator recorded for it; creates the migrations
-- engine's two tables; writes each of those migrations into the engine's
-- ledger as a finished one; and drops `_sqlx_migrations`. Nothing of
-- `_sqlx_migrations` goes into the engine's ledger: its rows are the ones
-- written below. It stops with nothing changed when `_sqlx_migrations` is
-- not there or is not what it checks for, or when the engine's tables are
-- there already.
--
-- A store that an earlier release migrated is brought to Waymark 0.30 first.

BEGIN;

SET LOCAL search_path TO observability;

DO $$
DECLARE
    differing TEXT;
BEGIN
    WITH expected (version, checksum, success) AS (
        VALUES
            (1, decode('b6935e9490eb178ec5b3817326eceb4026b0ad942f3378d71eeb699e2eab63ba3aca2da4eb7e71ff853c1535dda8cfc0', 'hex'), TRUE),
            (2, decode('71e2cf65378db1bb023d6b988ef3e65782599bdbf7f0c32b218221d851ea14b44cfba2afd7b65372553414ad9e7838bb', 'hex'), TRUE),
            (3, decode('2551e333fb9f03670841d52a3350209f5ceccfd4b86ebdb9e400392bbeecf24b05b8aa97091da7da777163bb2c94fc23', 'hex'), TRUE),
            (4, decode('c0b00674829bac8e605d1e6422ed50bb3f1b0872dddbe46e3ecfe8c33b02c966a24c6aab63aa8fbc74814a84e21c6a41', 'hex'), TRUE),
            (5, decode('6de56e200e7ad3f87ffc82e4195910b5d243a3ad769940510c85f8a55bf3fc7a0984ffcb9f1da81e9bab99eaa9f483f3', 'hex'), TRUE),
            (6, decode('5f3efd50defb0fcd5b94f685d395fdbaffc1ea6a1d21170cb33aedb53144ab480464fefd570be620c7bab32bf44c8c26', 'hex'), TRUE)
    ),
    actual AS (
        SELECT version, checksum, success
        FROM _sqlx_migrations
    ),
    difference AS (
        (TABLE expected EXCEPT TABLE actual)
        UNION ALL
        (TABLE actual EXCEPT TABLE expected)
    ),
    differing_versions AS (
        SELECT DISTINCT version
        FROM difference
    )
    SELECT string_agg(version::TEXT, ', ' ORDER BY version)
    INTO differing
    FROM differing_versions;

    IF differing IS NOT NULL THEN
        RAISE EXCEPTION
            '_sqlx_migrations is not as Waymark 0.30 leaves it; the migrations that differ: %',
            differing;
    END IF;
END
$$;

CREATE TABLE _waymark_observability_migrations (
    version BIGINT PRIMARY KEY,
    description TEXT NOT NULL,
    revision TEXT NOT NULL,
    applied_source_digest TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('started', 'finished')),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE _waymark_observability_migrations_locks (
    holder_id UUID PRIMARY KEY,
    slot BIGINT GENERATED ALWAYS AS IDENTITY,
    mode TEXT NOT NULL CHECK (mode IN ('shared', 'exclusive')),
    expires_at TIMESTAMPTZ NOT NULL,
    span INT8RANGE GENERATED ALWAYS AS (
        CASE
            WHEN mode = 'exclusive' THEN int8range(NULL, NULL)
            ELSE int8range(slot, slot + 1)
        END
    ) STORED,
    EXCLUDE USING gist (span WITH &&)
);

COMMENT ON TABLE _waymark_observability_migrations IS
    'The migrations ledger of the Waymark migrations engine: one row for each migration this database has seen.';
COMMENT ON COLUMN _waymark_observability_migrations.version IS
    'The migration''s number: its place in the order of the migrations.';
COMMENT ON COLUMN _waymark_observability_migrations.description IS
    'The migration''s name, for people.';
COMMENT ON COLUMN _waymark_observability_migrations.revision IS
    'The revision of the migration as it was applied.';
COMMENT ON COLUMN _waymark_observability_migrations.applied_source_digest IS
    'A hint for people, never read by the migrations engine: a digest of the source the migration had when it was applied to this database.';
COMMENT ON COLUMN _waymark_observability_migrations.state IS
    'How far the migration got: ''started'' or ''finished''.';
COMMENT ON COLUMN _waymark_observability_migrations.recorded_at IS
    'When the row was written.';

COMMENT ON TABLE _waymark_observability_migrations_locks IS
    'The migrations locks of the Waymark migrations engine: one row for each holder of a lock on this database''s migrations.';
COMMENT ON COLUMN _waymark_observability_migrations_locks.holder_id IS
    'The holder the lock belongs to.';
COMMENT ON COLUMN _waymark_observability_migrations_locks.slot IS
    'A number of the lock''s own, which sets shared locks apart from one another.';
COMMENT ON COLUMN _waymark_observability_migrations_locks.mode IS
    '''exclusive'' for a lock taken to migrate the database, ''shared'' for a lock taken to run on it as migrated.';
COMMENT ON COLUMN _waymark_observability_migrations_locks.expires_at IS
    'When the lock stops holding unless its holder renews it.';
COMMENT ON COLUMN _waymark_observability_migrations_locks.span IS
    'What the lock covers: everything for an exclusive lock, its own slot for a shared one. No two locks cover the same thing.';

INSERT INTO _waymark_observability_migrations (version, description, revision, applied_source_digest, state)
VALUES
    (1, 'essential metrics node samples', 'essential-metrics-node-samples-1', 'sha256:e59247127e00c16ad22b2c3c2c44b319a8fa1ab7a1c9b1f9c4e6bad07e1281c4', 'finished'),
    (2, 'observability events', 'observability-events-1', 'sha256:1ed2cdf7b8851a8dac8c8a02e0c12251675b54b8d54cd0f529c6547597269458', 'finished'),
    (3, 'observability events vm timeline', 'observability-events-vm-timeline-1', 'sha256:8a726b430a92721d7c43b67cd627c757b5c6d452480a3be2ef0ea51935dcd4f4', 'finished'),
    (4, 'observability vm instances', 'observability-vm-instances-1', 'sha256:9632be144e7ab58a882e38c30bc8f534c0d176c91669ac207ff1262aec716fe0', 'finished'),
    (5, 'essential metrics observability events dropped', 'essential-metrics-observability-events-dropped-1', 'sha256:eac4d70289218068a49489f720f662b40f5a207b2554aeec970d4fa1d8e6b7d7', 'finished'),
    (6, 'observability vm executables', 'observability-vm-executables-1', 'sha256:bbd81341ca5b8395b99011aecb89e9c55f1a40797570720a08fda11a9a72f4a8', 'finished');

DROP TABLE _sqlx_migrations;

COMMIT;
