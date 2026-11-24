-- Moves a main database that Waymark 0.30 migrated from the SQLx migrator's
-- ledger to the migrations engine's.
--
-- Run it once, before the first start of a later release, with every node,
-- bridge and app stopped, as the role that owns the tables:
--
--     psql "$WAYMARK_DATABASE_URL" --set ON_ERROR_STOP=1 --file sqlx_migrations_ledger.sql
--
-- In one transaction it checks that `_sqlx_migrations` has exactly the
-- migrations of Waymark 0.30, each successful and with the checksum the SQLx
-- migrator recorded for it; creates the migrations engine's two tables;
-- writes each of those migrations into the engine's ledger as a finished
-- one; and drops `_sqlx_migrations`. Nothing of `_sqlx_migrations` goes into
-- the engine's ledger: its rows are the ones written below. It stops with
-- nothing changed when `_sqlx_migrations` is not there or is not what it
-- checks for, or when the engine's tables are there already.
--
-- A database that an earlier release migrated is brought to Waymark 0.30
-- first.

BEGIN;

DO $$
DECLARE
    differing TEXT;
BEGIN
    WITH expected (version, checksum, success) AS (
        VALUES
            (1, decode('c593cda9d8b6d8d6706b94cd68606657ab7218deec4dfcb884762bf550288fe57422ed4ea3a41cf5b14fefbd5690d401', 'hex'), TRUE),
            (2, decode('d6a15c7d28ebe727161e92f01c3c172f9fa9d91766927607772f4ec4b004db04a9d7318d31a0e8b67c831f66350dc565', 'hex'), TRUE),
            (3, decode('73c7614eec2cc1f4fc80b3d0d5a64f1cfb9890c55eddbcaab8b67ac9b80303013eaefc0d27203bd643c280760cdab894', 'hex'), TRUE),
            (4, decode('429a82ab4125bc5b52345bd63b8bb9a0e2119dd0ad8f7b745b49f3bfc40be874fb248ff54533cad8affefd524f2cfbd4', 'hex'), TRUE),
            (5, decode('fcb688328fbc26d1622c5027bd86f2aa04a386e4c9d56873bad9ee4ca135bd33b5db5109e9413a07b314ecdf24e2a500', 'hex'), TRUE),
            (6, decode('837d03131cac99d27fe616daca8366295e1695014681503589e13569cd681777497a419deb8778d20c19b34578c7895d', 'hex'), TRUE),
            (7, decode('cc664fa7e12a70ec74d0fa81a3745a1a57d464e6bb40cb0fe228825ae6ffef62aebac0b790a149bea1483aa692b6f39f', 'hex'), TRUE),
            (8, decode('1341291069f39c5478c6dc612cce12fae69c567a9b180d3749fdc182fb7bf4d6405747de855060a6e43d0b14581dd0ac', 'hex'), TRUE),
            (9, decode('8428e4f7f1bd4ebca0b1ecab4d35bfc7254b05ec022583ff3d89d68a1de36b9a0de02242e723aba80fb0c71197df90fd', 'hex'), TRUE),
            (10, decode('9d2d49b3f5bcfdc5857c40be97c4c6512c12b0eed62ad5c16fad94e140f86de6abe69d23d87eb1a72da4713ced6519c6', 'hex'), TRUE),
            (11, decode('436750910a8431ba6fc935f0126b3bdbbdb7e8ac1afe14cc9e215f63f024554a6b6ce35e9de9fbfe20206a16d85b464b', 'hex'), TRUE),
            (12, decode('2b1276ffeec21a8c5a04be709637d8c04600e70ab9402a076b8dd1f5fd563f9cfccc846dbbb643e142db9b5a0a400aec', 'hex'), TRUE),
            (13, decode('04cdbc30a1f0bd1e213f5dc40e586ae8d4856119076dd0a02c6dc553bc31c38bfbd07f6138d9a04efdc5642960eb39cd', 'hex'), TRUE),
            (14, decode('c3d50cb4d87f64cac1f6c345b8ef87350230e85b103056769c25800204a31dc22717e4884cc3595d558807c9494e379c', 'hex'), TRUE),
            (15, decode('8139e5ded51f83aa5fcdda513e9695d2d0a91ecc4ccbb624ae7697cbe68c7b48a0fc592cce05036423a9c5ee41ca430d', 'hex'), TRUE),
            (16, decode('aca03f5be9fdc6b16f8a9fcaf798a931b82ca5712f2c6ca69facab1e56691b56145c039970b9e5d8a2a70a564509bd8c', 'hex'), TRUE),
            (17, decode('2f47d13a97153ae4f7e796fa78b1bdf4306d17940c8d2975802488f6fd8476f52bbd388aafad81fe13f4e1251dc5d404', 'hex'), TRUE),
            (18, decode('64676b865abc17425ee8306f15c6fb5153d259672a844175b3367866e3895fa78afbe9aac10d4cf101de73bcf1eb1f77', 'hex'), TRUE),
            (19, decode('70d2d0e5fc6976e0139605dc933c8049eaea436e7a20461053592e3694dc26234c1d50326d9505a730eb44b2644d69ba', 'hex'), TRUE),
            (20, decode('d0cad467de6e423fc9938ce966c20e5e0c857623ffe950559cb0271b1f218f6510f9142e4f6a50c0726256935b086a03', 'hex'), TRUE),
            (21, decode('52d7f295f1fa5da250068f4764b8bf06c00625fbc98f5cadc84e8beca51d62964c43289e06a0888b8d2b57f92613f8e3', 'hex'), TRUE),
            (22, decode('cee53df9672ae4047a97e44f72cc88c861513bfc3b0ef7c81500e8514c020ef2fe9880fa3e4984ecac60b1699affd791', 'hex'), TRUE),
            (23, decode('5ec79dac6cf739ff12e7a935a529e4e1453cce6c7ed8bb6f43c8df4107ce55bbfb4f4e1d55311ed4f7a253d6ed195aa1', 'hex'), TRUE),
            (24, decode('cbf7dc2daa36f123bb9a152eaa35c8506ed29b8e663cf0fea82fc616d59bd9c4c987fffff41cb47846057f9fbda0a1fc', 'hex'), TRUE),
            (25, decode('b9442a5393be59b77e590daa3fb28ae751b31eec9a4ef9c5a8f3461b4a1ccbdb33f59d0dc58efb825bd3e6c608035380', 'hex'), TRUE),
            (26, decode('12fd6ea20f4d51de5fa45836a5e566fa6cc6d4869cd49c6b503a0f0034fea385857f1e2f7d20e5ecfffd6a95c2317458', 'hex'), TRUE)
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

CREATE TABLE _waymark_main_migrations (
    version BIGINT PRIMARY KEY,
    description TEXT NOT NULL,
    revision TEXT NOT NULL,
    applied_source_digest TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('started', 'finished')),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE _waymark_main_migrations_locks (
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

COMMENT ON TABLE _waymark_main_migrations IS
    'The migrations ledger of the Waymark migrations engine: one row for each migration this database has seen.';
COMMENT ON COLUMN _waymark_main_migrations.version IS
    'The migration''s number: its place in the order of the migrations.';
COMMENT ON COLUMN _waymark_main_migrations.description IS
    'The migration''s name, for people.';
COMMENT ON COLUMN _waymark_main_migrations.revision IS
    'The revision of the migration as it was applied.';
COMMENT ON COLUMN _waymark_main_migrations.applied_source_digest IS
    'A hint for people, never read by the migrations engine: a digest of the source the migration had when it was applied to this database.';
COMMENT ON COLUMN _waymark_main_migrations.state IS
    'How far the migration got: ''started'' or ''finished''.';
COMMENT ON COLUMN _waymark_main_migrations.recorded_at IS
    'When the row was written.';

COMMENT ON TABLE _waymark_main_migrations_locks IS
    'The migrations locks of the Waymark migrations engine: one row for each holder of a lock on this database''s migrations.';
COMMENT ON COLUMN _waymark_main_migrations_locks.holder_id IS
    'The holder the lock belongs to.';
COMMENT ON COLUMN _waymark_main_migrations_locks.slot IS
    'A number of the lock''s own, which sets shared locks apart from one another.';
COMMENT ON COLUMN _waymark_main_migrations_locks.mode IS
    '''exclusive'' for a lock taken to migrate the database, ''shared'' for a lock taken to run on it as migrated.';
COMMENT ON COLUMN _waymark_main_migrations_locks.expires_at IS
    'When the lock stops holding unless its holder renews it.';
COMMENT ON COLUMN _waymark_main_migrations_locks.span IS
    'What the lock covers: everything for an exclusive lock, its own slot for a shared one. No two locks cover the same thing.';

INSERT INTO _waymark_main_migrations (version, description, revision, applied_source_digest, state)
VALUES
    (1, 'init', 'init-1', 'sha256:3e84868c711a0a7af2c6c28839ea497030537973b55c09419a4bb9d115b075db', 'finished'),
    (2, 'runner actions done execution id', 'runner-actions-done-execution-id-1', 'sha256:f591314c5330000fc9f78415ea121f698db05879b8e84c9932357eb84bfdfaa6', 'finished'),
    (3, 'instance locks', 'instance-locks-1', 'sha256:4459deedb111baee5462d01de2661c0b2d2a305e2b57e344fa4a92a279b3c3b0', 'finished'),
    (4, 'workflow versions', 'workflow-versions-1', 'sha256:920bb60329609da7cc536d424f7838d776a7cfec0715c31cd632bb047d2e7cd7', 'finished'),
    (5, 'runner instances workflow version id', 'runner-instances-workflow-version-id-1', 'sha256:4eded8d074bda7d0354a8413cd57a156ef51530f935776f3dd3e4e9232608e66', 'finished'),
    (6, 'drop unused runner tables', 'drop-unused-runner-tables-1', 'sha256:4ac4f151fbb6cadc167a32805da4a3c2b6fe8e1a3816c71a1e14d619714be4ef', 'finished'),
    (7, 'runner instances schedule id', 'runner-instances-schedule-id-1', 'sha256:38afdc05d1e4e6d870714534a5ddc151801b6b14cc472f67999914c77b85a448', 'finished'),
    (8, 'runner actions done timing', 'runner-actions-done-timing-1', 'sha256:811f82a41e46966ac700b1e0d6118286c8e2da5d5d7efade42af4ac2a4ca3ed9', 'finished'),
    (9, 'instance search columns', 'instance-search-columns-1', 'sha256:5676bacfce035099ee939561e76ac11703705cdac3b9b98b05f9c2140bc1027b', 'finished'),
    (10, 'runner instances perf', 'runner-instances-perf-1', 'sha256:daeef7afaa408aa205dc307d41985b4e2ac3bacf9bd2dfbba7d69dc593a79a3b', 'finished'),
    (11, 'runner instances state not null', 'runner-instances-state-not-null-1', 'sha256:2be3d1b24c04ce477dd8dcfa19a37c4b0a44784079b9c90c59afce735040805f', 'finished'),
    (12, 'vm runtime snapshots', 'vm-runtime-snapshots-1', 'sha256:593501e886993a643f3bc4df0e767ae5c387fd1f9839b95b7cf740e16d57bbda', 'finished'),
    (13, 'workload pinnings', 'workload-pinnings-1', 'sha256:ec41bc9faec1dd6d68b655abf4e5f175fa51aa427e289dae87c15e8bb1f4b57c', 'finished'),
    (14, 'vm execution results', 'vm-execution-results-1', 'sha256:67cee791306e4db1fc01c49cabcbfcbaeeaddd473a0d07ae6346ab15e5ec3155', 'finished'),
    (15, 'vm executables', 'vm-executables-1', 'sha256:a5d038538969f8305bee6db0af5d68517e62c3ee64bd1e6ed9da1aacfe3d3f1c', 'finished'),
    (16, 'action call completions', 'action-call-completions-1', 'sha256:5c6b5b6603f469acb0624e956d64c7ce0c3052cbd7b5b28ce6ad16601d696190', 'finished'),
    (17, 'action call requests', 'action-call-requests-1', 'sha256:5ec5e08a60c349d969bceeab850283477a0be3a6edc185f07ce6b3b6b2ae72a6', 'finished'),
    (18, 'rename workload pinnings to runnable workloads', 'rename-workload-pinnings-to-runnable-workloads-1', 'sha256:3729a578c6af6827c6ada9c78487f0d871fe8e339783817841b72657ff678df7', 'finished'),
    (19, 'sleep requests', 'sleep-requests-1', 'sha256:21d4b8e377f46998dab873df6a5c9aef54b9c6be6b489bb735d02909efc527c7', 'finished'),
    (20, 'snapshot delete cleanup', 'snapshot-delete-cleanup-1', 'sha256:6b8b834b5aee47d91ec7e8bb819b0df73254899c8f404bcf44297cb617c55368', 'finished'),
    (21, 'drop legacy tables', 'drop-legacy-tables-1', 'sha256:9f10fd02667b0638a8ce306c6a12def76610f55b296e07cfea114b303ab935b5', 'finished'),
    (22, 'schedules', 'schedules-1', 'sha256:15a73ee2410c351d863dec720c32291381feeeebdb55c6ed745d5e79141490a8', 'finished'),
    (23, 'canonical row lock order', 'canonical-row-lock-order-1', 'sha256:5bb366db18cbb999715cf8374e863299152604ef914b3b97113cb58203a46d49', 'finished'),
    (24, 'rename action call completions outcome', 'rename-action-call-completions-outcome-1', 'sha256:bd552a34613e685cb0e9e8598e990fc416da5b73d33fb14f67a9657840c92569', 'finished'),
    (25, 'drop worker status', 'drop-worker-status-1', 'sha256:82d8a4eeba0006c49011115b8cbd92f5851a369e33ba490399e6090ec4dd56e1', 'finished'),
    (26, 'action call request lock nonce', 'action-call-request-lock-nonce-1', 'sha256:41b77178666dd7b6db76f3b1a6ef8b692ea9bbf4680505a71b3641e480f0d1e4', 'finished');

DROP TABLE _sqlx_migrations;

COMMIT;
