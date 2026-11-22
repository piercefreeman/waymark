//! Setting the migrations engine up, and the migrations ledger.

use waymark_migrations_engine_backend::{MigrationsLedger, migrations_ledger};
use waymark_migrations_engine_migration::{MigrationMetadata, MigrationRevision, MigrationVersion};

/// The SQLSTATE code of a table that does not exist.
const UNDEFINED_TABLE: &str = "42P01";

/// The state column's value of a started entry.
const STARTED: &str = "started";

/// The state column's value of a finished entry.
const FINISHED: &str = "finished";

/// A migrations ledger entry has a version no migration can have.
#[derive(Debug, thiserror::Error)]
#[error("migrations ledger entry has the version {value}, which no migration can have")]
pub struct LedgerEntryVersionError {
    /// The stored version.
    pub value: i64,
}

/// A migrations ledger entry has a state the ledger does not have.
#[derive(Debug, thiserror::Error)]
#[error("migrations ledger entry of migration {version} has the unknown state {state:?}")]
pub struct LedgerEntryStateError {
    /// The version of the entry.
    pub version: i64,

    /// The stored state.
    pub state: String,
}

/// Error returned when marking a migration as finished.
#[derive(Debug, thiserror::Error)]
pub enum MarkMigrationFinishedError {
    /// The entry could not be updated.
    #[error("updating the migrations ledger entry: {0}")]
    Update(#[source] sqlx::Error),

    /// The ledger has no started entry of the migration.
    #[error(
        "the migrations ledger has no started entry of migration {}",
        .version.to_raw()
    )]
    NoStartedEntry {
        /// The version of the migration.
        version: MigrationVersion,
    },
}

impl<Tables> waymark_migrations_engine_backend::SetupMigrationsEngine for crate::Backend<Tables>
where
    Tables: crate::Tables,
{
    type Error = sqlx::Error;

    async fn setup_migrations_engine(&mut self) -> Result<(), Self::Error> {
        let mut transaction = sqlx::Connection::begin(&mut self.connection).await?;

        // Two sessions creating the same table at once fail one of them
        // even with `IF NOT EXISTS`; the lock makes them take turns. It is
        // released with the transaction.
        sqlx::query(
            r#"
SELECT pg_advisory_xact_lock(hashtextextended(current_schema() || '.' || $1, 0))
            "#,
        )
        .bind(Tables::LEDGER)
        .execute(&mut *transaction)
        .await?;

        sqlx::query(sqlx::AssertSqlSafe(format!(
            r#"
CREATE TABLE IF NOT EXISTS "{ledger}" (
    version BIGINT PRIMARY KEY,
    description TEXT NOT NULL,
    revision TEXT NOT NULL,
    applied_source_digest TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('{STARTED}', '{FINISHED}')),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
)
            "#,
            ledger = Tables::LEDGER,
        )))
        .execute(&mut *transaction)
        .await?;

        // An exclusive lock spans everything and a shared one only a slot
        // of its own, so the exclusion constraint lets shared locks be
        // together and an exclusive one be only alone.
        sqlx::query(sqlx::AssertSqlSafe(format!(
            r#"
CREATE TABLE IF NOT EXISTS "{locks}" (
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
)
            "#,
            locks = Tables::LOCKS,
        )))
        .execute(&mut *transaction)
        .await?;

        // The database itself tells what the tables and their columns are
        // for.
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            r#"
COMMENT ON TABLE "{ledger}" IS
    'The migrations ledger of the Waymark migrations engine: one row for each migration this database has seen.';
COMMENT ON COLUMN "{ledger}".version IS
    'The migration''s number: its place in the order of the migrations.';
COMMENT ON COLUMN "{ledger}".description IS
    'The migration''s name, for people.';
COMMENT ON COLUMN "{ledger}".revision IS
    'The revision of the migration as it was applied.';
COMMENT ON COLUMN "{ledger}".applied_source_digest IS
    'A hint for people, never read by the migrations engine: a digest of the source the migration had when it was applied to this database.';
COMMENT ON COLUMN "{ledger}".state IS
    'How far the migration got: ''{STARTED}'' or ''{FINISHED}''.';
COMMENT ON COLUMN "{ledger}".recorded_at IS
    'When the row was written.';

COMMENT ON TABLE "{locks}" IS
    'The migrations locks of the Waymark migrations engine: one row for each holder of a lock on this database''s migrations.';
COMMENT ON COLUMN "{locks}".holder_id IS
    'The holder the lock belongs to.';
COMMENT ON COLUMN "{locks}".slot IS
    'A number of the lock''s own, which sets shared locks apart from one another.';
COMMENT ON COLUMN "{locks}".mode IS
    '''exclusive'' for a lock taken to migrate the database, ''shared'' for a lock taken to run on it as migrated.';
COMMENT ON COLUMN "{locks}".expires_at IS
    'When the lock stops holding unless its holder renews it.';
COMMENT ON COLUMN "{locks}".span IS
    'What the lock covers: everything for an exclusive lock, its own slot for a shared one. No two locks cover the same thing.';
            "#,
            ledger = Tables::LEDGER,
            locks = Tables::LOCKS,
        )))
        .execute(&mut *transaction)
        .await?;

        transaction.commit().await
    }
}

impl<Tables> waymark_migrations_engine_backend::ReadMigrationsLedger for crate::Backend<Tables>
where
    Tables: crate::Tables,
{
    type Error = sqlx::Error;

    async fn read_migrations_ledger(&mut self) -> Result<MigrationsLedger, Self::Error> {
        let rows: Result<Vec<(i64, String, String)>, sqlx::Error> =
            sqlx::query_as(sqlx::AssertSqlSafe(format!(
                r#"
SELECT version, revision, state
FROM "{ledger}"
                "#,
                ledger = Tables::LEDGER,
            )))
            .fetch_all(&mut self.connection)
            .await;

        let rows = match rows {
            Ok(rows) => rows,
            Err(error) => {
                if crate::sqlstate(&error).as_deref() == Some(UNDEFINED_TABLE) {
                    return Ok(MigrationsLedger::NotSetUp);
                }

                return Err(error);
            }
        };

        let entries: Result<Vec<migrations_ledger::Entry>, sqlx::Error> =
            rows.into_iter().map(decode_entry).collect();
        let entries = entries?;

        Ok(MigrationsLedger::SetUp(entries))
    }
}

/// The ledger entry of a row; a value outside what the ledger writes is a
/// decode error.
fn decode_entry(
    (version, revision, state): (i64, String, String),
) -> Result<migrations_ledger::Entry, sqlx::Error> {
    let Ok(raw_version) = u32::try_from(version) else {
        return Err(sqlx::Error::Decode(Box::new(LedgerEntryVersionError {
            value: version,
        })));
    };

    let state = match state.as_str() {
        STARTED => migrations_ledger::EntryState::Started,
        FINISHED => migrations_ledger::EntryState::Finished,
        _ => {
            return Err(sqlx::Error::Decode(Box::new(LedgerEntryStateError {
                version,
                state,
            })));
        }
    };

    Ok(migrations_ledger::Entry {
        version: MigrationVersion::from_raw(raw_version),
        revision: MigrationRevision::from_raw(revision),
        state,
    })
}

impl<Tables> waymark_migrations_engine_backend::MarkMigrationStarted for crate::Backend<Tables>
where
    Tables: crate::Tables,
{
    type Error = sqlx::Error;

    async fn mark_migration_started(
        &mut self,
        metadata: MigrationMetadata<'_>,
    ) -> Result<(), Self::Error> {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            r#"
INSERT INTO "{ledger}" (version, description, revision, applied_source_digest, state)
VALUES ($1, $2, $3, $4, $5)
            "#,
            ledger = Tables::LEDGER,
        )))
        .bind(i64::from(metadata.version.to_raw()))
        .bind(metadata.description)
        .bind(metadata.revision.as_raw())
        .bind(metadata.source_digest)
        .bind(STARTED)
        .execute(&mut self.connection)
        .await?;

        Ok(())
    }
}

impl<Tables> waymark_migrations_engine_backend::MarkMigrationFinished for crate::Backend<Tables>
where
    Tables: crate::Tables,
{
    type Error = MarkMigrationFinishedError;

    async fn mark_migration_finished(
        &mut self,
        version: MigrationVersion,
    ) -> Result<(), Self::Error> {
        let updated = sqlx::query(sqlx::AssertSqlSafe(format!(
            r#"
UPDATE "{ledger}"
SET state = $2
WHERE version = $1
    AND state = $3
            "#,
            ledger = Tables::LEDGER,
        )))
        .bind(i64::from(version.to_raw()))
        .bind(FINISHED)
        .bind(STARTED)
        .execute(&mut self.connection)
        .await
        .map_err(MarkMigrationFinishedError::Update)?;

        if updated.rows_affected() == 0 {
            return Err(MarkMigrationFinishedError::NoStartedEntry { version });
        }

        Ok(())
    }
}
