//! Utilities for SQLx migrations on Postgres.

#![warn(missing_docs)]

/// Error returned by [`check`].
#[derive(Debug, thiserror::Error)]
pub enum CheckError {
    /// No connection could be acquired.
    #[error("acquiring a connection: {0}")]
    Acquire(#[source] sqlx::Error),

    /// The database has no migrations table: it was never migrated.
    #[error("the database has not been migrated")]
    NotMigrated,

    /// A migration of the migrator was not applied yet: the database is
    /// older than the migrator.
    #[error("migration {0} is not applied yet")]
    Pending(i64),

    /// A schema the migrator creates does not exist yet.
    #[error("schema {0} does not exist yet")]
    CreateSchema(String),

    /// The migrator found the applied migrations do not match its own, or
    /// failed to take the lock or read them.
    #[error("checking the applied migrations: {0}")]
    Migrate(#[source] sqlx::migrate::MigrateError),

    /// The migrations lock could not be released; the connection may still
    /// hold it.
    #[error("releasing the migrations lock: {0}")]
    Unlock(#[source] sqlx::migrate::MigrateError),
}

/// Check that the database is migrated exactly to `migrator`'s
/// migrations, without changing anything in it.
///
/// The migrator runs as usual, on a connection that refuses every change,
/// so the validation is the migrator's own. When `migrator` takes the
/// migrations lock, a migration running elsewhere is waited for.
///
/// Note that a call dropped while it holds the migrations lock leaves the
/// lock with the connection, so on a pooled connection it is not
/// cancel-safe.
pub async fn check<'m, Acquire>(
    migrator: &sqlx::migrate::Migrator,
    connection: Acquire,
) -> Result<(), CheckError>
where
    Acquire: sqlx::Acquire<'m, Database = sqlx::Postgres>,
{
    let mut connection = connection.acquire().await.map_err(CheckError::Acquire)?;

    let mut check_only = CheckOnly {
        connection: &mut connection,
        refusal: None,
        locked: false,
    };

    // `Migrator::run` would take the connection as an `Acquire`, which only
    // hands out the driver's own connection type, never a wrapper.
    // `run_direct` is hidden from sqlx's docs and so outside its semver:
    // the tests pin the behaviour this relies on.
    let checked = migrator.run_direct(None, &mut check_only, false).await;

    let Err(error) = checked else {
        return Ok(());
    };

    // The migrator returns on an error without releasing the lock, and the
    // lock belongs to the session, which outlives this call in a pool.
    if check_only.locked {
        let unlocked = sqlx::migrate::Migrate::unlock(&mut *check_only.connection).await;
        unlocked.map_err(CheckError::Unlock)?;
    }

    Err(check_only.refusal.unwrap_or(CheckError::Migrate(error)))
}

/// A connection for the migrator that reads, takes the migrations lock,
/// and refuses every change.
struct CheckOnly<'c> {
    connection: &'c mut sqlx::PgConnection,

    refusal: Option<CheckError>,

    locked: bool,
}

impl CheckOnly<'_> {
    /// Record why the run stops, and the error that stops it; [`check`]
    /// reports the recorded refusal instead of that error.
    fn refuse(&mut self, refusal: CheckError) -> sqlx::migrate::MigrateError {
        self.refusal = Some(refusal);

        refused()
    }
}

/// The error that stops the migrator at a refused change.
fn refused() -> sqlx::migrate::MigrateError {
    sqlx::migrate::MigrateError::Execute(sqlx::Error::Protocol(
        "refused by the migrations check".into(),
    ))
}

impl sqlx::migrate::Migrate for CheckOnly<'_> {
    fn create_schema_if_not_exists<'e>(
        &'e mut self,
        schema_name: &'e str,
    ) -> futures_core::future::BoxFuture<'e, Result<(), sqlx::migrate::MigrateError>> {
        Box::pin(async move {
            // The name is read the way the migrator's `CREATE SCHEMA` reads
            // it: case-folded unless quoted.
            let exists: bool = sqlx::query_scalar(
                r#"
SELECT to_regnamespace($1) IS NOT NULL
                "#,
            )
            .bind(schema_name)
            .fetch_one(&mut *self.connection)
            .await?;

            if !exists {
                return Err(self.refuse(CheckError::CreateSchema(schema_name.into())));
            }

            Ok(())
        })
    }

    // The driver's own version creates the table if missing, which needs
    // the rights to create it even when it exists, and fails on a
    // read-only connection.
    fn ensure_migrations_table<'e>(
        &'e mut self,
        table_name: &'e str,
    ) -> futures_core::future::BoxFuture<'e, Result<(), sqlx::migrate::MigrateError>> {
        Box::pin(async move {
            let exists: bool = sqlx::query_scalar(
                r#"
SELECT to_regclass($1) IS NOT NULL
                "#,
            )
            .bind(table_name)
            .fetch_one(&mut *self.connection)
            .await?;

            if !exists {
                return Err(self.refuse(CheckError::NotMigrated));
            }

            Ok(())
        })
    }

    fn dirty_version<'e>(
        &'e mut self,
        table_name: &'e str,
    ) -> futures_core::future::BoxFuture<'e, Result<Option<i64>, sqlx::migrate::MigrateError>> {
        sqlx::migrate::Migrate::dirty_version(&mut *self.connection, table_name)
    }

    fn list_applied_migrations<'e>(
        &'e mut self,
        table_name: &'e str,
    ) -> futures_core::future::BoxFuture<
        'e,
        Result<Vec<sqlx::migrate::AppliedMigration>, sqlx::migrate::MigrateError>,
    > {
        sqlx::migrate::Migrate::list_applied_migrations(&mut *self.connection, table_name)
    }

    fn lock(
        &mut self,
    ) -> futures_core::future::BoxFuture<'_, Result<(), sqlx::migrate::MigrateError>> {
        Box::pin(async move {
            sqlx::migrate::Migrate::lock(&mut *self.connection).await?;

            self.locked = true;

            Ok(())
        })
    }

    fn unlock(
        &mut self,
    ) -> futures_core::future::BoxFuture<'_, Result<(), sqlx::migrate::MigrateError>> {
        Box::pin(async move {
            sqlx::migrate::Migrate::unlock(&mut *self.connection).await?;

            self.locked = false;

            Ok(())
        })
    }

    fn apply<'e>(
        &'e mut self,
        _table_name: &'e str,
        migration: &'e sqlx::migrate::Migration,
    ) -> futures_core::future::BoxFuture<'e, Result<std::time::Duration, sqlx::migrate::MigrateError>>
    {
        Box::pin(async move { Err(self.refuse(CheckError::Pending(migration.version))) })
    }

    fn revert<'e>(
        &'e mut self,
        _table_name: &'e str,
        _migration: &'e sqlx::migrate::Migration,
    ) -> futures_core::future::BoxFuture<'e, Result<std::time::Duration, sqlx::migrate::MigrateError>>
    {
        Box::pin(async move { Err(refused()) })
    }
}

#[cfg(test)]
mod tests;
