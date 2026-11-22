//! The assertion that the SQLx migrator's own ledger is not in a
//! database.

use waymark_migrations_engine_assertion::Outcome;

/// The identifier of the ledger table the SQLx migrator keeps.
const SQLX_MIGRATIONS_LEDGER: &str = "_sqlx_migrations";

/// The assertion that a database has no ledger of the SQLx migrator: it
/// was never migrated by it, or its ledger was moved to the migrations
/// engine's and dropped.
///
/// The ledger is looked for through the connection's `search_path`, as
/// the migrations engine's own tables are.
#[derive(Debug)]
pub struct SqlxMigrationsLedgerAbsentAssertion;

impl waymark_migrations_engine_assertion::Assertion for SqlxMigrationsLedgerAbsentAssertion {
    type Connection = sqlx::PgConnection;

    type CheckError = sqlx::Error;

    async fn check(
        &self,
        connection: &mut sqlx::PgConnection,
    ) -> Result<Outcome, Self::CheckError> {
        let present: bool = sqlx::query_scalar(
            r#"
SELECT to_regclass($1) IS NOT NULL
            "#,
        )
        .bind(SQLX_MIGRATIONS_LEDGER)
        .fetch_one(connection)
        .await?;

        if present {
            return Ok(Outcome::Violated {
                message: format!(
                    "it has a `{SQLX_MIGRATIONS_LEDGER}` table left by the SQLx migrator"
                ),
            });
        }

        Ok(Outcome::Holds)
    }
}
