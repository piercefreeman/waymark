//! Destructive database reset helpers.
//!
//! Free functions over a pool — deliberately not methods on
//! [`Store`](crate::Store), so the destructive surface stays off the
//! production handle.

/// Truncate every live table, resetting identity sequences.
///
/// Tolerates an unprovisioned store — the observability bringup owns
/// provisioning, so before its first run a missing table simply counts
/// as already empty.
///
/// Keep the table list in sync with the migrations in
/// `waymark-observability-store-postgres-migrations`.
pub async fn truncate_all(pool: &sqlx::PgPool) -> Result<(), sqlx::Error> {
    let result = sqlx::query(
        r#"
        TRUNCATE essential_metrics_node_samples
        RESTART IDENTITY CASCADE
        "#,
    )
    .execute(pool)
    .await;
    match result {
        Ok(_) => Ok(()),
        Err(error) if is_undefined_table(&error) => Ok(()),
        Err(error) => Err(error),
    }
}

fn is_undefined_table(error: &sqlx::Error) -> bool {
    let sqlx::Error::Database(db_error) = error else {
        return false;
    };
    db_error.code().as_deref() == Some("42P01")
}
