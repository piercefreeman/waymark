//! Destructive database reset helpers.
//!
//! Free functions over a pool — deliberately not methods on
//! [`Store`](crate::Store), so the destructive surface stays off the
//! production handle.

/// Truncate every live table, resetting identity sequences.
///
/// The store must be provisioned: the caller runs the migrations first.
/// A missing table is an error, never "already empty" — one statement
/// truncates all the tables or none of them, so tolerating a missing one
/// would report a reset that cleared nothing.
///
/// Keep the table list in sync with the migrations in
/// `waymark-observability-store-postgres-migrations`.
pub async fn truncate_all(pool: &sqlx::PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        TRUNCATE essential_metrics_node_samples, observability_events, observability_vm_instances
        RESTART IDENTITY CASCADE
        "#,
    )
    .execute(pool)
    .await?;
    Ok(())
}
