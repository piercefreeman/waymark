//! Migrations for the Postgres observability store: one stream
//! covering every observability subsystem's tables.

#![warn(missing_docs)]

/// The embedded SQLx migrator; the bringup runs it on the observability
/// schema-scoped pool.
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();
