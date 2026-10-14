//! Config for a Postgres observability store.
//!
//! The URLs are parameters rather than variables of their own: the
//! backend-neutral `WAYMARK_OBSERVABILITY_DATABASE_URL`,
//! `WAYMARK_OBSERVABILITY_READ_DATABASE_URL` and
//! `WAYMARK_OBSERVABILITY_MIGRATIONS_DATABASE_URL` are read and dispatched
//! by scheme in `waymark-observability-store-config`.
//!
//! The pool configs here are the observability store's own, read from
//! `WAYMARK_OBSERVABILITY_POSTGRES_*` and
//! `WAYMARK_OBSERVABILITY_POSTGRES_READ_*`, never the main database's
//! `WAYMARK_DATABASE_*`.

pub mod migrate_only;
mod profiles;
pub mod read_only;
pub mod read_write;
pub mod read_write_migrate;
pub mod write_only;

pub use self::migrate_only::MigrateOnly;
pub use self::read_only::ReadOnly;
pub use self::read_write::ReadWrite;
pub use self::read_write_migrate::ReadWriteMigrate;
pub use self::write_only::WriteOnly;

#[cfg(test)]
mod tests;
