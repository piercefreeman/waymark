//! Shared integration harness helpers used by test binaries and Rust tests.

mod postgres;

pub use postgres::{
    LOCAL_POSTGRES_DSN, MigrateDatabaseError, MigrateMainDatabaseError, MigrationsRunErrorFor,
    connect_pool, ensure_local_postgres, migrate_database, migrate_main_database,
};
