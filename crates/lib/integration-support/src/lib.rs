//! Shared integration harness helpers used by test binaries and Rust tests.

mod postgres;

pub use postgres::{LOCAL_POSTGRES_DSN, connect_pool, ensure_local_postgres};
