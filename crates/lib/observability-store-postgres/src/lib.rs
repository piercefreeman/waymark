//! The Postgres observability store: one store implementing every
//! observability subsystem's backend traits over one pool.

#![warn(missing_docs)]

mod common;
mod essential_metrics;
mod observability_events;
pub mod reset;

pub use self::essential_metrics::*;
pub use self::observability_events::*;

/// The Postgres store over the observability schema-scoped pool.
#[derive(Debug)]
pub struct Store {
    /// The observability schema-scoped pool (tables are unqualified).
    pub pool: sqlx::PgPool,
}

#[cfg(test)]
mod test_helpers;
