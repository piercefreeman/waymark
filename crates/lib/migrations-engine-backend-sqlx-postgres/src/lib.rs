//! The migrations engine's backend for Postgres, through SQLx.

#![warn(missing_docs)]

use std::marker::PhantomData;

mod ledger;
mod locks;
mod sqlx_migrations_ledger_absent_assertion;
mod transaction;

pub use self::ledger::*;
pub use self::sqlx_migrations_ledger_absent_assertion::SqlxMigrationsLedgerAbsentAssertion;

/// The identifiers of the tables the migrations engine keeps in a
/// database. A database's migrations supply them; there are no defaults.
///
/// The identifiers are internal constants, not operator input: each must
/// be a plain identifier (it is only quote-wrapped, not escaped). They are
/// looked up through the connection's `search_path`.
pub trait Tables {
    /// The identifier of the migrations ledger table.
    const LEDGER: &'static str;

    /// The identifier of the migrations locks table.
    const LOCKS: &'static str;
}

/// A backend of the migrations engine over one Postgres connection, with
/// the tables `Tables` gives the identifiers of.
#[derive_where::derive_where(Debug)]
pub struct Backend<Tables> {
    /// The connection every operation goes through.
    connection: sqlx::PgConnection,

    /// The tables' identifiers.
    tables: PhantomData<fn() -> Tables>,
}

impl<Tables> Backend<Tables> {
    /// A backend over `connection`.
    pub fn new(connection: sqlx::PgConnection) -> Self {
        Self {
            connection,
            tables: PhantomData,
        }
    }

    /// The connection the backend was over.
    pub fn into_connection(self) -> sqlx::PgConnection {
        self.connection
    }
}

impl<Tables> waymark_migrations_engine_backend::HasConnection for Backend<Tables> {
    type Connection = sqlx::PgConnection;
}

impl<Tables> waymark_migrations_engine_backend::AccessMigrationsConnection for Backend<Tables> {
    fn migrations_connection(&mut self) -> &mut Self::Connection {
        &mut self.connection
    }
}

/// The SQLSTATE code of `error`, when the server reported it.
fn sqlstate(error: &sqlx::Error) -> Option<std::borrow::Cow<'_, str>> {
    let database_error = error.as_database_error()?;

    database_error.code()
}

#[cfg(test)]
mod tests;
