//! Bringup for Postgres observability stores: schema-scoped pools.
//!
//! Every observability consumer gets its OWN pool — never the main
//! database pool. Pool isolation is what keeps observability from
//! contending with the main pool's connection budget even when the URL
//! points at the main database; a slow observability write can only ever
//! wait on its own pool's connections.

#![warn(missing_docs)]

use waymark_observability_store_postgres_config::PostgresConfig;
use waymark_sqlx_postgres_schema_pool::PgPoolOptionsExt as _;

/// Error returned by [`schema_pool`].
#[derive(Debug, thiserror::Error)]
pub enum SchemaPoolError {
    /// The URL did not parse as Postgres connect options.
    #[error("invalid observability database URL: {0}")]
    Url(#[source] sqlx::Error),

    /// The pool could not connect or the schema could not be created.
    #[error("connecting the observability pool: {0}")]
    Connect(#[source] sqlx::Error),
}

/// Connect a pool for one observability consumer, scoped to `schema`:
/// the schema is created if missing, and every connection defaults its
/// `search_path` to it, so the consumer's queries stay unqualified.
///
/// `schema` is an internal constant, not operator input; it must be a
/// plain identifier (it is only quote-wrapped, not escaped).
pub async fn schema_pool(
    config: &PostgresConfig,
    schema: &str,
) -> Result<sqlx::PgPool, SchemaPoolError> {
    let options = config
        .url
        .expose_secret()
        .parse::<sqlx::postgres::PgConnectOptions>()
        .map_err(SchemaPoolError::Url)?;

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(config.max_connections.get())
        .connect_with_schema(options, schema)
        .await
        .map_err(SchemaPoolError::Connect)?;

    Ok(pool)
}

#[cfg(test)]
mod tests;
