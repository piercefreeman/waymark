//! Utilities for schema-scoped Postgres pools and connections: defaulting
//! connections' `search_path` to one schema, and creating that schema — or
//! requiring it, for a side that must not create it.

#![warn(missing_docs)]

pub mod connection;

/// Connect a pool scoped to `schema` from a database URL — the
/// schema-scoped `sqlx::PgPool::connect`: connections default their
/// `search_path` to the schema, and the schema is created if missing.
///
/// `schema` must be a plain identifier (it is only quote-wrapped, not
/// escaped) — a caller-internal constant, not operator input.
pub async fn connect(url: &str, schema: &str) -> Result<sqlx::PgPool, sqlx::Error> {
    let options = url.parse::<sqlx::postgres::PgConnectOptions>()?;

    sqlx::postgres::PgPoolOptions::new()
        .connect_creating_schema(options, schema)
        .await
}

/// Extension for [`sqlx::postgres::PgConnectOptions`].
pub trait PgConnectOptionsExt {
    /// Default the `search_path` of connections made with these options
    /// to `schema`, so their unqualified statements stay inside it.
    fn search_path(self, schema: &str) -> Self;
}

impl PgConnectOptionsExt for sqlx::postgres::PgConnectOptions {
    fn search_path(self, schema: &str) -> Self {
        self.options([("search_path", schema)])
    }
}

/// Error connecting a pool to a schema that has to exist already.
#[derive(Debug, thiserror::Error)]
pub enum ExistingSchemaError {
    /// The pool could not connect.
    #[error("connecting: {0}")]
    Connect(#[source] sqlx::Error),

    /// The catalog could not be read to check for the schema.
    #[error("checking for schema {schema}: {source}")]
    Check {
        /// The schema looked for.
        schema: String,
        /// What the catalog read failed with.
        #[source]
        source: sqlx::Error,
    },

    /// The schema does not exist on the database connected to.
    #[error("schema {schema} does not exist")]
    Missing {
        /// The schema looked for.
        schema: String,
    },
}

/// Extension for [`sqlx::postgres::PgPoolOptions`].
pub trait PgPoolOptionsExt {
    /// Connect a pool scoped to `schema`, creating the schema if missing:
    /// connections default their `search_path` to it — the pool for a
    /// side that writes and provisions.
    ///
    /// `schema` must be a plain identifier (it is only quote-wrapped, not
    /// escaped) — a caller-internal constant, not operator input.
    fn connect_creating_schema<'a>(
        self,
        options: sqlx::postgres::PgConnectOptions,
        schema: &'a str,
    ) -> impl Future<Output = Result<sqlx::PgPool, sqlx::Error>> + Send + 'a;

    /// Connect a pool scoped to `schema`, requiring the schema to exist:
    /// connections default their `search_path` to it, and nothing is
    /// created — the pool for a side that only reads, which may be
    /// connected to a replica, where nothing can be created.
    fn connect_requiring_schema<'a>(
        self,
        options: sqlx::postgres::PgConnectOptions,
        schema: &'a str,
    ) -> impl Future<Output = Result<sqlx::PgPool, ExistingSchemaError>> + Send + 'a;
}

impl PgPoolOptionsExt for sqlx::postgres::PgPoolOptions {
    async fn connect_creating_schema(
        self,
        options: sqlx::postgres::PgConnectOptions,
        schema: &str,
    ) -> Result<sqlx::PgPool, sqlx::Error> {
        let options = options.search_path(schema);

        let pool = self.connect_with(options).await?;

        create_schema_if_not_exists(&pool, schema).await?;

        Ok(pool)
    }

    async fn connect_requiring_schema(
        self,
        options: sqlx::postgres::PgConnectOptions,
        schema: &str,
    ) -> Result<sqlx::PgPool, ExistingSchemaError> {
        let options = options.search_path(schema);

        let pool = self
            .connect_with(options)
            .await
            .map_err(ExistingSchemaError::Connect)?;

        let mut connection = pool.acquire().await.map_err(ExistingSchemaError::Connect)?;
        connection::require_schema(&mut connection, schema).await?;
        drop(connection);

        Ok(pool)
    }
}

/// Create `schema` unless it already exists, on one connection of `pool`.
/// See [`connection::create_schema_if_not_exists`].
///
/// `schema` must be a plain identifier (it is only quote-wrapped, not
/// escaped) — a caller-internal constant, not operator input.
pub async fn create_schema_if_not_exists(
    pool: &sqlx::PgPool,
    schema: &str,
) -> Result<(), sqlx::Error> {
    let mut connection = pool.acquire().await?;

    connection::create_schema_if_not_exists(&mut connection, schema).await
}

/// Whether `schema` exists, checked on one connection of `pool`.
pub async fn schema_exists(pool: &sqlx::PgPool, schema: &str) -> Result<bool, sqlx::Error> {
    let mut connection = pool.acquire().await?;

    connection::schema_exists(&mut connection, schema).await
}

#[cfg(test)]
mod tests;
