//! Utilities for schema-scoped Postgres pools: defaulting connections'
//! `search_path` to one schema, and creating that schema — or requiring
//! it, for a pool that only reads.

#![warn(missing_docs)]

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

        let exists =
            schema_exists(&pool, schema)
                .await
                .map_err(|source| ExistingSchemaError::Check {
                    schema: schema.to_owned(),
                    source,
                })?;
        if !exists {
            return Err(ExistingSchemaError::Missing {
                schema: schema.to_owned(),
            });
        }

        Ok(pool)
    }
}

/// Create `schema` unless it already exists.
///
/// The existence check runs first, so an already-provisioned schema
/// requires no privilege beyond reading the catalog; creation — and its
/// `CREATE`-on-database privilege requirement — happens only when the
/// schema is actually missing. A concurrent creation racing this one
/// counts as success.
///
/// `schema` must be a plain identifier (it is only quote-wrapped, not
/// escaped) — a caller-internal constant, not operator input.
pub async fn create_schema_if_not_exists(
    pool: &sqlx::PgPool,
    schema: &str,
) -> Result<(), sqlx::Error> {
    if schema_exists(pool, schema).await? {
        return Ok(());
    }

    let created = sqlx::query(&format!(r#"CREATE SCHEMA IF NOT EXISTS "{schema}""#))
        .execute(pool)
        .await;
    let Err(error) = created else {
        return Ok(());
    };

    // The winner of a creation race may leave the loser with an error;
    // the schema existing now is still success.
    if schema_exists(pool, schema).await? {
        return Ok(());
    }

    Err(error)
}

/// Whether `schema` exists.
pub async fn schema_exists(pool: &sqlx::PgPool, schema: &str) -> Result<bool, sqlx::Error> {
    let (exists,): (bool,) =
        sqlx::query_as("SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = $1)")
            .bind(schema)
            .fetch_one(pool)
            .await?;
    Ok(exists)
}

#[cfg(test)]
mod tests;
