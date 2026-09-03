//! Utilities for schema-scoped Postgres pools: defaulting connections'
//! `search_path` to one schema, and creating that schema.

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
        .connect_with_schema(options, schema)
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

/// Extension for [`sqlx::postgres::PgPoolOptions`].
pub trait PgPoolOptionsExt {
    /// Connect a pool scoped to `schema`: connections default their
    /// `search_path` to it, and the schema is created if missing.
    ///
    /// `schema` must be a plain identifier (it is only quote-wrapped, not
    /// escaped) — a caller-internal constant, not operator input.
    fn connect_with_schema<'a>(
        self,
        options: sqlx::postgres::PgConnectOptions,
        schema: &'a str,
    ) -> impl Future<Output = Result<sqlx::PgPool, sqlx::Error>> + Send + 'a;
}

impl PgPoolOptionsExt for sqlx::postgres::PgPoolOptions {
    async fn connect_with_schema(
        self,
        options: sqlx::postgres::PgConnectOptions,
        schema: &str,
    ) -> Result<sqlx::PgPool, sqlx::Error> {
        let options = options.search_path(schema);

        let pool = self.connect_with(options).await?;

        create_schema_if_not_exists(&pool, schema).await?;

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
