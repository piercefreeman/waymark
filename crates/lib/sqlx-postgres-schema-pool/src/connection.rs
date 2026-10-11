//! Schema-scoped single Postgres connections, and the schema checks on
//! one connection that the pools run too.

use sqlx::Connection as _;

use crate::{ExistingSchemaError, PgConnectOptionsExt as _};

/// Connect one connection scoped to `schema`, creating the schema if
/// missing: the connection defaults its `search_path` to it.
///
/// `schema` must be a plain identifier (it is only quote-wrapped, not
/// escaped) — a caller-internal constant, not operator input.
pub async fn connect_creating_schema(
    options: sqlx::postgres::PgConnectOptions,
    schema: &str,
) -> Result<sqlx::PgConnection, sqlx::Error> {
    let options = options.search_path(schema);

    let mut connection = sqlx::PgConnection::connect_with(&options).await?;

    create_schema_if_not_exists(&mut connection, schema).await?;

    Ok(connection)
}

/// Connect one connection scoped to `schema`, requiring the schema to
/// exist: the connection defaults its `search_path` to it, and nothing is
/// created.
pub async fn connect_requiring_schema(
    options: sqlx::postgres::PgConnectOptions,
    schema: &str,
) -> Result<sqlx::PgConnection, ExistingSchemaError> {
    let options = options.search_path(schema);

    let mut connection = sqlx::PgConnection::connect_with(&options)
        .await
        .map_err(ExistingSchemaError::Connect)?;

    require_schema(&mut connection, schema).await?;

    Ok(connection)
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
    connection: &mut sqlx::PgConnection,
    schema: &str,
) -> Result<(), sqlx::Error> {
    if schema_exists(connection, schema).await? {
        return Ok(());
    }

    let created = sqlx::query(sqlx::AssertSqlSafe(format!(
        r#"CREATE SCHEMA IF NOT EXISTS "{schema}""#
    )))
    .execute(&mut *connection)
    .await;
    let Err(error) = created else {
        return Ok(());
    };

    // The winner of a creation race may leave the loser with an error;
    // the schema existing now is still success.
    if schema_exists(connection, schema).await? {
        return Ok(());
    }

    Err(error)
}

/// Whether `schema` exists.
pub async fn schema_exists(
    connection: &mut sqlx::PgConnection,
    schema: &str,
) -> Result<bool, sqlx::Error> {
    let (exists,): (bool,) =
        sqlx::query_as("SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = $1)")
            .bind(schema)
            .fetch_one(connection)
            .await?;
    Ok(exists)
}

/// Fail unless `schema` exists.
pub(crate) async fn require_schema(
    connection: &mut sqlx::PgConnection,
    schema: &str,
) -> Result<(), ExistingSchemaError> {
    let exists =
        schema_exists(connection, schema)
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

    Ok(())
}
