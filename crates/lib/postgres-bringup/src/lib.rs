//! Bringup for Postgres: pools and connections with their limits and
//! schema scope, and migrations through a connection of their own.

#![warn(missing_docs)]

use std::borrow::Cow;

use waymark_sqlx_postgres_schema_pool::{PgConnectOptionsExt as _, PgPoolOptionsExt as _};

/// What a pool's or a connection's transactions may do.
#[derive(Debug)]
pub enum Transactions {
    /// Read and write.
    Unrestricted,

    /// Read only: every transaction defaults to read-only, so a write that
    /// reaches it fails rather than landing.
    ReadOnly,
}

/// The schema a pool or a connection is scoped to: every connection
/// defaults its `search_path` to it, so queries stay unqualified.
///
/// The schema is an internal constant, not operator input; it must be a
/// plain identifier (it is only quote-wrapped, not escaped).
#[derive(Debug)]
pub enum Schema<'a> {
    /// The schema, created if missing.
    Create(Cow<'a, str>),

    /// The schema, which has to exist already; nothing is created.
    Require(Cow<'a, str>),

    /// The schema, neither created nor checked for: the opt-out for a
    /// caller whose schema is managed some other way.
    Unchecked(Cow<'a, str>),
}

/// What [`pool`] connects.
#[derive(Debug)]
pub struct Params<'a> {
    /// The pool's config.
    pub config: &'a waymark_postgres_config::pool::Config,

    /// What the pool's transactions may do.
    pub transactions: Transactions,

    /// The schema the pool is scoped to; unscoped when `None`.
    pub schema: Option<Schema<'a>>,
}

/// Error returned by [`pool`].
#[derive(Debug, thiserror::Error)]
pub enum PoolError {
    /// The URL did not parse as Postgres connect options.
    #[error("invalid database URL: {0}")]
    Url(#[source] sqlx::Error),

    /// The pool could not connect, or the schema could not be created.
    #[error("connecting the pool: {0}")]
    Connect(#[source] sqlx::Error),

    /// The pool could not connect, or the required schema is not there.
    #[error("connecting the pool: {0}")]
    ConnectRequiringSchema(#[source] waymark_sqlx_postgres_schema_pool::ExistingSchemaError),
}

/// Connect a pool: every connection carries the config's statement
/// timeout, the transaction limits and the schema scope `params` asks for.
pub async fn pool(params: Params<'_>) -> Result<sqlx::PgPool, PoolError> {
    let Params {
        config,
        transactions,
        schema,
    } = params;

    let options = connect_options(&config.url, config.statement_timeout, transactions)
        .map_err(PoolError::Url)?;

    let pool_options =
        sqlx::postgres::PgPoolOptions::new().max_connections(config.max_connections.get());

    let pool = match schema {
        None => pool_options
            .connect_with(options)
            .await
            .map_err(PoolError::Connect)?,
        Some(Schema::Create(schema)) => pool_options
            .connect_creating_schema(options, &schema)
            .await
            .map_err(PoolError::Connect)?,
        Some(Schema::Require(schema)) => pool_options
            .connect_requiring_schema(options, &schema)
            .await
            .map_err(PoolError::ConnectRequiringSchema)?,
        Some(Schema::Unchecked(schema)) => pool_options
            .connect_with(options.search_path(&schema))
            .await
            .map_err(PoolError::Connect)?,
    };

    Ok(pool)
}

/// What [`connection`] connects.
#[derive(Debug)]
pub struct ConnectionParams<'a> {
    /// The connection's config.
    pub config: &'a waymark_postgres_config::connection::Config,

    /// What the connection's transactions may do.
    pub transactions: Transactions,

    /// The schema the connection is scoped to; unscoped when `None`.
    pub schema: Option<Schema<'a>>,
}

/// Error returned by [`connection`].
#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    /// The URL did not parse as Postgres connect options.
    #[error("invalid database URL: {0}")]
    Url(#[source] sqlx::Error),

    /// The connection could not be made, or the schema could not be
    /// created.
    #[error("connecting to the database: {0}")]
    Connect(#[source] sqlx::Error),

    /// The connection could not be made, or the required schema is not
    /// there.
    #[error("connecting to the database: {0}")]
    ConnectRequiringSchema(#[source] waymark_sqlx_postgres_schema_pool::ExistingSchemaError),
}

/// Connect one connection: it carries the config's statement timeout, the
/// transaction limits and the schema scope `params` asks for.
pub async fn connection(
    params: ConnectionParams<'_>,
) -> Result<sqlx::PgConnection, ConnectionError> {
    let ConnectionParams {
        config,
        transactions,
        schema,
    } = params;

    let options = connect_options(&config.url, config.statement_timeout, transactions)
        .map_err(ConnectionError::Url)?;

    let connection = match schema {
        None => <sqlx::PgConnection as sqlx::Connection>::connect_with(&options)
            .await
            .map_err(ConnectionError::Connect)?,
        Some(Schema::Create(schema)) => {
            waymark_sqlx_postgres_schema_pool::connection::connect_creating_schema(options, &schema)
                .await
                .map_err(ConnectionError::Connect)?
        }
        Some(Schema::Require(schema)) => {
            waymark_sqlx_postgres_schema_pool::connection::connect_requiring_schema(
                options, &schema,
            )
            .await
            .map_err(ConnectionError::ConnectRequiringSchema)?
        }
        Some(Schema::Unchecked(schema)) => {
            <sqlx::PgConnection as sqlx::Connection>::connect_with(&options.search_path(&schema))
                .await
                .map_err(ConnectionError::Connect)?
        }
    };

    Ok(connection)
}

/// Error returned by [`migrate`].
#[derive(Debug, thiserror::Error)]
pub enum MigrateError<RunError> {
    /// The migrations connection could not be made.
    #[error("connecting for the migrations: {0}")]
    Connect(#[source] ConnectionError),

    /// The migrations failed.
    #[error("running the migrations: {0}")]
    Run(#[source] RunError),

    /// The migrations connection could not be closed.
    #[error("closing the migrations connection: {0}")]
    Close(#[source] sqlx::Error),
}

/// Migrate a database through a connection of its own, scoped to
/// `schema`, which it creates if missing, or unscoped when `schema` is
/// `None`: `run_fn` runs the migrations on it, and it is closed once they
/// are done, whether they succeeded or not.
///
/// A close that fails after the migrations succeeded still fails the
/// call: the link to the server broke.
pub async fn migrate<RunFn, RunError>(
    config: &waymark_postgres_config::connection::Config,
    schema: Option<Cow<'_, str>>,
    run_fn: RunFn,
) -> Result<(), MigrateError<RunError>>
where
    RunFn: AsyncFnOnce(&mut sqlx::PgConnection) -> Result<(), RunError>,
{
    let mut migrations_connection = connection(ConnectionParams {
        config,
        transactions: Transactions::Unrestricted,
        schema: schema.map(Schema::Create),
    })
    .await
    .map_err(MigrateError::Connect)?;

    let ran = run_fn(&mut migrations_connection).await;

    let closed = sqlx::Connection::close(migrations_connection).await;

    // A failed run is the error worth reporting; when the close fails too,
    // its error is dropped: the connection is gone either way.
    ran.map_err(MigrateError::Run)?;

    closed.map_err(MigrateError::Close)?;

    Ok(())
}

/// The connect options of `url`, every connection carrying
/// `statement_timeout`, or no statement timeout when it is `None`, and the
/// `transactions` limits.
fn connect_options(
    url: &waymark_secret_string::SecretString,
    statement_timeout: Option<waymark_nonzero_duration::NonZeroDuration>,
    transactions: Transactions,
) -> Result<sqlx::postgres::PgConnectOptions, sqlx::Error> {
    let options = url
        .expose_secret()
        .parse::<sqlx::postgres::PgConnectOptions>()?;

    // `0` is set rather than nothing, so a default the server or the role
    // carries does not apply either: the config is the only source.
    let statement_timeout_millis = statement_timeout.map_or(0, |timeout| timeout.as_millis());
    let options = options.options([("statement_timeout", statement_timeout_millis)]);

    let options = match transactions {
        Transactions::Unrestricted => options,
        Transactions::ReadOnly => options.options([("default_transaction_read_only", "on")]),
    };

    Ok(options)
}

#[cfg(test)]
mod tests;
