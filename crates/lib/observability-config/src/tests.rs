use waymark_secret_string::SecretString;

use super::*;

#[test]
fn postgres_scheme_dispatches() {
    let db = Db::from_url("postgres://localhost/waymark".into()).expect("postgres is supported");
    let Db::Postgres(config) = db;
    assert_eq!(config.url.expose_secret(), "postgres://localhost/waymark");
}

#[test]
fn postgresql_scheme_dispatches() {
    let db =
        Db::from_url("postgresql://localhost/waymark".into()).expect("postgresql is supported");
    let Db::Postgres(config) = db;
    assert_eq!(config.url.expose_secret(), "postgresql://localhost/waymark");
}

#[test]
fn unknown_scheme_is_refused() {
    let error =
        Db::from_url("mysql://localhost/waymark".into()).expect_err("mysql must not be dispatched");
    assert!(matches!(
        error,
        FromEnvError::UnsupportedScheme { scheme } if scheme == "mysql"
    ));
}

#[test]
fn schemeless_url_is_refused() {
    let error = Db::from_url("localhost/waymark".into()).expect_err("a scheme is required");
    assert!(matches!(
        error,
        FromEnvError::UnsupportedScheme { scheme } if scheme.is_empty()
    ));
}

#[test]
fn main_database_url_is_the_default() {
    // WAYMARK_OBSERVABILITY_DATABASE_URL is not set in the test environment.
    let config = ObservabilityConfig::from_env(&SecretString::from("postgres://prod/waymark"))
        .expect("default url is valid");
    let Db::Postgres(postgres) = config.db;
    assert_eq!(postgres.url.expose_secret(), "postgres://prod/waymark");
}
