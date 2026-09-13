use waymark_secret_string::SecretString;

use super::*;

#[test]
fn postgres_scheme_dispatches() {
    let db = Db::from_urls(
        "postgres://localhost/waymark".into(),
        "postgres://replica/waymark".into(),
    )
    .expect("postgres is supported");
    let Db::Postgres(config) = db;
    assert_eq!(
        config.write.url.expose_secret(),
        "postgres://localhost/waymark"
    );
    assert_eq!(
        config.read.url.expose_secret(),
        "postgres://replica/waymark"
    );
}

#[test]
fn postgresql_scheme_dispatches() {
    let db = Db::from_urls(
        "postgresql://localhost/waymark".into(),
        "postgresql://localhost/waymark".into(),
    )
    .expect("postgresql is supported");
    let Db::Postgres(config) = db;
    assert_eq!(
        config.write.url.expose_secret(),
        "postgresql://localhost/waymark"
    );
}

#[test]
fn unknown_scheme_is_refused() {
    let error = Db::from_urls(
        "mysql://localhost/waymark".into(),
        "mysql://localhost/waymark".into(),
    )
    .expect_err("mysql must not be dispatched");
    assert!(matches!(
        error,
        FromEnvError::UnsupportedScheme { scheme } if scheme == "mysql"
    ));
}

#[test]
fn schemeless_url_is_refused() {
    let error = Db::from_urls("localhost/waymark".into(), "localhost/waymark".into())
        .expect_err("a scheme is required");
    assert!(matches!(
        error,
        FromEnvError::UnsupportedScheme { scheme } if scheme.is_empty()
    ));
}

#[test]
fn a_read_url_of_another_backend_is_refused() {
    let error = Db::from_urls(
        "postgres://localhost/waymark".into(),
        "mysql://localhost/waymark".into(),
    )
    .expect_err("the reads must go to the backend the writes go to");
    assert!(matches!(
        error,
        FromEnvError::ReadSchemeDiffers { write, read } if write == "postgres" && read == "mysql"
    ));
}

#[test]
fn main_database_url_is_the_default_for_both() {
    // Neither WAYMARK_OBSERVABILITY_DATABASE_URL nor
    // WAYMARK_OBSERVABILITY_READ_DATABASE_URL is set in the test environment.
    let config = ObservabilityConfig::from_env(&SecretString::from("postgres://prod/waymark"))
        .expect("default url is valid");
    let Db::Postgres(postgres) = config.db;
    assert_eq!(
        postgres.write.url.expose_secret(),
        "postgres://prod/waymark"
    );
    assert_eq!(postgres.read.url.expose_secret(), "postgres://prod/waymark");
}
