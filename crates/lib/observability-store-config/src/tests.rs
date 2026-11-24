use waymark_secret_string::SecretString;

use super::*;

#[test]
fn postgres_scheme_dispatches() {
    let db = ReadWrite::from_urls_and_env(
        "postgres://localhost/waymark".into(),
        "postgres://replica/waymark".into(),
    )
    .expect("postgres is supported");
    let ReadWrite::Postgres(config) = db;
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
    let db = ReadWrite::from_urls_and_env(
        "postgresql://localhost/waymark".into(),
        "postgresql://localhost/waymark".into(),
    )
    .expect("postgresql is supported");
    let ReadWrite::Postgres(config) = db;
    assert_eq!(
        config.write.url.expose_secret(),
        "postgresql://localhost/waymark"
    );
}

#[test]
fn the_two_postgres_spellings_are_one_backend() {
    let db = ReadWrite::from_urls_and_env(
        "postgres://localhost/waymark".into(),
        "postgresql://replica/waymark".into(),
    )
    .expect("both spellings are the postgres store");
    let ReadWrite::Postgres(config) = db;
    assert_eq!(
        config.read.url.expose_secret(),
        "postgresql://replica/waymark"
    );
}

#[test]
fn unknown_scheme_is_refused() {
    let error = ReadWrite::from_urls_and_env(
        "mysql://localhost/waymark".into(),
        "mysql://localhost/waymark".into(),
    )
    .expect_err("mysql must not be dispatched");
    assert!(matches!(
        error,
        read_write::FromEnvError::UnsupportedScheme { scheme } if scheme == "mysql"
    ));
}

#[test]
fn schemeless_url_is_refused() {
    let error =
        ReadWrite::from_urls_and_env("localhost/waymark".into(), "localhost/waymark".into())
            .expect_err("a scheme is required");
    assert!(matches!(
        error,
        read_write::FromEnvError::UnsupportedScheme { scheme } if scheme.is_empty()
    ));
}

#[test]
fn a_read_url_of_another_backend_is_refused() {
    let error = ReadWrite::from_urls_and_env(
        "postgres://localhost/waymark".into(),
        "mysql://localhost/waymark".into(),
    )
    .expect_err("the reads must go to the backend the writes go to");
    assert!(matches!(
        error,
        read_write::FromEnvError::ReadSchemeDiffers { write, read } if write == "postgres" && read == "mysql"
    ));
}

#[test]
fn the_urls_come_from_the_documented_variables() {
    assert_eq!(
        crate::DATABASE_URL_VARIABLE,
        "WAYMARK_OBSERVABILITY_DATABASE_URL"
    );
    assert_eq!(
        crate::READ_DATABASE_URL_VARIABLE,
        "WAYMARK_OBSERVABILITY_READ_DATABASE_URL"
    );
    assert_eq!(
        crate::MIGRATIONS_DATABASE_URL_VARIABLE,
        "WAYMARK_OBSERVABILITY_MIGRATIONS_DATABASE_URL"
    );
}

#[test]
fn write_only_refuses_an_unknown_scheme() {
    let error = WriteOnly::from_url_and_env("mysql://localhost/waymark".into())
        .expect_err("mysql must not be dispatched");
    assert!(matches!(
        error,
        write_only::FromEnvError::UnsupportedScheme { scheme } if scheme == "mysql"
    ));
}

#[test]
fn read_only_refuses_an_unknown_scheme() {
    let error = ReadOnly::from_url_and_env("mysql://replica/waymark".into())
        .expect_err("mysql must not be dispatched");
    assert!(matches!(
        error,
        read_only::FromEnvError::UnsupportedScheme { scheme } if scheme == "mysql"
    ));
}

#[test]
fn given_unified_urls_stand_in_for_the_variables_that_are_not_set() {
    // None of the observability URL variables is set in the test
    // environment.
    let db = ReadWriteMigrate::from_env_with_unified_urls(
        &SecretString::from("postgres://app@prod/waymark"),
        &SecretString::from("postgres://owner@prod/waymark"),
    )
    .expect("default url is valid");
    let ReadWriteMigrate::Postgres(postgres) = db;
    assert_eq!(
        postgres.write.url.expose_secret(),
        "postgres://app@prod/waymark"
    );
    assert_eq!(
        postgres.read.url.expose_secret(),
        "postgres://app@prod/waymark"
    );
    assert_eq!(
        postgres.migrations.url.expose_secret(),
        "postgres://owner@prod/waymark"
    );
}

#[test]
fn a_migrations_url_of_another_backend_is_refused() {
    let error = ReadWriteMigrate::from_urls_and_env(
        "postgres://localhost/waymark".into(),
        "postgres://localhost/waymark".into(),
        "mysql://localhost/waymark".into(),
    )
    .expect_err("the migrations must go to the backend the writes go to");
    assert!(matches!(
        error,
        read_write_migrate::FromEnvError::MigrationsSchemeDiffers { write, migrations }
            if write == "postgres" && migrations == "mysql"
    ));
}

#[test]
fn migrate_only_refuses_an_unknown_scheme() {
    let error = MigrateOnly::from_url_and_env("mysql://localhost/waymark".into())
        .expect_err("mysql must not be dispatched");
    assert!(matches!(
        error,
        migrate_only::FromEnvError::UnsupportedScheme { scheme } if scheme == "mysql"
    ));
}
