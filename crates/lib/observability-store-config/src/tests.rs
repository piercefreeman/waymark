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
fn main_database_url_is_the_default_for_both() {
    // Neither WAYMARK_OBSERVABILITY_DATABASE_URL nor
    // WAYMARK_OBSERVABILITY_READ_DATABASE_URL is set in the test environment.
    let db = ReadWrite::from_env_urls_with_default(&SecretString::from("postgres://prod/waymark"))
        .expect("default url is valid");
    let ReadWrite::Postgres(postgres) = db;
    assert_eq!(
        postgres.write.url.expose_secret(),
        "postgres://prod/waymark"
    );
    assert_eq!(postgres.read.url.expose_secret(), "postgres://prod/waymark");
}

#[test]
fn write_only_takes_the_main_database_url_by_default() {
    // WAYMARK_OBSERVABILITY_DATABASE_URL is not set in the test environment.
    let db = WriteOnly::from_env_url_with_default(&SecretString::from("postgres://prod/waymark"))
        .expect("default url is valid");
    let WriteOnly::Postgres(postgres) = db;
    assert_eq!(
        postgres.write.url.expose_secret(),
        "postgres://prod/waymark"
    );
}

#[test]
fn read_only_takes_the_main_database_url_by_default() {
    // Neither WAYMARK_OBSERVABILITY_DATABASE_URL nor
    // WAYMARK_OBSERVABILITY_READ_DATABASE_URL is set in the test environment.
    let db = ReadOnly::from_env_url_with_default(&SecretString::from("postgres://prod/waymark"))
        .expect("default url is valid");
    let ReadOnly::Postgres(postgres) = db;
    assert_eq!(postgres.read.url.expose_secret(), "postgres://prod/waymark");
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
fn migrations_take_the_main_migrations_url_by_default() {
    // None of the observability URL variables is set in the test
    // environment.
    let db = ReadWriteMigrate::from_env_urls_with_default(
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
fn migrate_only_takes_the_main_migrations_url_by_default() {
    // Neither WAYMARK_OBSERVABILITY_DATABASE_URL nor
    // WAYMARK_OBSERVABILITY_MIGRATIONS_DATABASE_URL is set in the test
    // environment.
    let db = MigrateOnly::from_env_url_with_default(&SecretString::from(
        "postgres://owner@prod/waymark",
    ))
    .expect("default url is valid");
    let MigrateOnly::Postgres(postgres) = db;
    assert_eq!(
        postgres.migrations.url.expose_secret(),
        "postgres://owner@prod/waymark"
    );
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
