use waymark_nonzero_duration::NonZeroDuration;

use super::*;

#[test]
fn each_pool_takes_its_url_and_its_own_statement_timeout() {
    // No WAYMARK_OBSERVABILITY_POSTGRES_* variable is set in the test
    // environment.
    let config = ReadWrite::from_urls_and_env(
        "postgres://primary/waymark".into(),
        "postgres://replica/waymark".into(),
    )
    .expect("the defaults are valid");

    assert_eq!(
        config.write.url.expose_secret(),
        "postgres://primary/waymark"
    );
    assert_eq!(
        config.write.statement_timeout.map(NonZeroDuration::get),
        Some(std::time::Duration::from_secs(30)),
    );
    assert_eq!(
        config.read.url.expose_secret(),
        "postgres://replica/waymark"
    );
    assert_eq!(
        config.read.statement_timeout.map(NonZeroDuration::get),
        Some(std::time::Duration::from_secs(10)),
    );
    assert_eq!(
        config.migrations_check.url.expose_secret(),
        "postgres://primary/waymark"
    );
    assert_eq!(
        config
            .migrations_check
            .statement_timeout
            .map(NonZeroDuration::get),
        Some(std::time::Duration::from_secs(600)),
    );
}

#[test]
fn migrations_take_their_url_and_their_own_statement_timeout() {
    // No WAYMARK_OBSERVABILITY_POSTGRES_* variable is set in the test
    // environment.
    let config = ReadWriteMigrate::from_urls_and_env(
        "postgres://writer@primary/waymark".into(),
        "postgres://replica/waymark".into(),
        "postgres://owner@primary/waymark".into(),
    )
    .expect("the defaults are valid");

    assert_eq!(
        config.write.url.expose_secret(),
        "postgres://writer@primary/waymark"
    );
    assert_eq!(
        config.write.statement_timeout.map(NonZeroDuration::get),
        Some(std::time::Duration::from_secs(30)),
    );
    assert_eq!(
        config.migrations.url.expose_secret(),
        "postgres://owner@primary/waymark"
    );
    assert_eq!(
        config
            .migrations
            .statement_timeout
            .map(NonZeroDuration::get),
        Some(std::time::Duration::from_secs(600)),
    );
}

#[test]
fn migrate_only_takes_the_migrations_connection_alone() {
    let config = MigrateOnly::from_url_and_env("postgres://owner@primary/waymark".into())
        .expect("the defaults are valid");

    assert_eq!(
        config.migrations.url.expose_secret(),
        "postgres://owner@primary/waymark"
    );
    assert_eq!(
        config
            .migrations
            .statement_timeout
            .map(NonZeroDuration::get),
        Some(std::time::Duration::from_secs(600)),
    );
}

#[test]
fn write_only_checks_the_migrations_through_the_write_url() {
    let config = WriteOnly::from_url_and_env("postgres://primary/waymark".into())
        .expect("the defaults are valid");

    assert_eq!(
        config.write.url.expose_secret(),
        "postgres://primary/waymark"
    );
    assert_eq!(
        config.write.statement_timeout.map(NonZeroDuration::get),
        Some(std::time::Duration::from_secs(30)),
    );
    assert_eq!(
        config.migrations_check.url.expose_secret(),
        "postgres://primary/waymark"
    );
    assert_eq!(
        config
            .migrations_check
            .statement_timeout
            .map(NonZeroDuration::get),
        Some(std::time::Duration::from_secs(600)),
    );
}

#[test]
fn read_only_checks_the_migrations_through_the_read_url() {
    let config = ReadOnly::from_url_and_env("postgres://replica/waymark".into())
        .expect("the defaults are valid");

    assert_eq!(
        config.read.url.expose_secret(),
        "postgres://replica/waymark"
    );
    assert_eq!(
        config.read.statement_timeout.map(NonZeroDuration::get),
        Some(std::time::Duration::from_secs(10)),
    );
    assert_eq!(
        config.migrations_check.url.expose_secret(),
        "postgres://replica/waymark"
    );
    assert_eq!(
        config
            .migrations_check
            .statement_timeout
            .map(NonZeroDuration::get),
        Some(std::time::Duration::from_secs(600)),
    );
}

/// The variables a pool profile reads, built by a hand copy of
/// `waymark-postgres-config`'s naming: it pins the profile's prefix only.
fn pool_variables<Profile>() -> [String; 3]
where
    Profile: waymark_postgres_config::pool::Profile,
{
    [
        format!("WAYMARK_{}_MAX_CONNECTIONS", Profile::PREFIX),
        format!("WAYMARK_{}_STATEMENT_TIMEOUT_MS", Profile::PREFIX),
        format!("WAYMARK_{}_ACQUIRE_TIMEOUT_MS", Profile::PREFIX),
    ]
}

/// The variable a connection profile reads, built by a hand copy of
/// `waymark-postgres-config`'s naming: it pins the profile's prefix only.
fn connection_variables<Profile>() -> [String; 1]
where
    Profile: waymark_postgres_config::connection::Profile,
{
    [format!("WAYMARK_{}_STATEMENT_TIMEOUT_MS", Profile::PREFIX)]
}

#[test]
fn the_profiles_read_the_documented_variables() {
    assert_eq!(
        pool_variables::<crate::profiles::WritePool>(),
        [
            "WAYMARK_OBSERVABILITY_POSTGRES_MAX_CONNECTIONS",
            "WAYMARK_OBSERVABILITY_POSTGRES_STATEMENT_TIMEOUT_MS",
            "WAYMARK_OBSERVABILITY_POSTGRES_ACQUIRE_TIMEOUT_MS",
        ],
    );
    assert_eq!(
        pool_variables::<crate::profiles::ReadPool>(),
        [
            "WAYMARK_OBSERVABILITY_POSTGRES_READ_MAX_CONNECTIONS",
            "WAYMARK_OBSERVABILITY_POSTGRES_READ_STATEMENT_TIMEOUT_MS",
            "WAYMARK_OBSERVABILITY_POSTGRES_READ_ACQUIRE_TIMEOUT_MS",
        ],
    );
    assert_eq!(
        connection_variables::<crate::profiles::Migrations>(),
        ["WAYMARK_OBSERVABILITY_POSTGRES_MIGRATIONS_STATEMENT_TIMEOUT_MS"],
    );
}
