use waymark_nonzero_duration::NonZeroDuration;

use super::*;

// Of the WAYMARK_DATABASE_* and WAYMARK_MIGRATIONS_* variables, only
// WAYMARK_DATABASE_URL may be set in the test environment; the tests take
// their URLs as arguments.

#[test]
fn read_write_migrate_takes_both_urls_and_their_own_settings() {
    let config = ReadWriteMigrate::from_urls_and_env(
        DefaultsFor::Executor,
        "postgres://app@primary/waymark".into(),
        "postgres://owner@primary/waymark".into(),
    )
    .expect("the defaults are valid");

    assert_eq!(
        config.read_write.url.expose_secret(),
        "postgres://app@primary/waymark"
    );
    assert_eq!(config.read_write.max_connections.get(), 25);
    assert_eq!(
        config.read_write.acquire_timeout.get(),
        std::time::Duration::from_secs(30),
    );
    assert_eq!(
        config
            .read_write
            .statement_timeout
            .map(NonZeroDuration::get),
        Some(std::time::Duration::from_secs(60)),
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
fn read_write_only_takes_the_pool_and_the_migrations_check_connection() {
    let config = ReadWriteOnly::from_url_and_env(
        DefaultsFor::Executor,
        "postgres://app@primary/waymark".into(),
    )
    .expect("the defaults are valid");

    assert_eq!(
        config.read_write.url.expose_secret(),
        "postgres://app@primary/waymark"
    );
    assert_eq!(
        config
            .read_write
            .statement_timeout
            .map(NonZeroDuration::get),
        Some(std::time::Duration::from_secs(60)),
    );
    assert_eq!(
        config.migrations_check.url.expose_secret(),
        "postgres://app@primary/waymark"
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
fn the_bridge_defaults_its_pool_to_ten_connections() {
    let config = ReadWriteOnly::from_url_and_env(
        DefaultsFor::Bridge,
        "postgres://app@primary/waymark".into(),
    )
    .expect("the defaults are valid");

    assert_eq!(config.read_write.max_connections.get(), 10);
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
    let main_pool = [
        "WAYMARK_DATABASE_MAX_CONNECTIONS",
        "WAYMARK_DATABASE_STATEMENT_TIMEOUT_MS",
        "WAYMARK_DATABASE_ACQUIRE_TIMEOUT_MS",
    ];

    assert_eq!(pool_variables::<crate::profiles::ExecutorPool>(), main_pool);
    assert_eq!(pool_variables::<crate::profiles::BridgePool>(), main_pool);
    assert_eq!(
        connection_variables::<crate::profiles::Migrations>(),
        ["WAYMARK_MIGRATIONS_STATEMENT_TIMEOUT_MS"],
    );
}
