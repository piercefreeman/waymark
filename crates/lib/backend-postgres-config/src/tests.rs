use waymark_nonzero_duration::NonZeroDuration;

use super::*;

// Of the WAYMARK_DATABASE_* and WAYMARK_MIGRATIONS_* variables, only
// WAYMARK_DATABASE_URL may be set in the test environment; the tests take
// their URLs as arguments.

#[test]
fn read_write_migrate_takes_both_urls_and_their_own_settings() {
    let config = ReadWriteMigrate::from_urls_and_env(
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
fn read_write_only_takes_the_pool_alone() {
    let config = ReadWriteOnly::from_url_and_env("postgres://app@primary/waymark".into())
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
