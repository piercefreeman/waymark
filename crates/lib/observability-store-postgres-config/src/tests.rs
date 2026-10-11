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
fn write_only_takes_the_write_pool_alone() {
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
}

#[test]
fn read_only_takes_the_read_pool_alone() {
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
}
