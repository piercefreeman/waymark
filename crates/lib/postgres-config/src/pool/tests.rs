use super::*;

// No WAYMARK_POSTGRES_CONFIG_TEST_* variable is set in the test
// environment.

/// A pool profile whose variables are never set.
struct TestPool;

impl Profile for TestPool {
    const PREFIX: &'static str = "POSTGRES_CONFIG_TEST";

    const MAX_CONNECTIONS_DEFAULT: NonZeroU32 = NonZeroU32::new(7).unwrap();

    const STATEMENT_TIMEOUT_DEFAULT: Option<NonZeroDuration> = None;

    const ACQUIRE_TIMEOUT_DEFAULT: NonZeroDuration =
        NonZeroDuration::from_nonzero_secs(NonZeroU64::new(3).unwrap());
}

#[test]
fn a_pool_takes_the_profile_defaults() {
    let config = Config::from_url_and_env::<TestPool>("postgres://primary/waymark".into())
        .expect("the defaults are valid");

    assert_eq!(config.url.expose_secret(), "postgres://primary/waymark");
    assert_eq!(config.max_connections.get(), 7);
    assert!(config.statement_timeout.is_none());
    assert_eq!(
        config.acquire_timeout.get(),
        std::time::Duration::from_secs(3)
    );
}
