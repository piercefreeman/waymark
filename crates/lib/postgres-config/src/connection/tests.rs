use super::*;

// No WAYMARK_POSTGRES_CONFIG_TEST_* variable is set in the test
// environment.

/// A connection profile whose variables are never set.
struct TestConnection;

impl Profile for TestConnection {
    const PREFIX: &'static str = "POSTGRES_CONFIG_TEST";

    const STATEMENT_TIMEOUT_DEFAULT: Option<NonZeroDuration> = NonZeroDuration::from_secs(9);
}

#[test]
fn a_connection_takes_the_profile_defaults() {
    let config = Config::from_url_and_env::<TestConnection>("postgres://primary/waymark".into())
        .expect("the defaults are valid");

    assert_eq!(config.url.expose_secret(), "postgres://primary/waymark");
    assert_eq!(
        config.statement_timeout.map(NonZeroDuration::get),
        Some(std::time::Duration::from_secs(9)),
    );
}
