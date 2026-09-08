use super::*;

#[test]
fn each_pool_takes_its_url_and_its_own_statement_timeout() {
    // No WAYMARK_OBSERVABILITY_POSTGRES_* variable is set in the test
    // environment.
    let config = PostgresConfig::from_urls_and_env(
        "postgres://primary/waymark".into(),
        "postgres://replica/waymark".into(),
    )
    .expect("the defaults are valid");

    assert_eq!(
        config.write.url.expose_secret(),
        "postgres://primary/waymark"
    );
    assert_eq!(
        config.write.statement_timeout.get(),
        std::time::Duration::from_secs(600),
    );
    assert_eq!(
        config.read.url.expose_secret(),
        "postgres://replica/waymark"
    );
    assert_eq!(
        config.read.statement_timeout.get(),
        std::time::Duration::from_secs(10),
    );
}
