use std::collections::BTreeMap;

use super::*;

/// The outcome of [`first_set`] over `variables` with `set` standing in
/// for the environment, and the variables it read, in the order it read
/// them.
fn first_set_among(
    variables: &[&'static str],
    set: &[(&'static str, &'static str)],
) -> (Option<String>, Vec<&'static str>) {
    let set: BTreeMap<&str, &str> = set.iter().copied().collect();
    let mut read = Vec::new();

    let url = first_set(variables, |variable| {
        read.push(variable);

        Ok::<_, core::convert::Infallible>(set.get(variable).map(|url| SecretString::from(*url)))
    })
    .expect("the reader never fails");

    (url.map(|url| url.expose_secret().to_owned()), read)
}

#[test]
fn database_url_comes_from_the_dedicated_variable_before_the_unified_one() {
    let variables = database_url_variables("WAYMARK_TEST_DATABASE_URL");

    let (both, read_for_both) = first_set_among(
        &variables,
        &[
            ("WAYMARK_TEST_DATABASE_URL", "postgres://dedicated/waymark"),
            ("WAYMARK_DATABASE_URL", "postgres://unified/waymark"),
        ],
    );
    let (unified, read_for_unified) = first_set_among(
        &variables,
        &[("WAYMARK_DATABASE_URL", "postgres://unified/waymark")],
    );
    let (neither, _read) = first_set_among(&variables, &[]);

    assert_eq!(both.as_deref(), Some("postgres://dedicated/waymark"));
    // The unified variable is not read when the dedicated one is set.
    assert_eq!(read_for_both, ["WAYMARK_TEST_DATABASE_URL"]);
    assert_eq!(unified.as_deref(), Some("postgres://unified/waymark"));
    assert_eq!(
        read_for_unified,
        ["WAYMARK_TEST_DATABASE_URL", "WAYMARK_DATABASE_URL"]
    );
    assert_eq!(neither, None);
}

#[test]
fn migrations_database_url_comes_from_the_dedicated_variables_before_the_unified_ones() {
    let variables = migrations_database_url_variables(
        "WAYMARK_TEST_MIGRATIONS_DATABASE_URL",
        "WAYMARK_TEST_DATABASE_URL",
    );
    let everything = [
        (
            "WAYMARK_TEST_MIGRATIONS_DATABASE_URL",
            "postgres://dedicated-migrations/waymark",
        ),
        ("WAYMARK_TEST_DATABASE_URL", "postgres://dedicated/waymark"),
        (
            "WAYMARK_MIGRATIONS_DATABASE_URL",
            "postgres://unified-migrations/waymark",
        ),
        ("WAYMARK_DATABASE_URL", "postgres://unified/waymark"),
    ];

    // Each step takes the first variable away.
    let urls: Vec<Option<String>> = (0..=everything.len())
        .map(|skipped| {
            let set = everything.get(skipped..).unwrap_or(&[]);

            first_set_among(&variables, set).0
        })
        .collect();

    assert_eq!(
        urls,
        [
            Some("postgres://dedicated-migrations/waymark".to_owned()),
            // A database with a URL of its own is migrated there, whatever
            // the unified migrations variable is.
            Some("postgres://dedicated/waymark".to_owned()),
            Some("postgres://unified-migrations/waymark".to_owned()),
            Some("postgres://unified/waymark".to_owned()),
            None,
        ]
    );
}

#[test]
fn failed_read_stops_the_lookup() {
    let mut read = Vec::new();

    let failed = first_set(
        &database_url_variables("WAYMARK_TEST_DATABASE_URL"),
        |variable| {
            read.push(variable);

            Err::<Option<SecretString>, _>("not unicode")
        },
    );

    assert_eq!(failed.err(), Some("not unicode"));
    assert_eq!(read, ["WAYMARK_TEST_DATABASE_URL"]);
}

#[test]
fn url_that_is_set_nowhere_reports_every_variable_looked_at() {
    let error = FromEnvError::NotSet {
        variables: database_url_variables("WAYMARK_TEST_DATABASE_URL").to_vec(),
    };

    assert_eq!(
        error.to_string(),
        "none of WAYMARK_TEST_DATABASE_URL, WAYMARK_DATABASE_URL is set"
    );
}
