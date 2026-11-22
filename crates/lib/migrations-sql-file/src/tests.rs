//! Tests of the SQL-file migration and of the macro that makes one.

use waymark_migrations_engine_migration::{
    HasMigrationIdentity as _, HasMigrationTransactionSupport as _, MigrationRevision,
    MigrationTransactionSupport, MigrationVersion, PostValidateMigration as _,
    PreValidateMigration as _, RunMigration as _,
};

crate::source_checksums_test!("tests_fixtures");

/// The fixture file, as the macro makes it; a `static`, so the macro's
/// expansion is shown to be a constant expression.
static ADD_THE_WIDGETS_TABLE: super::Migration<sqlx::PgConnection> =
    super::migration!("tests_fixtures/0007_add_the_widgets_table.sql");

/// The content of the fixture file, read by the compiler itself.
const FIXTURE: &str = include_str!("../tests_fixtures/0007_add_the_widgets_table.sql");

#[test]
fn macro_reads_the_version_and_the_description_from_the_file_name() {
    assert_eq!(ADD_THE_WIDGETS_TABLE.version, MigrationVersion::from_raw(7));
    assert_eq!(ADD_THE_WIDGETS_TABLE.description, "add the widgets table");
}

#[test]
fn macro_embeds_the_content_and_its_sha_256_digest() {
    assert_eq!(ADD_THE_WIDGETS_TABLE.sql, FIXTURE);
    assert_eq!(
        ADD_THE_WIDGETS_TABLE.source_digest,
        waymark_migrations_source_checksums_core::source_digest(FIXTURE.as_bytes())
    );
}

#[test]
fn macro_reads_the_revision_from_the_header_of_the_file() {
    assert_eq!(
        ADD_THE_WIDGETS_TABLE.revision,
        MigrationRevision::from_static("add-the-widgets-table-1")
    );
}

#[test]
fn macro_reads_the_transaction_support_from_the_header_of_the_file() {
    let cannot: super::Migration<sqlx::PgConnection> =
        super::migration!("tests_fixtures/0008_index_the_widgets.sql");

    assert!(matches!(
        ADD_THE_WIDGETS_TABLE.migration_transaction_support(),
        MigrationTransactionSupport::Supported
    ));
    assert!(matches!(
        cannot.migration_transaction_support(),
        MigrationTransactionSupport::Unsupported
    ));
}

#[test]
fn identity_is_the_version_the_description_the_revision_and_the_source_digest() {
    let identity = ADD_THE_WIDGETS_TABLE.migration_identity();

    assert_eq!(identity.version, ADD_THE_WIDGETS_TABLE.version);
    assert_eq!(identity.description, ADD_THE_WIDGETS_TABLE.description);
    assert_eq!(identity.revision, &ADD_THE_WIDGETS_TABLE.revision);
    assert_eq!(identity.source_digest, ADD_THE_WIDGETS_TABLE.source_digest);
}

/// The connect options of the test database, every connection scoped to
/// `schema`.
fn scoped_options(schema: &str) -> sqlx::postgres::PgConnectOptions {
    let options: sqlx::postgres::PgConnectOptions = waymark_support_integration::LOCAL_POSTGRES_DSN
        .expose_secret()
        .parse()
        .expect("parse test DSN");

    options.options([("search_path", schema)])
}

#[tokio::test]
async fn run_sends_every_statement_of_the_file() {
    let schema = "migrations_sql_file_run";
    let bootstrap = waymark_support_test::postgres_setup().await;

    sqlx::query(sqlx::AssertSqlSafe(format!(
        r#"
DROP SCHEMA IF EXISTS "{schema}" CASCADE
        "#
    )))
    .execute(&bootstrap)
    .await
    .expect("drop leftover test schema");
    sqlx::query(sqlx::AssertSqlSafe(format!(
        r#"
CREATE SCHEMA "{schema}"
        "#
    )))
    .execute(&bootstrap)
    .await
    .expect("create test schema");

    let mut connection =
        <sqlx::PgConnection as sqlx::Connection>::connect_with(&scoped_options(schema))
            .await
            .expect("connect to the test schema");

    ADD_THE_WIDGETS_TABLE
        .pre_validate_migration(&mut connection)
        .await
        .expect("the pre-validation passes");
    ADD_THE_WIDGETS_TABLE
        .run_migration(&mut connection)
        .await
        .expect("the migration runs");
    ADD_THE_WIDGETS_TABLE
        .post_validate_migration(&mut connection)
        .await
        .expect("the post-validation passes");

    let label: String = sqlx::query_scalar(
        r#"
SELECT label
FROM widgets
WHERE id = 1
        "#,
    )
    .fetch_one(&mut connection)
    .await
    .expect("read the row the file's second statement wrote");

    assert_eq!(label, "the first widget");

    sqlx::query(sqlx::AssertSqlSafe(format!(
        r#"
DROP SCHEMA "{schema}" CASCADE
        "#
    )))
    .execute(&bootstrap)
    .await
    .expect("drop test schema");
}
