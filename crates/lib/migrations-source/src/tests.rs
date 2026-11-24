//! Tests of what a migration source does through the migration it holds,
//! and of the macro that makes an array of them.

use waymark_migrations_engine_migration::{
    HasMigrationIdentity as _, HasMigrationTransactionSupport as _, MigrationIdentity,
    MigrationRevision, MigrationTransactionSupport, MigrationVersion, PostValidateMigration as _,
    PreValidateMigration as _, RunMigration as _,
};

crate::source_checksums_tests!(sql_files("tests_fixtures"));

use super::{Error, MigrationSource};

/// The error of the test Rust-code migration's operations.
#[derive(Debug, thiserror::Error)]
#[error("test code error")]
struct TestCodeError;

mockall::mock! {
    TestOperations {
        fn pre_validate_migration(&self) -> Result<(), TestCodeError>;

        fn run_migration(&self) -> Result<(), TestCodeError>;

        fn post_validate_migration(&self) -> Result<(), TestCodeError>;
    }
}

/// The revision of the test Rust-code migration.
static TEST_CODE_REVISION: MigrationRevision = MigrationRevision::from_static("move-the-widgets-1");

/// The Rust-code migration of the tests: version 27, unable to run inside
/// a transaction, with mocked operations.
struct TestCodeMigration {
    /// What the migration's operations do.
    operations: MockTestOperations,
}

impl waymark_migrations_engine_migration::HasConnection for TestCodeMigration {
    type Connection = sqlx::PgConnection;
}

impl waymark_migrations_engine_migration::HasMigrationIdentity for TestCodeMigration {
    fn migration_identity(&self) -> MigrationIdentity<'_> {
        MigrationIdentity {
            version: MigrationVersion::from_raw(27),
            description: "move the widgets",
            revision: &TEST_CODE_REVISION,
            source_digest: "sha256:09",
        }
    }
}

impl waymark_migrations_engine_migration::HasMigrationTransactionSupport for TestCodeMigration {
    fn migration_transaction_support(&self) -> MigrationTransactionSupport {
        MigrationTransactionSupport::Unsupported
    }
}

impl waymark_migrations_engine_migration::PreValidateMigration for TestCodeMigration {
    type Error = TestCodeError;

    async fn pre_validate_migration(
        &self,
        _connection: &mut sqlx::PgConnection,
    ) -> Result<(), Self::Error> {
        self.operations.pre_validate_migration()
    }
}

impl waymark_migrations_engine_migration::RunMigration for TestCodeMigration {
    type Error = TestCodeError;

    async fn run_migration(&self, _connection: &mut sqlx::PgConnection) -> Result<(), Self::Error> {
        self.operations.run_migration()
    }
}

impl waymark_migrations_engine_migration::PostValidateMigration for TestCodeMigration {
    type Error = TestCodeError;

    async fn post_validate_migration(
        &self,
        _connection: &mut sqlx::PgConnection,
    ) -> Result<(), Self::Error> {
        self.operations.post_validate_migration()
    }
}

/// The SQL-file migration of version 7 whose run is `sql`, able to run
/// inside a transaction.
fn sql_file_migration(
    sql: &'static str,
) -> waymark_migrations_sql_file::Migration<sqlx::PgConnection> {
    waymark_migrations_sql_file::Migration {
        version: MigrationVersion::from_raw(7),
        description: "add the widgets table",
        sql,
        revision: MigrationRevision::from_static("add-the-widgets-table-1"),
        source_digest: "sha256:03",
        transaction_support: MigrationTransactionSupport::Supported,
        connection_type: core::marker::PhantomData,
    }
}

/// The test Rust-code migration over `operations`, with its type erased.
fn rust_code_migration(
    operations: MockTestOperations,
) -> waymark_migrations_rust_code::Migration<sqlx::PgConnection> {
    waymark_migrations_rust_code::Migration::new(TestCodeMigration { operations })
}

/// The Rust-code migration this crate stands in for in the test of the
/// macro: the macro asks the crate of a Rust-code migration for its
/// `migration()`.
pub fn migration() -> waymark_migrations_rust_code::Migration<sqlx::PgConnection> {
    rust_code_migration(MockTestOperations::new())
}

/// A connection of its own to the test database.
async fn connect() -> sqlx::PgConnection {
    let _bootstrap = waymark_support_test::postgres_setup().await;

    let options: sqlx::postgres::PgConnectOptions = waymark_support_integration::LOCAL_POSTGRES_DSN
        .expose_secret()
        .parse()
        .expect("parse test DSN");

    <sqlx::PgConnection as sqlx::Connection>::connect_with(&options)
        .await
        .expect("connect to the test database")
}

#[test]
fn macro_makes_an_sql_file_source_of_a_path_and_a_rust_code_source_of_a_crate() {
    let migrations: [MigrationSource<sqlx::PgConnection>; 2] = crate::migrations![
        "tests_fixtures/0001_add_the_widgets_table.sql",
        waymark_migrations_source,
    ];

    let [
        MigrationSource::SqlFile(sql_file),
        MigrationSource::RustCode(rust_code),
    ] = &migrations
    else {
        panic!("the array has an SQL-file migration and then a Rust-code one");
    };
    assert_eq!(sql_file.version, MigrationVersion::from_raw(1));
    assert_eq!(sql_file.description, "add the widgets table");
    assert_eq!(
        rust_code.migration_identity().version,
        MigrationVersion::from_raw(27)
    );
}

#[test]
fn macro_makes_an_empty_array_of_nothing() {
    let migrations: [MigrationSource<sqlx::PgConnection>; 0] = crate::migrations![];

    assert!(migrations.is_empty());
}

#[test]
fn sql_file_source_has_the_identity_and_transaction_support_of_its_migration() {
    let source = MigrationSource::SqlFile(sql_file_migration(""));

    let identity = source.migration_identity();

    assert_eq!(identity.version, MigrationVersion::from_raw(7));
    assert_eq!(identity.description, "add the widgets table");
    assert_eq!(
        identity.revision,
        &MigrationRevision::from_static("add-the-widgets-table-1")
    );
    assert_eq!(identity.source_digest, "sha256:03");
    assert!(matches!(
        source.migration_transaction_support(),
        MigrationTransactionSupport::Supported
    ));
}

#[test]
fn rust_code_source_has_the_identity_and_transaction_support_of_its_migration() {
    let source = MigrationSource::RustCode(rust_code_migration(MockTestOperations::new()));

    let identity = source.migration_identity();

    assert_eq!(identity.version, MigrationVersion::from_raw(27));
    assert_eq!(identity.description, "move the widgets");
    assert_eq!(identity.revision, &TEST_CODE_REVISION);
    assert_eq!(identity.source_digest, "sha256:09");
    assert!(matches!(
        source.migration_transaction_support(),
        MigrationTransactionSupport::Unsupported
    ));
}

#[tokio::test]
async fn sql_file_source_operations_reach_its_migration() {
    let source = MigrationSource::SqlFile(sql_file_migration(
        r#"
CREATE TEMPORARY TABLE migrations_source_widgets (
    id INTEGER PRIMARY KEY,
    label TEXT NOT NULL
);

INSERT INTO migrations_source_widgets (id, label)
VALUES (1, 'the first widget');
        "#,
    ));
    let mut connection = connect().await;

    source
        .pre_validate_migration(&mut connection)
        .await
        .expect("the pre-validation passes");
    source
        .run_migration(&mut connection)
        .await
        .expect("the migration runs");
    source
        .post_validate_migration(&mut connection)
        .await
        .expect("the post-validation passes");

    let label: String = sqlx::query_scalar(
        r#"
SELECT label
FROM migrations_source_widgets
WHERE id = 1
        "#,
    )
    .fetch_one(&mut connection)
    .await
    .expect("read the row the migration's second statement wrote");

    assert_eq!(label, "the first widget");
}

#[tokio::test]
async fn rust_code_source_operations_reach_its_migration() {
    let mut operations = MockTestOperations::new();
    operations
        .expect_pre_validate_migration()
        .times(1)
        .returning(|| Ok(()));
    operations
        .expect_run_migration()
        .times(1)
        .returning(|| Ok(()));
    operations
        .expect_post_validate_migration()
        .times(1)
        .returning(|| Ok(()));

    // The mock is moved into the migration: its expectations are checked
    // when the source is dropped.
    let source = MigrationSource::RustCode(rust_code_migration(operations));
    let mut connection = connect().await;

    source
        .pre_validate_migration(&mut connection)
        .await
        .expect("the pre-validation passes");
    source
        .run_migration(&mut connection)
        .await
        .expect("the migration runs");
    source
        .post_validate_migration(&mut connection)
        .await
        .expect("the post-validation passes");
}

#[tokio::test]
async fn sql_file_source_run_error_is_an_sql_file_error() {
    let source = MigrationSource::SqlFile(sql_file_migration(
        r#"
SELECT label
FROM migrations_source_no_such_table
        "#,
    ));
    let mut connection = connect().await;

    let ran = source.run_migration(&mut connection).await;

    assert!(matches!(ran, Err(Error::SqlFile(sqlx::Error::Database(_)))));
}

#[tokio::test]
async fn rust_code_source_operation_errors_are_rust_code_errors() {
    let mut operations = MockTestOperations::new();
    operations
        .expect_pre_validate_migration()
        .times(1)
        .returning(|| Err(TestCodeError));
    operations
        .expect_run_migration()
        .times(1)
        .returning(|| Err(TestCodeError));
    operations
        .expect_post_validate_migration()
        .times(1)
        .returning(|| Err(TestCodeError));

    let source = MigrationSource::RustCode(rust_code_migration(operations));
    let mut connection = connect().await;

    let pre_validated = source.pre_validate_migration(&mut connection).await;
    let ran = source.run_migration(&mut connection).await;
    let post_validated = source.post_validate_migration(&mut connection).await;

    for operation in [pre_validated, ran, post_validated] {
        let Err(Error::RustCode(error)) = operation else {
            panic!("the operation fails with the Rust-code migration's error");
        };

        assert!(error.inner().is::<TestCodeError>());
        assert_eq!(
            Error::RustCode(error).to_string(),
            "rust code migration: test code error"
        );
    }
}

#[test]
fn sql_file_error_shows_which_source_failed_and_its_error() {
    let error = Error::SqlFile(sqlx::Error::RowNotFound);

    assert_eq!(
        error.to_string(),
        format!("sql file migration: {}", sqlx::Error::RowNotFound)
    );
}

/// Accept only the types that are migrations running through a Postgres
/// connection, and that several threads can share.
fn assert_is_a_shareable_migration<T>()
where
    T: waymark_migrations_engine_migration::Migration<Connection = sqlx::PgConnection>,
    T: Send + Sync,
{
}

#[test]
fn source_of_a_postgres_connection_is_a_migration_threads_can_share() {
    assert_is_a_shareable_migration::<MigrationSource<sqlx::PgConnection>>();
}
