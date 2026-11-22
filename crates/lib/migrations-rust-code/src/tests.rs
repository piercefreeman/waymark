//! Tests of the erased migration, over a mocked migration behind it.

use waymark_migrations_engine_migration::{
    HasMigrationIdentity as _, HasMigrationTransactionSupport as _, MigrationIdentity,
    MigrationRevision, MigrationTransactionSupport, MigrationVersion, PostValidateMigration as _,
    PreValidateMigration as _, RunMigration as _,
};

/// The connection the test migrations run through: it keeps what ran
/// through it, in order.
#[derive(Debug, Default)]
struct TestConnection {
    /// The operations that ran through this connection.
    operations: Vec<&'static str>,
}

/// The error of the test migration's pre-validation.
#[derive(Debug, thiserror::Error)]
#[error("test pre-validation error")]
struct TestPreValidationError;

/// The cause of the test migration's run error.
#[derive(Debug, thiserror::Error)]
#[error("test cause")]
struct TestCauseError;

/// The error of the test migration's run, with a cause.
#[derive(Debug, thiserror::Error)]
#[error("test run error: {0}")]
struct TestRunError(#[source] TestCauseError);

/// The error of the test migration's post-validation.
#[derive(Debug, thiserror::Error)]
#[error("test post-validation error")]
struct TestPostValidationError;

mockall::mock! {
    TestOperations {
        fn pre_validate_migration(
            &self,
            connection: &mut TestConnection,
        ) -> Result<(), TestPreValidationError>;

        fn run_migration(&self, connection: &mut TestConnection) -> Result<(), TestRunError>;

        fn post_validate_migration(
            &self,
            connection: &mut TestConnection,
        ) -> Result<(), TestPostValidationError>;
    }
}

/// The migration behind the erased one in the tests: version 27, with
/// plain values for its identity and its transaction support, and mocked
/// operations.
struct TestMigration {
    /// The migration's revision.
    revision: MigrationRevision,

    /// Whether the migration can run inside a transaction.
    runs_inside_a_transaction: bool,

    /// What the migration's operations do.
    operations: MockTestOperations,
}

impl TestMigration {
    /// A test migration over `operations` that can run inside a
    /// transaction.
    fn new(operations: MockTestOperations) -> Self {
        Self {
            revision: MigrationRevision::from_static("move-the-widgets-1"),
            runs_inside_a_transaction: true,
            operations,
        }
    }
}

impl waymark_migrations_engine_migration::HasConnection for TestMigration {
    type Connection = TestConnection;
}

impl waymark_migrations_engine_migration::HasMigrationIdentity for TestMigration {
    fn migration_identity(&self) -> MigrationIdentity<'_> {
        MigrationIdentity {
            version: MigrationVersion::from_raw(27),
            description: "move the widgets",
            revision: &self.revision,
            source_digest: "sha256:07",
        }
    }
}

impl waymark_migrations_engine_migration::HasMigrationTransactionSupport for TestMigration {
    fn migration_transaction_support(&self) -> MigrationTransactionSupport {
        if self.runs_inside_a_transaction {
            MigrationTransactionSupport::Supported
        } else {
            MigrationTransactionSupport::Unsupported
        }
    }
}

impl waymark_migrations_engine_migration::PreValidateMigration for TestMigration {
    type Error = TestPreValidationError;

    async fn pre_validate_migration(
        &self,
        connection: &mut TestConnection,
    ) -> Result<(), Self::Error> {
        self.operations.pre_validate_migration(connection)
    }
}

impl waymark_migrations_engine_migration::RunMigration for TestMigration {
    type Error = TestRunError;

    async fn run_migration(&self, connection: &mut TestConnection) -> Result<(), Self::Error> {
        self.operations.run_migration(connection)
    }
}

impl waymark_migrations_engine_migration::PostValidateMigration for TestMigration {
    type Error = TestPostValidationError;

    async fn post_validate_migration(
        &self,
        connection: &mut TestConnection,
    ) -> Result<(), Self::Error> {
        self.operations.post_validate_migration(connection)
    }
}

#[test]
fn erased_migration_has_the_identity_of_the_migration_behind() {
    let migration = super::Migration::new(TestMigration::new(MockTestOperations::new()));

    let identity = migration.migration_identity();

    assert_eq!(identity.version, MigrationVersion::from_raw(27));
    assert_eq!(identity.description, "move the widgets");
    assert_eq!(
        identity.revision,
        &MigrationRevision::from_static("move-the-widgets-1")
    );
    assert_eq!(identity.source_digest, "sha256:07");
}

#[test]
fn erased_migration_has_the_transaction_support_of_the_migration_behind() {
    let can = super::Migration::new(TestMigration::new(MockTestOperations::new()));

    let mut behind = TestMigration::new(MockTestOperations::new());
    behind.runs_inside_a_transaction = false;
    let cannot = super::Migration::new(behind);

    assert!(matches!(
        can.migration_transaction_support(),
        MigrationTransactionSupport::Supported
    ));
    assert!(matches!(
        cannot.migration_transaction_support(),
        MigrationTransactionSupport::Unsupported
    ));
}

#[test]
fn erased_migration_shows_its_identity_when_debugged() {
    let migration = super::Migration::new(TestMigration::new(MockTestOperations::new()));

    let shown = format!("{migration:?}");

    assert!(shown.starts_with("Migration {"));
    assert!(shown.contains("move the widgets"));
}

#[tokio::test]
async fn operations_reach_the_migration_behind_with_the_connection() {
    let mut operations = MockTestOperations::new();
    operations
        .expect_pre_validate_migration()
        .times(1)
        .returning(|connection| {
            connection.operations.push("pre-validate");

            Ok(())
        });
    operations
        .expect_run_migration()
        .times(1)
        .returning(|connection| {
            connection.operations.push("run");

            Ok(())
        });
    operations
        .expect_post_validate_migration()
        .times(1)
        .returning(|connection| {
            connection.operations.push("post-validate");

            Ok(())
        });

    // The mock is moved into the migration: its expectations are checked
    // when the migration is dropped.
    let migration = super::Migration::new(TestMigration::new(operations));
    let mut connection = TestConnection::default();

    migration
        .pre_validate_migration(&mut connection)
        .await
        .expect("the pre-validation passes");
    migration
        .run_migration(&mut connection)
        .await
        .expect("the migration runs");
    migration
        .post_validate_migration(&mut connection)
        .await
        .expect("the post-validation passes");

    assert_eq!(
        connection.operations,
        ["pre-validate", "run", "post-validate"]
    );
}

#[tokio::test]
async fn errors_of_the_migration_behind_show_as_they_are_and_keep_their_types_and_sources() {
    let mut operations = MockTestOperations::new();
    operations
        .expect_pre_validate_migration()
        .times(1)
        .returning(|_| Err(TestPreValidationError));
    operations
        .expect_run_migration()
        .times(1)
        .returning(|_| Err(TestRunError(TestCauseError)));
    operations
        .expect_post_validate_migration()
        .times(1)
        .returning(|_| Err(TestPostValidationError));

    let migration = super::Migration::new(TestMigration::new(operations));
    let mut connection = TestConnection::default();

    let pre_validation_error = migration
        .pre_validate_migration(&mut connection)
        .await
        .expect_err("the pre-validation fails");
    let run_error = migration
        .run_migration(&mut connection)
        .await
        .expect_err("the run fails");
    let post_validation_error = migration
        .post_validate_migration(&mut connection)
        .await
        .expect_err("the post-validation fails");

    assert_eq!(
        pre_validation_error.to_string(),
        "test pre-validation error"
    );
    assert!(pre_validation_error.inner().is::<TestPreValidationError>());

    assert_eq!(run_error.to_string(), "test run error: test cause");
    assert!(run_error.inner().is::<TestRunError>());
    let source = std::error::Error::source(&run_error).expect("the run error has a source");
    assert!(source.is::<TestCauseError>());

    assert_eq!(
        post_validation_error.to_string(),
        "test post-validation error"
    );
    assert!(
        post_validation_error
            .inner()
            .is::<TestPostValidationError>()
    );
}

/// Accept only the types that are migrations running through the test
/// connection, and that several threads can share.
fn assert_is_a_shareable_migration<T>()
where
    T: waymark_migrations_engine_migration::Migration<Connection = TestConnection>,
    T: Send + Sync,
{
}

#[test]
fn erased_migration_is_a_migration_threads_can_share() {
    assert_is_a_shareable_migration::<super::Migration<TestConnection>>();
}
