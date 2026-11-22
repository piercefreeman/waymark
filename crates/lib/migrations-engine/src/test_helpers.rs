//! The test doubles the migrations engine's tests share.

use std::sync::Arc;
use std::time::Duration;

use waymark_migrations_engine_backend::{MigrationsLedger, migrations_ledger, migrations_lock};
use waymark_migrations_engine_migration::{
    MigrationIdentity, MigrationRevision, MigrationTransactionSupport, MigrationVersion,
};

/// What happened in a test, in order: each test double adds what it was
/// asked to do.
#[derive(Debug, Default)]
pub struct Events(std::sync::Mutex<Vec<String>>);

impl Events {
    /// Add `event` after the ones already there.
    pub fn push(&self, event: impl Into<String>) {
        self.0
            .lock()
            .expect("the events are not poisoned")
            .push(event.into());
    }

    /// The events so far.
    pub fn so_far(&self) -> Vec<String> {
        self.0.lock().expect("the events are not poisoned").clone()
    }
}

/// The connection a test backend holds and the test migrations run
/// through.
#[derive(Debug, Default)]
pub struct TestConnection {
    /// Where what runs through this connection is recorded.
    pub events: Arc<Events>,
}

/// The error of a test backend's operation.
#[derive(Debug, thiserror::Error)]
#[error("test backend error")]
pub struct TestBackendError;

mockall::mock! {
    pub BackendOperations {
        pub fn setup_migrations_engine(&mut self) -> Result<(), TestBackendError>;

        pub fn read_migrations_ledger(&mut self) -> Result<MigrationsLedger, TestBackendError>;

        pub fn mark_migration_started(
            &mut self,
            version: MigrationVersion,
            description: String,
            revision: String,
            source_digest: String,
        ) -> Result<(), TestBackendError>;

        pub fn mark_migration_finished(
            &mut self,
            version: MigrationVersion,
        ) -> Result<(), TestBackendError>;

        pub fn begin_whole_run_transaction(&mut self) -> Result<(), TestBackendError>;

        pub fn commit_whole_run_transaction(&mut self) -> Result<(), TestBackendError>;

        pub fn rollback_whole_run_transaction(&mut self) -> Result<(), TestBackendError>;

        pub fn take_shared_migrations_lock(
            &mut self,
            holder_id: &migrations_lock::HolderId,
            expiry: migrations_lock::Expiry,
        ) -> Result<
            waymark_migrations_engine_backend::take_shared_migrations_lock::Outcome,
            TestBackendError,
        >;

        pub fn take_exclusive_migrations_lock(
            &mut self,
            holder_id: &migrations_lock::HolderId,
            expiry: migrations_lock::Expiry,
        ) -> Result<
            waymark_migrations_engine_backend::take_exclusive_migrations_lock::Outcome,
            TestBackendError,
        >;

        pub fn convert_migrations_lock_to_shared(
            &mut self,
            holder_id: &migrations_lock::HolderId,
            expiry: migrations_lock::Expiry,
        ) -> Result<
            waymark_migrations_engine_backend::convert_migrations_lock_to_shared::Outcome,
            TestBackendError,
        >;

        pub fn renew_migrations_lock(
            &mut self,
            holder_id: &migrations_lock::HolderId,
            expiry: migrations_lock::Expiry,
        ) -> Result<
            waymark_migrations_engine_backend::renew_migrations_lock::Outcome,
            TestBackendError,
        >;

        pub fn release_migrations_lock(
            &mut self,
            holder_id: &migrations_lock::HolderId,
        ) -> Result<(), TestBackendError>;

        pub fn find_exclusive_migrations_lock(
            &mut self,
        ) -> Result<
            waymark_migrations_engine_backend::find_exclusive_migrations_lock::Outcome,
            TestBackendError,
        >;
    }
}

/// A backend for tests: its operations are a mock's, and a renewal takes
/// `renewal_takes` before it reaches the mock.
pub struct TestBackend {
    /// What the backend's operations do.
    pub operations: MockBackendOperations,

    /// The connection the backend holds.
    pub connection: TestConnection,

    /// How long a renewal is on its way.
    pub renewal_takes: Duration,
}

impl TestBackend {
    /// A test backend over `operations`, whose renewals take no time.
    pub fn new(operations: MockBackendOperations) -> Self {
        Self {
            operations,
            connection: TestConnection::default(),
            renewal_takes: Duration::ZERO,
        }
    }
}

impl waymark_migrations_engine_backend::HasConnection for TestBackend {
    type Connection = TestConnection;
}

impl waymark_migrations_engine_backend::AccessMigrationsConnection for TestBackend {
    fn migrations_connection(&mut self) -> &mut Self::Connection {
        &mut self.connection
    }
}

impl waymark_migrations_engine_backend::SetupMigrationsEngine for TestBackend {
    type Error = TestBackendError;

    async fn setup_migrations_engine(&mut self) -> Result<(), Self::Error> {
        self.operations.setup_migrations_engine()
    }
}

impl waymark_migrations_engine_backend::ReadMigrationsLedger for TestBackend {
    type Error = TestBackendError;

    async fn read_migrations_ledger(&mut self) -> Result<MigrationsLedger, Self::Error> {
        self.operations.read_migrations_ledger()
    }
}

impl waymark_migrations_engine_backend::MarkMigrationStarted for TestBackend {
    type Error = TestBackendError;

    async fn mark_migration_started(
        &mut self,
        identity: MigrationIdentity<'_>,
    ) -> Result<(), Self::Error> {
        self.operations.mark_migration_started(
            identity.version,
            identity.description.to_owned(),
            identity.revision.as_raw().to_owned(),
            identity.source_digest.to_owned(),
        )
    }
}

impl waymark_migrations_engine_backend::MarkMigrationFinished for TestBackend {
    type Error = TestBackendError;

    async fn mark_migration_finished(
        &mut self,
        version: MigrationVersion,
    ) -> Result<(), Self::Error> {
        self.operations.mark_migration_finished(version)
    }
}

impl waymark_migrations_engine_backend::BeginWholeRunTransaction for TestBackend {
    type Error = TestBackendError;

    async fn begin_whole_run_transaction(&mut self) -> Result<(), Self::Error> {
        self.operations.begin_whole_run_transaction()
    }
}

impl waymark_migrations_engine_backend::CommitWholeRunTransaction for TestBackend {
    type Error = TestBackendError;

    async fn commit_whole_run_transaction(&mut self) -> Result<(), Self::Error> {
        self.operations.commit_whole_run_transaction()
    }
}

impl waymark_migrations_engine_backend::RollbackWholeRunTransaction for TestBackend {
    type Error = TestBackendError;

    async fn rollback_whole_run_transaction(&mut self) -> Result<(), Self::Error> {
        self.operations.rollback_whole_run_transaction()
    }
}

impl waymark_migrations_engine_backend::TakeSharedMigrationsLock for TestBackend {
    type Error = TestBackendError;

    async fn take_shared_migrations_lock(
        &mut self,
        holder_id: &migrations_lock::HolderId,
        expiry: migrations_lock::Expiry,
    ) -> Result<waymark_migrations_engine_backend::take_shared_migrations_lock::Outcome, Self::Error>
    {
        self.operations
            .take_shared_migrations_lock(holder_id, expiry)
    }
}

impl waymark_migrations_engine_backend::TakeExclusiveMigrationsLock for TestBackend {
    type Error = TestBackendError;

    async fn take_exclusive_migrations_lock(
        &mut self,
        holder_id: &migrations_lock::HolderId,
        expiry: migrations_lock::Expiry,
    ) -> Result<
        waymark_migrations_engine_backend::take_exclusive_migrations_lock::Outcome,
        Self::Error,
    > {
        self.operations
            .take_exclusive_migrations_lock(holder_id, expiry)
    }
}

impl waymark_migrations_engine_backend::ConvertMigrationsLockToShared for TestBackend {
    type Error = TestBackendError;

    async fn convert_migrations_lock_to_shared(
        &mut self,
        holder_id: &migrations_lock::HolderId,
        expiry: migrations_lock::Expiry,
    ) -> Result<
        waymark_migrations_engine_backend::convert_migrations_lock_to_shared::Outcome,
        Self::Error,
    > {
        self.operations
            .convert_migrations_lock_to_shared(holder_id, expiry)
    }
}

impl waymark_migrations_engine_backend::RenewMigrationsLock for TestBackend {
    type Error = TestBackendError;

    async fn renew_migrations_lock(
        &mut self,
        holder_id: &migrations_lock::HolderId,
        expiry: migrations_lock::Expiry,
    ) -> Result<waymark_migrations_engine_backend::renew_migrations_lock::Outcome, Self::Error>
    {
        tokio::time::sleep(self.renewal_takes).await;

        self.operations.renew_migrations_lock(holder_id, expiry)
    }
}

impl waymark_migrations_engine_backend::ReleaseMigrationsLock for TestBackend {
    type Error = TestBackendError;

    async fn release_migrations_lock(
        &mut self,
        holder_id: &migrations_lock::HolderId,
    ) -> Result<(), Self::Error> {
        self.operations.release_migrations_lock(holder_id)
    }
}

impl waymark_migrations_engine_backend::FindExclusiveMigrationsLock for TestBackend {
    type Error = TestBackendError;

    async fn find_exclusive_migrations_lock(
        &mut self,
    ) -> Result<
        waymark_migrations_engine_backend::find_exclusive_migrations_lock::Outcome,
        Self::Error,
    > {
        self.operations.find_exclusive_migrations_lock()
    }
}

/// The error of a test migration's operation.
#[derive(Debug, thiserror::Error)]
#[error("test migration error")]
pub struct TestMigrationError;

mockall::mock! {
    pub MigrationOperations {
        pub fn pre_validate_migration(
            &self,
            connection: &mut TestConnection,
        ) -> Result<(), TestMigrationError>;

        pub fn run_migration(
            &self,
            connection: &mut TestConnection,
        ) -> Result<(), TestMigrationError>;

        pub fn post_validate_migration(
            &self,
            connection: &mut TestConnection,
        ) -> Result<(), TestMigrationError>;
    }
}

/// A migration for tests: its identity and transaction support are plain
/// values, its operations a mock's.
pub struct TestMigration {
    /// The migration's version.
    pub version: MigrationVersion,

    /// The migration's revision.
    pub revision: MigrationRevision,

    /// Whether the migration can run inside a transaction.
    pub runs_inside_a_transaction: bool,

    /// What the migration's operations do.
    pub operations: MockMigrationOperations,
}

/// The revision the test migration of `version` has unless a test gives
/// it another: `test-migration-N`.
pub fn revision_of(version: u32) -> MigrationRevision {
    MigrationRevision::from_raw(format!("test-migration-{version}"))
}

impl TestMigration {
    /// The description every test migration has.
    pub const DESCRIPTION: &'static str = "test migration";

    /// The source digest every test migration has.
    pub const SOURCE_DIGEST: &'static str = "sha256:test";

    /// A test migration of `version` over `operations`, with the revision
    /// [`revision_of`] its version, that can run inside a transaction.
    pub fn new(version: u32, operations: MockMigrationOperations) -> Self {
        Self {
            version: MigrationVersion::from_raw(version),
            revision: revision_of(version),
            runs_inside_a_transaction: true,
            operations,
        }
    }

    /// A test migration of `version` none of whose operations is expected
    /// to be called.
    pub fn untouched(version: u32) -> Self {
        Self::new(version, MockMigrationOperations::new())
    }

    /// A test migration of `version` whose three operations each succeed
    /// once, recording `pre-validate N`, `run N` and `post-validate N` in
    /// the events of the connection they run through.
    pub fn succeeding(version: u32) -> Self {
        let mut operations = MockMigrationOperations::new();

        operations
            .expect_pre_validate_migration()
            .times(1)
            .returning(move |connection| {
                connection.events.push(format!("pre-validate {version}"));

                Ok(())
            });
        operations
            .expect_run_migration()
            .times(1)
            .returning(move |connection| {
                connection.events.push(format!("run {version}"));

                Ok(())
            });
        operations
            .expect_post_validate_migration()
            .times(1)
            .returning(move |connection| {
                connection.events.push(format!("post-validate {version}"));

                Ok(())
            });

        Self::new(version, operations)
    }
}

/// `migrations` as a valid migrations array; they have to be in the
/// order of their versions.
pub fn valid_migrations<Migrations>(
    migrations: Migrations,
) -> crate::ValidMigrationsArray<Migrations>
where
    Migrations: AsRef<[TestMigration]>,
{
    crate::validate_migrations_array(migrations)
        .expect("the test migrations are in the order of their versions")
}

impl waymark_migrations_engine_migration::HasConnection for TestMigration {
    type Connection = TestConnection;
}

impl waymark_migrations_engine_migration::HasMigrationIdentity for TestMigration {
    fn migration_identity(&self) -> MigrationIdentity<'_> {
        MigrationIdentity {
            version: self.version,
            description: Self::DESCRIPTION,
            revision: &self.revision,
            source_digest: Self::SOURCE_DIGEST,
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
    type Error = TestMigrationError;

    async fn pre_validate_migration(
        &self,
        connection: &mut TestConnection,
    ) -> Result<(), Self::Error> {
        self.operations.pre_validate_migration(connection)
    }
}

impl waymark_migrations_engine_migration::RunMigration for TestMigration {
    type Error = TestMigrationError;

    async fn run_migration(&self, connection: &mut TestConnection) -> Result<(), Self::Error> {
        self.operations.run_migration(connection)
    }
}

impl waymark_migrations_engine_migration::PostValidateMigration for TestMigration {
    type Error = TestMigrationError;

    async fn post_validate_migration(
        &self,
        connection: &mut TestConnection,
    ) -> Result<(), Self::Error> {
        self.operations.post_validate_migration(connection)
    }
}

/// The error of a test assertion's check.
#[derive(Debug, thiserror::Error)]
#[error("test assertion error")]
pub struct TestAssertionError;

mockall::mock! {
    pub AssertionOperations {
        pub fn check(
            &self,
            connection: &mut TestConnection,
        ) -> Result<waymark_migrations_engine_assertion::Outcome, TestAssertionError>;
    }
}

/// An assertion for tests: its check is a mock's.
pub struct TestAssertion {
    /// What the assertion's check does.
    pub operations: MockAssertionOperations,
}

impl TestAssertion {
    /// No assertions.
    pub const NONE: waymark_migrations_engine_assertion::NoAssertions<TestConnection> =
        waymark_migrations_engine_assertion::NoAssertions {
            connection_type: core::marker::PhantomData,
        };

    /// A test assertion whose check is not expected to be called.
    pub fn untouched() -> Self {
        Self {
            operations: MockAssertionOperations::new(),
        }
    }

    /// A test assertion whose check is called once and comes to
    /// `outcome`, recording `check <label>` in the events of the
    /// connection it goes through.
    pub fn checked_once<Outcome>(label: &'static str, outcome: Outcome) -> Self
    where
        Outcome: Fn() -> Result<waymark_migrations_engine_assertion::Outcome, TestAssertionError>
            + Send
            + 'static,
    {
        let mut operations = MockAssertionOperations::new();

        operations
            .expect_check()
            .times(1)
            .returning(move |connection| {
                connection.events.push(format!("check {label}"));

                outcome()
            });

        Self { operations }
    }

    /// A violation with the message `message`, as a check comes to it.
    pub fn violated(
        message: &str,
    ) -> Result<waymark_migrations_engine_assertion::Outcome, TestAssertionError> {
        Ok(waymark_migrations_engine_assertion::Outcome::Violated {
            message: message.to_owned(),
        })
    }
}

impl waymark_migrations_engine_assertion::Assertion for TestAssertion {
    type Connection = TestConnection;

    type CheckError = TestAssertionError;

    async fn check(
        &self,
        connection: &mut TestConnection,
    ) -> Result<waymark_migrations_engine_assertion::Outcome, Self::CheckError> {
        self.operations.check(connection)
    }
}

/// A ledger whose entries are finished ones of `versions`, each with the
/// revision [`revision_of`] its version.
pub fn ledger_of_finished(versions: &[u32]) -> MigrationsLedger {
    MigrationsLedger::SetUp(
        versions
            .iter()
            .map(|version| migrations_ledger::Entry {
                version: MigrationVersion::from_raw(*version),
                revision: revision_of(*version),
                state: migrations_ledger::EntryState::Finished,
            })
            .collect(),
    )
}
