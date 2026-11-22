//! The test doubles the migrations engine's tests share.

use std::time::Duration;

use waymark_migrations_engine_backend::migrations_lock;

/// The error of a test backend's operation.
#[derive(Debug, thiserror::Error)]
#[error("test backend error")]
pub struct TestBackendError;

mockall::mock! {
    pub BackendOperations {
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

    /// How long a renewal is on its way.
    pub renewal_takes: Duration,
}

impl TestBackend {
    /// A test backend over `operations`, whose renewals take no time.
    pub fn new(operations: MockBackendOperations) -> Self {
        Self {
            operations,
            renewal_takes: Duration::ZERO,
        }
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
