//! Sleep acknowledgement trait.

use nonempty_collections::NESlice;

use super::common::{HasVmId, SleepKey};

/// Backend capability for acknowledging settled sleeps.
pub trait AckSleeps: HasVmId {
    /// The error type for ack operations.
    type Error: core::fmt::Debug;

    /// Remove the sleep requests identified by `keys`.
    ///
    /// Called after the corresponding settlements have been applied and
    /// the resulting VM state persisted.  Acking is idempotent: keys with
    /// no matching row are silently skipped, so re-acking after a crash
    /// (or racing a purge) is safe.
    fn ack_sleeps<'a>(
        &'a self,
        keys: NESlice<'a, SleepKey<Self::VmId>>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}
