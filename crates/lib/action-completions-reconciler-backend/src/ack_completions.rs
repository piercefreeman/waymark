//! Completion acknowledgement trait.

use nonempty_collections::NESlice;

use super::common::{CompletionKey, HasVmId};

/// Backend capability for acknowledging consumed completions.
pub trait AckCompletions: HasVmId {
    /// The error type for ack operations.
    type Error: core::fmt::Debug;

    /// Remove the completions identified by `keys`.
    ///
    /// Called after the corresponding settlements have been applied and
    /// the resulting VM state persisted.  Acking is idempotent: keys with
    /// no matching row are silently skipped, so re-acking after a crash
    /// (or racing a purge) is safe.
    fn ack_completions<'a>(
        &'a self,
        keys: NESlice<'a, CompletionKey<Self::VmId>>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}
