//! Demand-driven completion polling trait.

use nonempty_collections::NESlice;

use super::common::{CompletionKey, CompletionRecord, HasVmId};

/// Backend capability for polling recorded completions by demand.
pub trait PollCompletions: HasVmId {
    /// The error type for poll operations.
    type Error: core::fmt::Debug;

    /// Fetch the recorded completions matching the demanded keys.
    ///
    /// Returns only rows whose key is in `demand`; an empty result means
    /// none of the demanded completions have been recorded yet, which is
    /// normal — the caller polls again.
    fn poll_completions<'a>(
        &'a self,
        demand: NESlice<'a, CompletionKey<Self::VmId>>,
    ) -> impl Future<Output = Result<Vec<CompletionRecord<Self::VmId>>, Self::Error>> + Send + 'a;
}
