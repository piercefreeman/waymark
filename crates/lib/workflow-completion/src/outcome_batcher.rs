//! Batched durable recording of workflow terminal outcomes.
//!
//! Every VM's [`EffectHandler`](crate::EffectHandler) submits its terminal
//! outcome into one shared [`waymark_batcher`] write-batcher and awaits its
//! own verdict, so persist-before-continue still holds per VM while many
//! single-row upserts coalesce into one multi-row
//! [`record_outcomes`](waymark_workflow_completion_backend::RecordOutcomes::record_outcomes)
//! statement.
//!
//! Conflicts are per-row: a VM named in
//! [`RecordingSuccess::SomeConflicted`] gets [`RecordError::Conflict`]
//! while every other waiter in the batch gets its `Ok` — no batch-wide
//! failure.  An `Err` from the backend means the recording itself failed
//! and nothing landed; it is retried here, whole-batch, with backoff.

use std::collections::HashSet;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use nonempty_collections::{NEVec, NonEmptyIterator as _};
use waymark_workflow_completion_backend::{
    Outcome, RecordOutcomes, RecordOutcomesItem, RecordingSuccess,
};

/// Initial delay between retries of a failed (retryable) batch recording.
const RETRY_INITIAL_BACKOFF: Duration = Duration::from_millis(25);

/// Cap on the retry delay.
const RETRY_MAX_BACKOFF: Duration = Duration::from_secs(1);

/// Fatal per-VM error for a batched outcome recording.
#[derive(Debug, Clone, Copy, thiserror::Error)]
pub enum RecordError {
    /// A different terminal outcome is already recorded for this VM —
    /// first-write-wins kept the stored value.
    #[error("a different terminal outcome is already recorded")]
    Conflict,

    /// The outcome batcher has shut down; the outcome was never persisted.
    #[error("the outcome batcher is closed")]
    Closed,
}

/// Handle for submitting terminal outcomes to the shared outcome batcher.
pub type OutcomeRecorderHandle<VmId> =
    waymark_batcher::BatcherHandle<(VmId, Outcome), Result<(), RecordError>>;

/// Create the shared outcome batcher: a recorder handle for the per-VM
/// effect handlers, and the batcher future for the caller to spawn.
///
/// The future resolves once `shutdown` fires (or every handle is dropped)
/// and the last buffered batch has been flushed.
pub fn outcome_batcher<Backend>(
    backend: Arc<Backend>,
    policy: waymark_batcher::Policy,
    shutdown: impl Future<Output = ()>,
) -> (
    OutcomeRecorderHandle<Backend::VmId>,
    impl Future<Output = ()>,
)
where
    Backend: RecordOutcomes,
    Backend::VmId: Clone + Hash + Eq,
{
    waymark_batcher::write_batcher(
        policy,
        move |batch: NEVec<(Backend::VmId, Outcome)>| {
            let backend = Arc::clone(&backend);
            async move {
                let items: Vec<RecordOutcomesItem<'_, Backend::VmId>> = batch
                    .iter()
                    .map(|(vm_id, outcome)| RecordOutcomesItem { vm_id, outcome })
                    .collect();

                let mut backoff = RETRY_INITIAL_BACKOFF;
                loop {
                    let items = nonempty_collections::NESlice::try_from_slice(&items)
                        .expect("the batch is non-empty, so the items are too");
                    match backend.record_outcomes(items).await {
                        Ok(RecordingSuccess::AllRecorded) => {
                            return NEVec::from_elem(Ok(()), batch.len());
                        }
                        Ok(RecordingSuccess::SomeConflicted(keys)) => {
                            let conflicted: HashSet<Backend::VmId> = keys.into_iter().collect();
                            return batch
                                .nonempty_iter()
                                .map(|(vm_id, _)| {
                                    if conflicted.contains(vm_id) {
                                        Err(RecordError::Conflict)
                                    } else {
                                        Ok(())
                                    }
                                })
                                .collect();
                        }
                        Err(error) => {
                            tracing::warn!(
                                ?error,
                                ?backoff,
                                "recording an outcome batch failed; retrying"
                            );
                            tokio::time::sleep(backoff).await;
                            backoff = (backoff * 2).min(RETRY_MAX_BACKOFF);
                        }
                    }
                }
            }
        },
        shutdown,
    )
}

#[cfg(test)]
mod tests;
