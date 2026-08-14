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
//! failure.
//!
//! Duplicate vm_ids are deduped first-wins by the batcher itself (the
//! upsert cannot affect the same row twice, SQLSTATE 21000): the batcher
//! is a [`deduplicating_write_batcher`](waymark_batcher::deduplicating_write_batcher)
//! keyed by vm_id.  [`FirstWriteWins`] folds a same-vm newcomer out,
//! recording whether it was byte-identical to the incumbent, and settles
//! its verdict against the incumbent's actual flush output — exactly
//! what first-write-wins would have said had the two arrived in separate
//! batches, including when the incumbent's write conflicts with a
//! pre-existing stored outcome or fails to land at all.
//!
//! An `Err` from the backend means the recording itself failed and
//! nothing landed.  Retryable ([`ErrorKind::Internal`]) failures are
//! retried here, whole-batch, with backoff, up to
//! [`RETRY_MAX_ATTEMPTS`]; an [`ErrorKind::InvalidBatch`] failure, or
//! exhausted retries, fans [`RecordError::Failed`] to every waiter —
//! the drive loops fail, and revival re-records the outcomes later.

use std::collections::HashSet;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use nonempty_collections::{NEVec, NonEmptyIterator as _};
use waymark_workflow_completion_backend::record_outcomes::{Error as _, ErrorKind};
use waymark_workflow_completion_backend::{
    Outcome, RecordOutcomes, RecordOutcomesItem, RecordingSuccess,
};

/// Initial delay between retries of a failed (retryable) batch recording.
const RETRY_INITIAL_BACKOFF: Duration = Duration::from_millis(25);

/// Cap on the retry delay.
const RETRY_MAX_BACKOFF: Duration = Duration::from_secs(1);

/// Cap on recording attempts per batch — enough backoff (a few seconds)
/// to ride out a transient blip.  A longer outage fails the batch: the
/// waiting drive loops fail, and revival re-records the outcomes later.
const RETRY_MAX_ATTEMPTS: usize = 10;

/// Fatal per-VM error for a batched outcome recording.
#[derive(Debug, Clone, Copy, thiserror::Error)]
pub enum RecordError {
    /// A different terminal outcome is already recorded for this VM —
    /// first-write-wins kept the stored value.
    #[error("a different terminal outcome is already recorded")]
    Conflict,

    /// The batched recording failed — retries exhausted, or a failure
    /// retrying cannot fix.  The outcome was not persisted.
    #[error("recording the outcome batch failed")]
    Failed,

    /// The outcome batcher has shut down; the outcome was never persisted.
    #[error("the outcome batcher is closed")]
    Closed,
}

/// Handle for submitting terminal outcomes to the shared outcome batcher.
pub type OutcomeRecorderHandle<VmId> =
    waymark_batcher::BatcherHandle<(VmId, Outcome), Result<(), RecordError>>;

/// What the fold recorded about a folded-out duplicate submission.
enum DuplicateKind {
    /// Byte-identical to the incumbent — its recording is represented by
    /// the incumbent's write.
    Identical,

    /// A different outcome for the same VM — it lost first-write-wins
    /// within the window.
    Conflicting,
}

/// The outcome batcher's conflict resolver: first-write-wins.
///
/// The incumbent always stays.  The fold records whether the newcomer
/// was byte-identical; settling derives the duplicate's verdict from
/// that fact plus the winner's actual flush output — an identical
/// duplicate inherits the winner's verdict (including `Conflict` against
/// a pre-existing stored outcome), and nobody is told anything but
/// [`RecordError::Failed`] when the batch write did not land.
struct FirstWriteWins;

impl<VmId>
    waymark_batcher::deduplicating_write::ConflictResolver<(VmId, Outcome), Result<(), RecordError>>
    for FirstWriteWins
{
    type Placeholder = DuplicateKind;

    fn resolve_conflict<'a>(
        &self,
        slot: waymark_batcher::deduplicating_write::ConflictedSlot<
            'a,
            (VmId, Outcome),
            Result<(), RecordError>,
            DuplicateKind,
        >,
        newcomer: (VmId, Outcome),
    ) -> waymark_batcher::deduplicating_write::ConflictResolvedToken<'a> {
        let kind = if slot.incumbent().1 == newcomer.1 {
            DuplicateKind::Identical
        } else {
            DuplicateKind::Conflicting
        };
        slot.keep(kind)
    }

    fn settle_conflict(
        &self,
        conflicted_out: DuplicateKind,
        winner_out: &Result<(), RecordError>,
    ) -> Result<(), RecordError> {
        match (conflicted_out, winner_out) {
            // Nothing of the batch landed: `Conflict` would claim
            // knowledge of stored state that does not exist.
            (_, Err(RecordError::Failed)) => Err(RecordError::Failed),
            (DuplicateKind::Conflicting, _) => Err(RecordError::Conflict),
            (DuplicateKind::Identical, _) => *winner_out,
        }
    }
}

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
    waymark_batcher::deduplicating_write_batcher(
        policy,
        |(vm_id, _): &(Backend::VmId, Outcome)| vm_id.clone(),
        FirstWriteWins,
        move |batch: NEVec<(Backend::VmId, Outcome)>| {
            let backend = Arc::clone(&backend);
            async move {
                let items: Vec<RecordOutcomesItem<'_, Backend::VmId>> = batch
                    .iter()
                    .map(|(vm_id, outcome)| RecordOutcomesItem { vm_id, outcome })
                    .collect();

                let mut backoff = RETRY_INITIAL_BACKOFF;
                let mut attempts_left = RETRY_MAX_ATTEMPTS;
                loop {
                    let sent = nonempty_collections::NESlice::try_from_slice(&items)
                        .expect("the batch is non-empty, so the items are too");
                    match backend.record_outcomes(sent).await {
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
                        Err(error) => match error.kind() {
                            ErrorKind::Internal => {
                                attempts_left -= 1;
                                if attempts_left == 0 {
                                    tracing::error!(
                                        ?error,
                                        "recording an outcome batch failed; retries exhausted"
                                    );
                                    return NEVec::from_elem(Err(RecordError::Failed), batch.len());
                                }
                                tracing::warn!(
                                    ?error,
                                    ?backoff,
                                    "recording an outcome batch failed; retrying"
                                );
                                tokio::time::sleep(backoff).await;
                                backoff = (backoff * 2).min(RETRY_MAX_BACKOFF);
                            }
                            ErrorKind::InvalidBatch => {
                                tracing::error!(?error, "an outcome batch was rejected as invalid");
                                return NEVec::from_elem(Err(RecordError::Failed), batch.len());
                            }
                        },
                    }
                }
            }
        },
        shutdown,
    )
}

#[cfg(test)]
mod tests;
