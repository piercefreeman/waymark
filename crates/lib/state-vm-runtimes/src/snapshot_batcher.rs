//! Batched durable storing of VM runtime snapshots.
//!
//! Every VM's [`SnapshotAdapter`](crate::SnapshotAdapter) submits its
//! snapshot into one shared
//! [`deduplicating_write_batcher`](waymark_batcher::deduplicating_write_batcher)
//! keyed by the vm id and awaits its own outcome, so persist-before-ack
//! still holds per VM while many single-row updates coalesce into one
//! multi-row [`store_snapshots`](waymark_state_vm_runtimes_backend::StoreSnapshots::store_snapshots)
//! statement.
//!
//! The concrete backend error is logged in full at the flush — the one
//! place that has it — and the waiters receive the
//! [`SnapshotBatchError`] category: their drive loops fail and the
//! workloads re-pin either way.

use std::hash::Hash;
use std::sync::Arc;

/// The batcher item: a VM id and its owned snapshot bytes. The bytes must be
/// owned because the batcher holds them until the batch flushes.
pub type SnapshotJob<VmId> = (VmId, Vec<u8>);

/// The per-submission result the snapshot batcher hands back.
pub type SnapshotOutcome = Result<(), SnapshotBatchError>;

/// Handle for submitting snapshots to the shared snapshot batcher.
pub type SnapshotBatcherHandle<VmId> =
    waymark_batcher::BatcherHandle<SnapshotJob<VmId>, SnapshotOutcome>;

/// Error from persisting a snapshot through the shared batcher.
#[derive(Debug, Clone, Copy, thiserror::Error)]
pub enum SnapshotBatchError {
    /// The batched store failed; nothing of the batch was persisted.
    #[error("batched snapshot store failed")]
    Store,

    /// The snapshot batcher has shut down and can no longer persist.
    #[error("snapshot batcher is closed")]
    Closed,
}

/// The snapshot batcher's conflict resolver: the newest snapshot wins.
///
/// A same-vm duplicate within one window cannot happen today — a driver
/// awaits each submission inline — but if one ever arrives, sequential
/// semantics must hold: the newest snapshot is what a later unbatched
/// store would have persisted, and the arbitrary-row behavior of a
/// duplicate-key `UPDATE ... FROM` must never pick the older one.  The
/// newcomer takes the slot; the ousted earlier submission settles to the
/// winner's verdict — in the sequential story its own store succeeded
/// and was then overwritten, so its fate is the fate of the write that
/// superseded it.
struct NewestSnapshotWins;

impl<VmId>
    waymark_batcher::deduplicating_write::ConflictResolver<SnapshotJob<VmId>, SnapshotOutcome>
    for NewestSnapshotWins
{
    type Placeholder = ();

    fn resolve_conflict<'a>(
        &self,
        slot: waymark_batcher::deduplicating_write::ConflictedSlot<
            'a,
            SnapshotJob<VmId>,
            SnapshotOutcome,
            (),
        >,
        newcomer: SnapshotJob<VmId>,
    ) -> waymark_batcher::deduplicating_write::ConflictResolvedToken<'a> {
        let (_superseded, resolving) = slot.replace(newcomer);
        resolving.resolve(())
    }

    fn settle_conflict(
        &self,
        _conflicted_out: (),
        winner_out: &SnapshotOutcome,
    ) -> SnapshotOutcome {
        *winner_out
    }
}

/// Create the shared snapshot batcher: a handle for the per-VM
/// [`SnapshotAdapter`](crate::SnapshotAdapter)s, and the batcher future
/// for the caller to spawn.
///
/// The future resolves once `shutdown` fires (or every handle is
/// dropped) and the last buffered batch has been flushed.
pub fn snapshot_batcher<Backend>(
    backend: Arc<Backend>,
    policy: waymark_batcher::Policy,
    shutdown: impl Future<Output = ()>,
) -> (
    SnapshotBatcherHandle<Backend::VmId>,
    impl Future<Output = ()>,
)
where
    Backend: waymark_state_vm_runtimes_backend::StoreSnapshots,
    Backend::VmId: Clone + Hash + Eq,
{
    waymark_batcher::deduplicating_write_batcher(
        policy,
        |(vm_id, _): &SnapshotJob<Backend::VmId>| vm_id.clone(),
        NewestSnapshotWins,
        move |batch: nonempty_collections::NEVec<SnapshotJob<Backend::VmId>>| {
            let backend = Arc::clone(&backend);
            async move {
                let outcome = {
                    let items: Vec<_> = batch
                        .iter()
                        .map(|(vm_id, snapshot)| {
                            waymark_state_vm_runtimes_backend::StoreSnapshotsItem {
                                vm_id,
                                snapshot: snapshot.as_slice(),
                            }
                        })
                        .collect();
                    backend.store_snapshots(&items).await
                };
                let result = outcome.map_err(|error| {
                    tracing::warn!(?error, "storing a snapshot batch failed");
                    SnapshotBatchError::Store
                });
                nonempty_collections::NEVec::from_elem(result, batch.len())
            }
        },
        shutdown,
    )
}
