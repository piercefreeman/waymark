//! The snapshot-persisted hook.

use typle::typle;

/// Observes the snapshots the driver persists.
pub trait SnapshotPersisted {
    /// A snapshot has been persisted.
    ///
    /// Called after the persister has accepted the snapshot, with the size of
    /// the serialized snapshot in bytes.
    fn snapshot_persisted(&self, size_in_bytes: usize);
}

/// A tuple of hooks observes as each of its components in turn, first to
/// last.
#[typle(Tuple for 1..=8)]
impl<T: Tuple> SnapshotPersisted for T
where
    T<_>: SnapshotPersisted,
{
    fn snapshot_persisted(&self, size_in_bytes: usize) {
        for typle_index!(i) in 0..T::LEN {
            self[[i]].snapshot_persisted(size_in_bytes);
        }
    }
}

/// An optional hook observes when present and not at all when absent.
impl<Hooks> SnapshotPersisted for Option<Hooks>
where
    Hooks: SnapshotPersisted,
{
    fn snapshot_persisted(&self, size_in_bytes: usize) {
        if let Some(hooks) = self {
            hooks.snapshot_persisted(size_in_bytes);
        }
    }
}
