//! Scheduled VM runtime registration trait.

use std::borrow::Cow;

use nonempty_collections::{NESlice, NEVec};

use super::common::{HasTimestamp, HasVmId};

/// One scheduled VM runtime to register, passed to
/// [`RegisterScheduledVmRuntimes::register_scheduled_vm_runtimes`].
///
/// Every value field is a [`Cow`], so each call site picks per field
/// whether the item borrows from longer-lived storage or owns a value
/// minted for this registration. The `ToOwned` bounds are `Cow`'s
/// well-formedness requirement — "has an owned form" — satisfied for
/// free by any `Clone` type.
#[derive_where::derive_where(Debug; VmId, <VmId as ToOwned>::Owned, Timestamp, <Timestamp as ToOwned>::Owned)]
pub struct Item<'a, VmId, Timestamp>
where
    VmId: ToOwned,
    Timestamp: ToOwned,
{
    /// The schedule this registration originates from.
    pub schedule_name: Cow<'a, str>,

    /// The fence: the run cursor this occurrence was polled at. The row
    /// is a no-op ([`Superseded`](Outcome::Superseded)) if the
    /// schedule's cursor no longer matches.
    pub expected_next_run_at: Cow<'a, Timestamp>,

    /// The id to register the spawned VM runtime under.
    pub vm_id: Cow<'a, VmId>,

    /// The schedule's advanced run cursor: the next occurrence computed
    /// from the definition, jitter applied. The statement stores it
    /// verbatim.
    pub new_next_run_at: Cow<'a, Timestamp>,

    /// Whether the overlap gate applies. Callers derive this from the
    /// definition's overlap policy.
    pub check_overlap: bool,
}

/// The per-row outcome of a scheduled VM runtime registration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The VM runtime was registered and the schedule's cursor advanced.
    Registered,

    /// The overlap gate held — the previous instance is still running:
    /// the schedule's cursor advanced, no VM was registered.
    SkippedOverlap,

    /// The fence did not match — another registrar already took this
    /// occurrence. Nothing was written for this row.
    Superseded,
}

/// Backend capability for registering scheduled VM runtimes, in a batch.
///
/// For each item, atomically with the whole batch: advance the
/// schedule's run cursor past this occurrence and register a fresh VM
/// runtime from the schedule's baked initial snapshot (a snapshot-table
/// row plus a matching runnable-workload row).
///
/// Outcomes are per-row conditions, not failures: fence mismatches and
/// held overlap gates are reported positionally and do not affect their
/// neighbors. An `Err` means the registration itself failed; nothing of
/// the batch landed and the whole batch is retryable.
///
/// The schedule names within a batch must be distinct. Implementations
/// rely on this to attribute per-row outcomes and may panic on a
/// violating batch.
pub trait RegisterScheduledVmRuntimes: HasVmId + HasTimestamp
where
    Self::VmId: ToOwned,
    Self::Timestamp: ToOwned,
{
    /// The error type for registration operations.
    type Error: std::fmt::Debug;

    /// Durably register the given scheduled VM runtimes in one batch.
    /// Outcomes are returned in input order, one per item.
    fn register_scheduled_vm_runtimes<'a>(
        &'a self,
        items: NESlice<'a, Item<'a, Self::VmId, Self::Timestamp>>,
    ) -> impl Future<Output = Result<NEVec<Outcome>, Self::Error>> + Send + 'a;
}
