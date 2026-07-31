//! Demand-driven settlement polling — a single shared loop polls the
//! backend for exactly the completions the VMs are currently waiting on
//! and settles their promises.
//!
//! # Architecture
//!
//! [`state`] creates the shared state of per-VM entries, taking the
//! sender of the ack channel; it returns the cloneable
//! [`DemandRegistrar`] and the opaque [`StateToken`].  Use
//! [`subscribe`](DemandRegistrar::subscribe) to wire up each VM with a
//! [`SettlementsHandle`] — the per-VM implementation of
//! [`waymark_extcall_reconciler_core::ActionPromiseSettler`] — and drive
//! [`run`] with the [`StateToken`] in its [`Params`] as the single
//! shared poll loop.
//!
//! A handle call registers its demand (the promise ids the VM is waiting
//! on) and parks; the poller loop queries the union of all registered
//! demand (minus rows already buffered), decodes the fetched outcomes,
//! and delivers them into the owning handles' buffers.  The handle turns
//! delivered rows into promise settlements, each carrying an [`Ack`]
//! minted from the row's own key: acknowledging (after the settlement has
//! been applied and the VM state persisted) pushes the key onto the ack
//! channel the registry was created with, from where the background
//! [`acker::run`](crate::acker::run) loop deletes the row.
//!
//! Rows stay in the backend until acked, so all in-memory state here is
//! disposable: a crashed or re-registered VM simply re-demands and
//! re-fetches.
//!
//! # Cancellation safety
//!
//! A handle's `poll_action_settlements` future may be dropped at any
//! time.  Rows fetched for it land in its buffer and are returned by the
//! next call; demand registered by a cancelled call is refreshed
//! (replaced) by the next call.  Dropping the handle itself removes its
//! registry entry (identity-guarded, so a stale handle cannot displace a
//! re-registered successor) and discards its buffer — the rows are still
//! in the backend.

#[cfg(test)]
mod tests;

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use dashmap::DashMap;
use nonempty_collections::{IntoNonEmptyIterator as _, NESlice, NEVec, NonEmptyIterator as _};
use waymark_action_completions_reconciler_backend::{
    CompletionKey, CompletionRecord, PollCompletions,
};
use waymark_action_runtime_core::ActionCallOutcome;
use waymark_vm_driver_core::{PromiseResolution, PromiseSettlement};
use waymark_vm_runtime_promise_core::PromiseStateId;
use waymark_vm_value::ReadyValue;

/// Error returned when the shared poll loop stops on a critical failure.
#[derive(Debug, thiserror::Error)]
pub enum Error<PollError, DecodeError> {
    /// The backend failed to poll for completions.
    #[error("polling completions: {0}")]
    Poll(#[source] PollError),

    /// A stored outcome could not be decoded — the blob we wrote cannot
    /// be read back, which is a bug.
    #[error("unable to decode a stored action-call outcome")]
    OutcomeDecode(#[source] DecodeError),
}

/// Error returned when polling a [`SettlementsHandle`] for action
/// settlements fails.
#[derive(Debug, thiserror::Error)]
pub enum PollActionSettlementsError {
    /// The shared [`run`] loop is gone; no settlements will ever be
    /// delivered again.
    #[error("demand poller gone")]
    PollerGone,
}

/// Settlement acknowledgement for durably-stored completions.
///
/// Minted by a [`SettlementsHandle`] from the delivered row's own key.
/// Acknowledging (after the settlement has been applied and the VM state
/// persisted) pushes the key onto the poller's ack channel, from where
/// the background [`acker::run`](crate::acker::run) loop deletes the row.
pub struct Ack<VmId> {
    key: CompletionKey<VmId>,
    ack_tx: tokio::sync::mpsc::UnboundedSender<CompletionKey<VmId>>,
}

impl<VmId> waymark_vm_driver_core::PromiseSettlementAck for Ack<VmId> {
    fn acknowledge_promise_settlement(self) {
        // A closed channel means the acker is shutting down; the row stays
        // in the backend and is re-settled (and re-acked) on revive.
        let _ = self.ack_tx.send(self.key);
    }
}

impl<VmId, SleepAck> From<Ack<VmId>> for waymark_extcall_reconciler_core::Ack<Ack<VmId>, SleepAck> {
    fn from(value: Ack<VmId>) -> Self {
        waymark_extcall_reconciler_core::Ack::Action(value)
    }
}

/// One decoded completion parked in a handle's buffer, keyed by the
/// promise it settles.
struct BufferedCompletion {
    promise_state_id: PromiseStateId,
    outcome: ActionCallOutcome<ReadyValue>,
}

/// Per-VM mutable state: the currently registered demand and the rows
/// fetched-but-not-yet-consumed.
#[derive(Default)]
struct EntryState {
    demand: HashSet<PromiseStateId>,
    buffer: Vec<BufferedCompletion>,
}

/// One registered VM: its state plus the waiter wakeup.
#[derive(Default)]
struct VmEntry {
    state: std::sync::Mutex<EntryState>,
    notify: tokio::sync::Notify,
}

/// State shared between the poller, the registrar, and the handles.
struct Shared<VmId> {
    entries: DashMap<VmId, Arc<VmEntry>>,
    /// Wakes the poll loop when new demand is registered.
    demand_notify: tokio::sync::Notify,
    /// Set when the [`run`] loop exits so waiters fail instead of hanging.
    closed: AtomicBool,
    /// The channel settlement acks push their keys onto.
    ack_tx: tokio::sync::mpsc::UnboundedSender<CompletionKey<VmId>>,
}

// ---------------------------------------------------------------------------
// DemandRegistrar
// ---------------------------------------------------------------------------

/// Create the shared poller state.
///
/// Returns the cloneable [`DemandRegistrar`] for subscribing VMs and the
/// opaque [`StateToken`] that [`run`] consumes.
///
/// `ack_tx` is where the settlements produced by the subscribed handles
/// push their keys on acknowledgement — pair it with an
/// [`acker::run`](crate::acker::run) loop driving the receiving half.
pub fn state<VmId>(
    ack_tx: tokio::sync::mpsc::UnboundedSender<CompletionKey<VmId>>,
) -> (DemandRegistrar<VmId>, StateToken<VmId>)
where
    VmId: Eq + std::hash::Hash,
{
    let shared = Arc::new(Shared {
        entries: DashMap::new(),
        demand_notify: tokio::sync::Notify::new(),
        closed: AtomicBool::new(false),
        ack_tx,
    });
    (
        DemandRegistrar {
            shared: Arc::clone(&shared),
        },
        StateToken { shared },
    )
}

/// The opaque token of the shared poller state.
///
/// Created by [`state`]; its only job is to be handed to [`run`] via
/// [`Params`].
pub struct StateToken<VmId> {
    shared: Arc<Shared<VmId>>,
}

/// Subscribes VMs to the shared poller state.
///
/// Created by [`state`]; cloneable, so it can be handed out to every
/// place that wires up VMs.
pub struct DemandRegistrar<VmId> {
    shared: Arc<Shared<VmId>>,
}

impl<VmId> Clone for DemandRegistrar<VmId> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<VmId> DemandRegistrar<VmId>
where
    VmId: Copy + Eq + std::hash::Hash,
{
    /// Subscribe a VM and return its demand-driven settlements handle.
    ///
    /// Subscribing a VM that already has a live entry replaces the entry:
    /// the previous handle keeps its buffer but will no longer receive
    /// deliveries, and its eventual drop does not disturb the new entry.
    pub fn subscribe(&self, vm_id: VmId) -> SettlementsHandle<VmId> {
        let entry = Arc::new(VmEntry::default());
        self.shared.entries.insert(vm_id, Arc::clone(&entry));
        SettlementsHandle {
            vm_id,
            entry,
            shared: Arc::clone(&self.shared),
        }
    }
}

// ---------------------------------------------------------------------------
// run
// ---------------------------------------------------------------------------

/// Parameters for [`run`].
pub struct Params<Backend, Codec>
where
    Backend: PollCompletions,
{
    /// The durable completions backend to poll.
    pub backend: Arc<Backend>,

    /// The codec used to decode stored action-call outcomes.
    pub codec: Codec,

    /// The token of the shared state to poll for and deliver to.
    pub state: StateToken<Backend::VmId>,
}

/// Poll the backend for demanded completions until a critical failure.
///
/// Drive this in a background task.  Parks while no demand is registered;
/// otherwise polls in a tight loop.  The loop never completes normally —
/// errors are critical, and the caller should stop the subsystem.  When
/// the loop returns (or its future is dropped after starting), the shared
/// state is marked closed and all waiting handles fail.
pub async fn run<Backend, Codec>(
    params: Params<Backend, Codec>,
) -> Result<std::convert::Infallible, Error<Backend::Error, Codec::Error>>
where
    Backend: PollCompletions,
    Backend::VmId: Copy + Eq + std::hash::Hash,
    Codec: waymark_vm_codec_core::DeserializerProvider,
{
    let Params {
        backend,
        codec,
        state,
    } = params;
    let shared = state.shared;
    let _closed_guard = ClosedGuard(Arc::clone(&shared));

    loop {
        // Arm the wakeup before collecting so a registration racing the
        // collection is never missed.
        let registered = shared.demand_notify.notified();

        let Some(demand) = collect_demand(&shared) else {
            registered.await;
            continue;
        };

        let records = backend
            .poll_completions(demand.as_nonempty_slice())
            .await
            .map_err(Error::Poll)?;

        for record in records {
            deliver(&shared, &codec, record).map_err(Error::OutcomeDecode)?;
        }
    }
}

/// Marks the registry closed when the [`run`] loop goes away, waking every
/// parked waiter so it fails instead of hanging.
struct ClosedGuard<VmId>(Arc<Shared<VmId>>)
where
    VmId: Eq + std::hash::Hash;

impl<VmId> Drop for ClosedGuard<VmId>
where
    VmId: Eq + std::hash::Hash,
{
    fn drop(&mut self) {
        self.0.closed.store(true, Ordering::SeqCst);
        // `notify_one` (not `notify_waiters`) so a permit is stored for a
        // waiter that has checked `closed` but not yet parked — otherwise
        // it would sleep through the shutdown and hang forever.
        for entry in self.0.entries.iter() {
            entry.value().notify.notify_one();
        }
    }
}

/// The union of all registered demand, minus rows already buffered.
fn collect_demand<VmId>(shared: &Shared<VmId>) -> Option<NEVec<CompletionKey<VmId>>>
where
    VmId: Copy + Eq + std::hash::Hash,
{
    let mut keys = Vec::new();
    for entry in shared.entries.iter() {
        let vm_id = *entry.key();
        let state = entry.value().state.lock().expect("entry state poisoned");
        let buffered: HashSet<PromiseStateId> = state
            .buffer
            .iter()
            .map(|completion| completion.promise_state_id)
            .collect();
        keys.extend(
            state
                .demand
                .iter()
                .filter(|id| !buffered.contains(id))
                .map(|id| CompletionKey {
                    vm_id,
                    promise_state_id: *id,
                }),
        );
    }
    NEVec::try_from_vec(keys)
}

/// Decode a fetched record and deliver it to its VM's buffer.
fn deliver<VmId, Codec>(
    shared: &Shared<VmId>,
    codec: &Codec,
    record: CompletionRecord<VmId>,
) -> Result<(), Codec::Error>
where
    VmId: Eq + std::hash::Hash,
    Codec: waymark_vm_codec_core::DeserializerProvider,
{
    let outcome: ActionCallOutcome<ReadyValue> = codec
        .with_deserializer(&record.outcome, |deserializer| {
            serde::Deserialize::deserialize(deserializer)
        })?;

    let Some(entry) = shared.entries.get(&record.vm_id) else {
        // The handle was dropped while the query was in flight; the
        // row stays in the backend for a future registration.
        return Ok(());
    };

    let mut state = entry.state.lock().expect("entry state poisoned");
    let already_buffered = state
        .buffer
        .iter()
        .any(|completion| completion.promise_state_id == record.promise_state_id);
    if !already_buffered {
        state.buffer.push(BufferedCompletion {
            promise_state_id: record.promise_state_id,
            outcome,
        });
    }
    drop(state);
    entry.notify.notify_one();

    Ok(())
}

// ---------------------------------------------------------------------------
// SettlementsHandle
// ---------------------------------------------------------------------------

/// Per-VM demand-driven promise settler over durably-stored completions.
///
/// Created by [`DemandRegistrar::subscribe`].  Implements
/// [`waymark_extcall_reconciler_core::ActionPromiseSettler`]: each call
/// registers the demanded promise ids, waits until the shared poller
/// delivers matching completions, and settles them with [`Ack`]s minted
/// from the rows' own keys.
pub struct SettlementsHandle<VmId>
where
    VmId: Eq + std::hash::Hash,
{
    vm_id: VmId,
    entry: Arc<VmEntry>,
    shared: Arc<Shared<VmId>>,
}

impl<VmId> Drop for SettlementsHandle<VmId>
where
    VmId: Eq + std::hash::Hash,
{
    fn drop(&mut self) {
        // Only remove the entry if it is still ours — the VM may have been
        // re-subscribed while this handle was winding down.
        self.shared
            .entries
            .remove_if(&self.vm_id, |_, entry| Arc::ptr_eq(entry, &self.entry));
    }
}

impl<VmId> waymark_extcall_reconciler_core::SettlerAck for SettlementsHandle<VmId>
where
    VmId: Eq + std::hash::Hash,
{
    type Ack = Ack<VmId>;
}

impl<VmId> waymark_extcall_reconciler_core::HasValue for SettlementsHandle<VmId>
where
    VmId: Eq + std::hash::Hash,
{
    type Value = ReadyValue;
}

impl<VmId, UnifiedAck> waymark_extcall_reconciler_core::ActionPromiseSettler<UnifiedAck>
    for SettlementsHandle<VmId>
where
    VmId: Copy + Eq + std::hash::Hash + Send + Sync + 'static,
    UnifiedAck: From<Ack<VmId>>,
{
    type Error = PollActionSettlementsError;

    async fn poll_action_settlements<'a>(
        &'a mut self,
        waiting_promise_state_ids: NESlice<'a, PromiseStateId>,
    ) -> Result<NEVec<PromiseSettlement<Self::Value, UnifiedAck>>, Self::Error>
    where
        UnifiedAck: 'a,
    {
        loop {
            {
                let mut state = self.entry.state.lock().expect("entry state poisoned");
                // Refresh the registered demand — this call's set replaces
                // whatever a previous (possibly cancelled) call left.
                state.demand = waiting_promise_state_ids.iter().copied().collect();

                let mut matched = Vec::new();
                let mut index = 0;
                while index < state.buffer.len() {
                    if state.demand.contains(&state.buffer[index].promise_state_id) {
                        matched.push(state.buffer.swap_remove(index));
                    } else {
                        index += 1;
                    }
                }

                if let Some(matched) = NEVec::try_from_vec(matched) {
                    // Satisfied: clear the demand so the poller stops
                    // fetching for this VM until the next call.
                    state.demand.clear();
                    drop(state);
                    return Ok(self.settle(matched));
                }
            }

            // Checked after the buffer so rows delivered before the poller
            // went away can still be drained.
            if self.shared.closed.load(Ordering::SeqCst) {
                return Err(PollActionSettlementsError::PollerGone);
            }

            self.shared.demand_notify.notify_one();
            self.entry.notify.notified().await;
        }
    }
}

impl<VmId> SettlementsHandle<VmId>
where
    VmId: Copy + Eq + std::hash::Hash,
{
    /// Turn buffered completions into settlements with key-carrying acks.
    fn settle<UnifiedAck>(
        &self,
        completions: NEVec<BufferedCompletion>,
    ) -> NEVec<PromiseSettlement<ReadyValue, UnifiedAck>>
    where
        UnifiedAck: From<Ack<VmId>>,
    {
        completions
            .into_nonempty_iter()
            .map(|completion| {
                let BufferedCompletion {
                    promise_state_id,
                    outcome,
                } = completion;

                let resolution = match outcome {
                    ActionCallOutcome::Value(value) => PromiseResolution::Resolved(value),
                    ActionCallOutcome::Exception(exception) => {
                        PromiseResolution::Rejected(exception)
                    }
                };

                PromiseSettlement {
                    promise_state_id,
                    resolution,
                    ack: Ack {
                        key: CompletionKey {
                            vm_id: self.vm_id,
                            promise_state_id,
                        },
                        ack_tx: self.shared.ack_tx.clone(),
                    }
                    .into(),
                }
            })
            .collect()
    }
}
