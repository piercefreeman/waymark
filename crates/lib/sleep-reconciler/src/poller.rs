//! Demand-driven settlement polling — a single shared loop polls the
//! backend for exactly the sleeps the VMs are currently waiting on, as
//! they come due, and settles their promises.
//!
//! # Architecture
//!
//! [`state`] creates the shared state of per-VM entries, taking the
//! sender of the ack channel; it returns the cloneable
//! [`DemandRegistrar`] and the opaque [`StateToken`].  Use
//! [`subscribe`](DemandRegistrar::subscribe) to wire up each VM with a
//! [`SettlementsHandle`] — the per-VM implementation of
//! [`waymark_extcall_reconciler_core::SleepPromiseSettler`] — and drive
//! [`run`] with the [`StateToken`] in its [`Params`] as the single
//! shared poll loop.
//!
//! A handle call registers its demand (the promise ids the VM is waiting
//! on) and parks; the poller loop queries the union of all registered
//! demand (minus rows already buffered) for rows that have come due and
//! delivers them into the owning handles' buffers.  The handle turns
//! delivered keys into promise settlements — an elapsed sleep resolves
//! to the value minted by the handle's
//! [`waymark_sleep_core::SleepValueProvider`] — each carrying an [`Ack`]
//! minted from the row's own key: acknowledging (after the settlement
//! has been applied and the VM state persisted) pushes the key onto the
//! ack channel the registry was created with, from where the background
//! [`acker::run`](crate::acker::run) loop deletes the row.
//!
//! Unlike the completions poller, satisfied demand is not the only thing
//! that changes between polls: a recorded sleep becomes due purely by
//! time passing, and rows can be recorded outside the local process's
//! notifications.  The loop therefore re-polls on a configured interval
//! ([`Params::poll_interval`]) while demand is registered, and
//! immediately when new demand arrives.
//!
//! Rows stay in the backend until acked, so all in-memory state here is
//! disposable: a crashed or re-registered VM simply re-demands and
//! re-fetches.
//!
//! # Cancellation safety
//!
//! A handle's `poll_sleep_settlements` future may be dropped at any
//! time.  Keys fetched for it land in its buffer and are returned by the
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
use waymark_nonzero_duration::NonZeroDuration;
use waymark_sleep_reconciler_backend::{PollDueSleeps, SleepKey};
use waymark_vm_driver_core::{PromiseResolution, PromiseSettlement};
use waymark_vm_runtime_promise_core::PromiseStateId;

/// Error returned when the shared poll loop stops on a critical failure.
#[derive(Debug, thiserror::Error)]
pub enum Error<PollError> {
    /// The backend failed to poll for due sleeps.
    #[error("polling due sleeps: {0}")]
    Poll(#[source] PollError),
}

/// Error returned when polling a [`SettlementsHandle`] for sleep
/// settlements fails.
#[derive(Debug, thiserror::Error)]
pub enum PollSleepSettlementsError {
    /// The shared [`run`] loop is gone; no settlements will ever be
    /// delivered again.
    #[error("demand poller gone")]
    PollerGone,
}

/// Settlement acknowledgement for durably-recorded sleeps.
///
/// Minted by a [`SettlementsHandle`] from the delivered row's own key.
/// Acknowledging (after the settlement has been applied and the VM state
/// persisted) pushes the key onto the poller's ack channel, from where
/// the background [`acker::run`](crate::acker::run) loop deletes the
/// row.
pub struct Ack<VmId> {
    key: SleepKey<VmId>,
    ack_tx: tokio::sync::mpsc::UnboundedSender<SleepKey<VmId>>,
}

impl<VmId> waymark_vm_driver_core::PromiseSettlementAck for Ack<VmId> {
    fn acknowledge_promise_settlement(self) {
        // A closed channel means the acker is shutting down; the row stays
        // in the backend and is re-settled (and re-acked) on revive.
        let _ = self.ack_tx.send(self.key);
    }
}

impl<ActionAck, VmId> From<Ack<VmId>>
    for waymark_extcall_reconciler_core::Ack<ActionAck, Ack<VmId>>
{
    fn from(value: Ack<VmId>) -> Self {
        waymark_extcall_reconciler_core::Ack::Sleep(value)
    }
}

/// Per-VM mutable state: the currently registered demand and the keys
/// fetched-but-not-yet-consumed.
#[derive(Default)]
struct EntryState {
    demand: HashSet<PromiseStateId>,
    buffer: Vec<PromiseStateId>,
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
    ack_tx: tokio::sync::mpsc::UnboundedSender<SleepKey<VmId>>,
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
    ack_tx: tokio::sync::mpsc::UnboundedSender<SleepKey<VmId>>,
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
    VmId: Clone + Eq + std::hash::Hash,
{
    /// Subscribe a VM and return its demand-driven settlements handle.
    ///
    /// `SleepValueProvider` supplies the value the handle's settlements
    /// resolve with.
    ///
    /// Subscribing a VM that already has a live entry replaces the entry:
    /// the previous handle keeps its buffer but will no longer receive
    /// deliveries, and its eventual drop does not disturb the new entry.
    pub fn subscribe<SleepValueProvider>(
        &self,
        vm_id: VmId,
    ) -> SettlementsHandle<VmId, SleepValueProvider> {
        let entry = Arc::new(VmEntry::default());
        self.shared
            .entries
            .insert(vm_id.clone(), Arc::clone(&entry));
        SettlementsHandle {
            vm_id,
            entry,
            shared: Arc::clone(&self.shared),
            provider: std::marker::PhantomData,
        }
    }
}

// ---------------------------------------------------------------------------
// run
// ---------------------------------------------------------------------------

/// Parameters for [`run`].
pub struct Params<Backend>
where
    Backend: PollDueSleeps,
{
    /// The durable sleeps backend to poll.
    pub backend: Arc<Backend>,

    /// The token of the shared state to poll for and deliver to.
    pub state: StateToken<Backend::VmId>,

    /// How long the loop waits between polls while demand is registered.
    ///
    /// Bounds the settlement latency of a sleep that comes due (or is
    /// recorded outside the local process's notifications) between
    /// polls.
    pub poll_interval: NonZeroDuration,
}

/// Poll the backend for demanded due sleeps until a critical failure.
///
/// Drive this in a background task.  Parks while no demand is registered;
/// otherwise polls every [`Params::poll_interval`], and immediately when
/// new demand arrives.  The loop never completes normally — errors are
/// critical, and the caller should stop the subsystem.  When the loop
/// returns (or its future is dropped after starting), the shared state is
/// marked closed and all waiting handles fail.
pub async fn run<Backend>(
    params: Params<Backend>,
) -> Result<std::convert::Infallible, Error<Backend::Error>>
where
    Backend: PollDueSleeps<Timestamp = chrono::DateTime<chrono::Utc>>,
    Backend::VmId: Clone + Eq + std::hash::Hash,
{
    let Params {
        backend,
        state,
        poll_interval,
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

        let due = backend
            .poll_due_sleeps(chrono::Utc::now(), demand.as_nonempty_slice())
            .await
            .map_err(Error::Poll)?;

        for key in due {
            deliver(&shared, key);
        }

        // Wait out the poll interval; new demand cuts the wait short —
        // a just-registered sleep may already be due.
        tokio::select! {
            () = tokio::time::sleep(poll_interval.get()) => {}
            () = registered => {}
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

/// The union of all registered demand, minus keys already buffered.
fn collect_demand<VmId>(shared: &Shared<VmId>) -> Option<NEVec<SleepKey<VmId>>>
where
    VmId: Clone + Eq + std::hash::Hash,
{
    let mut keys = Vec::new();
    for entry in shared.entries.iter() {
        let vm_id = entry.key();
        let state = entry.value().state.lock().expect("entry state poisoned");
        keys.extend(
            state
                .demand
                .iter()
                .filter(|id| !state.buffer.contains(id))
                .map(|id| SleepKey {
                    vm_id: vm_id.clone(),
                    promise_state_id: *id,
                }),
        );
    }
    NEVec::try_from_vec(keys)
}

/// Deliver a due key to its VM's buffer.
fn deliver<VmId>(shared: &Shared<VmId>, key: SleepKey<VmId>)
where
    VmId: Eq + std::hash::Hash,
{
    let Some(entry) = shared.entries.get(&key.vm_id) else {
        // The handle was dropped while the query was in flight; the
        // row stays in the backend for a future registration.
        return;
    };

    let mut state = entry.state.lock().expect("entry state poisoned");
    if !state.buffer.contains(&key.promise_state_id) {
        state.buffer.push(key.promise_state_id);
    }
    drop(state);
    entry.notify.notify_one();
}

// ---------------------------------------------------------------------------
// SettlementsHandle
// ---------------------------------------------------------------------------

/// Per-VM demand-driven promise settler over durably-recorded sleeps.
///
/// Created by [`DemandRegistrar::subscribe`].  Implements
/// [`waymark_extcall_reconciler_core::SleepPromiseSettler`]: each call
/// registers the demanded promise ids, waits until the shared poller
/// delivers due sleeps, and settles them — resolving to the value minted
/// by `SleepValueProvider` — with [`Ack`]s minted from the rows' own
/// keys.
pub struct SettlementsHandle<VmId, SleepValueProvider>
where
    VmId: Eq + std::hash::Hash,
{
    vm_id: VmId,
    entry: Arc<VmEntry>,
    shared: Arc<Shared<VmId>>,
    /// The sleep value provider is purely type-level.
    provider: std::marker::PhantomData<fn() -> SleepValueProvider>,
}

impl<VmId, SleepValueProvider> Drop for SettlementsHandle<VmId, SleepValueProvider>
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

impl<VmId, SleepValueProvider> waymark_extcall_reconciler_core::SettlerAck
    for SettlementsHandle<VmId, SleepValueProvider>
where
    VmId: Eq + std::hash::Hash,
{
    type Ack = Ack<VmId>;
}

impl<VmId, SleepValueProvider> waymark_extcall_reconciler_core::HasValue
    for SettlementsHandle<VmId, SleepValueProvider>
where
    VmId: Eq + std::hash::Hash,
    SleepValueProvider: waymark_sleep_core::SleepValueProvider,
{
    type Value = SleepValueProvider::Value;
}

impl<VmId, SleepValueProvider, UnifiedAck>
    waymark_extcall_reconciler_core::SleepPromiseSettler<UnifiedAck>
    for SettlementsHandle<VmId, SleepValueProvider>
where
    VmId: Clone + Eq + std::hash::Hash + Send + Sync + 'static,
    SleepValueProvider: waymark_sleep_core::SleepValueProvider,
    UnifiedAck: From<Ack<VmId>>,
{
    type Error = PollSleepSettlementsError;

    async fn poll_sleep_settlements<'a>(
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
                    if state.demand.contains(&state.buffer[index]) {
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

            // Checked after the buffer so keys delivered before the poller
            // went away can still be drained.
            if self.shared.closed.load(Ordering::SeqCst) {
                return Err(PollSleepSettlementsError::PollerGone);
            }

            self.shared.demand_notify.notify_one();
            self.entry.notify.notified().await;
        }
    }
}

impl<VmId, SleepValueProvider> SettlementsHandle<VmId, SleepValueProvider>
where
    VmId: Clone + Eq + std::hash::Hash,
    SleepValueProvider: waymark_sleep_core::SleepValueProvider,
{
    /// Turn buffered due sleeps into settlements with key-carrying acks.
    fn settle<UnifiedAck>(
        &self,
        due: NEVec<PromiseStateId>,
    ) -> NEVec<PromiseSettlement<SleepValueProvider::Value, UnifiedAck>>
    where
        UnifiedAck: From<Ack<VmId>>,
    {
        due.into_nonempty_iter()
            .map(|promise_state_id| {
                tracing::debug!(?promise_state_id, "sleep elapsed");
                PromiseSettlement {
                    promise_state_id,
                    resolution: PromiseResolution::Resolved(SleepValueProvider::value()),
                    ack: Ack {
                        key: SleepKey {
                            vm_id: self.vm_id.clone(),
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
