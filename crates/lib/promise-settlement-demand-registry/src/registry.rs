//! The demand registry — per-VM demand registration, item buffering,
//! and delivery between a shared poll loop and per-VM waiters.
//!
//! # Architecture
//!
//! [`state`] creates the shared state of per-VM entries, taking the
//! sender of the ack channel; it returns the cloneable
//! [`DemandRegistrar`] and the opaque [`StateToken`].  Use
//! [`subscribe`](DemandRegistrar::subscribe) to wire up each VM with a
//! [`DemandHandle`], and turn the token into the [`PollDriver`] that the
//! single shared poll loop drives.
//!
//! A [`DemandHandle::wait_for_matching`] call registers its demand (the
//! promise ids the VM is waiting on) and parks; the poll loop collects
//! the union of all registered demand (minus items already buffered) via
//! [`PollDriver::collect_demand`], queries its backend however the
//! domain dictates, and hands the fetched items to
//! [`PollDriver::deliver`], which routes them into the owning handles'
//! buffers.  The domain wraps the returned items into promise
//! settlements; the acks it mints push keys onto the ack channel the
//! registry was created with (see [`DemandHandle::ack_sender`]), from
//! where an [`acker::run`](crate::acker::run) loop deletes the rows.
//!
//! Rows stay in the backend until acked, so all in-memory state here is
//! disposable: a crashed or re-registered VM simply re-demands and
//! re-fetches.
//!
//! # Cancellation safety
//!
//! A `wait_for_matching` future may be dropped at any time.  Items
//! delivered for it land in its buffer and are returned by the next
//! call; demand registered by a cancelled call is refreshed (replaced)
//! by the next call.  Dropping the handle itself removes its registry
//! entry (identity-guarded, so a stale handle cannot displace a
//! re-registered successor) and discards its buffer — the rows are still
//! in the backend.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use dashmap::DashMap;
use nonempty_collections::NEVec;
use waymark_vm_runtime_promise_core::PromiseStateId;

/// A buffered item, keyed by the promise it settles.
pub trait HasPromiseStateId {
    /// The promise this item settles, scoped to its VM.
    fn promise_state_id(&self) -> PromiseStateId;
}

impl HasPromiseStateId for PromiseStateId {
    fn promise_state_id(&self) -> PromiseStateId {
        *self
    }
}

/// Error returned when the registry's poll loop is gone; no items will
/// ever be delivered again.
#[derive(Debug, thiserror::Error)]
#[error("demand poller gone")]
pub struct RegistryClosedError;

/// Per-VM mutable state: the currently registered demand and the items
/// fetched-but-not-yet-consumed.
struct EntryState<Item> {
    demand: HashSet<PromiseStateId>,
    buffer: Vec<Item>,
}

impl<Item> Default for EntryState<Item> {
    fn default() -> Self {
        Self {
            demand: HashSet::new(),
            buffer: Vec::new(),
        }
    }
}

/// One registered VM: its state plus the waiter wakeup.
struct VmEntry<Item> {
    state: std::sync::Mutex<EntryState<Item>>,
    notify: tokio::sync::Notify,
}

impl<Item> Default for VmEntry<Item> {
    fn default() -> Self {
        Self {
            state: std::sync::Mutex::new(EntryState::default()),
            notify: tokio::sync::Notify::new(),
        }
    }
}

/// State shared between the poll driver, the registrar, and the handles.
struct Shared<VmId, Item, Key>
where
    VmId: Eq + std::hash::Hash,
{
    entries: DashMap<VmId, Arc<VmEntry<Item>>>,
    /// Wakes the poll loop when new demand is registered.
    demand_notify: tokio::sync::Notify,
    /// Set when the [`PollDriver`] goes away so waiters fail instead of
    /// hanging.
    closed: AtomicBool,
    /// The channel settlement acks push their keys onto.
    ack_tx: tokio::sync::mpsc::UnboundedSender<Key>,
}

/// Create the shared registry state.
///
/// Returns the cloneable [`DemandRegistrar`] for subscribing VMs and the
/// opaque [`StateToken`] the poll loop turns into its [`PollDriver`].
///
/// `ack_tx` is where the settlements produced from the delivered items
/// push their keys on acknowledgement — pair it with an
/// [`acker::run`](crate::acker::run) loop driving the receiving half.
pub fn state<VmId, Item, Key>(
    ack_tx: tokio::sync::mpsc::UnboundedSender<Key>,
) -> (
    DemandRegistrar<VmId, Item, Key>,
    StateToken<VmId, Item, Key>,
)
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

/// The opaque token of the shared registry state.
///
/// Created by [`state`]; its only job is to be turned into the poll
/// loop's [`PollDriver`] via [`StateToken::into_driver`].
pub struct StateToken<VmId, Item, Key>
where
    VmId: Eq + std::hash::Hash,
{
    shared: Arc<Shared<VmId, Item, Key>>,
}

impl<VmId, Item, Key> StateToken<VmId, Item, Key>
where
    VmId: Eq + std::hash::Hash,
{
    /// Turn the token into the poll loop's driver.
    ///
    /// When the driver is dropped (the poll loop returned or its future
    /// was dropped after starting), the registry is marked closed and
    /// every parked waiter fails instead of hanging.
    pub fn into_driver(self) -> PollDriver<VmId, Item, Key> {
        PollDriver {
            shared: self.shared,
        }
    }
}

/// Subscribes VMs to the shared registry state.
///
/// Created by [`state`]; cloneable, so it can be handed out to every
/// place that wires up VMs.
pub struct DemandRegistrar<VmId, Item, Key>
where
    VmId: Eq + std::hash::Hash,
{
    shared: Arc<Shared<VmId, Item, Key>>,
}

impl<VmId, Item, Key> Clone for DemandRegistrar<VmId, Item, Key>
where
    VmId: Eq + std::hash::Hash,
{
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<VmId, Item, Key> DemandRegistrar<VmId, Item, Key>
where
    VmId: Clone + Eq + std::hash::Hash,
{
    /// Subscribe a VM and return its demand handle.
    ///
    /// Subscribing a VM that already has a live entry replaces the entry:
    /// the previous handle keeps its buffer but will no longer receive
    /// deliveries, and its eventual drop does not disturb the new entry.
    pub fn subscribe(&self, vm_id: VmId) -> DemandHandle<VmId, Item, Key> {
        let entry = Arc::new(VmEntry::default());
        self.shared
            .entries
            .insert(vm_id.clone(), Arc::clone(&entry));
        DemandHandle {
            vm_id,
            entry,
            shared: Arc::clone(&self.shared),
        }
    }
}

// ---------------------------------------------------------------------------
// PollDriver
// ---------------------------------------------------------------------------

/// The poll loop's side of the registry.
///
/// Created by [`StateToken::into_driver`].  Dropping it marks the
/// registry closed, waking every parked waiter so it fails instead of
/// hanging.
pub struct PollDriver<VmId, Item, Key>
where
    VmId: Eq + std::hash::Hash,
{
    shared: Arc<Shared<VmId, Item, Key>>,
}

impl<VmId, Item, Key> Drop for PollDriver<VmId, Item, Key>
where
    VmId: Eq + std::hash::Hash,
{
    fn drop(&mut self) {
        self.shared.closed.store(true, Ordering::SeqCst);
        // `notify_one` (not `notify_waiters`) so a permit is stored for a
        // waiter that has checked `closed` but not yet parked — otherwise
        // it would sleep through the shutdown and hang forever.
        for entry in self.shared.entries.iter() {
            entry.value().notify.notify_one();
        }
    }
}

impl<VmId, Item, Key> PollDriver<VmId, Item, Key>
where
    VmId: Eq + std::hash::Hash,
{
    /// The new-demand wakeup.
    ///
    /// Arm it (create the future) before [`collect_demand`] so a
    /// registration racing the collection is never missed.
    ///
    /// [`collect_demand`]: PollDriver::collect_demand
    pub fn demand_registered(&self) -> tokio::sync::futures::Notified<'_> {
        self.shared.demand_notify.notified()
    }

    /// The union of all registered demand, minus items already buffered.
    ///
    /// Keys are built by `make_key` from the owning VM and the demanded
    /// promise id.
    pub fn collect_demand(
        &self,
        mut make_key: impl FnMut(&VmId, PromiseStateId) -> Key,
    ) -> Option<NEVec<Key>>
    where
        Item: HasPromiseStateId,
    {
        let mut keys = Vec::new();
        for entry in self.shared.entries.iter() {
            let vm_id = entry.key();
            let state = entry.value().state.lock().expect("entry state poisoned");
            let buffered: HashSet<PromiseStateId> = state
                .buffer
                .iter()
                .map(HasPromiseStateId::promise_state_id)
                .collect();
            keys.extend(
                state
                    .demand
                    .iter()
                    .filter(|id| !buffered.contains(id))
                    .map(|id| make_key(vm_id, *id)),
            );
        }
        NEVec::try_from_vec(keys)
    }

    /// Deliver a fetched item to its VM's buffer.
    ///
    /// An item whose promise is already buffered is dropped (the next
    /// consumption settles it either way); an item whose VM has no live
    /// entry is dropped too — the handle was dropped while the query was
    /// in flight, and the row stays in the backend for a future
    /// registration.
    pub fn deliver(&self, vm_id: &VmId, item: Item)
    where
        Item: HasPromiseStateId,
    {
        let Some(entry) = self.shared.entries.get(vm_id) else {
            return;
        };

        let mut state = entry.state.lock().expect("entry state poisoned");
        let already_buffered = state
            .buffer
            .iter()
            .any(|buffered| buffered.promise_state_id() == item.promise_state_id());
        if !already_buffered {
            state.buffer.push(item);
        }
        drop(state);
        entry.notify.notify_one();
    }
}

// ---------------------------------------------------------------------------
// DemandHandle
// ---------------------------------------------------------------------------

/// Per-VM demand handle.
///
/// Created by [`DemandRegistrar::subscribe`].  Each
/// [`wait_for_matching`](DemandHandle::wait_for_matching) call registers
/// the demanded promise ids and waits until the poll loop delivers
/// matching items.
pub struct DemandHandle<VmId, Item, Key>
where
    VmId: Eq + std::hash::Hash,
{
    vm_id: VmId,
    entry: Arc<VmEntry<Item>>,
    shared: Arc<Shared<VmId, Item, Key>>,
}

impl<VmId, Item, Key> Drop for DemandHandle<VmId, Item, Key>
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

impl<VmId, Item, Key> DemandHandle<VmId, Item, Key>
where
    VmId: Eq + std::hash::Hash,
{
    /// The VM this handle demands settlements for.
    pub fn vm_id(&self) -> &VmId {
        &self.vm_id
    }

    /// The sender of the ack channel the registry was created with.
    ///
    /// Clone it into the acks minted for the settlements built from the
    /// items this handle returns.
    pub fn ack_sender(&self) -> &tokio::sync::mpsc::UnboundedSender<Key> {
        &self.shared.ack_tx
    }

    /// Register the demanded promise ids and wait for matching items.
    ///
    /// This call's demand set replaces whatever a previous (possibly
    /// cancelled) call left; it is cleared again once satisfied, so the
    /// poll loop stops fetching for this VM until the next call.
    pub async fn wait_for_matching(
        &mut self,
        waiting_promise_state_ids: nonempty_collections::NESlice<'_, PromiseStateId>,
    ) -> Result<NEVec<Item>, RegistryClosedError>
    where
        Item: HasPromiseStateId,
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
                    if state
                        .demand
                        .contains(&state.buffer[index].promise_state_id())
                    {
                        matched.push(state.buffer.swap_remove(index));
                    } else {
                        index += 1;
                    }
                }

                if let Some(matched) = NEVec::try_from_vec(matched) {
                    // Satisfied: clear the demand so the poll loop stops
                    // fetching for this VM until the next call.
                    state.demand.clear();
                    drop(state);
                    return Ok(matched);
                }
            }

            // Checked after the buffer so items delivered before the poll
            // loop went away can still be drained.
            if self.shared.closed.load(Ordering::SeqCst) {
                return Err(RegistryClosedError);
            }

            self.shared.demand_notify.notify_one();
            self.entry.notify.notified().await;
        }
    }
}
