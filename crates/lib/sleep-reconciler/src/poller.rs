//! Demand-driven settlement polling — a single shared loop polls the
//! backend for exactly the sleeps the VMs are currently waiting on, as
//! they come due, and settles their promises.
//!
//! Built on [`waymark_promise_settlement_demand_registry::registry`]:
//! the registry owns demand registration, item buffering, delivery, and
//! the waiter lifecycle — see its docs for the architecture and the
//! cancellation-safety story.  This module owns the backend query and
//! the mapping of delivered due sleeps into promise settlements — an
//! elapsed sleep resolves to the value minted by the handle's
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

#[cfg(test)]
mod tests;

use std::sync::Arc;

use nonempty_collections::{IntoNonEmptyIterator as _, NESlice, NEVec, NonEmptyIterator as _};
use waymark_nonzero_duration::NonZeroDuration;
use waymark_promise_settlement_demand_registry::registry;
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
    let (registrar, token) = registry::state(ack_tx);
    (
        DemandRegistrar { inner: registrar },
        StateToken { inner: token },
    )
}

/// The opaque token of the shared poller state.
///
/// Created by [`state`]; its only job is to be handed to [`run`] via
/// [`Params`].
pub struct StateToken<VmId>
where
    VmId: Eq + std::hash::Hash,
{
    inner: registry::StateToken<VmId, PromiseStateId, SleepKey<VmId>>,
}

/// Subscribes VMs to the shared poller state.
///
/// Created by [`state`]; cloneable, so it can be handed out to every
/// place that wires up VMs.
pub struct DemandRegistrar<VmId>
where
    VmId: Eq + std::hash::Hash,
{
    inner: registry::DemandRegistrar<VmId, PromiseStateId, SleepKey<VmId>>,
}

impl<VmId> Clone for DemandRegistrar<VmId>
where
    VmId: Eq + std::hash::Hash,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
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
        SettlementsHandle {
            inner: self.inner.subscribe(vm_id),
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
    Backend::VmId: Eq + std::hash::Hash,
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
    let driver = state.inner.into_driver();

    loop {
        // Arm the wakeup before collecting so a registration racing the
        // collection is never missed.
        let registered = driver.demand_registered();

        let Some(demand) = driver.collect_demand(|vm_id, promise_state_id| SleepKey {
            vm_id: vm_id.clone(),
            promise_state_id,
        }) else {
            registered.await;
            continue;
        };

        let due = backend
            .poll_due_sleeps(chrono::Utc::now(), demand.as_nonempty_slice())
            .await
            .map_err(Error::Poll)?;

        for key in due {
            driver.deliver(&key.vm_id, key.promise_state_id);
        }

        // Wait out the poll interval; new demand cuts the wait short —
        // a just-registered sleep may already be due.
        tokio::select! {
            () = tokio::time::sleep(poll_interval.get()) => {}
            () = registered => {}
        }
    }
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
    inner: registry::DemandHandle<VmId, PromiseStateId, SleepKey<VmId>>,
    /// The sleep value provider is purely type-level.
    provider: std::marker::PhantomData<fn() -> SleepValueProvider>,
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
        let matched = self
            .inner
            .wait_for_matching(waiting_promise_state_ids)
            .await
            .map_err(|registry::RegistryClosedError| PollSleepSettlementsError::PollerGone)?;
        Ok(self.settle(matched))
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
                            vm_id: self.inner.vm_id().clone(),
                            promise_state_id,
                        },
                        ack_tx: self.inner.ack_sender().clone(),
                    }
                    .into(),
                }
            })
            .collect()
    }
}
