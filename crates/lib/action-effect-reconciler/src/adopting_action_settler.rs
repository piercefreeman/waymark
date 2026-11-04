//! The per-VM settler decorator adopting the VM's lapsed action-call
//! requests while the VM waits for settlements.

pub mod recheck;
#[cfg(test)]
mod tests;

use std::sync::Arc;

use nonempty_collections::NESlice;
use waymark_action_runtime_core::ActionCallStage;
use waymark_action_runtime_metadata::{ActionCallCorrelation, WithVmId};
use waymark_nonzero_duration::NonZeroDuration;
use waymark_vm_runtime_promise_core::PromiseStateId;

use crate::action_call_request::lock_batcher::{
    LockedRequest, PerVmLockerHandle, PerVmLockingError, PerVmLocks,
};
use crate::action_call_request::locks::Tracker;
use crate::lost_execution;

/// Error returned when polling an [`AdoptingActionSettler`] for settlements
/// fails.
#[derive(Debug, thiserror::Error)]
pub enum Error<SettleError> {
    /// The decorated settler failed.
    #[error("polling action settlements: {0}")]
    Settle(#[source] SettleError),

    /// Locking the VM's pending requests failed.
    #[error("locking pending action-call requests: {0}")]
    Lock(#[source] PerVmLockingError),

    /// An adopted call could not be reported as lost.
    #[error("reporting an adopted action call as lost: {0}")]
    Report(#[source] lost_execution::ProviderGoneError),
}

/// Where the VM stands with the requests whose calls this process has
/// not requested.
enum Adoption {
    /// No pass completed yet: the VM's outstanding requests may have been
    /// issued by another owner, or locked here for a call that never
    /// went through.
    Unchecked,

    /// The last pass found every request of the VM tracked here.
    Covered,

    /// Some request is held by another owner, or held here untracked;
    /// the next pass is due at the instant.
    Watching { next_pass_at: tokio::time::Instant },
}

/// An [`waymark_extcall_reconciler_core::ActionPromiseSettler`]
/// decorator: adopts the VM's action-call requests whose locks lapsed,
/// then polls the decorated settler.
///
/// A VM resident here owns the liveness of its action-call requests: a
/// request whose lock lapsed (its owner died, or its call never went
/// through) is adopted — locked through the shared lock batcher, its
/// call reported as a lost execution, its lock handed to the tracker — while
/// a request another live owner holds is left alone, an attempt is
/// presumed running in that owner's pool.  A request this process holds
/// was requested only if its lock is tracked; one held here
/// and untracked is a call that never went through, unlocked by the
/// renewal loop and adopted once it is — or a lock taken by a statement
/// that committed but reported failure, never tracked, left to lapse by
/// its time-to-live and adopted then.
///
/// A pass runs on the first wait of the VM and, for as long as the last
/// completed pass found a request not tracked here, once per heartbeat
/// while the VM waits.  A pass that found every request of the VM
/// tracked here is the last one: a tracked lock changes owner only
/// through an action-request lock fence breach, which stops the process
/// (see [`lock_renewal::run`](crate::action_call_request::lock_renewal::run)).
/// One thing reopens it: the VM's effect handler requesting a
/// [`recheck`], which runs a pass at once, in the pending wait or the
/// next one.  Dropping the settler drops the watch with it.
///
/// A failed pass fails the wait, and with it the VM's drive; the VM is
/// evicted and the next pinning cycle revives it.  The wait's future may
/// be dropped at any time: the locks a pass took and did not get to
/// request are reported by their pending call requests as never gone
/// through, and the pass's state is left as it was, so the next pass
/// adopts them again.
pub struct AdoptingActionSettler<Inner, VmId, Value, RaisedException>
where
    VmId: Eq + std::hash::Hash,
{
    /// The decorated settler.
    pub inner: Inner,

    /// The adopting side.
    pub adopter: Adopter<VmId, Value, RaisedException>,
}

/// The adopting side of an [`AdoptingActionSettler`].
pub struct Adopter<VmId, Value, RaisedException>
where
    VmId: Eq + std::hash::Hash,
{
    vm_id: VmId,

    locker: PerVmLockerHandle<VmId>,

    reporter: lost_execution::Reporter<VmId, Value, RaisedException>,

    action_call_request_locks_tracker: Arc<std::sync::Mutex<Tracker<VmId>>>,

    /// `None` once the tx is gone: no recheck is ever requested again.
    recheck: Option<recheck::Rx>,

    heartbeat: NonZeroDuration,

    adoption: Adoption,
}

impl<VmId, Value, RaisedException> Adopter<VmId, Value, RaisedException>
where
    VmId: Eq + std::hash::Hash,
{
    /// The adopter of the VM `vm_id`.
    ///
    /// `locker` is the shared lock batcher the passes go through;
    /// `reporter` reports the adopted calls as lost executions;
    /// `action_call_request_locks_tracker` is the tracker of this
    /// process's action-call request locks; `recheck` listens for the
    /// VM's effect handler finding a fresh emission already recorded;
    /// `heartbeat` paces the passes while a request of the VM is not
    /// tracked here.
    pub fn new(
        vm_id: VmId,
        locker: PerVmLockerHandle<VmId>,
        reporter: lost_execution::Reporter<VmId, Value, RaisedException>,
        action_call_request_locks_tracker: Arc<std::sync::Mutex<Tracker<VmId>>>,
        recheck: recheck::Rx,
        heartbeat: NonZeroDuration,
    ) -> Self {
        Self {
            vm_id,
            locker,
            reporter,
            action_call_request_locks_tracker,
            recheck: Some(recheck),
            heartbeat,
            adoption: Adoption::Unchecked,
        }
    }
}

impl<VmId, Value, RaisedException> Adopter<VmId, Value, RaisedException>
where
    VmId: Clone + Eq + std::hash::Hash + core::fmt::Debug,
{
    /// Lock the VM's eligible requests and report their calls as lost;
    /// then settle where the VM stands.
    async fn pass<SettleError>(&mut self) -> Result<(), Error<SettleError>> {
        // Requests up to here are answered by this pass; a request during
        // it buys another.
        if let Some(recheck) = &mut self.recheck {
            recheck.clear_recheck_request();
        }

        let PerVmLocks {
            vm_id: _,
            locked,
            held_elsewhere,
            held_by_this_owner,
        } = match self.locker.submit(self.vm_id.clone()).await {
            Ok(Ok(locks)) => locks,
            Ok(Err(error)) => return Err(Error::Lock(error)),
            Err(waymark_batcher::Closed) => return Err(Error::Lock(PerVmLockingError::Closed)),
        };

        for LockedRequest {
            record,
            pending_call_request,
        } in locked
        {
            // Whether the lapsed owner's attempt ran at all is unknown
            // here, even for a lock this process took itself.
            self.reporter
                .report_loss(
                    WithVmId {
                        vm_id: self.vm_id.clone(),
                        inner: ActionCallCorrelation {
                            effect_number: record.effect_number,
                            promise_state_id: record.promise_state_id,
                        },
                    },
                    ActionCallStage::Unknown,
                )
                .map_err(Error::Report)?;

            pending_call_request.confirm_requested();
        }

        let untracked_here = {
            let tracker = self
                .action_call_request_locks_tracker
                .lock()
                .expect("action-call request locks tracker poisoned");
            held_by_this_owner
                .iter()
                .filter(|lock_id| !tracker.is_tracking(lock_id))
                .count()
        };
        self.adoption = if held_elsewhere.is_empty() && untracked_here == 0 {
            Adoption::Covered
        } else {
            tracing::debug!(
                vm_id = ?self.vm_id,
                held_elsewhere = held_elsewhere.len(),
                untracked_here,
                "requests not tracked here; adopting them once their locks lapse"
            );
            Adoption::Watching {
                next_pass_at: tokio::time::Instant::now() + self.heartbeat.get(),
            }
        };

        Ok(())
    }
}

impl<Inner, VmId, Value, RaisedException> waymark_extcall_reconciler_core::SettlerAck
    for AdoptingActionSettler<Inner, VmId, Value, RaisedException>
where
    Inner: waymark_extcall_reconciler_core::SettlerAck,
    VmId: Eq + std::hash::Hash,
{
    type Ack = Inner::Ack;
}

impl<Inner, VmId, Value, RaisedException> waymark_extcall_reconciler_core::HasValue
    for AdoptingActionSettler<Inner, VmId, Value, RaisedException>
where
    Inner: waymark_extcall_reconciler_core::HasValue,
    VmId: Eq + std::hash::Hash,
{
    type Value = Inner::Value;
}

impl<Inner, VmId, Value, RaisedException> waymark_extcall_reconciler_core::HasRaisedException
    for AdoptingActionSettler<Inner, VmId, Value, RaisedException>
where
    Inner: waymark_extcall_reconciler_core::HasRaisedException,
    VmId: Eq + std::hash::Hash,
{
    type RaisedException = Inner::RaisedException;
}

impl<Inner, VmId, Value, RaisedException, UnifiedAck>
    waymark_extcall_reconciler_core::ActionPromiseSettler<UnifiedAck>
    for AdoptingActionSettler<Inner, VmId, Value, RaisedException>
where
    Inner: waymark_extcall_reconciler_core::ActionPromiseSettler<UnifiedAck> + Send,
    VmId: Clone + Eq + std::hash::Hash + Send + Sync + core::fmt::Debug,
    Value: Send,
    RaisedException: Send,
    UnifiedAck: From<Inner::Ack>,
{
    type Error =
        Error<<Inner as waymark_extcall_reconciler_core::ActionPromiseSettler<UnifiedAck>>::Error>;

    async fn poll_action_settlements<'a>(
        &'a mut self,
        waiting_promise_state_ids: NESlice<'a, PromiseStateId>,
    ) -> Result<waymark_extcall_reconciler_core::SettlementsFor<Self, UnifiedAck>, Self::Error>
    where
        UnifiedAck: 'a,
    {
        let mut poll = std::pin::pin!(
            self.inner
                .poll_action_settlements(waiting_promise_state_ids)
        );
        loop {
            let next_pass_at = match self.adopter.adoption {
                Adoption::Unchecked => Some(tokio::time::Instant::now()),
                Adoption::Watching { next_pass_at } => Some(next_pass_at),
                Adoption::Covered => None,
            };
            let recheck = self.adopter.recheck.as_mut();

            // A pass that is due runs before the wait: the timer branch
            // comes first and a reached deadline resolves at once.
            tokio::select! {
                biased;
                () = async {
                    tokio::time::sleep_until(next_pass_at.unwrap()).await;
                }, if next_pass_at.is_some() => {}
                requested = async { recheck.unwrap().recheck_requested().await }, if recheck.is_some() => {
                    match requested {
                        Ok(()) => self.adopter.adoption = Adoption::Unchecked,
                        Err(recheck::TxGoneError) => self.adopter.recheck = None,
                    }
                    continue;
                }
                settlements = &mut poll => return settlements.map_err(Error::Settle),
            }

            self.adopter.pass().await?;
        }
    }
}
