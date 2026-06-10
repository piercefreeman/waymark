//! Support for running extcalls emitted by the extcall interpreter.
//!
//! This crate bridges the VM extcall interpreter's effects
//! ([`waymark_vm_interpreter_extcallset::Effect`]) with a worker pool
//! ([`waymark_worker_core::BaseWorkerPool`]).
//!
//! # To do
//!
//! TODO: this is a skeleton implementation that is purposely limited to
//! push the integration forward.
//!
//! Things that need to be implemented:
//!
//! - Retries
//!
//!   If requested, action is assumed to be idempotent and is tried again
//!   on failure.
//!
//! - Timeouts
//!
//!   If requested, actions taking longer than a fixed time period are
//!   cancelled and a timeout error is returned to the caller (or retried).
//!
//! - Persistence
//!
//!   An attempt to execute an action has to be recorded into a database
//!   instead of actually being passed to the worker pool for execution;
//!   then, another completely independent subroutine polls the database
//!   for actions to execute and picks up such recorded action request, and
//!   actually executes it in its worker pool; then the result of the action
//!   call execution are communicated back from the worker pool and recorded
//!   back into the same database; the results are then polled from the database
//!   by whatever host is processing the VM at the time, and injected into the VM
//!   state via promise settlement, after which point the VM snapshot gets
//!   persisted, after which the runtime acks the promise at which point we
//!   can clear it from the database.
//!
//! Another concern here is that we probably want to adjust the interface to
//! the worker pool before we do all that. There are a lot of hardcoded
//! concepts that are legacy and foreign to the new VM-based system - we should
//! probably clean them up before touching the above.

#![warn(missing_docs)]

pub mod action_call;
pub mod sleep;

#[cfg(test)]
mod tests;

use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use nonempty_collections::NEVec;
use tokio::sync::mpsc;
use waymark_extcall_core::ActionRef;
use waymark_vm_driver_core::{PromiseSettlement, PromiseSettlementAck};
use waymark_vm_interpreter_extcallset::Effect;
use waymark_vm_runtime_exception::Exception;
use waymark_vm_runtime_promise_core::PromiseStateId;
use waymark_worker_core::BaseWorkerPool;

/// Settlement acknowledgement.
///
/// [`PromiseSettlementAck::acknowledge_promise_settlement`] is called by
/// the driver after the VM state has been persisted.
pub enum Ack {
    /// Action-call settlement — the dispatch token identifies the
    /// pending action to clean up on ack.
    Action(uuid::Uuid, mpsc::UnboundedSender<uuid::Uuid>),

    /// Sleep settlement — nothing to clean up.
    Sleep,
}

impl PromiseSettlementAck for Ack {
    fn acknowledge_promise_settlement(self) {
        match self {
            Ack::Action(dispatch_token, tx) => {
                let _ = tx.send(dispatch_token);
            }
            Ack::Sleep => {}
        }
    }
}

/// Error returned when there are no pending promises to wait for.
#[derive(Debug, thiserror::Error)]
pub enum NoSettlementsError {
    /// No pending actions or sleeps remain - nothing to wait for.
    #[error("no pending promises to settle")]
    NoPendingPromises,
}

/// Error returned when handling an extcall effect fails.
#[derive(Debug, thiserror::Error)]
pub enum HandleEffectError {
    /// The worker pool rejected the action request.
    #[error("failed to queue action: {0}")]
    Queue(#[source] waymark_worker_core::WorkerPoolError),
}

/// Handles extcall effects emitted by the VM — dispatches actions to
/// the worker pool and records sleep deadlines.
pub struct EffectHandler<WorkerPool, Converter, ActionCallArgument> {
    action: Arc<action_call::Handler<WorkerPool>>,
    sleep: Arc<sleep::Handler>,
    _phantom: PhantomData<(Converter, ActionCallArgument)>,
}

/// Produces promise settlements from completed actions and elapsed
/// sleeps.
pub struct PromiseSettler<WorkerPool, Converter, Value> {
    action: action_call::Poller<WorkerPool>,
    sleep: sleep::Poller,
    _phantom: PhantomData<(Converter, Value)>,
}

/// Create a paired handler and settler.
pub fn new<WorkerPool, Converter, ActionCallArgument, Value>(
    worker_pool: WorkerPool,
) -> (
    EffectHandler<WorkerPool, Converter, ActionCallArgument>,
    PromiseSettler<WorkerPool, Converter, Value>,
) {
    let (action_handler, action_poller) = action_call::new(worker_pool);
    let (sleep_handler, sleep_poller) = sleep::new();
    let handler = EffectHandler {
        action: Arc::new(action_handler),
        sleep: Arc::new(sleep_handler),
        _phantom: PhantomData,
    };
    let settler = PromiseSettler {
        action: action_poller,
        sleep: sleep_poller,
        _phantom: PhantomData,
    };
    (handler, settler)
}

impl<WorkerPool, Converter, ActionCallArgument>
    EffectHandler<WorkerPool, Converter, ActionCallArgument>
where
    WorkerPool: BaseWorkerPool + Send + Sync,
    ActionCallArgument: Into<serde_json::Value> + Debug + Send,
{
    /// Create a handler from already-initialized handlers/pollers.
    pub fn new(action: Arc<action_call::Handler<WorkerPool>>, sleep: Arc<sleep::Handler>) -> Self {
        Self {
            action,
            sleep,
            _phantom: PhantomData,
        }
    }
}

impl<WorkerPool, Converter, ActionCallArgument> waymark_vm_driver_core::EffectHandler
    for EffectHandler<WorkerPool, Converter, ActionCallArgument>
where
    WorkerPool: BaseWorkerPool + Send + Sync,
    Converter: waymark_convert_core::Convert<ActionCallArgument, serde_json::Value> + Send,
    ActionCallArgument: Send,
{
    type Effect = Effect<ActionRef, ActionCallArgument>;
    type Error = HandleEffectError;

    async fn handle_effect(&mut self, effect: Self::Effect) -> Result<(), Self::Error> {
        match effect {
            Effect::ActionCall {
                promise_state_id,
                action_ref,
                args,
            } => {
                self.action.dispatch::<Converter, ActionCallArgument>(
                    promise_state_id,
                    &action_ref,
                    args,
                )?;
            }
            Effect::Sleep {
                promise_state_id,
                duration,
            } => {
                self.sleep.record(promise_state_id, duration);
            }
        }

        Ok(())
    }
}

impl<WorkerPool, Converter, Value> PromiseSettler<WorkerPool, Converter, Value> {
    /// Create a settler from already-initialized pollers.
    pub fn new(action: action_call::Poller<WorkerPool>, sleep: sleep::Poller) -> Self {
        Self {
            action,
            sleep,
            _phantom: PhantomData,
        }
    }
}

impl<WorkerPool, Converter, Value> waymark_vm_driver_core::PromiseSettler
    for PromiseSettler<WorkerPool, Converter, Value>
where
    WorkerPool: BaseWorkerPool + Send + Sync,
    Converter: waymark_convert_core::Convert<serde_json::Value, Value> + Send,
    Converter: waymark_convert_core::Convert<serde_json::Value, Exception<Value>> + Send,
    Value: Send,
{
    type Value = Value;
    type Error = NoSettlementsError;
    type Ack = Ack;

    async fn get_promise_settlements(
        &mut self,
        _waiting_ids: NEVec<PromiseStateId>,
    ) -> Result<NEVec<PromiseSettlement<Value, Ack>>, Self::Error> {
        Ok(tokio::select! {
            Some(settlements) = self.sleep.poll::<Converter, Value>() => settlements,
            Some(settlements) = self.action.poll::<Converter, Value>() => settlements,
            else => return Err(NoSettlementsError::NoPendingPromises),
        })
    }
}
