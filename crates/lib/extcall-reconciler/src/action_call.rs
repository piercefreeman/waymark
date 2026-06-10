//! Action call extcall reconciler.
//!
//! Handles dispatching action calls to the worker pool and processing
//! completions into promise settlements.

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::sync::Arc;

use nonempty_collections::{IntoNonEmptyIterator, NEVec};
use tokio::sync::mpsc;
use waymark_extcall_core::ActionRef;
use waymark_runner_executor_core::{
    ExecutionException, ExecutionSuccess, UncheckedExecutionResult,
};
use waymark_vm_driver_core::{PromiseResolution, PromiseSettlement};
use waymark_vm_runtime_exception::Exception;
use waymark_vm_runtime_promise_core::PromiseStateId;
use waymark_worker_core::{ActionRequest, BaseWorkerPool};

use crate::{Ack, HandleEffectError};

/// Dispatches action calls to the worker pool.
pub struct Handler<VmId, WorkerPool> {
    /// Shared worker pool for queuing actions.
    pub worker_pool: Arc<WorkerPool>,
    /// Sends dispatch tokens to the poller for tracking.
    pub tx: mpsc::UnboundedSender<(uuid::Uuid, VmId, PromiseStateId)>,
}

impl<VmId, WorkerPool> Handler<VmId, WorkerPool>
where
    WorkerPool: BaseWorkerPool + Send + Sync,
{
    /// Dispatch an action call to the worker pool.
    ///
    /// Returns the `dispatch_token` for tracking the pending action.
    pub fn dispatch<Converter, ActionCallArgument>(
        &self,
        vm_id: VmId,
        promise_state_id: PromiseStateId,
        action_ref: &ActionRef,
        args: Vec<ActionCallArgument>,
    ) -> Result<uuid::Uuid, HandleEffectError<Converter::Error>>
    where
        Converter: waymark_convert_core::TryConvert<ActionCallArgument, serde_json::Value>,
    {
        let dispatch_token = uuid::Uuid::new_v4();

        tracing::debug!(
            ?promise_state_id,
            %action_ref.action_name,
            ?dispatch_token,
            "dispatching action call"
        );

        let kwargs: HashMap<String, serde_json::Value> = action_ref
            .call_args
            .iter()
            .zip(args)
            .map(|(name, arg)| Ok((name.clone(), Converter::try_convert(arg)?)))
            .collect::<Result<_, Converter::Error>>()
            .map_err(HandleEffectError::ConvertArg)?;

        let request = ActionRequest {
            executor_id: waymark_ids::InstanceId::new_uuid_v4(),
            execution_id: waymark_ids::ExecutionId::new_uuid_v4(),
            action_name: action_ref.action_name.clone(),
            module_name: action_ref.module_name.clone(),
            kwargs,
            timeout_seconds: action_ref.timeout_seconds,
            attempt_number: 1,
            dispatch_token,
        };

        self.worker_pool
            .queue(request)
            .map_err(HandleEffectError::Queue)?;

        let _ = self.tx.send((dispatch_token, vm_id, promise_state_id));

        Ok(dispatch_token)
    }
}

/// Always-active polling handle for worker-pool action completions.
pub struct Poller<VmId, WorkerPool> {
    /// Shared worker pool for polling completions.
    pub worker_pool: Arc<WorkerPool>,
    /// Receives new dispatch-token→promise-state mappings from the handler.
    pub dispatch_rx: mpsc::UnboundedReceiver<(uuid::Uuid, VmId, PromiseStateId)>,
    /// Receives ack signals to clear tracked dispatch tokens.
    pub ack_rx: mpsc::UnboundedReceiver<uuid::Uuid>,
    /// Sends ack signals (cloned into each settlement).
    pub ack_tx: mpsc::UnboundedSender<uuid::Uuid>,
    /// Tracks in-flight dispatches keyed by dispatch token.
    pub pending: HashMap<uuid::Uuid, (VmId, PromiseStateId)>,
}

impl<VmId, WorkerPool> Poller<VmId, WorkerPool>
where
    WorkerPool: BaseWorkerPool + Send + Sync,
{
    /// Wait for the next batch of action-completion settlements.
    pub async fn poll<Converter, Value>(&mut self) -> Option<NEVec<PromiseSettlement<Value, Ack>>>
    where
        Converter: waymark_convert_core::Convert<serde_json::Value, Value>,
        Converter: waymark_convert_core::Convert<serde_json::Value, Exception<Value>>,
        VmId: core::fmt::Debug,
    {
        loop {
            // Drain any buffered messages before blocking.
            while let Ok((dispatch_token, vm_id, promise_state_id)) = self.dispatch_rx.try_recv() {
                self.pending
                    .insert(dispatch_token, (vm_id, promise_state_id));
            }
            while let Ok(dispatch_token) = self.ack_rx.try_recv() {
                self.pending.remove(&dispatch_token);
            }

            tokio::select! {
                Some((dispatch_token, vm_id,promise_state_id)) = self.dispatch_rx.recv() => {
                    self.pending.insert(dispatch_token, (vm_id, promise_state_id));
                }
                Some(dispatch_token) = self.ack_rx.recv() => {
                    self.pending.remove(&dispatch_token);
                }
                completions = self.worker_pool.poll_complete() => {
                    let completions = completions?;
                    if let Some(settlements) = process_completions::<VmId, Converter, Value>(
                        &self.pending,
                        &self.ack_tx,
                        completions,
                    ) {
                        return Some(settlements);
                    }
                }
            }
        }
    }
}

/// Create a paired action handler and poller.
pub fn new<VmId, WorkerPool>(
    worker_pool: WorkerPool,
) -> (Handler<VmId, WorkerPool>, Poller<VmId, WorkerPool>) {
    let worker_pool = Arc::new(worker_pool);
    let (dispatch_tx, dispatch_rx) = mpsc::unbounded_channel();
    let (ack_tx, ack_rx) = mpsc::unbounded_channel();

    let handler = Handler {
        worker_pool: Arc::clone(&worker_pool),
        tx: dispatch_tx,
    };
    let poller = Poller {
        worker_pool,
        dispatch_rx,
        ack_rx,
        ack_tx,
        pending: HashMap::new(),
    };
    (handler, poller)
}

/// Process completed actions into settlements.
///
/// Does **not** remove the dispatch token from the pending map;
/// that is deferred until the settlement ack fires.
fn process_completions<VmId, Converter, Value>(
    pending: &HashMap<uuid::Uuid, (VmId, PromiseStateId)>,
    ack_tx: &mpsc::UnboundedSender<uuid::Uuid>,
    completions: impl IntoNonEmptyIterator<Item = waymark_worker_core::ActionCompletion>,
) -> Option<NEVec<PromiseSettlement<Value, Ack>>>
where
    Converter: waymark_convert_core::Convert<serde_json::Value, Value>,
    Converter: waymark_convert_core::Convert<serde_json::Value, Exception<Value>>,
    VmId: core::fmt::Debug,
{
    let mut settlements = Vec::new();
    for completion in completions {
        if let Some((vm_id, promise_state_id)) = pending.get(&completion.dispatch_token) {
            let resolution = completion_to_resolution::<Converter, Value>(completion.result);
            tracing::debug!(?vm_id, ?promise_state_id, "action completed");
            settlements.push(PromiseSettlement {
                promise_state_id: *promise_state_id,
                resolution,
                ack: Ack::Action(completion.dispatch_token, ack_tx.clone()),
            });
        }
    }
    NEVec::try_from_vec(settlements)
}

/// Convert an action completion result into a promise resolution.
fn completion_to_resolution<Converter, Value>(
    result: UncheckedExecutionResult,
) -> PromiseResolution<Value>
where
    Converter: waymark_convert_core::Convert<serde_json::Value, Value>,
    Converter: waymark_convert_core::Convert<serde_json::Value, Exception<Value>>,
{
    match result.check() {
        Ok(ExecutionSuccess(success)) => PromiseResolution::Resolved(Converter::convert(success)),
        Err(ExecutionException(exception)) => {
            PromiseResolution::Rejected(Converter::convert(exception))
        }
    }
}
