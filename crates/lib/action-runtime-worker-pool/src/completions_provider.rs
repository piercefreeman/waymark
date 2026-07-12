use std::marker::PhantomData;

use nonempty_collections::NEVec;
use waymark_action_runtime_core::{ActionCallCompletion, ActionCallCompletionFor};
use waymark_action_runtime_metadata::{ActionCallCorrelation, WithVmId, WithVmIdDecodeError};
use waymark_action_runtime_metadata_codec::Decode;
use waymark_convert_core::Convert as _;

/// Errors that can occur when waiting for action completions from
/// the worker pool.
#[derive(Debug, thiserror::Error)]
pub enum WorkerPoolCompletionsError<VmIdError: core::fmt::Display + 'static> {
    /// The worker pool has shut down and can no longer provide
    /// completions.
    #[error("worker pool gone")]
    WorkerPoolGone,

    /// A completion carried correlation metadata that could not be decoded,
    /// so it cannot be routed back to the promise that awaits it.
    #[error("unable to decode correlation metadata for an action completion")]
    Decode(#[source] WithVmIdDecodeError<VmIdError>),
}

/// Provides action outcomes by polling a [`waymark_worker_core::BaseWorkerPool`].
///
/// This polls the worker pool for ALL completions — there is no per-VM
/// filtering; each completion carries a [`WithVmId`] metadata, decoded from the
/// bytes the requester encoded and the worker echoed back, so the owning VM can
/// be recovered.  Consumers that need per-VM demultiplexing (e.g. the durable
/// completions writer feeding the demand poller) recover the owning VM from
/// that metadata.
pub struct WorkerPoolActionCallCompletionsProvider<Pool, VmId> {
    /// The worker pool to poll for completed actions.
    pub pool: Pool,

    /// Phantom data for the VM identifier type parameter.
    pub _vm_id: PhantomData<VmId>,
}

impl<Pool, VmId> WorkerPoolActionCallCompletionsProvider<Pool, VmId> {
    /// Create a new completions provider backed by the given worker pool.
    pub fn new(pool: Pool) -> Self {
        Self {
            pool,
            _vm_id: PhantomData,
        }
    }
}

impl<Pool, VmId> waymark_action_runtime_core::ActionCallCompletionsProvider
    for WorkerPoolActionCallCompletionsProvider<Pool, VmId>
where
    Pool: waymark_worker_core::BaseWorkerPool + Send + Sync + 'static,
    VmId: Decode + Send + Sync + 'static,
{
    type Value = waymark_vm_value::ReadyValue;
    type Error = WorkerPoolCompletionsError<VmId::Error>;
    type Metadata = WithVmId<VmId, ActionCallCorrelation>;

    async fn wait_for_completions(
        &mut self,
    ) -> Result<NEVec<ActionCallCompletionFor<Self>>, Self::Error> {
        loop {
            let maybe_completions = self.pool.poll_complete().await;
            let completions =
                maybe_completions.ok_or(WorkerPoolCompletionsError::WorkerPoolGone)?;

            let vec: Vec<_> = completions
                .into_iter()
                .map(resolve_completion)
                .collect::<Result<_, _>>()
                .map_err(WorkerPoolCompletionsError::Decode)?;

            let Some(nevec) = NEVec::try_from_vec(vec) else {
                continue;
            };

            return Ok(nevec);
        }
    }
}

/// Convert a raw worker-pool completion into an [`ActionCallCompletion`]
/// by decoding the [`WithVmId`] correlation from the completion's echoed
/// `metadata` bytes and converting the result value.
///
/// The VM id is recovered from the metadata the requester encoded — NOT from
/// the completion's `executor_id` field — so routing depends only on data the
/// action runtime controls end-to-end.
///
/// A decode failure is fatal rather than skippable: the correlation is the
/// only link back to the awaiting promise, so a completion we cannot decode
/// can neither be routed nor turned into a promise settlement.  Dropping it
/// would strand that promise forever (the action's side effects have already
/// happened), so we surface the error to the caller instead.
#[allow(clippy::type_complexity)]
fn resolve_completion<VmId: Decode>(
    completion: waymark_worker_core::ActionCompletion,
) -> Result<
    ActionCallCompletion<waymark_vm_value::ReadyValue, WithVmId<VmId, ActionCallCorrelation>>,
    WithVmIdDecodeError<VmId::Error>,
> {
    let waymark_worker_core::ActionCompletion {
        executor_id: _,
        execution_id: _,
        attempt_number: _,
        dispatch_token,
        result,
        metadata,
    } = completion;

    let metadata = WithVmId::<VmId, _>::decode(&mut metadata.as_slice()).inspect_err(|error| {
        tracing::error!(
            ?dispatch_token,
            %error,
            "unable to decode correlation metadata for an action completion"
        );
    })?;

    let outcome = waymark_action_runtime_convert::Converter::convert(result);

    Ok(ActionCallCompletion { metadata, outcome })
}

#[cfg(test)]
mod tests {
    use waymark_action_runtime_metadata_codec::Encode as _;
    use waymark_ids::{ExecutionId, InstanceId};
    use waymark_runner_executor_core::UncheckedExecutionResult;

    use super::*;

    fn completion(
        executor_id: InstanceId,
        metadata: Vec<u8>,
    ) -> waymark_worker_core::ActionCompletion {
        waymark_worker_core::ActionCompletion {
            executor_id,
            execution_id: ExecutionId::new_uuid_v4(),
            attempt_number: 1,
            dispatch_token: uuid::Uuid::new_v4(),
            result: UncheckedExecutionResult(serde_json::Value::Null),
            metadata,
        }
    }

    #[test]
    fn undecodable_metadata_surfaces_as_error() {
        // Metadata that is not a valid WithVmId encoding carries no route back
        // to a promise, so it must be an error rather than a silently dropped
        // completion.
        let result =
            resolve_completion::<InstanceId>(completion(InstanceId::new_uuid_v4(), Vec::new()));
        assert!(result.is_err());
    }

    #[test]
    fn recovers_vm_id_from_metadata_ignoring_executor_id() {
        // The VM id must come from the metadata the requester encoded, NOT from
        // the completion's executor_id field. Use a DIFFERENT executor_id to
        // prove resolve_completion never reads it.
        let vm_id = InstanceId::new_uuid_v4();
        let unrelated_executor_id = InstanceId::new_uuid_v4();
        let mut encoded = Vec::new();
        let correlation = WithVmId {
            vm_id,
            inner: ActionCallCorrelation::decode(&mut &[0u8; 16][..]).unwrap(),
        };
        correlation.encode(&mut encoded);

        let resolved = resolve_completion::<InstanceId>(completion(unrelated_executor_id, encoded))
            .expect("WithVmId metadata should decode");
        assert_eq!(resolved.metadata.vm_id, vm_id);
    }
}
