use std::marker::PhantomData;

use nonempty_collections::NEVec;
use waymark_action_runtime_core::{
    ActionCallCompletion, ActionCallCompletionFor, ActionCallLossError, ActionCallStage,
};
use waymark_action_runtime_metadata_codec::Decode;
use waymark_convert_core::TryConvert as _;

/// Errors that can occur when waiting for action completions from
/// the worker pool.
#[derive(Debug, thiserror::Error)]
pub enum WorkerPoolCompletionsError<PollError, MetadataDecodeError: core::fmt::Display + 'static> {
    /// The worker pool can no longer provide completions.
    ///
    /// Whatever polling failed with, expressed as the pool's own error:
    /// this provider merely propagates it.
    #[error("polling the worker pool")]
    Poll(#[source] PollError),

    /// A completion carried correlation metadata that could not be decoded,
    /// so it cannot be routed back to the promise that awaits it.
    #[error("unable to decode correlation metadata for an action completion")]
    Decode(#[source] MetadataDecodeError),

    /// A completion carried a payload that could not be converted, so
    /// there is nothing valid to settle the promise with.
    #[error("unable to convert an action-completion payload")]
    Payload(#[source] ActionResultConvertError),
}

/// The error of the action-result conversion the provider delegates to.
///
/// Expressed as a projection through
/// [`waymark_action_runtime_convert::Converter`] rather than named
/// concretely: this provider merely propagates that conversion's
/// failure, whatever it is.
pub type ActionResultConvertError = waymark_convert_core::ConvertErrorFor<
    waymark_action_runtime_convert::Converter<waymark_vm_value_python_convert_proto::Converter>,
    &'static waymark_proto::messages::ActionResult,
    waymark_action_runtime_core::ActionCallOutcome<waymark_vm_value_python::ReadyValue>,
>;

/// Provides action outcomes by polling a
/// [`waymark_worker_core::PollActionResults`].
///
/// This polls the worker pool for ALL completions — there is no per-VM
/// filtering.  Each completion's metadata is decoded as `Metadata` from the
/// bytes the requester encoded and the worker echoed back: the pair of this
/// provider and a
/// [`WorkerPoolActionRequester`](crate::WorkerPoolActionRequester)
/// instantiated with the same `Metadata` round-trips it end-to-end.
/// Consumers that need per-VM demultiplexing (e.g. the durable completions
/// writer feeding the demand poller) instantiate with a
/// [`WithVmId`](waymark_action_runtime_metadata::WithVmId)-wrapped metadata
/// and recover the owning VM from it.
pub struct WorkerPoolActionCallCompletionsProvider<Pool, Metadata> {
    /// The worker pool to poll for completed actions.
    pub pool: Pool,

    /// Phantom data for the metadata type parameter.
    pub _metadata: PhantomData<Metadata>,
}

impl<Pool, Metadata> WorkerPoolActionCallCompletionsProvider<Pool, Metadata> {
    /// Create a new completions provider backed by the given worker pool.
    pub fn new(pool: Pool) -> Self {
        Self {
            pool,
            _metadata: PhantomData,
        }
    }
}

impl<Pool, Metadata> waymark_action_runtime_core::ActionCallCompletionsProvider
    for WorkerPoolActionCallCompletionsProvider<Pool, Metadata>
where
    Pool: waymark_worker_core::PollActionResults + Send + Sync + 'static,
    Pool::Error: core::fmt::Debug,
    Metadata: Decode + Send + Sync + 'static,
    Metadata::Error: core::fmt::Display,
{
    type Value = waymark_vm_value_python::ReadyValue;
    type ActionExecutionError = waymark_action_runtime_core::ActionCallLossError;
    type WaitError = WorkerPoolCompletionsError<Pool::Error, Metadata::Error>;
    type Metadata = Metadata;

    async fn wait_for_completions(
        &mut self,
    ) -> Result<NEVec<ActionCallCompletionFor<Self>>, Self::WaitError> {
        loop {
            let completions = self
                .pool
                .poll_complete()
                .await
                .map_err(WorkerPoolCompletionsError::Poll)?;

            let vec: Vec<_> = completions
                .into_iter()
                .map(resolve_execution)
                .collect::<Result<_, _>>()?;

            let Some(nevec) = NEVec::try_from_vec(vec) else {
                continue;
            };

            return Ok(nevec);
        }
    }
}

/// Convert a finished execution into an [`ActionCallCompletion`] by
/// decoding the correlation metadata from the echoed `metadata` bytes
/// and reading how the execution ended.
///
/// A completed execution carries the worker's result, converted as the
/// outcome it encodes.  A LOST execution carries no outcome at all — it
/// resolves to an [`ActionCallLossError`] execution error stating the stage
/// the call provably reached; what the loss means for the awaiting
/// promise is decided where completions are lowered into settlements,
/// not here.
///
/// The metadata is recovered from the bytes the requester encoded — NOT from
/// the completion's `executor_id` field — so routing depends only on data the
/// action runtime controls end-to-end.
///
/// A decode failure is fatal rather than skippable: the correlation is the
/// only link back to the awaiting promise, so an execution we cannot decode
/// can neither be routed nor turned into a promise settlement.  Dropping it
/// would strand that promise forever (the action's side effects may have
/// already happened), so we surface the error to the caller instead.
fn resolve_execution<PollError, Metadata>(
    report: waymark_worker_core::ActionExecutionReport,
) -> Result<
    ActionCallCompletion<Metadata, waymark_vm_value_python::ReadyValue, ActionCallLossError>,
    WorkerPoolCompletionsError<PollError, Metadata::Error>,
>
where
    Metadata: Decode,
    Metadata::Error: core::fmt::Display,
{
    let (metadata_bytes, execution_result) = match report {
        waymark_worker_core::ActionExecutionReport::Completed(result) => {
            let outcome = waymark_action_runtime_convert::Converter::<
                waymark_vm_value_python_convert_proto::Converter,
            >::try_convert(&result)
            .map_err(WorkerPoolCompletionsError::Payload)?;
            (result.metadata, Ok(outcome))
        }
        waymark_worker_core::ActionExecutionReport::Lost(loss) => {
            let stage = match loss.progress {
                waymark_worker_core::ExecutionProgress::NotStarted => ActionCallStage::NotStarted,
                waymark_worker_core::ExecutionProgress::Unknown => ActionCallStage::Unknown,
            };
            (loss.metadata, Err(ActionCallLossError { stage }))
        }
    };

    let metadata = Metadata::decode(&mut metadata_bytes.as_slice())
        .inspect_err(|error| {
            tracing::error!(
                %error,
                "unable to decode correlation metadata for an action execution"
            );
        })
        .map_err(WorkerPoolCompletionsError::Decode)?;

    Ok(ActionCallCompletion {
        metadata,
        execution_result,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use waymark_action_runtime_metadata::{ActionCallCorrelation, WithVmId};
    use waymark_action_runtime_metadata_codec::Encode as _;
    use waymark_ids::InstanceId;

    fn completion(metadata: Vec<u8>) -> waymark_proto::messages::ActionResult {
        let payload = waymark_vm_value_python_convert_proto::Converter::try_convert(
            waymark_action_runtime_core::ActionCallOutcome::Value(
                waymark_vm_value_python::ReadyValue::None,
            ),
        )
        .expect("a None holds no pending promise");

        waymark_proto::messages::ActionResult {
            payload,
            metadata,
            ..Default::default()
        }
    }

    #[test]
    fn a_lost_execution_resolves_to_a_loss() {
        // The provider states the protocol fact and nothing more: no
        // outcome exists, only the stage the call provably reached.
        // What the loss means for the awaiting promise is decided at
        // settlement lowering, not here.
        let vm_id = InstanceId::new_uuid_v4();
        let mut encoded = Vec::new();
        let correlation = WithVmId {
            vm_id,
            inner: ActionCallCorrelation::decode(&mut &[0u8; 16][..]).unwrap(),
        };
        correlation.encode(&mut encoded);

        let resolved = resolve_execution::<
            core::convert::Infallible,
            WithVmId<InstanceId, ActionCallCorrelation>,
        >(waymark_worker_core::ActionExecutionReport::Lost(
            waymark_worker_core::ActionExecutionLoss {
                metadata: encoded,
                progress: waymark_worker_core::ExecutionProgress::Unknown,
            },
        ))
        .expect("a lost execution still routes by its metadata");

        assert_eq!(resolved.metadata.vm_id, vm_id);
        let Err(loss) = resolved.execution_result else {
            panic!("a lost execution carries no outcome");
        };
        assert_eq!(
            loss,
            ActionCallLossError {
                stage: ActionCallStage::Unknown
            },
        );
    }

    #[test]
    fn undecodable_metadata_surfaces_as_error() {
        // Metadata that is not a valid WithVmId encoding carries no route back
        // to a promise, so it must be an error rather than a silently dropped
        // completion.
        let result = resolve_execution::<
            core::convert::Infallible,
            WithVmId<InstanceId, ActionCallCorrelation>,
        >(waymark_worker_core::ActionExecutionReport::Completed(
            completion(Vec::new()),
        ));
        assert!(result.is_err());
    }

    #[test]
    fn recovers_vm_id_from_metadata() {
        // The VM id comes from the metadata the requester encoded — the
        // completion carries no other identity at all.
        let vm_id = InstanceId::new_uuid_v4();
        let mut encoded = Vec::new();
        let correlation = WithVmId {
            vm_id,
            inner: ActionCallCorrelation::decode(&mut &[0u8; 16][..]).unwrap(),
        };
        correlation.encode(&mut encoded);

        let resolved = resolve_execution::<
            core::convert::Infallible,
            WithVmId<InstanceId, ActionCallCorrelation>,
        >(waymark_worker_core::ActionExecutionReport::Completed(
            completion(encoded),
        ))
        .expect("WithVmId metadata should decode");
        assert_eq!(resolved.metadata.vm_id, vm_id);
    }
}
