use std::marker::PhantomData;

use nonempty_collections::NEVec;
use waymark_action_runtime_core::{ActionCallCompletion, ActionCallCompletionFor};
use waymark_action_runtime_metadata_codec::Decode;
use waymark_convert_core::TryConvert as _;

/// Errors that can occur when waiting for action completions from
/// the worker pool.
#[derive(Debug, thiserror::Error)]
pub enum WorkerPoolCompletionsError<MetadataDecodeError: core::fmt::Display + 'static> {
    /// The worker pool has shut down and can no longer provide
    /// completions.
    #[error("worker pool gone")]
    WorkerPoolGone,

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
    waymark_action_runtime_convert::Converter,
    &'static waymark_proto::messages::ActionResult,
    waymark_action_runtime_core::ActionCallOutcome<waymark_vm_value_python::ReadyValue>,
>;

/// Provides action outcomes by polling a [`waymark_worker_core::BaseWorkerPool`].
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
    Pool: waymark_worker_core::BaseWorkerPool + Send + Sync + 'static,
    Metadata: Decode + Send + Sync + 'static,
    Metadata::Error: core::fmt::Display,
{
    type Value = waymark_vm_value_python::ReadyValue;
    type Error = WorkerPoolCompletionsError<Metadata::Error>;
    type Metadata = Metadata;

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
                .collect::<Result<_, _>>()?;

            let Some(nevec) = NEVec::try_from_vec(vec) else {
                continue;
            };

            return Ok(nevec);
        }
    }
}

/// Convert a raw worker-pool completion into an [`ActionCallCompletion`]
/// by decoding the correlation metadata from the completion's echoed
/// `metadata` bytes and converting the result value.
///
/// The metadata is recovered from the bytes the requester encoded — NOT from
/// the completion's `executor_id` field — so routing depends only on data the
/// action runtime controls end-to-end.
///
/// A decode failure is fatal rather than skippable: the correlation is the
/// only link back to the awaiting promise, so a completion we cannot decode
/// can neither be routed nor turned into a promise settlement.  Dropping it
/// would strand that promise forever (the action's side effects have already
/// happened), so we surface the error to the caller instead.
fn resolve_completion<Metadata>(
    completion: waymark_proto::messages::ActionResult,
) -> Result<
    ActionCallCompletion<waymark_vm_value_python::ReadyValue, Metadata>,
    WorkerPoolCompletionsError<Metadata::Error>,
>
where
    Metadata: Decode,
    Metadata::Error: core::fmt::Display,
{
    let metadata = Metadata::decode(&mut completion.metadata.as_slice())
        .inspect_err(|error| {
            tracing::error!(
                %error,
                "unable to decode correlation metadata for an action completion"
            );
        })
        .map_err(WorkerPoolCompletionsError::Decode)?;

    let outcome = waymark_action_runtime_convert::Converter::try_convert(&completion)
        .map_err(WorkerPoolCompletionsError::Payload)?;

    Ok(ActionCallCompletion { metadata, outcome })
}

#[cfg(test)]
mod tests {
    use super::*;
    use waymark_action_runtime_metadata::{ActionCallCorrelation, WithVmId};
    use waymark_action_runtime_metadata_codec::Encode as _;
    use waymark_ids::InstanceId;

    fn completion(metadata: Vec<u8>) -> waymark_proto::messages::ActionResult {
        let returned = waymark_proto_python_value_conversions::returned_value(
            waymark_vm_value_convert_proto::Converter::try_convert(
                &waymark_vm_value_python::ReadyValue::None,
            )
            .expect("a None holds no pending promise"),
        );

        waymark_proto::messages::ActionResult {
            payload: waymark_proto_python_value_conversions::encode_action_result_value(&returned),
            metadata,
            ..Default::default()
        }
    }

    #[test]
    fn undecodable_metadata_surfaces_as_error() {
        // Metadata that is not a valid WithVmId encoding carries no route back
        // to a promise, so it must be an error rather than a silently dropped
        // completion.
        let result = resolve_completion::<WithVmId<InstanceId, ActionCallCorrelation>>(completion(
            Vec::new(),
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

        let resolved =
            resolve_completion::<WithVmId<InstanceId, ActionCallCorrelation>>(completion(encoded))
                .expect("WithVmId metadata should decode");
        assert_eq!(resolved.metadata.vm_id, vm_id);
    }
}
