use std::marker::PhantomData;

use waymark_action_runtime_metadata_codec::Encode;
use waymark_convert_core::TryConvert;

/// Dispatches action calls through a [`waymark_worker_core::BaseWorkerPool`].
///
/// The request's correlation metadata is encoded verbatim into the dispatch
/// and echoed back by the worker, so a
/// [`WorkerPoolActionCallCompletionsProvider`](crate::WorkerPoolActionCallCompletionsProvider)
/// instantiated with the same `Metadata` recovers exactly what was attached
/// here.  Deployments that need to route completions back to a VM inject the
/// VM id into the metadata before it reaches this requester (e.g. via
/// [`waymark_action_runtime_metadata_compat::WithVmIdActionCallRequester`]).
pub struct WorkerPoolActionRequester<Pool, Metadata> {
    /// The worker pool to submit action requests to.
    pub pool: Pool,

    /// Phantom data for the metadata type parameter.
    pub _metadata: PhantomData<Metadata>,
}

impl<Pool, Metadata> WorkerPoolActionRequester<Pool, Metadata> {
    /// Create a new requester backed by the given worker pool.
    pub fn new(pool: Pool) -> Self {
        Self {
            pool,
            _metadata: PhantomData,
        }
    }
}

/// Errors that can occur when requesting an action call through
/// the worker pool.
#[derive(Debug, thiserror::Error)]
pub enum RequestActionCallError<QueueError> {
    /// Failed to convert call arguments for the worker pool.
    #[error("call arguments conversion: {0}")]
    ArgumentsConversion(#[source] waymark_vm_value_convert_core::PendingPromiseError),

    /// The worker pool rejected the action request.
    ///
    /// Whatever queueing failed with, expressed as the pool's own error:
    /// this requester merely propagates it.
    #[error("worker pool queue")]
    PoolQueue(#[source] QueueError),
}

impl<Pool, Metadata> waymark_action_runtime_core::ActionCallRequester
    for WorkerPoolActionRequester<Pool, Metadata>
where
    Pool: waymark_worker_core::QueueActionDispatch + Send + Sync + 'static,
    Pool::Error: core::fmt::Debug,
    Metadata: Encode + Send + Sync,
{
    type Error = RequestActionCallError<Pool::Error>;

    type Argument = waymark_vm_value_python::ReadyValue;

    type Metadata = Metadata;

    async fn request_action_call(
        &self,
        request: waymark_action_runtime_core::ActionCallRequest<Self::Argument, Self::Metadata>,
    ) -> Result<(), Self::Error> {
        let dispatch = waymark_action_runtime_convert::Converter::try_convert(request)
            .map_err(RequestActionCallError::ArgumentsConversion)?;

        self.pool
            .queue(dispatch)
            .await
            .map_err(RequestActionCallError::PoolQueue)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use waymark_action_core::ActionRef;
    use waymark_action_runtime_core::{ActionCallRequest, ActionCallRequester as _};
    use waymark_action_runtime_metadata::{ActionCallCorrelation, WithVmId};
    use waymark_action_runtime_metadata_codec::Decode;
    use waymark_action_runtime_metadata_compat::WithVmIdActionCallRequester;
    use waymark_ids::InstanceId;

    use super::*;

    /// Worker pool that records every queued request so tests can inspect it.
    #[derive(Default)]
    struct RecordingPool {
        queued: Mutex<Vec<waymark_proto::messages::ActionDispatch>>,
    }

    // Only queueing: a requester never polls, so the pool it is given
    // need not know how to.
    impl waymark_worker_core::QueueActionDispatch for RecordingPool {
        type Error = waymark_worker_core::WorkerPoolError;

        async fn queue(
            &self,
            dispatch: waymark_proto::messages::ActionDispatch,
        ) -> Result<(), Self::Error> {
            self.queued.lock().unwrap().push(dispatch);
            Ok(())
        }
    }

    fn action_call_request<Metadata>(
        metadata: Metadata,
    ) -> ActionCallRequest<waymark_vm_value_python::ReadyValue, Metadata> {
        ActionCallRequest {
            action_ref: ActionRef {
                action_name: "act".to_owned(),
                module_name: None,
                call_args: Vec::new(),
            },
            arguments: Vec::new(),
            metadata,
        }
    }

    /// Sixteen zero bytes are a valid correlation encoding; the correlation's
    /// contents are irrelevant to tests that only assert on the wrapping.
    fn correlation() -> ActionCallCorrelation {
        ActionCallCorrelation::decode(&mut &[0u8; 16][..]).unwrap()
    }

    #[tokio::test]
    async fn queued_request_encodes_the_metadata_verbatim() {
        let requester =
            WorkerPoolActionRequester::<_, ActionCallCorrelation>::new(RecordingPool::default());

        let metadata = correlation();
        requester
            .request_action_call(action_call_request(metadata))
            .await
            .expect("queueing succeeds");

        let queued = requester.pool.queued.lock().unwrap();
        let [request] = &queued[..] else {
            panic!("expected exactly one queued request, got {}", queued.len());
        };
        let decoded = ActionCallCorrelation::decode(&mut request.metadata.as_slice())
            .expect("metadata decodes");
        assert_eq!(decoded, metadata);
    }

    #[tokio::test]
    async fn with_vm_id_requester_encodes_the_vm_id_into_the_metadata() {
        let vm_id = InstanceId::new_uuid_v4();
        let requester = WithVmIdActionCallRequester {
            vm_id,
            action_call_requester: WorkerPoolActionRequester::<
                _,
                WithVmId<InstanceId, ActionCallCorrelation>,
            >::new(RecordingPool::default()),
        };

        requester
            .request_action_call(action_call_request(correlation()))
            .await
            .expect("queueing succeeds");

        let queued = requester.action_call_requester.pool.queued.lock().unwrap();
        let [request] = &queued[..] else {
            panic!("expected exactly one queued request, got {}", queued.len());
        };
        // The vm id rides in the correlation metadata bytes (round-tripping
        // through the worker), NOT piggybacked on the pool's executor_id field.
        let decoded =
            WithVmId::<InstanceId, ActionCallCorrelation>::decode(&mut request.metadata.as_slice())
                .expect("metadata decodes");
        assert_eq!(decoded.vm_id, vm_id);
    }
}
