use waymark_action_runtime_metadata_codec::Encode;
use waymark_convert_core::TryConvert;

/// Dispatches action calls through a [`waymark_worker_core::BaseWorkerPool`].
pub struct WorkerPoolActionRequester<Pool, VmId> {
    /// The worker pool to submit action requests to.
    pub pool: Pool,

    /// The VM instance that owns the calls this requester dispatches.
    ///
    /// It is encoded into each dispatch's correlation metadata (via
    /// [`WithVmId`](waymark_action_runtime_metadata::WithVmId)) so completions
    /// can be routed back to this VM, and travels end-to-end through the worker
    /// rather than being recovered from the worker pool's `executor_id` field.
    pub vm_id: VmId,
}

/// Errors that can occur when requesting an action call through
/// the worker pool.
#[derive(Debug, thiserror::Error)]
pub enum RequestActionCallError {
    /// Failed to convert call arguments for the worker pool.
    #[error("call arguments conversion: {0}")]
    ArgumentsConversion(#[source] waymark_vm_value_convert_core::PendingPromiseError),

    /// The worker pool rejected the action request.
    #[error("worker pool queue: {0}")]
    PoolQueue(#[source] waymark_worker_core::WorkerPoolError),
}

impl<Pool, VmId> waymark_action_runtime_core::ActionCallRequester
    for WorkerPoolActionRequester<Pool, VmId>
where
    Pool: waymark_worker_core::BaseWorkerPool + Send + Sync + 'static,
    VmId: Copy + Encode + Sync,
{
    type Error = RequestActionCallError;

    type Argument = waymark_vm_value::ReadyValue;

    type Metadata = waymark_action_runtime_metadata::ActionCallCorrelation;

    async fn request_action_call(
        &self,
        request: waymark_action_runtime_core::ActionCallRequest<Self::Argument, Self::Metadata>,
    ) -> Result<(), Self::Error> {
        let kwargs = waymark_action_runtime_convert::Converter::try_convert((
            &request.action_ref.call_args[..],
            &request.arguments[..],
        ))
        .map_err(RequestActionCallError::ArgumentsConversion)?;

        // The vm id rides in the correlation metadata itself — encoded here and
        // recovered by the completions provider — so routing never depends on
        // the worker pool's `executor_id` field.
        let metadata = waymark_action_runtime_metadata::WithVmId::<VmId, _> {
            vm_id: self.vm_id,
            inner: request.metadata,
        };

        let mut encoded_metadata = Vec::new();
        metadata.encode(&mut encoded_metadata);

        let worker_request = waymark_worker_core::ActionRequest {
            executor_id: waymark_ids::InstanceId::new_uuid_v4(),
            execution_id: waymark_ids::ExecutionId::new_uuid_v4(),
            action_name: request.action_ref.action_name,
            module_name: request.action_ref.module_name,
            kwargs,
            timeout_seconds: request.action_ref.timeout_seconds,
            attempt_number: 1,
            dispatch_token: uuid::Uuid::new_v4(),
            metadata: encoded_metadata,
        };

        self.pool
            .queue(worker_request)
            .map_err(RequestActionCallError::PoolQueue)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use nonempty_collections::NEVec;
    use waymark_action_core::ActionRef;
    use waymark_action_runtime_core::{ActionCallRequest, ActionCallRequester as _};
    use waymark_action_runtime_metadata::{ActionCallCorrelation, WithVmId};
    use waymark_action_runtime_metadata_codec::Decode;
    use waymark_ids::InstanceId;

    use super::*;

    /// Worker pool that records every queued request so tests can inspect it.
    #[derive(Default)]
    struct RecordingPool {
        queued: Mutex<Vec<waymark_worker_core::ActionRequest>>,
    }

    impl waymark_worker_core::BaseWorkerPool for RecordingPool {
        fn queue(
            &self,
            request: waymark_worker_core::ActionRequest,
        ) -> Result<(), waymark_worker_core::WorkerPoolError> {
            self.queued.lock().unwrap().push(request);
            Ok(())
        }

        async fn poll_complete(&self) -> Option<NEVec<waymark_worker_core::ActionCompletion>> {
            unreachable!("the requester test never polls for completions")
        }
    }

    #[tokio::test]
    async fn queued_request_encodes_the_vm_id_into_the_metadata() {
        let vm_id = InstanceId::new_uuid_v4();
        let requester = WorkerPoolActionRequester::<_, InstanceId> {
            pool: RecordingPool::default(),
            vm_id,
        };

        requester
            .request_action_call(ActionCallRequest {
                action_ref: ActionRef {
                    action_name: "act".to_owned(),
                    module_name: None,
                    call_args: Vec::new(),
                    timeout_seconds: 1,
                    max_retries: 0,
                    exception_types: Vec::new(),
                },
                arguments: Vec::new(),
                // Sixteen zero bytes are a valid correlation encoding; the
                // correlation's contents are irrelevant to this test, which only
                // asserts on the separately-carried vm id.
                metadata: ActionCallCorrelation::decode(&mut &[0u8; 16][..]).unwrap(),
            })
            .await
            .expect("queueing succeeds");

        let queued = requester.pool.queued.lock().unwrap();
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
