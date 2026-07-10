use waymark_convert_core::TryConvert;

use crate::DispatchCorrelationMap;

/// Dispatches action calls through a [`waymark_worker_core::BaseWorkerPool`].
pub struct WorkerPoolRequester<Pool> {
    /// The worker pool to submit action requests to.
    pub pool: Pool,

    /// The executor (workflow instance) identifier.
    pub executor_id: waymark_ids::InstanceId,

    /// Map from dispatch tokens to (effect_number, promise_state_id)
    /// for correlating action completions back to VM promises.
    pub correlation_map: DispatchCorrelationMap,
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

impl<Pool> waymark_action_runtime_core::ActionCallRequester for WorkerPoolRequester<Pool>
where
    Pool: waymark_worker_core::BaseWorkerPool + Send + Sync + 'static,
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

        let dispatch_token = uuid::Uuid::new_v4();

        // Store the correlation so the completions provider can route
        // the result back to the correct VM promise.
        {
            let waymark_action_runtime_metadata::ActionCallCorrelation {
                effect_number,
                promise_state_id,
            } = request.metadata;
            let mut map = self.correlation_map.lock().unwrap();
            map.insert(dispatch_token, (effect_number, promise_state_id));
        }

        let worker_request = waymark_worker_core::ActionRequest {
            executor_id: self.executor_id,
            execution_id: waymark_ids::ExecutionId::new_uuid_v4(),
            action_name: request.action_ref.action_name,
            module_name: request.action_ref.module_name,
            kwargs,
            timeout_seconds: request.action_ref.timeout_seconds,
            attempt_number: 1,
            dispatch_token,
        };

        self.pool
            .queue(worker_request)
            .map_err(RequestActionCallError::PoolQueue)
    }
}
