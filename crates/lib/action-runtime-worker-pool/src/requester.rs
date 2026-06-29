use waymark_convert_core::TryConvert;

/// Dispatches action calls through a [`waymark_worker_core::BaseWorkerPool`].
pub struct WorkerPoolRequester<Pool> {
    /// The worker pool to submit action requests to.
    pub pool: Pool,

    /// The executor (workflow instance) identifier.
    pub executor_id: waymark_ids::InstanceId,
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

    async fn request_action_call(
        &self,
        request: waymark_action_runtime_core::ActionCallRequest<Self::Argument>,
    ) -> Result<(), Self::Error> {
        let kwargs = waymark_action_runtime_convert::Converter::try_convert((
            &request.action_ref.call_args[..],
            &request.arguments[..],
        ))
        .map_err(RequestActionCallError::ArgumentsConversion)?;

        let request = waymark_worker_core::ActionRequest {
            executor_id: self.executor_id,
            execution_id: waymark_ids::ExecutionId::new_uuid_v4(),
            action_name: request.action_ref.action_name,
            module_name: request.action_ref.module_name,
            kwargs,
            timeout_seconds: request.action_ref.timeout_seconds,
            attempt_number: 1,
            dispatch_token: uuid::Uuid::new_v4(),
        };

        self.pool
            .queue(request)
            .map_err(RequestActionCallError::PoolQueue)
    }
}
