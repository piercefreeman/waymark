use waymark_convert_core::TryConvert;

/// Dispatches action calls through a [`waymark_worker_core::BaseWorkerPool`].
pub struct WorkerPoolRequester<Pool, Metadata> {
    /// The worker pool to submit action requests to.
    pub pool: Pool,

    /// Phantom data.
    pub phantom_data: std::marker::PhantomData<Metadata>,
}

impl<Pool, Metadata> WorkerPoolRequester<Pool, Metadata> {
    /// Create a new worker pool requester.
    pub fn new(pool: Pool) -> Self {
        Self {
            pool,
            phantom_data: std::marker::PhantomData,
        }
    }
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

impl<Pool, Metadata> waymark_action_runtime_core::WithActionCallMetadata
    for WorkerPoolRequester<Pool, Metadata>
{
    type ActionCallMetadata = Metadata;
}

impl<Pool, Metadata> waymark_action_runtime_core::ActionCallRequester
    for WorkerPoolRequester<Pool, Metadata>
where
    Pool: waymark_worker_core::BaseWorkerPool + Send + Sync + 'static,
    (waymark_ids::InstanceId, waymark_ids::ExecutionId): From<Metadata>,
    Metadata: Send + Sync,
{
    type Error = RequestActionCallError;

    type Argument = waymark_vm_value::ReadyValue;

    async fn request_action_call(
        &self,
        request: waymark_action_runtime_core::ActionCallRequestFor<Self>,
    ) -> Result<(), Self::Error> {
        let waymark_action_runtime_core::ActionCallRequest {
            action_ref,
            arguments,
            metadata,
        } = request;

        let kwargs = waymark_action_runtime_convert::Converter::try_convert((
            &action_ref.call_args[..],
            &arguments[..],
        ))
        .map_err(RequestActionCallError::ArgumentsConversion)?;

        let dispatch_token = uuid::Uuid::new_v4();

        let (executor_id, execution_id) = metadata.into();

        let worker_request = waymark_worker_core::ActionRequest {
            executor_id,
            execution_id,
            action_name: action_ref.action_name,
            module_name: action_ref.module_name,
            kwargs,
            timeout_seconds: action_ref.timeout_seconds,
            attempt_number: 1,
            dispatch_token,
        };

        self.pool
            .queue(worker_request)
            .map_err(RequestActionCallError::PoolQueue)
    }
}
