use std::collections::HashMap;

use waymark_convert_core::TryConvert;

/// Dispatches action calls through a [`waymark_worker_core::BaseWorkerPool`].
pub struct WorkerPoolRequester<Pool> {
    /// The worker pool to submit action requests to.
    pub pool: Pool,

    /// The executor (workflow instance) identifier.
    pub executor_id: waymark_ids::InstanceId,
}

impl<Pool> waymark_action_runtime_core::ActionCallRequester for WorkerPoolRequester<Pool>
where
    Pool: waymark_worker_core::BaseWorkerPool + Send + Sync + 'static,
{
    type Error = waymark_worker_core::WorkerPoolError;

    type Arguments = Vec<waymark_vm_value::ReadyValue>;

    fn request_action_call(
        &self,
        request: waymark_action_runtime_core::ActionCallRequest<Self::Arguments>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + '_ {
        let result = build_action_request(&self.executor_id, request)
            .map(|action_request| self.pool.queue(action_request))
            .unwrap_or_else(Err);
        async move { result }
    }
}

fn build_action_request(
    executor_id: &waymark_ids::InstanceId,
    request: waymark_action_runtime_core::ActionCallRequest<Vec<waymark_vm_value::ReadyValue>>,
) -> Result<waymark_worker_core::ActionRequest, waymark_worker_core::WorkerPoolError> {
    let kwargs: HashMap<String, serde_json::Value> = request
        .action_ref
        .call_args
        .iter()
        .zip(request.arguments)
        .map(|(name, value)| {
            waymark_extcall_convert::Converter::try_convert(value).map(|json| (name.clone(), json))
        })
        .collect::<Result<_, _>>()
        .map_err(|err| {
            waymark_worker_core::WorkerPoolError::new(
                "WorkerPoolRequester",
                format!("action argument conversion: {err}"),
            )
        })?;

    Ok(waymark_worker_core::ActionRequest {
        executor_id: *executor_id,
        execution_id: waymark_ids::ExecutionId::new_uuid_v4(),
        action_name: request.action_ref.action_name,
        module_name: request.action_ref.module_name,
        kwargs,
        timeout_seconds: request.action_ref.timeout_seconds,
        attempt_number: 1,
        dispatch_token: uuid::Uuid::new_v4(),
    })
}
