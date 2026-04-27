use std::collections::HashMap;

use waymark_proto::messages as proto;
use waymark_runner_executor_core::UncheckedExecutionResult;
use waymark_worker_core::{ActionCompletion, ActionRequest, WorkerPoolError, error_to_value};
use waymark_worker_message_protocol::ActionDispatchPayload;

fn kwargs_to_workflow_arguments(
    kwargs: &HashMap<String, serde_json::Value>,
) -> proto::WorkflowArguments {
    let mut arguments = Vec::with_capacity(kwargs.len());
    for (key, value) in kwargs {
        let arg_value = waymark_message_conversions::json_to_workflow_argument_value(value);
        arguments.push(proto::WorkflowArgument {
            key: key.clone(),
            value: Some(arg_value),
        });
    }
    proto::WorkflowArguments { arguments }
}

pub fn to_dispatch_payload(
    request: ActionRequest,
) -> Result<ActionDispatchPayload, ActionCompletion> {
    let ActionRequest {
        executor_id,
        execution_id,
        action_name,
        module_name,
        kwargs,
        timeout_seconds,
        attempt_number,
        dispatch_token,
    } = request;

    let Some(module_name) = module_name else {
        return Err(ActionCompletion {
            executor_id,
            execution_id,
            attempt_number,
            dispatch_token,
            result: UncheckedExecutionResult(error_to_value(&WorkerPoolError::new(
                "RemoteWorkerPoolError",
                "missing module name for action request",
            ))),
        });
    };

    let dispatch = ActionDispatchPayload {
        action_id: execution_id.to_string(),
        instance_id: executor_id.to_string(),
        sequence: 0,
        action_name,
        module_name,
        kwargs: kwargs_to_workflow_arguments(&kwargs),
        timeout_seconds,
        max_retries: 0,
        attempt_number,
        dispatch_token,
    };

    Ok(dispatch)
}
