use std::collections::HashMap;

use waymark_proto::messages as proto;
use waymark_worker_core::UncheckedExecutionResult;
use waymark_worker_core::{ActionCompletion, ActionRequest, WorkerPoolError, error_to_value};
use waymark_worker_message_protocol::ActionDispatchPayload;

fn kwargs_to_workflow_arguments(
    kwargs: &HashMap<String, serde_json::Value>,
) -> proto::WorkflowArguments {
    let mut arguments = Vec::with_capacity(kwargs.len());
    for (key, value) in kwargs {
        let arg_value =
            waymark_proto_python_value_conversions::json_to_workflow_argument_value(value);
        arguments.push(proto::WorkflowArgument {
            key: key.clone(),
            value: waymark_proto_python_value_conversions::encode_workflow_argument_value(
                &arg_value,
            ),
        });
    }
    proto::WorkflowArguments { arguments }
}

pub fn to_dispatch_payload(
    request: ActionRequest,
) -> Result<ActionDispatchPayload, ActionCompletion> {
    let ActionRequest {
        action_name,
        module_name,
        kwargs,
        metadata,
    } = request;

    let Some(module_name) = module_name else {
        return Err(ActionCompletion {
            result: UncheckedExecutionResult(error_to_value(&WorkerPoolError::new(
                "RemoteWorkerPoolError",
                "missing module name for action request",
            ))),
            metadata,
        });
    };

    // The wire ids and the per-attempt token are the transport's own
    // vocabulary, not the caller's: filled with inert values, minted
    // here where a real one is needed.
    let dispatch = ActionDispatchPayload {
        action_id: String::new(),
        instance_id: String::new(),
        sequence: 0,
        action_name,
        module_name,
        kwargs: kwargs_to_workflow_arguments(&kwargs),
        timeout_seconds: 0,
        max_retries: 0,
        attempt_number: 1,
        dispatch_token: uuid::Uuid::new_v4(),
        metadata,
    };

    Ok(dispatch)
}
