use std::collections::HashMap;

use uuid::Uuid;
use waymark_runner_executor_core::UncheckedExecutionResult;
use waymark_vcr_core::CorrelationId;

#[derive(Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Params {
    pub action_name: String,
    pub module_name: Option<String>,
    pub kwargs: HashMap<String, serde_json::Value>,
    pub timeout_seconds: u32,
}

pub fn deconstruct_request(
    value: waymark_worker_core::ActionRequest,
) -> (CorrelationId, Params, Uuid) {
    let waymark_worker_core::ActionRequest {
        action_name,
        module_name,
        kwargs,
        timeout_seconds,
        dispatch_token,
        attempt_number,
        executor_id,
        execution_id,
    } = value;

    let correlation_id = CorrelationId {
        executor_id,
        execution_id,
        attempt_number,
    };

    let params = Params {
        action_name,
        module_name,
        kwargs,
        timeout_seconds,
    };

    (correlation_id, params, dispatch_token)
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct LogItem {
    pub execution_time: std::time::Duration,
    pub params: Params,
    pub result: UncheckedExecutionResult,
}
