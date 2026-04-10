use waymark_ids::{ExecutionId, InstanceId};

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct CorrelationId {
    pub executor_id: InstanceId,
    pub execution_id: ExecutionId,
    pub attempt_number: u32,
}

impl CorrelationId {
    pub fn from_request(value: &waymark_worker_core::ActionRequest) -> Self {
        Self {
            executor_id: value.executor_id,
            execution_id: value.execution_id,
            attempt_number: value.attempt_number,
        }
    }

    pub fn from_completion(value: &waymark_worker_core::ActionCompletion) -> Self {
        Self {
            executor_id: value.executor_id,
            execution_id: value.execution_id,
            attempt_number: value.attempt_number,
        }
    }
}
