use uuid::Uuid;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct CorrelationId {
    pub executor_id: Uuid,
    pub execution_id: Uuid,
    pub attempt_number: u32,
    pub dispatch_token: Uuid,
}

impl CorrelationId {
    pub fn from_request(value: &waymark_worker_core::ActionRequest) -> Self {
        Self {
            executor_id: value.executor_id,
            execution_id: value.execution_id,
            attempt_number: value.attempt_number,
            dispatch_token: value.dispatch_token,
        }
    }

    pub fn from_completion(value: &waymark_worker_core::ActionCompletion) -> Self {
        Self {
            executor_id: value.executor_id,
            execution_id: value.execution_id,
            attempt_number: value.attempt_number,
            dispatch_token: value.dispatch_token,
        }
    }
}
