use uuid::Uuid;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct ActionId {
    pub executor_id: Uuid,
    pub execution_id: Uuid,
    pub attempt_number: u32,
    pub dispatch_token: Uuid,
}

impl From<&waymark_worker_core::ActionRequest> for ActionId {
    fn from(value: &waymark_worker_core::ActionRequest) -> Self {
        Self {
            executor_id: value.executor_id,
            execution_id: value.execution_id,
            attempt_number: value.attempt_number,
            dispatch_token: value.dispatch_token,
        }
    }
}

impl From<&waymark_worker_core::ActionCompletion> for ActionId {
    fn from(value: &waymark_worker_core::ActionCompletion) -> Self {
        Self {
            executor_id: value.executor_id,
            execution_id: value.execution_id,
            attempt_number: value.attempt_number,
            dispatch_token: value.dispatch_token,
        }
    }
}
