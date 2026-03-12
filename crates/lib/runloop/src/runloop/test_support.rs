use std::collections::HashMap;

use chrono::{DateTime, Utc};
use uuid::Uuid;
use waymark_worker_core::{ActionCompletion, ActionRequest, BaseWorkerPool, WorkerPoolError};

use crate::runloop::InflightActionDispatch;

pub struct NoOpPool;

impl BaseWorkerPool for NoOpPool {
    fn queue(&self, _request: ActionRequest) -> Result<(), WorkerPoolError> {
        Ok(())
    }

    fn get_complete<'a>(
        &'a self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Vec<ActionCompletion>> + Send + 'a>>
    {
        Box::pin(std::future::ready(vec![]))
    }
}

pub fn make_inflight_dispatch(
    executor_id: Uuid,
    dispatch_token: Uuid,
    attempt_number: u32,
    timeout_seconds: u32,
    deadline_at: Option<DateTime<Utc>>,
) -> InflightActionDispatch {
    InflightActionDispatch {
        executor_id,
        attempt_number,
        dispatch_token,
        timeout_seconds,
        deadline_at,
    }
}

pub fn make_action_completion(
    executor_id: Uuid,
    execution_id: Uuid,
    dispatch_token: Uuid,
    attempt_number: u32,
) -> ActionCompletion {
    ActionCompletion {
        executor_id,
        execution_id,
        attempt_number,
        dispatch_token,
        result: serde_json::json!(null),
    }
}

pub fn empty_kwargs() -> HashMap<String, serde_json::Value> {
    HashMap::new()
}
