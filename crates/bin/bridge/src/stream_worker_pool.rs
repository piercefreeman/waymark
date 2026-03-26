use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use nonempty_collections::NEVec;
use prost::Message as _;
use tokio::sync::{Mutex as AsyncMutex, mpsc};
use uuid::Uuid;

use waymark_proto::messages as proto;
use waymark_worker_core::{ActionCompletion, ActionRequest, BaseWorkerPool, WorkerPoolError};

pub struct StreamWorkerPool {
    dispatch_tx: mpsc::Sender<proto::WorkflowStreamResponse>,
    completion_rx: Arc<AsyncMutex<mpsc::Receiver<ActionCompletion>>>,
    inflight: Arc<Mutex<HashMap<String, StreamInflightAction>>>,
}

#[derive(Clone, Copy)]
pub struct StreamInflightAction {
    executor_id: Uuid,
    attempt_number: u32,
    dispatch_token: Uuid,
}

impl StreamWorkerPool {
    pub fn new(
        dispatch_tx: mpsc::Sender<proto::WorkflowStreamResponse>,
        completion_rx: mpsc::Receiver<ActionCompletion>,
    ) -> Self {
        Self {
            dispatch_tx,
            completion_rx: Arc::new(AsyncMutex::new(completion_rx)),
            inflight: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn inflight(&self) -> Arc<Mutex<HashMap<String, StreamInflightAction>>> {
        Arc::clone(&self.inflight)
    }
}

impl BaseWorkerPool for StreamWorkerPool {
    fn queue(&self, request: ActionRequest) -> Result<(), WorkerPoolError> {
        let module_name = request
            .module_name
            .clone()
            .ok_or_else(|| WorkerPoolError::new("StreamWorkerPoolError", "missing module name"))?;
        let action_id = request.execution_id.to_string();

        if let Ok(mut inflight) = self.inflight.lock() {
            inflight.insert(
                action_id.clone(),
                StreamInflightAction {
                    executor_id: request.executor_id,
                    attempt_number: request.attempt_number,
                    dispatch_token: request.dispatch_token,
                },
            );
        }

        let dispatch = proto::ActionDispatch {
            action_id: action_id.clone(),
            instance_id: request.executor_id.to_string(),
            sequence: 0,
            action_name: request.action_name,
            module_name,
            kwargs: Some(crate::utils::kwargs_to_workflow_arguments(&request.kwargs)),
            timeout_seconds: Some(request.timeout_seconds),
            max_retries: None,
            attempt_number: Some(request.attempt_number),
            dispatch_token: Some(request.dispatch_token.to_string()),
        };

        let response = proto::WorkflowStreamResponse {
            kind: Some(proto::workflow_stream_response::Kind::ActionDispatch(
                dispatch,
            )),
        };

        self.dispatch_tx.try_send(response).map_err(|err| {
            WorkerPoolError::new(
                "StreamWorkerPoolError",
                format!("failed to dispatch action: {err}"),
            )
        })?;

        Ok(())
    }

    async fn poll_complete(&self) -> Option<NEVec<ActionCompletion>> {
        let mut receiver = self.completion_rx.lock().await;

        let first = receiver.recv().await?;

        let mut completions = NEVec::new(first);

        while let Ok(item) = receiver.try_recv() {
            completions.push(item);
        }

        Some(completions)
    }
}

pub fn action_result_to_completion(
    result: proto::ActionResult,
    inflight: &Arc<Mutex<HashMap<String, StreamInflightAction>>>,
) -> Option<ActionCompletion> {
    let action_id = result.action_id.clone();
    let inflight_action = inflight
        .lock()
        .ok()
        .and_then(|mut map| map.remove(&action_id))?;
    let execution_id = Uuid::parse_str(&action_id).ok()?;

    let payload = result
        .payload
        .as_ref()
        .map(|payload| payload.encode_to_vec())
        .and_then(|bytes| proto::WorkflowArguments::decode(bytes.as_slice()).ok())
        .map(waymark_message_conversions::workflow_arguments_to_json)
        .unwrap_or(serde_json::Value::Null);

    let value = if result.success {
        if let serde_json::Value::Object(mut map) = payload {
            map.remove("result")
                .unwrap_or(serde_json::Value::Object(map))
        } else {
            payload
        }
    } else if let serde_json::Value::Object(mut map) = payload {
        let error = map
            .remove("error")
            .unwrap_or(serde_json::Value::Object(map));
        crate::utils::normalize_error_value(error)
    } else {
        crate::utils::normalize_error_value(payload)
    };

    Some(ActionCompletion {
        executor_id: inflight_action.executor_id,
        execution_id,
        attempt_number: inflight_action.attempt_number,
        dispatch_token: inflight_action.dispatch_token,
        result: value,
    })
}
