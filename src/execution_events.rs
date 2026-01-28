//! Execution event log model and applier for reconstructing execution state.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::dag::DAG;
use crate::execution_graph::{BatchCompletionResult, Completion, ExecutionState};

/// Differential events that mutate execution state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ExecutionEvent {
    /// Action dispatched to a worker.
    Dispatch {
        node_id: String,
        worker_id: String,
        inputs: Option<Vec<u8>>,
        started_at_ms: Option<i64>,
    },
    /// Action completed (success or failure).
    Completion {
        node_id: String,
        success: bool,
        result: Option<Vec<u8>>,
        error: Option<String>,
        error_type: Option<String>,
        worker_id: String,
        duration_ms: i64,
        worker_duration_ms: Option<i64>,
    },
    /// Update next wakeup time when only sleeping paths remain.
    SetNextWakeup { next_wakeup_ms: Option<i64> },
}

#[derive(Debug, Error)]
pub enum ApplyEventError {
    #[error("DAG required to apply completion events")]
    MissingDag,
    #[error("Execution node not found: {0}")]
    MissingNode(String),
}

/// Lightweight state machine for applying an event log to an execution state.
#[derive(Debug)]
pub struct ExecutionStateMachine<'a> {
    state: ExecutionState,
    dag: Option<&'a DAG>,
}

impl<'a> ExecutionStateMachine<'a> {
    pub fn new(state: ExecutionState, dag: Option<&'a DAG>) -> Self {
        Self { state, dag }
    }

    pub fn state(&self) -> &ExecutionState {
        &self.state
    }

    pub fn state_mut(&mut self) -> &mut ExecutionState {
        &mut self.state
    }

    pub fn into_state(self) -> ExecutionState {
        self.state
    }

    pub fn apply(
        &mut self,
        event: &ExecutionEvent,
    ) -> Result<BatchCompletionResult, ApplyEventError> {
        apply_event(&mut self.state, self.dag, event)
    }

    pub fn apply_all<'e, I>(&mut self, events: I) -> Result<BatchCompletionResult, ApplyEventError>
    where
        I: IntoIterator<Item = &'e ExecutionEvent>,
    {
        let mut summary = BatchCompletionResult::default();
        for event in events {
            let result = self.apply(event)?;
            let BatchCompletionResult {
                newly_ready,
                workflow_completed,
                result_payload,
                workflow_failed,
                error_message,
            } = result;
            summary.newly_ready.extend(newly_ready);
            summary.workflow_completed |= workflow_completed;
            if result_payload.is_some() {
                summary.result_payload = result_payload;
            }
            summary.workflow_failed |= workflow_failed;
            if error_message.is_some() {
                summary.error_message = error_message;
            }
        }
        Ok(summary)
    }
}

pub fn apply_event(
    state: &mut ExecutionState,
    dag: Option<&DAG>,
    event: &ExecutionEvent,
) -> Result<BatchCompletionResult, ApplyEventError> {
    match event {
        ExecutionEvent::Dispatch {
            node_id,
            worker_id,
            inputs,
            started_at_ms,
        } => {
            if !state.graph.nodes.contains_key(node_id) {
                return Err(ApplyEventError::MissingNode(node_id.clone()));
            }
            state.mark_running(node_id, worker_id, inputs.clone());
            if let Some(started_at) = started_at_ms
                && let Some(node) = state.graph.nodes.get_mut(node_id)
            {
                node.started_at_ms = Some(*started_at);
            }
            Ok(BatchCompletionResult::default())
        }
        ExecutionEvent::Completion {
            node_id,
            success,
            result,
            error,
            error_type,
            worker_id,
            duration_ms,
            worker_duration_ms,
        } => {
            if !state.graph.nodes.contains_key(node_id) {
                return Err(ApplyEventError::MissingNode(node_id.clone()));
            }
            let dag = dag.ok_or(ApplyEventError::MissingDag)?;
            let completion = Completion {
                node_id: node_id.clone(),
                success: *success,
                result: result.clone(),
                error: error.clone(),
                error_type: error_type.clone(),
                worker_id: worker_id.clone(),
                duration_ms: *duration_ms,
                worker_duration_ms: *worker_duration_ms,
            };
            Ok(state.apply_completions_batch(vec![completion], dag))
        }
        ExecutionEvent::SetNextWakeup { next_wakeup_ms } => {
            state.graph.next_wakeup_time = *next_wakeup_ms;
            Ok(BatchCompletionResult::default())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::{DAG, DAGNode};
    use crate::messages::execution::ExecutionNode;
    use crate::messages::execution::NodeStatus;

    fn build_single_node_dag(node_id: &str) -> DAG {
        let mut dag = DAG::new();
        let mut node = DAGNode::new(
            node_id.to_string(),
            "action_call".to_string(),
            node_id.to_string(),
        );
        node.action_name = Some("action".to_string());
        node.module_name = Some("module".to_string());
        node.is_output = true;
        dag.add_node(node);
        dag
    }

    fn build_state(node_id: &str) -> ExecutionState {
        let mut state = ExecutionState::new();
        state.graph.nodes.insert(
            node_id.to_string(),
            ExecutionNode {
                template_id: node_id.to_string(),
                status: NodeStatus::Pending as i32,
                ..Default::default()
            },
        );
        state.graph.ready_queue.push(node_id.to_string());
        state
    }

    #[test]
    fn dispatch_marks_running_and_removes_ready() {
        let mut state = build_state("node1");
        let event = ExecutionEvent::Dispatch {
            node_id: "node1".to_string(),
            worker_id: "worker-1".to_string(),
            inputs: Some(vec![1, 2, 3]),
            started_at_ms: Some(1234),
        };
        let result = apply_event(&mut state, None, &event).expect("apply dispatch");
        assert!(result.newly_ready.is_empty());
        let node = state.graph.nodes.get("node1").unwrap();
        assert_eq!(node.status, NodeStatus::Running as i32);
        assert_eq!(node.worker_id.as_deref(), Some("worker-1"));
        assert_eq!(node.started_at_ms, Some(1234));
        assert!(state.graph.ready_queue.is_empty());
    }

    #[test]
    fn completion_applies_and_records_attempt() {
        let mut state = build_state("node1");
        let dag = build_single_node_dag("node1");
        let dispatch = ExecutionEvent::Dispatch {
            node_id: "node1".to_string(),
            worker_id: "worker-1".to_string(),
            inputs: Some(vec![1]),
            started_at_ms: Some(1000),
        };
        apply_event(&mut state, None, &dispatch).expect("dispatch");

        let completion = ExecutionEvent::Completion {
            node_id: "node1".to_string(),
            success: true,
            result: Some(vec![9, 9]),
            error: None,
            error_type: None,
            worker_id: "worker-1".to_string(),
            duration_ms: 50,
            worker_duration_ms: Some(50),
        };
        let result = apply_event(&mut state, Some(&dag), &completion).expect("completion");
        assert!(result.newly_ready.is_empty());
        let node = state.graph.nodes.get("node1").unwrap();
        assert_eq!(node.status, NodeStatus::Completed as i32);
        assert_eq!(node.result, Some(vec![9, 9]));
        assert_eq!(node.attempts.len(), 1);
    }

    #[test]
    fn completion_requires_dag() {
        let mut state = build_state("node1");
        let completion = ExecutionEvent::Completion {
            node_id: "node1".to_string(),
            success: true,
            result: Some(vec![1]),
            error: None,
            error_type: None,
            worker_id: "worker-1".to_string(),
            duration_ms: 10,
            worker_duration_ms: None,
        };
        let err = apply_event(&mut state, None, &completion).unwrap_err();
        assert!(matches!(err, ApplyEventError::MissingDag));
    }

    #[test]
    fn set_next_wakeup_updates_state() {
        let mut state = build_state("node1");
        let event = ExecutionEvent::SetNextWakeup {
            next_wakeup_ms: Some(555),
        };
        apply_event(&mut state, None, &event).expect("apply wakeup");
        assert_eq!(state.graph.next_wakeup_time, Some(555));
    }
}
