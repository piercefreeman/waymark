use std::collections::HashMap;

use chrono::Utc;
use uuid::Uuid;
use waymark_worker_core::ActionCompletion;

use crate::runloop::InflightActionDispatch;

#[derive(Default)]
struct TestHarness {
    pub dispatches: HashMap<Uuid, InflightActionDispatch>,
    pub completions: Vec<ActionCompletion>,
}

impl TestHarness {
    fn params<'a>(&'a mut self) -> super::Params<'a> {
        super::Params {
            all_completions: &mut self.completions,
            inflight_dispatches: &self.dispatches,
        }
    }
}

#[test]
fn no_deadline_not_timed_out() {
    let executor_id = Uuid::new_v4();
    let execution_id = Uuid::new_v4();
    let mut harness = TestHarness::default();
    harness.dispatches.insert(
        execution_id,
        InflightActionDispatch {
            executor_id,
            attempt_number: 1,
            dispatch_token: Uuid::new_v4(),
            timeout_seconds: 0,
            deadline_at: None,
        },
    );

    super::handle(harness.params());

    assert!(harness.completions.is_empty());
}

#[test]
fn past_deadline_generates_timeout_completion() {
    let executor_id = Uuid::new_v4();
    let execution_id = Uuid::new_v4();
    let dispatch_token = Uuid::new_v4();
    let mut harness = TestHarness::default();
    harness.dispatches.insert(
        execution_id,
        InflightActionDispatch {
            executor_id,
            attempt_number: 2,
            dispatch_token,
            timeout_seconds: 10,
            deadline_at: Some(Utc::now() - chrono::Duration::seconds(5)),
        },
    );

    super::handle(harness.params());

    assert_eq!(harness.completions.len(), 1);
    let completion = &harness.completions[0];
    assert_eq!(completion.executor_id, executor_id);
    assert_eq!(completion.execution_id, execution_id);
    assert_eq!(completion.attempt_number, 2);
    assert_eq!(completion.dispatch_token, dispatch_token);
    assert_eq!(
        completion.result["type"],
        serde_json::json!("ActionTimeout")
    );
}

#[test]
fn future_deadline_not_timed_out() {
    let executor_id = Uuid::new_v4();
    let execution_id = Uuid::new_v4();
    let mut harness = TestHarness::default();
    harness.dispatches.insert(
        execution_id,
        InflightActionDispatch {
            executor_id,
            attempt_number: 1,
            dispatch_token: Uuid::new_v4(),
            timeout_seconds: 60,
            deadline_at: Some(Utc::now() + chrono::Duration::seconds(60)),
        },
    );

    super::handle(harness.params());

    assert!(harness.completions.is_empty());
}

#[test]
fn timeout_is_prepended_before_existing_completions() {
    let executor_id = Uuid::new_v4();
    let timed_out_execution_id = Uuid::new_v4();
    let normal_execution_id = Uuid::new_v4();
    let dispatch_token = Uuid::new_v4();
    let mut harness = TestHarness::default();
    harness.dispatches.insert(
        timed_out_execution_id,
        InflightActionDispatch {
            executor_id,
            attempt_number: 1,
            dispatch_token,
            timeout_seconds: 5,
            deadline_at: Some(Utc::now() - chrono::Duration::seconds(1)),
        },
    );
    let existing = ActionCompletion {
        executor_id,
        execution_id: normal_execution_id,
        attempt_number: 1,
        dispatch_token: Uuid::new_v4(),
        result: serde_json::json!(42),
    };
    harness.completions.push(existing);

    super::handle(harness.params());

    assert_eq!(harness.completions.len(), 2);
    assert_eq!(harness.completions[0].execution_id, timed_out_execution_id);
    assert_eq!(harness.completions[1].execution_id, normal_execution_id);
}

#[test]
fn timeout_completion_contains_attempt_and_timeout_seconds_fields() {
    let executor_id = Uuid::new_v4();
    let execution_id = Uuid::new_v4();
    let dispatch_token = Uuid::new_v4();
    let mut harness = TestHarness::default();
    harness.dispatches.insert(
        execution_id,
        InflightActionDispatch {
            executor_id,
            attempt_number: 3,
            dispatch_token,
            timeout_seconds: 12,
            deadline_at: Some(Utc::now() - chrono::Duration::seconds(1)),
        },
    );

    super::handle(harness.params());

    assert_eq!(harness.completions.len(), 1);
    let payload = &harness.completions[0].result;
    assert_eq!(payload["type"], serde_json::json!("ActionTimeout"));
    assert_eq!(payload["attempt"], serde_json::json!(3));
    assert_eq!(payload["timeout_seconds"], serde_json::json!(12));
    let message = payload["message"].as_str().expect("timeout message string");
    assert!(
        message.contains("timed out after 12s"),
        "message should mention timeout duration"
    );
}
