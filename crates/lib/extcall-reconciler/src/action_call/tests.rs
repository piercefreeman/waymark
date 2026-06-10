//! Tests for the action-call handler and poller.

use mockall::mock;
use mockall::predicate;
use nonempty_collections::NEVec;
use waymark_action_core::ActionRef;
use waymark_runner_executor_core::UncheckedExecutionResult;
use waymark_vm_runtime_promise_core::PromiseStateId;
use waymark_worker_core::{ActionCompletion, ActionRequest, BaseWorkerPool, WorkerPoolError};

// ------------------------------------------------------------------
// Mock worker pool
// ------------------------------------------------------------------

mock! {
    pub WorkerPool {}

    impl BaseWorkerPool for WorkerPool {
        fn queue(&self, request: ActionRequest) -> Result<(), WorkerPoolError>;

        async fn poll_complete(&self) -> Option<NEVec<ActionCompletion>>;
    }
}

// ------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------

fn action_completion(dispatch_token: uuid::Uuid) -> ActionCompletion {
    ActionCompletion {
        executor_id: waymark_ids::InstanceId::new_uuid_v4(),
        execution_id: waymark_ids::ExecutionId::new_uuid_v4(),
        attempt_number: 1,
        dispatch_token,
        result: UncheckedExecutionResult(serde_json::Value::String("ok".into())),
    }
}

fn action_completion_error(dispatch_token: uuid::Uuid) -> ActionCompletion {
    let mut map = serde_json::Map::new();
    map.insert("type".into(), "TestError".into());
    map.insert("message".into(), "something went wrong".into());
    ActionCompletion {
        executor_id: waymark_ids::InstanceId::new_uuid_v4(),
        execution_id: waymark_ids::ExecutionId::new_uuid_v4(),
        attempt_number: 1,
        dispatch_token,
        result: UncheckedExecutionResult(serde_json::Value::Object(map)),
    }
}

// ------------------------------------------------------------------
// Tests
// ------------------------------------------------------------------

fn test_action_ref() -> ActionRef {
    ActionRef {
        action_name: "test".into(),
        module_name: None,
        call_args: vec!["value".into()],
        timeout_seconds: 30,
        max_retries: 0,
        exception_types: vec![],
    }
}

#[tokio::test]
async fn dispatch_calls_queue_with_expected_fields() {
    let psid = PromiseStateId(0);
    let action_ref = test_action_ref();

    let mut mock = MockWorkerPool::new();
    mock.expect_queue()
        .with(predicate::function(|req: &ActionRequest| {
            req.action_name == "test"
                && req.module_name.is_none()
                && req.timeout_seconds == 30
                && req.attempt_number == 1
                && req.kwargs.get("value") == Some(&serde_json::Value::String("hi".into()))
        }))
        .returning(|_| Ok(()));

    let (handler, _poller) = super::new::<(), _>(mock);

    let token = handler
        .dispatch::<waymark_extcall_convert::Converter, _>(
            (),
            psid,
            &action_ref,
            vec![serde_json::Value::String("hi".into())],
        )
        .unwrap();

    assert!(!token.is_nil());
}

#[tokio::test]
async fn dispatch_sends_token_on_channel() {
    let psid = PromiseStateId(0);
    let action_ref = test_action_ref();
    let (dispatch_tx, mut dispatch_rx) = tokio::sync::mpsc::unbounded_channel();

    let mock = {
        let mut m = MockWorkerPool::new();
        m.expect_queue().returning(|_| Ok(()));
        m
    };
    let handler = super::Handler::<(), _> {
        worker_pool: std::sync::Arc::new(mock),
        tx: dispatch_tx,
    };

    let token = handler
        .dispatch::<waymark_extcall_convert::Converter, _>(
            (),
            psid,
            &action_ref,
            vec![serde_json::Value::String("hi".into())],
        )
        .unwrap();

    let (received_token, _received_vm_id, received_psid) = dispatch_rx.try_recv().unwrap();
    assert_eq!(received_token, token);
    assert_eq!(received_psid, psid);
}

#[tokio::test]
async fn dispatch_queue_error_propagates() {
    let psid = PromiseStateId(0);
    let action_ref = test_action_ref();

    let mut mock = MockWorkerPool::new();
    mock.expect_queue()
        .returning(|_| Err(waymark_worker_core::WorkerPoolError::new("test", "boom")));

    let (handler, _poller) = super::new::<(), _>(mock);

    let result = handler.dispatch::<waymark_extcall_convert::Converter, _>(
        (),
        psid,
        &action_ref,
        vec![serde_json::Value::String("hi".into())],
    );

    assert!(result.is_err());
}

#[tokio::test]
async fn dispatch_zips_call_args_with_positional_args() {
    let psid = PromiseStateId(0);
    let action_ref = ActionRef {
        call_args: vec!["first".into(), "second".into()],
        ..test_action_ref()
    };

    let mut mock = MockWorkerPool::new();
    mock.expect_queue()
        .with(predicate::function(|req: &ActionRequest| {
            req.kwargs.get("first") == Some(&serde_json::Value::String("a".into()))
                && req.kwargs.get("second") == Some(&serde_json::Value::String("b".into()))
        }))
        .returning(|_| Ok(()));

    let (handler, _poller) = super::new::<(), _>(mock);

    handler
        .dispatch::<waymark_extcall_convert::Converter, _>(
            (),
            psid,
            &action_ref,
            vec![
                serde_json::Value::String("a".into()),
                serde_json::Value::String("b".into()),
            ],
        )
        .unwrap();
}

#[tokio::test]
async fn poller_completion_produces_resolved_settlement() {
    let dispatch_token = uuid::Uuid::new_v4();
    let psid = PromiseStateId(0);
    let (dispatch_tx, dispatch_rx) = tokio::sync::mpsc::unbounded_channel();
    let (ack_tx, ack_rx) = tokio::sync::mpsc::unbounded_channel();

    let completion = action_completion(dispatch_token);

    let mut mock = MockWorkerPool::new();
    mock.expect_poll_complete()
        .return_once(move || Some(nonempty_collections::NEVec::new(completion)));

    let mut poller = super::Poller::<(), _> {
        worker_pool: std::sync::Arc::new(mock),
        dispatch_rx,
        ack_rx,
        ack_tx,
        pending: {
            let mut m = std::collections::HashMap::new();
            m.insert(dispatch_token, ((), psid));
            m
        },
    };
    drop(dispatch_tx); // No more dispatches coming.

    let settlements = poller
        .poll::<waymark_extcall_convert::Converter, waymark_vm_value::Value>()
        .await
        .unwrap();

    assert_eq!(settlements.len().get(), 1);
    let s = &settlements[0];
    assert_eq!(s.promise_state_id, psid);
    assert!(matches!(
        s.resolution,
        waymark_vm_driver_core::PromiseResolution::Resolved(_)
    ));
}

#[tokio::test]
async fn poller_error_completion_produces_rejected_settlement() {
    let dispatch_token = uuid::Uuid::new_v4();
    let psid = PromiseStateId(0);
    let (_dispatch_tx, dispatch_rx) = tokio::sync::mpsc::unbounded_channel();
    let (ack_tx, ack_rx) = tokio::sync::mpsc::unbounded_channel();

    let completion = action_completion_error(dispatch_token);

    let mut mock = MockWorkerPool::new();
    mock.expect_poll_complete()
        .return_once(move || Some(nonempty_collections::NEVec::new(completion)));

    let mut poller = super::Poller::<(), _> {
        worker_pool: std::sync::Arc::new(mock),
        dispatch_rx,
        ack_rx,
        ack_tx,
        pending: {
            let mut m = std::collections::HashMap::new();
            m.insert(dispatch_token, ((), psid));
            m
        },
    };

    let settlements = poller
        .poll::<waymark_extcall_convert::Converter, waymark_vm_value::Value>()
        .await
        .unwrap();

    assert_eq!(settlements.len().get(), 1);
    let s = &settlements[0];
    assert_eq!(s.promise_state_id, psid);
    assert!(matches!(
        s.resolution,
        waymark_vm_driver_core::PromiseResolution::Rejected(_)
    ));
}

#[tokio::test]
async fn poller_ack_clears_pending_entry() {
    let dispatch_token = uuid::Uuid::new_v4();
    let psid = PromiseStateId(0);
    let (ack_tx, ack_rx) = tokio::sync::mpsc::unbounded_channel();
    let (_dispatch_tx, dispatch_rx) = tokio::sync::mpsc::unbounded_channel();

    let completion = action_completion(dispatch_token);

    let mut mock = MockWorkerPool::new();
    mock.expect_poll_complete()
        .return_once(move || Some(NEVec::new(completion)));

    let mut poller = super::Poller::<(), _> {
        worker_pool: std::sync::Arc::new(mock),
        dispatch_rx,
        ack_rx,
        ack_tx: ack_tx.clone(),
        pending: {
            let mut m = std::collections::HashMap::new();
            m.insert(dispatch_token, ((), psid));
            m
        },
    };

    // Produce a settlement and ack it.
    let settlements = poller
        .poll::<waymark_extcall_convert::Converter, waymark_vm_value::Value>()
        .await
        .unwrap();

    assert_eq!(settlements.len().get(), 1);

    // Ack the settlement via the PromiseSettlementAck trait.
    let settlement = settlements.into_iter().next().unwrap();
    waymark_vm_driver_core::PromiseSettlementAck::acknowledge_promise_settlement(settlement.ack);

    // The ack sends on ack_tx, which routes to poller.ack_rx. Since poll
    // isn't running, drain it manually to simulate what poll's try_recv does.
    while let Ok(token) = poller.ack_rx.try_recv() {
        poller.pending.remove(&token);
    }

    assert!(!poller.pending.contains_key(&dispatch_token));
}

#[tokio::test]
async fn poller_stale_completion_skipped_past() {
    // Batch of two completions: one stale (unknown token), one real.
    let real_token = uuid::Uuid::new_v4();
    let stale_token = uuid::Uuid::new_v4();
    let psid = PromiseStateId(0);
    let (_dispatch_tx, dispatch_rx) = tokio::sync::mpsc::unbounded_channel();
    let (ack_tx, ack_rx) = tokio::sync::mpsc::unbounded_channel();

    let batch = NEVec::try_from_vec(vec![
        action_completion(stale_token),
        action_completion(real_token),
    ])
    .unwrap();

    let mut mock = MockWorkerPool::new();
    mock.expect_poll_complete().return_once(move || Some(batch));

    let mut poller = super::Poller::<(), _> {
        worker_pool: std::sync::Arc::new(mock),
        dispatch_rx,
        ack_rx,
        ack_tx,
        pending: std::collections::HashMap::from([(real_token, ((), psid))]),
    };

    let settlements = poller
        .poll::<waymark_extcall_convert::Converter, waymark_vm_value::Value>()
        .await
        .unwrap();

    // Only the real completion should produce a settlement.
    assert_eq!(settlements.len().get(), 1);
    assert_eq!(settlements[0].promise_state_id, psid);
}

#[tokio::test]
async fn poller_receives_dispatch_during_poll() {
    let dispatch_token = uuid::Uuid::new_v4();
    let psid = PromiseStateId(0);
    let (dispatch_tx, dispatch_rx) = tokio::sync::mpsc::unbounded_channel();
    let (ack_tx, ack_rx) = tokio::sync::mpsc::unbounded_channel();

    let completion = action_completion(dispatch_token);

    let mut mock = MockWorkerPool::new();
    mock.expect_poll_complete()
        .return_once(move || Some(nonempty_collections::NEVec::new(completion)));

    let mut poller = super::Poller::<(), _> {
        worker_pool: std::sync::Arc::new(mock),
        dispatch_rx,
        ack_rx,
        ack_tx,
        pending: std::collections::HashMap::new(),
    };

    // Send a dispatch asynchronously and then trigger a completion.
    dispatch_tx.send((dispatch_token, (), psid)).unwrap();

    let settlements = poller
        .poll::<waymark_extcall_convert::Converter, waymark_vm_value::Value>()
        .await
        .unwrap();

    assert_eq!(settlements.len().get(), 1);
    assert_eq!(settlements[0].promise_state_id, psid);
}

#[tokio::test]
async fn poller_returns_none_when_all_sources_closed() {
    let (_dispatch_tx, dispatch_rx) =
        tokio::sync::mpsc::unbounded_channel::<(uuid::Uuid, (), PromiseStateId)>();
    let (ack_tx, ack_rx) = tokio::sync::mpsc::unbounded_channel();

    let mut mock = MockWorkerPool::new();
    mock.expect_poll_complete().returning(|| None);

    let mut poller = super::Poller::<(), _> {
        worker_pool: std::sync::Arc::new(mock),
        dispatch_rx,
        ack_rx,
        ack_tx: ack_tx.clone(),
        pending: std::collections::HashMap::new(),
    };

    // Close all senders.
    drop(ack_tx);
    // dispatch_rx sender is already dropped via _dispatch_tx at end of scope.

    // poll should return None when all sources are exhausted.
    let result = poller
        .poll::<waymark_extcall_convert::Converter, waymark_vm_value::Value>()
        .await;
    assert!(result.is_none());
}
