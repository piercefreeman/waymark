use serial_test::serial;

use waymark_action_reconciler_backend::{
    LoadPendingActionCalls as _, PendingActionCall, PendingActionCallOutcome,
    RemovePendingActionCall as _, StoreActionCallOutcome as _, StoreActionCallOutcomeStatus,
    StorePendingActionCall as _,
};
use waymark_action_runtime_metadata::ActionCallCorrelation;
use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

use super::super::test_helpers::{register_test_vm, setup_backend};

fn correlation(effect_number: usize, promise_state_id: usize) -> ActionCallCorrelation {
    ActionCallCorrelation {
        effect_number: EffectNumber(effect_number),
        promise_state_id: PromiseStateId(promise_state_id),
    }
}

#[serial(postgres)]
#[tokio::test]
async fn store_load_remove_happy_path() {
    let backend = setup_backend().await;
    let (vm_id, _executable_id) = register_test_vm(&backend).await;

    // Stored out of effect order to verify the load ordering.
    backend
        .store_pending_action_call(&vm_id, correlation(3, 7), b"payload-b")
        .await
        .expect("store second call");
    backend
        .store_pending_action_call(&vm_id, correlation(1, 5), b"payload-a")
        .await
        .expect("store first call");

    let pending = backend
        .load_pending_action_calls(&vm_id)
        .await
        .expect("load pending calls");
    assert_eq!(
        pending,
        vec![
            PendingActionCall {
                correlation: correlation(1, 5),
                payload: b"payload-a".to_vec(),
                outcome: None,
            },
            PendingActionCall {
                correlation: correlation(3, 7),
                payload: b"payload-b".to_vec(),
                outcome: None,
            },
        ]
    );

    backend
        .remove_pending_action_call(&vm_id, PromiseStateId(5))
        .await
        .expect("remove first call");

    let pending = backend
        .load_pending_action_calls(&vm_id)
        .await
        .expect("load pending calls");
    assert_eq!(
        pending,
        vec![PendingActionCall {
            correlation: correlation(3, 7),
            payload: b"payload-b".to_vec(),
            outcome: None,
        }]
    );
}

#[serial(postgres)]
#[tokio::test]
async fn store_is_idempotent_for_identical_values() {
    let backend = setup_backend().await;
    let (vm_id, _executable_id) = register_test_vm(&backend).await;

    backend
        .store_pending_action_call(&vm_id, correlation(1, 5), b"payload")
        .await
        .expect("store call");
    backend
        .store_pending_action_call(&vm_id, correlation(1, 5), b"payload")
        .await
        .expect("store identical call again");

    let pending = backend
        .load_pending_action_calls(&vm_id)
        .await
        .expect("load pending calls");
    assert_eq!(pending.len(), 1);
}

#[serial(postgres)]
#[tokio::test]
async fn store_rejects_diverging_value() {
    let backend = setup_backend().await;
    let (vm_id, _executable_id) = register_test_vm(&backend).await;

    backend
        .store_pending_action_call(&vm_id, correlation(1, 5), b"payload")
        .await
        .expect("store call");

    let result = backend
        .store_pending_action_call(&vm_id, correlation(1, 5), b"diverged")
        .await;
    assert!(matches!(
        result,
        Err(super::error::StoreError::Conflict { .. })
    ));

    let pending = backend
        .load_pending_action_calls(&vm_id)
        .await
        .expect("load pending calls");
    assert_eq!(
        pending,
        vec![PendingActionCall {
            correlation: correlation(1, 5),
            payload: b"payload".to_vec(),
            outcome: None,
        }]
    );
}

#[serial(postgres)]
#[tokio::test]
async fn remove_absent_record_is_not_an_error() {
    let backend = setup_backend().await;
    let (vm_id, _executable_id) = register_test_vm(&backend).await;

    backend
        .remove_pending_action_call(&vm_id, PromiseStateId(5))
        .await
        .expect("remove absent call");
}

#[serial(postgres)]
#[tokio::test]
async fn load_only_returns_calls_of_the_requested_vm() {
    let backend = setup_backend().await;
    let (vm_a, _executable_id) = register_test_vm(&backend).await;
    let (vm_b, _executable_id) = register_test_vm(&backend).await;

    backend
        .store_pending_action_call(&vm_a, correlation(1, 5), b"payload-a")
        .await
        .expect("store call for vm a");
    backend
        .store_pending_action_call(&vm_b, correlation(1, 5), b"payload-b")
        .await
        .expect("store call for vm b");

    let pending = backend
        .load_pending_action_calls(&vm_a)
        .await
        .expect("load pending calls");
    assert_eq!(
        pending,
        vec![PendingActionCall {
            correlation: correlation(1, 5),
            payload: b"payload-a".to_vec(),
            outcome: None,
        }]
    );
}

#[serial(postgres)]
#[tokio::test]
async fn store_outcome_records_onto_the_pending_call() {
    let backend = setup_backend().await;
    let (vm_id, _executable_id) = register_test_vm(&backend).await;

    backend
        .store_pending_action_call(&vm_id, correlation(1, 5), b"payload")
        .await
        .expect("store call");

    let status = backend
        .store_action_call_outcome(
            &vm_id,
            PromiseStateId(5),
            PendingActionCallOutcome::Value(b"outcome".to_vec()),
        )
        .await
        .expect("store outcome");
    assert_eq!(status, StoreActionCallOutcomeStatus::Stored);

    let pending = backend
        .load_pending_action_calls(&vm_id)
        .await
        .expect("load pending calls");
    assert_eq!(
        pending,
        vec![PendingActionCall {
            correlation: correlation(1, 5),
            payload: b"payload".to_vec(),
            outcome: Some(PendingActionCallOutcome::Value(b"outcome".to_vec())),
        }]
    );
}

#[serial(postgres)]
#[tokio::test]
async fn store_outcome_first_write_wins() {
    let backend = setup_backend().await;
    let (vm_id, _executable_id) = register_test_vm(&backend).await;

    backend
        .store_pending_action_call(&vm_id, correlation(1, 5), b"payload")
        .await
        .expect("store call");
    backend
        .store_action_call_outcome(
            &vm_id,
            PromiseStateId(5),
            PendingActionCallOutcome::Value(b"first".to_vec()),
        )
        .await
        .expect("store outcome");

    let status = backend
        .store_action_call_outcome(
            &vm_id,
            PromiseStateId(5),
            PendingActionCallOutcome::Exception(b"second".to_vec()),
        )
        .await
        .expect("store duplicate outcome");
    assert_eq!(status, StoreActionCallOutcomeStatus::NotPending);

    let pending = backend
        .load_pending_action_calls(&vm_id)
        .await
        .expect("load pending calls");
    assert_eq!(
        pending[0].outcome,
        Some(PendingActionCallOutcome::Value(b"first".to_vec()))
    );
}

#[serial(postgres)]
#[tokio::test]
async fn store_outcome_without_a_pending_call_is_not_pending() {
    let backend = setup_backend().await;
    let (vm_id, _executable_id) = register_test_vm(&backend).await;

    let status = backend
        .store_action_call_outcome(
            &vm_id,
            PromiseStateId(5),
            PendingActionCallOutcome::Value(b"outcome".to_vec()),
        )
        .await
        .expect("store outcome");
    assert_eq!(status, StoreActionCallOutcomeStatus::NotPending);
}
