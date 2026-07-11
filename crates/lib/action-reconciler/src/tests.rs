//! Tests for the persistent action reconciler.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use mockall::mock;
use nonempty_collections::{NEVec, nev};
use waymark_action_core::ActionRef;
use waymark_action_reconciler_backend::{
    PendingActionCall, PendingActionCallOutcome, StoreActionCallOutcomeStatus,
};
use waymark_action_runtime_core::{
    ActionCallCompletion, ActionCallCompletionsProvider, ActionCallOutcome, ActionCallRequest,
    ActionCallRequester,
};
use waymark_action_runtime_metadata::{ActionCallCorrelation, WithVmId};
use waymark_ids::InstanceId;
use waymark_vm_codec_core::SerializerProvider as _;
use waymark_vm_codec_rmp::RmpCodec;
use waymark_vm_driver_core::{PromiseResolution, PromiseSettlementAck as _};
use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_exception::Exception;
use waymark_vm_runtime_promise_core::PromiseStateId;
use waymark_vm_value::ReadyValue;

use super::PersistedActionCall;

// ------------------------------------------------------------------
// Test doubles
// ------------------------------------------------------------------

/// Error type for the mock action requester.
#[derive(Debug, thiserror::Error)]
#[error("mock requester error")]
pub struct MockRequesterError;

/// Error type for the mock completions provider.
#[derive(Debug, thiserror::Error)]
#[error("mock provider error")]
pub struct MockProviderError;

mock! {
    pub ActionRequester {}

    impl ActionCallRequester for ActionRequester {
        type Error = MockRequesterError;
        type Argument = ReadyValue;
        type Metadata = ActionCallCorrelation;

        async fn request_action_call(
            &self,
            request: ActionCallRequest<ReadyValue, ActionCallCorrelation>,
        ) -> Result<(), MockRequesterError>;
    }
}

mock! {
    pub CompletionsProvider {}

    impl ActionCallCompletionsProvider for CompletionsProvider {
        type Value = ReadyValue;
        type Error = MockProviderError;
        type Metadata = ActionCallCorrelation;

        fn wait_for_completions(
            &mut self,
        ) -> impl Future<
            Output = Result<
                NEVec<ActionCallCompletion<ReadyValue, ActionCallCorrelation>>,
                MockProviderError,
            >,
        > + Send;
    }
}

mock! {
    pub VmScopedCompletionsProvider {}

    impl ActionCallCompletionsProvider for VmScopedCompletionsProvider {
        type Value = ReadyValue;
        type Error = MockProviderError;
        type Metadata = WithVmId<ActionCallCorrelation>;

        fn wait_for_completions(
            &mut self,
        ) -> impl Future<
            Output = Result<
                NEVec<ActionCallCompletion<ReadyValue, WithVmId<ActionCallCorrelation>>>,
                MockProviderError,
            >,
        > + Send;
    }
}

/// Error type for the in-memory backend.
#[derive(Debug, thiserror::Error)]
pub enum FakeBackendError {
    #[error("wrong vm id")]
    WrongVmId,

    #[error("conflicting pending action call for promise {0:?}")]
    Conflict(PromiseStateId),
}

/// In-memory pending-action-call backend that retains records for
/// assertions.
struct FakeBackend {
    vm_id: InstanceId,
    records: Mutex<BTreeMap<PromiseStateId, PendingActionCall>>,
}

impl FakeBackend {
    fn new(vm_id: InstanceId) -> Self {
        Self {
            vm_id,
            records: Mutex::new(BTreeMap::new()),
        }
    }

    fn records(&self) -> Vec<PendingActionCall> {
        self.records
            .lock()
            .expect("records poisoned")
            .values()
            .cloned()
            .collect()
    }

    fn seed(&self, correlation: ActionCallCorrelation, payload: Vec<u8>) {
        self.seed_record(PendingActionCall {
            correlation,
            payload,
            outcome: None,
        });
    }

    fn seed_record(&self, record: PendingActionCall) {
        self.records
            .lock()
            .expect("records poisoned")
            .insert(record.correlation.promise_state_id, record);
    }
}

impl waymark_action_reconciler_backend::HasVmId for FakeBackend {
    type VmId = InstanceId;
}

impl waymark_action_reconciler_backend::StorePendingActionCall for FakeBackend {
    type Error = FakeBackendError;

    async fn store_pending_action_call<'a>(
        &'a self,
        vm_id: &'a InstanceId,
        correlation: ActionCallCorrelation,
        payload: impl AsRef<[u8]> + Send + 'a,
    ) -> Result<(), Self::Error> {
        if *vm_id != self.vm_id {
            return Err(FakeBackendError::WrongVmId);
        }
        let record = PendingActionCall {
            correlation,
            payload: payload.as_ref().to_vec(),
            outcome: None,
        };
        let mut records = self.records.lock().expect("records poisoned");
        match records.get(&correlation.promise_state_id) {
            Some(existing) if *existing != record => {
                Err(FakeBackendError::Conflict(correlation.promise_state_id))
            }
            _ => {
                records.insert(correlation.promise_state_id, record);
                Ok(())
            }
        }
    }
}

impl waymark_action_reconciler_backend::StoreActionCallOutcome for FakeBackend {
    type Error = FakeBackendError;

    async fn store_action_call_outcome<'a>(
        &'a self,
        vm_id: &'a InstanceId,
        promise_state_id: PromiseStateId,
        outcome: PendingActionCallOutcome,
    ) -> Result<StoreActionCallOutcomeStatus, Self::Error> {
        if *vm_id != self.vm_id {
            return Err(FakeBackendError::WrongVmId);
        }
        let mut records = self.records.lock().expect("records poisoned");
        match records.get_mut(&promise_state_id) {
            Some(record) if record.outcome.is_none() => {
                record.outcome = Some(outcome);
                Ok(StoreActionCallOutcomeStatus::Stored)
            }
            _ => Ok(StoreActionCallOutcomeStatus::NotPending),
        }
    }
}

impl waymark_action_reconciler_backend::RemovePendingActionCall for FakeBackend {
    type Error = FakeBackendError;

    async fn remove_pending_action_call<'a>(
        &'a self,
        vm_id: &'a InstanceId,
        promise_state_id: PromiseStateId,
    ) -> Result<(), Self::Error> {
        if *vm_id != self.vm_id {
            return Err(FakeBackendError::WrongVmId);
        }
        self.records
            .lock()
            .expect("records poisoned")
            .remove(&promise_state_id);
        Ok(())
    }
}

impl waymark_action_reconciler_backend::LoadPendingActionCalls for FakeBackend {
    type Error = FakeBackendError;

    async fn load_pending_action_calls<'a>(
        &'a self,
        vm_id: &'a InstanceId,
    ) -> Result<Vec<PendingActionCall>, Self::Error> {
        if *vm_id != self.vm_id {
            return Err(FakeBackendError::WrongVmId);
        }
        Ok(self.records())
    }
}

// ------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------

fn test_action_ref() -> ActionRef {
    ActionRef {
        action_name: "test".into(),
        module_name: None,
        call_args: vec!["arg".into()],
        timeout_seconds: 30,
        max_retries: 0,
        exception_types: vec![],
    }
}

fn correlation(effect_number: usize, promise_state_id: usize) -> ActionCallCorrelation {
    ActionCallCorrelation {
        effect_number: EffectNumber(effect_number),
        promise_state_id: PromiseStateId(promise_state_id),
    }
}

fn encode<T: serde::Serialize>(value: &T) -> Vec<u8> {
    let mut bytes = Vec::new();
    RmpCodec
        .with_serializer(&mut bytes, |serializer| {
            serde::Serialize::serialize(value, serializer)
        })
        .expect("encode value");
    bytes
}

fn encoded_call(arguments: Vec<ReadyValue>) -> Vec<u8> {
    encode(&PersistedActionCall {
        action_ref: test_action_ref(),
        arguments,
    })
}

fn test_exception() -> Exception<ReadyValue> {
    Exception {
        type_id: "TestError".into(),
        details: ReadyValue::String("boom".into()),
    }
}

fn completion(
    effect_number: usize,
    promise_state_id: usize,
) -> ActionCallCompletion<ReadyValue, ActionCallCorrelation> {
    ActionCallCompletion {
        metadata: correlation(effect_number, promise_state_id),
        outcome: ActionCallOutcome::Value(ReadyValue::Int(1)),
    }
}

fn vm_scoped_completion(
    vm_id: InstanceId,
    promise_state_id: usize,
    outcome: ActionCallOutcome<ReadyValue>,
) -> ActionCallCompletion<ReadyValue, WithVmId<ActionCallCorrelation>> {
    ActionCallCompletion {
        metadata: WithVmId {
            vm_id,
            inner: correlation(0, promise_state_id),
        },
        outcome,
    }
}

// ------------------------------------------------------------------
// Handler / Poller tests
// ------------------------------------------------------------------

#[tokio::test]
async fn request_stores_record_and_dispatches() {
    let vm_id = InstanceId::new_uuid_v4();
    let mut requester = MockActionRequester::new();
    requester
        .expect_request_action_call()
        .withf(|request| request.metadata == correlation(0, 5))
        .times(1)
        .returning(|_| Ok(()));

    let backend = Arc::new(FakeBackend::new(vm_id));
    let (handler, _poller) = super::new(
        requester,
        MockCompletionsProvider::new(),
        Arc::clone(&backend),
        RmpCodec,
        vm_id,
    );

    handler
        .request(
            EffectNumber(0),
            PromiseStateId(5),
            test_action_ref(),
            vec![ReadyValue::String("hi".into())],
        )
        .await
        .expect("request");

    assert_eq!(
        backend.records(),
        vec![PendingActionCall {
            correlation: correlation(0, 5),
            payload: encoded_call(vec![ReadyValue::String("hi".into())]),
            outcome: None,
        }]
    );
}

#[tokio::test]
async fn settlement_ack_removes_record() {
    let vm_id = InstanceId::new_uuid_v4();
    let mut provider = MockCompletionsProvider::new();
    provider
        .expect_wait_for_completions()
        .times(1)
        .returning(|| Box::pin(std::future::ready(Ok(nev![completion(0, 5)]))));

    let backend = Arc::new(FakeBackend::new(vm_id));
    backend.seed(correlation(0, 5), encoded_call(vec![]));

    let mut requester = MockActionRequester::new();
    requester
        .expect_request_action_call()
        .times(1)
        .returning(|_| Ok(()));

    let (_handler, mut poller) =
        super::new(requester, provider, Arc::clone(&backend), RmpCodec, vm_id);

    let waiting = nev![PromiseStateId(5)];
    let settlements = poller
        .poll::<super::Ack<FakeBackend>>(&waiting)
        .await
        .expect("poll");
    assert_eq!(settlements.len().get(), 1);

    // The record survives until the settlement is acknowledged (i.e. the
    // VM state that contains it has been persisted).
    assert_eq!(backend.records().len(), 1);

    let settlement = settlements.into_iter().next().expect("one settlement");
    assert_eq!(settlement.promise_state_id, PromiseStateId(5));
    assert!(matches!(
        settlement.resolution,
        PromiseResolution::Resolved(ReadyValue::Int(1))
    ));
    settlement.ack.acknowledge_promise_settlement().await;

    assert_eq!(backend.records(), vec![]);
}

#[tokio::test]
async fn first_poll_redispatches_orphaned_calls_and_removes_stale_records() {
    let vm_id = InstanceId::new_uuid_v4();
    let backend = Arc::new(FakeBackend::new(vm_id));
    // Orphaned: the VM is waiting on this promise, but this session never
    // dispatched the call — the previous session died with it in flight.
    backend.seed(
        correlation(0, 5),
        encoded_call(vec![ReadyValue::String("orphaned".into())]),
    );
    // Stale: the VM is not waiting on this promise anymore — the settlement
    // was persisted but the record removal was lost.
    backend.seed(correlation(1, 7), encoded_call(vec![]));

    let mut requester = MockActionRequester::new();
    requester
        .expect_request_action_call()
        .withf(|request| {
            request.metadata == correlation(0, 5)
                && request.arguments == vec![ReadyValue::String("orphaned".into())]
                && request.action_ref == test_action_ref()
        })
        .times(1)
        .returning(|_| Ok(()));

    let mut provider = MockCompletionsProvider::new();
    provider
        .expect_wait_for_completions()
        .times(1)
        .returning(|| Box::pin(std::future::ready(Ok(nev![completion(0, 5)]))));

    let (_handler, mut poller) =
        super::new(requester, provider, Arc::clone(&backend), RmpCodec, vm_id);

    let waiting = nev![PromiseStateId(5)];
    let settlements = poller
        .poll::<super::Ack<FakeBackend>>(&waiting)
        .await
        .expect("poll");
    assert_eq!(settlements.len().get(), 1);
    assert_eq!(settlements[0].promise_state_id, PromiseStateId(5));

    // The stale record is gone; the re-dispatched one remains until its
    // settlement is acknowledged.
    assert_eq!(
        backend.records(),
        vec![PendingActionCall {
            correlation: correlation(0, 5),
            payload: encoded_call(vec![ReadyValue::String("orphaned".into())]),
            outcome: None,
        }]
    );
}

#[tokio::test]
async fn first_poll_settles_completed_records_without_reexecuting() {
    let vm_id = InstanceId::new_uuid_v4();
    let backend = Arc::new(FakeBackend::new(vm_id));
    // The call finished while nobody was looking: the outcome was recorded,
    // but the settlement never made it into a persisted VM snapshot.
    backend.seed_record(PendingActionCall {
        correlation: correlation(0, 5),
        payload: encoded_call(vec![]),
        outcome: Some(PendingActionCallOutcome::Value(encode(&ReadyValue::Int(7)))),
    });

    // Neither the requester nor the provider may be touched: the settlement
    // comes from the recorded outcome, without re-executing the action.
    let requester = MockActionRequester::new();
    let provider = MockCompletionsProvider::new();

    let (_handler, mut poller) =
        super::new(requester, provider, Arc::clone(&backend), RmpCodec, vm_id);

    let waiting = nev![PromiseStateId(5)];
    let settlements = poller
        .poll::<super::Ack<FakeBackend>>(&waiting)
        .await
        .expect("poll");
    assert_eq!(settlements.len().get(), 1);

    let settlement = settlements.into_iter().next().expect("one settlement");
    assert_eq!(settlement.promise_state_id, PromiseStateId(5));
    assert!(matches!(
        settlement.resolution,
        PromiseResolution::Resolved(ReadyValue::Int(7))
    ));
    settlement.ack.acknowledge_promise_settlement().await;

    assert_eq!(backend.records(), vec![]);
}

#[tokio::test]
async fn first_poll_settles_recorded_exceptions_as_rejections() {
    let vm_id = InstanceId::new_uuid_v4();
    let backend = Arc::new(FakeBackend::new(vm_id));
    backend.seed_record(PendingActionCall {
        correlation: correlation(0, 5),
        payload: encoded_call(vec![]),
        outcome: Some(PendingActionCallOutcome::Exception(encode(
            &test_exception(),
        ))),
    });

    let requester = MockActionRequester::new();
    let provider = MockCompletionsProvider::new();

    let (_handler, mut poller) =
        super::new(requester, provider, Arc::clone(&backend), RmpCodec, vm_id);

    let waiting = nev![PromiseStateId(5)];
    let settlements = poller
        .poll::<super::Ack<FakeBackend>>(&waiting)
        .await
        .expect("poll");

    let settlement = settlements.into_iter().next().expect("one settlement");
    assert_eq!(settlement.promise_state_id, PromiseStateId(5));
    match settlement.resolution {
        PromiseResolution::Rejected(exception) => assert_eq!(exception, test_exception()),
        PromiseResolution::Resolved(value) => panic!("expected a rejection, got {value:?}"),
    }
}

#[tokio::test]
async fn first_poll_does_not_redispatch_calls_of_this_session() {
    let vm_id = InstanceId::new_uuid_v4();
    let backend = Arc::new(FakeBackend::new(vm_id));

    let mut requester = MockActionRequester::new();
    // Exactly one dispatch: the handler's. Reconciliation must not
    // re-dispatch a call this session already sent.
    requester
        .expect_request_action_call()
        .times(1)
        .returning(|_| Ok(()));

    let mut provider = MockCompletionsProvider::new();
    provider
        .expect_wait_for_completions()
        .times(1)
        .returning(|| Box::pin(std::future::ready(Ok(nev![completion(0, 5)]))));

    let (handler, mut poller) =
        super::new(requester, provider, Arc::clone(&backend), RmpCodec, vm_id);

    handler
        .request(
            EffectNumber(0),
            PromiseStateId(5),
            test_action_ref(),
            vec![],
        )
        .await
        .expect("request");

    let waiting = nev![PromiseStateId(5)];
    let settlements = poller
        .poll::<super::Ack<FakeBackend>>(&waiting)
        .await
        .expect("poll");
    assert_eq!(settlements.len().get(), 1);
}

#[tokio::test]
async fn poll_drops_completions_for_promises_that_are_not_waiting() {
    let vm_id = InstanceId::new_uuid_v4();
    let backend = Arc::new(FakeBackend::new(vm_id));

    let requester = MockActionRequester::new();

    let mut sequence = mockall::Sequence::new();
    let mut provider = MockCompletionsProvider::new();
    // The whole first batch is duplicates — poll must keep waiting rather
    // than return an empty settlement list.
    provider
        .expect_wait_for_completions()
        .times(1)
        .in_sequence(&mut sequence)
        .returning(|| Box::pin(std::future::ready(Ok(nev![completion(0, 9)]))));
    provider
        .expect_wait_for_completions()
        .times(1)
        .in_sequence(&mut sequence)
        .returning(|| {
            Box::pin(std::future::ready(Ok(nev![
                completion(0, 9),
                completion(1, 5)
            ])))
        });

    let (_handler, mut poller) =
        super::new(requester, provider, Arc::clone(&backend), RmpCodec, vm_id);

    let waiting = nev![PromiseStateId(5)];
    let settlements = poller
        .poll::<super::Ack<FakeBackend>>(&waiting)
        .await
        .expect("poll");
    assert_eq!(settlements.len().get(), 1);
    assert_eq!(settlements[0].promise_state_id, PromiseStateId(5));
}

// ------------------------------------------------------------------
// PersistingCompletionsProvider tests
// ------------------------------------------------------------------

#[tokio::test]
async fn persisting_provider_records_outcomes_before_delivering() {
    use waymark_action_runtime_core::ActionCallCompletionsProvider as _;

    let vm_id = InstanceId::new_uuid_v4();
    let backend = Arc::new(FakeBackend::new(vm_id));
    backend.seed(correlation(0, 5), encoded_call(vec![]));
    backend.seed(correlation(1, 7), encoded_call(vec![]));

    let mut inner = MockVmScopedCompletionsProvider::new();
    inner.expect_wait_for_completions().times(1).returning({
        move || {
            Box::pin(std::future::ready(Ok(nev![
                vm_scoped_completion(vm_id, 5, ActionCallOutcome::Value(ReadyValue::Int(7))),
                vm_scoped_completion(vm_id, 7, ActionCallOutcome::Exception(test_exception())),
            ])))
        }
    });

    let mut provider =
        super::PersistingCompletionsProvider::new(inner, Arc::clone(&backend), RmpCodec);

    let batch = provider
        .wait_for_completions()
        .await
        .expect("wait for completions");

    // The batch is delivered unchanged...
    assert_eq!(batch.len().get(), 2);

    // ...and the outcomes were recorded onto the records first.
    assert_eq!(
        backend.records(),
        vec![
            PendingActionCall {
                correlation: correlation(0, 5),
                payload: encoded_call(vec![]),
                outcome: Some(PendingActionCallOutcome::Value(encode(&ReadyValue::Int(7)))),
            },
            PendingActionCall {
                correlation: correlation(1, 7),
                payload: encoded_call(vec![]),
                outcome: Some(PendingActionCallOutcome::Exception(encode(
                    &test_exception()
                ))),
            },
        ]
    );
}

#[tokio::test]
async fn persisting_provider_delivers_even_without_a_record() {
    use waymark_action_runtime_core::ActionCallCompletionsProvider as _;

    let vm_id = InstanceId::new_uuid_v4();
    let backend = Arc::new(FakeBackend::new(vm_id));

    let mut inner = MockVmScopedCompletionsProvider::new();
    inner.expect_wait_for_completions().times(1).returning({
        move || {
            Box::pin(std::future::ready(Ok(nev![vm_scoped_completion(
                vm_id,
                5,
                ActionCallOutcome::Value(ReadyValue::Int(7))
            )])))
        }
    });

    let mut provider =
        super::PersistingCompletionsProvider::new(inner, Arc::clone(&backend), RmpCodec);

    // No record awaits an outcome (e.g. the call already settled) — the
    // completion is still delivered downstream.
    let batch = provider
        .wait_for_completions()
        .await
        .expect("wait for completions");
    assert_eq!(batch.len().get(), 1);
    assert_eq!(backend.records(), vec![]);
}
