use std::collections::VecDeque;
use std::sync::Arc;

use nonempty_collections::NEVec;
use waymark_action_completions_reconciler_backend::record_completions;
use waymark_action_completions_reconciler_backend::record_completions::RecordingSuccess;
use waymark_action_runtime_core::{ActionCallCompletion, ActionCallOutcome};
use waymark_action_runtime_metadata::{ActionCallCorrelation, WithVmId};
use waymark_ids::InstanceId;
use waymark_vm_codec_rmp::RmpCodec;
use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;
use waymark_vm_value::ReadyValue;

use super::{Error, Params};
use crate::test_support::{MockBackend, MockRecordError, key};

type TestMetadata = WithVmId<InstanceId, ActionCallCorrelation>;
type TestCompletion = ActionCallCompletion<ReadyValue, TestMetadata>;

#[derive(Debug, thiserror::Error)]
#[error("fake provider exhausted")]
struct FakeProviderError;

/// Completion source yielding scripted batches, then failing.
struct FakeProvider {
    batches: VecDeque<NEVec<TestCompletion>>,
}

impl waymark_action_runtime_core::ActionCallCompletionsProvider for FakeProvider {
    type Value = ReadyValue;
    type Error = FakeProviderError;
    type Metadata = TestMetadata;

    async fn wait_for_completions(&mut self) -> Result<NEVec<TestCompletion>, Self::Error> {
        self.batches.pop_front().ok_or(FakeProviderError)
    }
}

fn completion(vm_id: InstanceId, promise: usize, effect: usize, value: &str) -> TestCompletion {
    ActionCallCompletion {
        metadata: WithVmId {
            vm_id,
            inner: ActionCallCorrelation {
                effect_number: EffectNumber(effect),
                promise_state_id: PromiseStateId(promise),
            },
        },
        outcome: ActionCallOutcome::Value(ReadyValue::String(value.to_owned())),
    }
}

fn params(
    batches: impl IntoIterator<Item = NEVec<TestCompletion>>,
    backend: &MockBackend,
) -> Params<FakeProvider, MockBackend, RmpCodec> {
    Params {
        provider: FakeProvider {
            batches: batches.into_iter().collect(),
        },
        backend: Arc::new(backend.clone()),
        codec: RmpCodec,
    }
}

#[tokio::test]
async fn records_provider_completions_until_the_provider_fails() {
    let vm_id = InstanceId::new_uuid_v4();
    let backend = MockBackend::default();
    let params = params([NEVec::new(completion(vm_id, 3, 7, "done"))], &backend);

    // The provider fails once its script is exhausted; the writer treats
    // that as critical.
    let error = super::run(params)
        .await
        .expect_err("provider exhaustion ends the writer");
    assert!(matches!(error, Error::Completions(_)));

    let recorded = backend.inner.recorded.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].vm_id, vm_id);
    assert_eq!(recorded[0].promise_state_id, PromiseStateId(3));
    assert_eq!(recorded[0].effect_number, EffectNumber(7));

    // The stored blob round-trips back to the outcome.
    let outcome: ActionCallOutcome<ReadyValue> =
        waymark_vm_codec_core::DeserializerProvider::with_deserializer(
            &RmpCodec,
            &recorded[0].outcome,
            |de| serde::Deserialize::deserialize(de),
        )
        .expect("stored outcome decodes");
    assert!(matches!(
        outcome,
        ActionCallOutcome::Value(ReadyValue::String(ref s)) if s == "done"
    ));
}

#[tokio::test(start_paused = true)]
async fn retries_internal_failures_until_recorded() {
    let vm_id = InstanceId::new_uuid_v4();
    let backend = MockBackend::default();
    backend.inner.record_responses.lock().unwrap().extend([
        Err(MockRecordError {
            kind: record_completions::ErrorKind::Internal,
        }),
        Err(MockRecordError {
            kind: record_completions::ErrorKind::Internal,
        }),
    ]);

    let params = params([NEVec::new(completion(vm_id, 1, 1, "done"))], &backend);
    let error = super::run(params)
        .await
        .expect_err("provider exhaustion ends the writer");
    assert!(matches!(error, Error::Completions(_)));
    assert_eq!(backend.inner.recorded.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn conflicting_outcomes_are_logged_and_skipped() {
    let vm_id = InstanceId::new_uuid_v4();
    let backend = MockBackend::default();
    backend.inner.record_responses.lock().unwrap().push_back(Ok(
        RecordingSuccess::SomeConflictingOutcomes(NEVec::new(key(vm_id, 1))),
    ));

    let params = params([NEVec::new(completion(vm_id, 1, 1, "done"))], &backend);
    // The batch is consumed without a divergence error; the writer only
    // stops once the provider is exhausted.
    let error = super::run(params)
        .await
        .expect_err("provider exhaustion ends the writer");
    assert!(matches!(error, Error::Completions(_)));
}

#[tokio::test]
async fn divergent_effect_number_is_critical() {
    let vm_id = InstanceId::new_uuid_v4();
    let backend = MockBackend::default();
    backend
        .inner
        .record_responses
        .lock()
        .unwrap()
        .push_back(Err(MockRecordError {
            kind: record_completions::ErrorKind::DivergentEffectNumber,
        }));

    let params = params([NEVec::new(completion(vm_id, 1, 1, "done"))], &backend);
    let error = super::run(params)
        .await
        .expect_err("divergence is critical");
    assert!(matches!(error, Error::DivergentEffectNumber(_)));
}
