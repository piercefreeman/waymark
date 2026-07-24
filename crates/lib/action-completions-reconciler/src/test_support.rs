//! Shared test support: a scriptable mock backend and record helpers.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use waymark_action_completions_reconciler_backend::record_completions::RecordingSuccess;
use waymark_action_completions_reconciler_backend::{
    CompletionKey, CompletionRecord, record_completions,
};
use waymark_action_runtime_core::ActionCallOutcome;
use waymark_ids::InstanceId;
use waymark_vm_codec_rmp::RmpCodec;
use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;
use waymark_vm_value::ReadyValue;

#[derive(Debug, thiserror::Error)]
#[error("mock record error ({kind:?})")]
pub(crate) struct MockRecordError {
    pub(crate) kind: record_completions::ErrorKind,
}

impl record_completions::Error for MockRecordError {
    fn kind(&self) -> record_completions::ErrorKind {
        self.kind
    }
}

#[derive(Debug, thiserror::Error)]
#[error("mock poll error")]
pub(crate) struct MockPollError;

#[derive(Debug, thiserror::Error)]
#[error("mock ack error")]
pub(crate) struct MockAckError;

/// Scriptable in-memory backend shared by all loop tests.
///
/// Cheap to clone (`Arc`-shared state) so a test can hand one clone to
/// the loop under test and keep another for assertions.
#[derive(Default, Clone)]
pub(crate) struct MockBackend {
    pub(crate) inner: std::sync::Arc<MockState>,
}

#[derive(Default)]
pub(crate) struct MockState {
    /// Scripted responses for `record_completions`; when empty, records
    /// are accepted (`AllRecorded`).
    pub(crate) record_responses:
        Mutex<VecDeque<Result<RecordingSuccess<InstanceId>, MockRecordError>>>,
    /// Every record batch accepted (i.e. not scripted as an error).
    pub(crate) recorded: Mutex<Vec<CompletionRecord<InstanceId>>>,
    /// Scripted `poll_completions` result batches; when exhausted, a poll
    /// pends forever (so a tight loop cannot spin on an empty mock).
    pub(crate) poll_batches: Mutex<VecDeque<Vec<CompletionRecord<InstanceId>>>>,
    pub(crate) poll_calls: AtomicUsize,
    /// Scripted failures for `ack_completions`; when empty, acks succeed.
    pub(crate) ack_responses: Mutex<VecDeque<Result<(), MockAckError>>>,
    pub(crate) acked: Mutex<Vec<CompletionKey<InstanceId>>>,
}

impl waymark_action_completions_reconciler_backend::HasVmId for MockBackend {
    type VmId = InstanceId;
}

impl waymark_action_completions_reconciler_backend::RecordCompletions for MockBackend {
    type Error = MockRecordError;

    async fn record_completions(
        &self,
        records: nonempty_collections::NESlice<'_, CompletionRecord<InstanceId>>,
    ) -> Result<RecordingSuccess<InstanceId>, Self::Error> {
        let scripted = self.inner.record_responses.lock().unwrap().pop_front();
        match scripted {
            Some(Err(error)) => Err(error),
            Some(Ok(success)) => Ok(success),
            None => {
                self.inner
                    .recorded
                    .lock()
                    .unwrap()
                    .extend(records.iter().cloned());
                Ok(RecordingSuccess::AllRecorded)
            }
        }
    }
}

impl waymark_action_completions_reconciler_backend::PollCompletions for MockBackend {
    type Error = MockPollError;

    async fn poll_completions(
        &self,
        _demand: nonempty_collections::NESlice<'_, CompletionKey<InstanceId>>,
    ) -> Result<Vec<CompletionRecord<InstanceId>>, Self::Error> {
        self.inner.poll_calls.fetch_add(1, Ordering::SeqCst);
        let batch = self.inner.poll_batches.lock().unwrap().pop_front();
        match batch {
            Some(batch) => Ok(batch),
            // Exhausted: block forever so the tight loop parks on the
            // "database" instead of spinning on an empty mock.
            None => std::future::pending().await,
        }
    }
}

impl waymark_action_completions_reconciler_backend::AckCompletions for MockBackend {
    type Error = MockAckError;

    async fn ack_completions(
        &self,
        keys: nonempty_collections::NESlice<'_, CompletionKey<InstanceId>>,
    ) -> Result<(), Self::Error> {
        if let Some(Err(error)) = self.inner.ack_responses.lock().unwrap().pop_front() {
            return Err(error);
        }
        self.inner
            .acked
            .lock()
            .unwrap()
            .extend(keys.iter().copied());
        Ok(())
    }
}

fn encoded_outcome(value: &str) -> Vec<u8> {
    let outcome = ActionCallOutcome::Value(ReadyValue::String(value.to_owned()));
    let mut blob = Vec::new();
    waymark_vm_codec_core::SerializerProvider::with_serializer(&RmpCodec, &mut blob, |ser| {
        serde::Serialize::serialize(&outcome, ser)
    })
    .expect("encoding a test outcome succeeds");
    blob
}

pub(crate) fn record(
    vm_id: InstanceId,
    promise: usize,
    effect: usize,
    value: &str,
) -> CompletionRecord<InstanceId> {
    CompletionRecord {
        vm_id,
        promise_state_id: PromiseStateId(promise),
        effect_number: EffectNumber(effect),
        outcome: encoded_outcome(value),
    }
}

pub(crate) fn key(vm_id: InstanceId, promise: usize) -> CompletionKey<InstanceId> {
    CompletionKey {
        vm_id,
        promise_state_id: PromiseStateId(promise),
    }
}
