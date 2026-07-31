//! Shared test support: a scriptable mock backend and key helpers.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use waymark_ids::InstanceId;
use waymark_sleep_reconciler_backend::{SleepKey, SleepRecord, record_sleeps};
use waymark_vm_runtime_promise_core::PromiseStateId;

#[derive(Debug, thiserror::Error)]
#[error("mock record error ({kind:?})")]
pub(crate) struct MockRecordError {
    pub(crate) kind: record_sleeps::ErrorKind,
}

impl record_sleeps::Error for MockRecordError {
    fn kind(&self) -> record_sleeps::ErrorKind {
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
    /// Scripted responses for `record_sleeps`; when empty, records are
    /// accepted.
    pub(crate) record_responses: Mutex<VecDeque<Result<(), MockRecordError>>>,
    /// Every record batch accepted (i.e. not scripted as an error).
    pub(crate) recorded: Mutex<Vec<SleepRecord<InstanceId, chrono::DateTime<chrono::Utc>>>>,
    /// Scripted `poll_due_sleeps` result batches; when exhausted, a poll
    /// pends forever (so a tight loop cannot spin on an empty mock).
    pub(crate) poll_batches: Mutex<VecDeque<Vec<SleepKey<InstanceId>>>>,
    pub(crate) poll_calls: AtomicUsize,
    /// Scripted failures for `ack_sleeps`; when empty, acks succeed.
    pub(crate) ack_responses: Mutex<VecDeque<Result<(), MockAckError>>>,
    pub(crate) acked: Mutex<Vec<SleepKey<InstanceId>>>,
}

impl waymark_sleep_reconciler_backend::HasVmId for MockBackend {
    type VmId = InstanceId;
}

impl waymark_sleep_reconciler_backend::HasTimestamp for MockBackend {
    type Timestamp = chrono::DateTime<chrono::Utc>;
}

impl waymark_sleep_reconciler_backend::RecordSleeps for MockBackend {
    type Error = MockRecordError;

    async fn record_sleeps(
        &self,
        records: nonempty_collections::NESlice<
            '_,
            SleepRecord<InstanceId, chrono::DateTime<chrono::Utc>>,
        >,
    ) -> Result<(), Self::Error> {
        if let Some(Err(error)) = self.inner.record_responses.lock().unwrap().pop_front() {
            return Err(error);
        }
        self.inner
            .recorded
            .lock()
            .unwrap()
            .extend(records.iter().cloned());
        Ok(())
    }
}

impl waymark_sleep_reconciler_backend::PollDueSleeps for MockBackend {
    type Error = MockPollError;

    async fn poll_due_sleeps(
        &self,
        _now: chrono::DateTime<chrono::Utc>,
        _demand: nonempty_collections::NESlice<'_, SleepKey<InstanceId>>,
    ) -> Result<Vec<SleepKey<InstanceId>>, Self::Error> {
        self.inner.poll_calls.fetch_add(1, Ordering::SeqCst);
        let batch = self.inner.poll_batches.lock().unwrap().pop_front();
        match batch {
            Some(batch) => Ok(batch),
            // Exhausted: block forever so the loop parks on the
            // "database" instead of spinning on an empty mock.
            None => std::future::pending().await,
        }
    }
}

impl waymark_sleep_reconciler_backend::AckSleeps for MockBackend {
    type Error = MockAckError;

    async fn ack_sleeps(
        &self,
        keys: nonempty_collections::NESlice<'_, SleepKey<InstanceId>>,
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

pub(crate) fn key(vm_id: InstanceId, promise: usize) -> SleepKey<InstanceId> {
    SleepKey {
        vm_id,
        promise_state_id: PromiseStateId(promise),
    }
}
