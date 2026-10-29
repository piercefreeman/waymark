//! Shared test fixtures: a mock durable-requests backend upholding the
//! removal invariant, and a capturing requester.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use nonempty_collections::{NESlice, NEVec};
use waymark_action_effect_reconciler_backend::lock_action_call_requests::PerVmActionCallRequestLockingOutcome;
use waymark_action_effect_reconciler_backend::record_action_call_requests::RecordingSuccess;
use waymark_action_effect_reconciler_backend::renew_action_call_request_locks::{
    ActionCallRequestLockRenewal, ActionCallRequestLockRenewalStatus,
};
use waymark_action_effect_reconciler_backend::{
    ActionCallRequestKey, ActionCallRequestLock, ActionCallRequestLockId, ActionCallRequestRecord,
};
use waymark_action_runtime_core::ActionCallRequest;
use waymark_action_runtime_metadata::ActionCallCorrelation;

pub(crate) type TestVmId = u64;
pub(crate) type TestLockOwnerId = u32;
pub(crate) type TestKey = ActionCallRequestKey<TestVmId>;
pub(crate) type TestLock = ActionCallRequestLock<TestLockOwnerId, DateTime<Utc>>;

/// One stored request row.
#[derive(Debug, Clone)]
pub(crate) struct MockRow {
    pub effect_number: waymark_vm_runtime_effect::EffectNumber,
    pub request: Vec<u8>,
    pub locked_by: Option<TestLockOwnerId>,
    pub lock_expires_at: Option<DateTime<Utc>>,
    pub lock_nonce: Option<u64>,
}

/// An in-memory durable-requests backend.
///
/// Rows are retained for assertions (never merely logged), and the
/// removal invariant is exercised by tests deleting rows directly —
/// standing in for the schema trigger.
#[derive(Default)]
pub(crate) struct MockBackend {
    pub rows: Mutex<HashMap<TestKey, MockRow>>,
    /// Fail this many record calls with a retryable error first.
    pub fail_records: Mutex<u32>,
    /// Insert the rows, then fail this many record calls with a retryable
    /// error: the statement committed but reported failure.
    pub fail_records_after_insert: Mutex<u32>,
    /// Fail every lock call when set.
    pub fail_locks: Mutex<bool>,
    pub lock_calls: Mutex<u32>,
    /// Fail every unlock call when set.
    pub fail_unlocks: Mutex<bool>,
    pub unlock_calls: Mutex<u32>,
    /// Fail every renew call when set.
    pub fail_renewals: Mutex<bool>,
    /// Report every present row as
    /// [`ActionCallRequestLockRenewalStatus::Unconfirmed`] when set.
    pub report_unconfirmed_renewals: Mutex<bool>,
    /// Hold the next renew call that does not fail for this long before it
    /// answers; taken by that call, so only one call is slow.
    pub renew_delay: Mutex<Option<Duration>>,
    pub renew_calls: Mutex<u32>,
}

impl waymark_action_effect_reconciler_backend::HasVmId for MockBackend {
    type VmId = TestVmId;
}

impl waymark_action_effect_reconciler_backend::HasLockOwnerId for MockBackend {
    type LockOwnerId = TestLockOwnerId;
}

impl waymark_action_effect_reconciler_backend::HasTimestamp for MockBackend {
    type Timestamp = DateTime<Utc>;
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum MockRecordError {
    #[error("mock internal failure")]
    Internal,

    #[error("mock divergent payloads: {0:?}")]
    DivergentPayload(NEVec<TestKey>),
}

impl waymark_action_effect_reconciler_backend::record_action_call_requests::Error
    for MockRecordError
{
    fn kind(
        &self,
    ) -> waymark_action_effect_reconciler_backend::record_action_call_requests::ErrorKind {
        use waymark_action_effect_reconciler_backend::record_action_call_requests::ErrorKind;
        match self {
            Self::Internal => ErrorKind::Internal,
            Self::DivergentPayload(_) => ErrorKind::DivergentPayload,
        }
    }
}

impl waymark_action_effect_reconciler_backend::RecordActionCallRequests for MockBackend {
    type Error = MockRecordError;

    async fn record_action_call_requests(
        &self,
        // The mock stores `lock.expires_at` verbatim rather than translating
        // it onto a store clock, so the baseline instant goes unused.
        _now: chrono::DateTime<chrono::Utc>,
        lock: TestLock,
        records: NESlice<'_, ActionCallRequestRecord<TestVmId>>,
    ) -> Result<RecordingSuccess<TestVmId>, MockRecordError> {
        {
            let mut fail_records = self.fail_records.lock().unwrap();
            if *fail_records > 0 {
                *fail_records -= 1;
                return Err(MockRecordError::Internal);
            }
        }

        let mut rows = self.rows.lock().unwrap();
        let mut already_recorded = Vec::new();
        let mut divergent = Vec::new();
        for record in records.iter() {
            let key = ActionCallRequestKey {
                vm_id: record.vm_id,
                promise_state_id: record.promise_state_id,
            };
            match rows.get(&key) {
                None => {
                    rows.insert(
                        key,
                        MockRow {
                            effect_number: record.effect_number,
                            request: record.request.clone(),
                            locked_by: Some(lock.owner),
                            lock_expires_at: Some(lock.expires_at),
                            lock_nonce: Some(lock.nonce),
                        },
                    );
                }
                Some(existing) => {
                    if existing.effect_number == record.effect_number
                        && existing.request == record.request
                    {
                        already_recorded.push(key);
                    } else {
                        divergent.push(key);
                    }
                }
            }
        }

        if let Some(keys) = NEVec::try_from_vec(divergent) {
            return Err(MockRecordError::DivergentPayload(keys));
        }

        {
            let mut fail_records_after_insert = self.fail_records_after_insert.lock().unwrap();
            if *fail_records_after_insert > 0 {
                *fail_records_after_insert -= 1;
                return Err(MockRecordError::Internal);
            }
        }

        Ok(match NEVec::try_from_vec(already_recorded) {
            Some(keys) => RecordingSuccess::SomeAlreadyRecorded(keys),
            None => RecordingSuccess::AllRecorded,
        })
    }
}

#[derive(Debug, thiserror::Error)]
#[error("mock lock failure")]
pub(crate) struct MockLockError;

impl waymark_action_effect_reconciler_backend::LockActionCallRequests for MockBackend {
    type Error = MockLockError;

    async fn lock_action_call_requests<'a>(
        &'a self,
        now: DateTime<Utc>,
        lock: TestLock,
        vm_ids: NESlice<'a, TestVmId>,
    ) -> Result<NEVec<PerVmActionCallRequestLockingOutcome<TestVmId>>, MockLockError> {
        *self.lock_calls.lock().unwrap() += 1;
        if *self.fail_locks.lock().unwrap() {
            return Err(MockLockError);
        }

        let mut rows = self.rows.lock().unwrap();
        let mut outcomes: Vec<PerVmActionCallRequestLockingOutcome<TestVmId>> = vm_ids
            .iter()
            .map(|vm_id| PerVmActionCallRequestLockingOutcome {
                vm_id: *vm_id,
                locked: Vec::new(),
                held_elsewhere: Vec::new(),
                held_by_this_owner: Vec::new(),
            })
            .collect();
        for outcome in &mut outcomes {
            for (key, row) in rows.iter_mut() {
                if key.vm_id != outcome.vm_id {
                    continue;
                }
                let eligible = match (row.locked_by, row.lock_expires_at) {
                    (None, _) => true,
                    (Some(_), Some(expires_at)) => expires_at <= now,
                    (Some(_), None) => unreachable!("lock owner and expiry are paired"),
                };
                if eligible {
                    row.locked_by = Some(lock.owner);
                    row.lock_expires_at = Some(lock.expires_at);
                    row.lock_nonce = Some(lock.nonce);
                    outcome.locked.push(ActionCallRequestRecord {
                        vm_id: key.vm_id,
                        promise_state_id: key.promise_state_id,
                        effect_number: row.effect_number,
                        request: row.request.clone(),
                    });
                } else if row.locked_by == Some(lock.owner) {
                    outcome.held_by_this_owner.push(ActionCallRequestLockId {
                        request: *key,
                        nonce: row
                            .lock_nonce
                            .expect("a locked row carries its lock's nonce"),
                    });
                } else {
                    outcome.held_elsewhere.push(*key);
                }
            }
        }

        Ok(NEVec::try_from_vec(outcomes).expect("one outcome per input vm; inputs are non-empty"))
    }
}

#[derive(Debug, thiserror::Error)]
#[error("mock unlock failure")]
pub(crate) struct MockUnlockError;

impl waymark_action_effect_reconciler_backend::UnlockActionCallRequests for MockBackend {
    type Error = MockUnlockError;

    async fn unlock_action_call_requests<'a>(
        &'a self,
        owner: &'a TestLockOwnerId,
        lock_ids: NESlice<'a, ActionCallRequestLockId<TestVmId>>,
    ) -> Result<(), MockUnlockError> {
        *self.unlock_calls.lock().unwrap() += 1;
        if *self.fail_unlocks.lock().unwrap() {
            return Err(MockUnlockError);
        }

        let mut rows = self.rows.lock().unwrap();
        for lock_id in lock_ids.iter() {
            let Some(row) = rows.get_mut(&lock_id.request) else {
                continue;
            };
            if row.locked_by == Some(*owner) && row.lock_nonce == Some(lock_id.nonce) {
                row.locked_by = None;
                row.lock_expires_at = None;
                row.lock_nonce = None;
            }
        }

        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("mock renew failure")]
pub(crate) struct MockRenewError;

impl waymark_action_effect_reconciler_backend::RenewActionCallRequestLocks for MockBackend {
    type Error = MockRenewError;

    async fn renew_action_call_request_locks(
        &self,
        // The mock stores `lock.expires_at` verbatim rather than translating
        // it onto a store clock, so the baseline instant goes unused.
        _now: DateTime<Utc>,
        owner: &TestLockOwnerId,
        expires_at: DateTime<Utc>,
        lock_ids: NESlice<'_, ActionCallRequestLockId<TestVmId>>,
    ) -> Result<NEVec<ActionCallRequestLockRenewal<TestVmId>>, Self::Error> {
        *self.renew_calls.lock().unwrap() += 1;
        if *self.fail_renewals.lock().unwrap() {
            return Err(MockRenewError);
        }
        // Taken out of its guard before the sleep: no std guard is held
        // across the await.
        let delay = self.renew_delay.lock().unwrap().take();
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }

        let report_unconfirmed = *self.report_unconfirmed_renewals.lock().unwrap();
        let mut rows = self.rows.lock().unwrap();
        let renewals = lock_ids
            .iter()
            .map(|lock_id| {
                let status = match rows.get_mut(&lock_id.request) {
                    None => ActionCallRequestLockRenewalStatus::Missing,
                    Some(_) if report_unconfirmed => {
                        ActionCallRequestLockRenewalStatus::Unconfirmed
                    }
                    Some(row) if row.locked_by == Some(*owner) => {
                        if row.lock_nonce == Some(lock_id.nonce) {
                            row.lock_expires_at = Some(expires_at);
                            ActionCallRequestLockRenewalStatus::Renewed
                        } else {
                            ActionCallRequestLockRenewalStatus::Relocked
                        }
                    }
                    Some(_) => ActionCallRequestLockRenewalStatus::HeldElsewhere,
                };
                ActionCallRequestLockRenewal {
                    lock_id: *lock_id,
                    status,
                }
            })
            .collect();

        Ok(NEVec::try_from_vec(renewals).expect("non-empty keys yield non-empty renewals"))
    }
}

/// A requester that captures every delivered request for assertions.
#[derive(Clone, Default)]
pub(crate) struct CapturingRequester {
    pub requests: Arc<Mutex<Vec<ActionCallRequest<i64, ActionCallCorrelation>>>>,
}

impl waymark_action_runtime_core::ActionCallRequester for CapturingRequester {
    type Error = std::convert::Infallible;
    type Argument = i64;
    type Metadata = ActionCallCorrelation;

    async fn request_action_call(
        &self,
        request: ActionCallRequest<i64, ActionCallCorrelation>,
    ) -> Result<(), Self::Error> {
        self.requests.lock().unwrap().push(request);
        Ok(())
    }
}

/// A requester that rejects every delivery.
#[derive(Clone, Default)]
pub(crate) struct FailingRequester;

#[derive(Debug, thiserror::Error)]
#[error("mock delivery failure")]
pub(crate) struct MockDeliveryError;

impl waymark_action_runtime_core::ActionCallRequester for FailingRequester {
    type Error = MockDeliveryError;
    type Argument = i64;
    type Metadata = ActionCallCorrelation;

    async fn request_action_call(
        &self,
        _request: ActionCallRequest<i64, ActionCallCorrelation>,
    ) -> Result<(), Self::Error> {
        Err(MockDeliveryError)
    }
}

/// A test action reference.
pub(crate) fn action_ref(name: &str) -> waymark_action_core::ActionRef {
    waymark_action_core::ActionRef {
        action_name: name.to_owned(),
        module_name: None,
        call_args: vec!["value".to_owned()],
    }
}

/// The lock time-to-live the test batchers take locks with.
pub(crate) const LOCK_TIME_TO_LIVE: Duration = Duration::from_secs(60);

/// The lock owner the test batchers take locks as.
pub(crate) const LOCK_OWNER: TestLockOwnerId = 7;

/// Whether the tracker tracks a lock on the request, whichever lock.
pub(crate) fn tracks_request(
    tracker: &crate::action_call_request::locks::Tracker<TestVmId>,
    key: &TestKey,
) -> bool {
    tracker
        .tracked_lock_ids()
        .iter()
        .any(|lock_id| lock_id.request == *key)
}

/// The id of a lock on the request of `key`, under `nonce`.
pub(crate) fn lock_id(key: TestKey, nonce: u64) -> ActionCallRequestLockId<TestVmId> {
    ActionCallRequestLockId {
        request: key,
        nonce,
    }
}

/// A tracker of action-call request locks, shared the way the bringup
/// shares it.
pub(crate) fn shared_tracker() -> Arc<Mutex<crate::action_call_request::locks::Tracker<TestVmId>>> {
    Arc::new(Mutex::new(crate::action_call_request::locks::Tracker::new()))
}

/// A locker over the mock backend, with its batcher spawned onto the test
/// runtime.  Small delay window so single submissions flush promptly.
pub(crate) fn spawn_locker(
    backend: &Arc<MockBackend>,
    tracker: &Arc<Mutex<crate::action_call_request::locks::Tracker<TestVmId>>>,
) -> crate::action_call_request::lock_batcher::PerVmLockerHandle<TestVmId> {
    let (locker, batcher) = crate::action_call_request::lock_batcher::run(
        Arc::clone(backend),
        LOCK_OWNER,
        LOCK_TIME_TO_LIVE.try_into().unwrap(),
        Arc::clone(tracker),
        waymark_batcher::Policy {
            max_batch: 16.try_into().unwrap(),
            max_delay: waymark_nonzero_duration::NonZeroDuration::from_millis(1).unwrap(),
        },
        std::future::pending(),
    );
    tokio::spawn(batcher);
    locker
}

/// A recorder over the mock backend, with its batcher spawned onto the
/// test runtime.  Small delay window so single submissions flush promptly.
pub(crate) fn spawn_recorder(
    backend: &Arc<MockBackend>,
    tracker: &Arc<Mutex<crate::action_call_request::locks::Tracker<TestVmId>>>,
) -> crate::action_call_request::batcher::RecorderHandle<TestVmId> {
    let (recorder, batcher) = crate::action_call_request::batcher::run(
        Arc::clone(backend),
        LOCK_OWNER,
        LOCK_TIME_TO_LIVE.try_into().unwrap(),
        Arc::clone(tracker),
        waymark_batcher::Policy {
            max_batch: 16.try_into().unwrap(),
            max_delay: waymark_nonzero_duration::NonZeroDuration::from_millis(1).unwrap(),
        },
        std::future::pending(),
    );
    tokio::spawn(batcher);
    recorder
}

/// An rmp-encoded request payload for the `greet` action.
pub(crate) fn encode_payload(arguments: Vec<i64>) -> Vec<u8> {
    let payload = crate::action_call_request::payload::Payload {
        action_ref: action_ref("greet"),
        arguments,
    };
    let mut blob = Vec::new();
    waymark_vm_codec_core::SerializerProvider::with_serializer(
        &waymark_vm_codec_rmp::RmpCodec,
        &mut blob,
        |serializer| serde::Serialize::serialize(&payload, serializer),
    )
    .expect("encode payload");
    blob
}

/// Seed a stored request row with an rmp-encoded `greet` payload.
pub(crate) fn seed_row(
    backend: &MockBackend,
    vm_id: TestVmId,
    promise: usize,
    effect: usize,
    arguments: Vec<i64>,
    locked_by: Option<TestLockOwnerId>,
    expires_in: chrono::Duration,
) {
    backend.rows.lock().unwrap().insert(
        TestKey {
            vm_id,
            promise_state_id: waymark_vm_runtime_promise_core::PromiseStateId(promise),
        },
        MockRow {
            effect_number: waymark_vm_runtime_effect::EffectNumber(effect),
            request: encode_payload(arguments),
            locked_by,
            lock_expires_at: locked_by.map(|_| Utc::now() + expires_in),
            lock_nonce: locked_by.map(|_| 1),
        },
    );
}
