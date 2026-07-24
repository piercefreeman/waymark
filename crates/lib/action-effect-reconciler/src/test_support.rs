//! Shared test fixtures: a mock durable-requests backend upholding the
//! removal invariant, and a capturing requester.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use nonempty_collections::{NESlice, NEVec};
use waymark_action_effect_reconciler_backend::lock_vm_action_call_requests::VmLockOutcome;
use waymark_action_effect_reconciler_backend::record_action_call_requests::RecordingSuccess;
use waymark_action_effect_reconciler_backend::renew_action_call_request_locks::{
    RenewalStatus, RequestLockRenewal,
};
use waymark_action_effect_reconciler_backend::{
    ActionCallRequestKey, ActionCallRequestRecord, RequestLock,
};
use waymark_action_runtime_core::ActionCallRequest;
use waymark_action_runtime_metadata::ActionCallCorrelation;

pub(crate) type TestVmId = u64;
pub(crate) type TestLockOwnerId = u32;
pub(crate) type TestKey = ActionCallRequestKey<TestVmId>;
pub(crate) type TestLock = RequestLock<TestLockOwnerId, DateTime<Utc>>;

/// One stored request row.
#[derive(Debug, Clone)]
pub(crate) struct MockRow {
    pub effect_number: waymark_vm_runtime_effect::EffectNumber,
    pub request: Vec<u8>,
    pub locked_by: Option<TestLockOwnerId>,
    pub lock_expires_at: Option<DateTime<Utc>>,
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
    /// Fail every lock call when set.
    pub fail_locks: Mutex<bool>,
    /// Fail every renew call when set.
    pub fail_renewals: Mutex<bool>,
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
        Ok(match NEVec::try_from_vec(already_recorded) {
            Some(keys) => RecordingSuccess::SomeAlreadyRecorded(keys),
            None => RecordingSuccess::AllRecorded,
        })
    }
}

#[derive(Debug, thiserror::Error)]
#[error("mock lock failure")]
pub(crate) struct MockLockError;

impl waymark_action_effect_reconciler_backend::LockVmActionCallRequests for MockBackend {
    type Error = MockLockError;

    async fn lock_vm_action_call_requests(
        &self,
        now: DateTime<Utc>,
        lock: TestLock,
        vm_id: &TestVmId,
    ) -> Result<VmLockOutcome<TestVmId>, MockLockError> {
        if *self.fail_locks.lock().unwrap() {
            return Err(MockLockError);
        }

        let mut rows = self.rows.lock().unwrap();
        let mut locked = Vec::new();
        let mut held_elsewhere = Vec::new();
        for (key, row) in rows.iter_mut() {
            if key.vm_id != *vm_id {
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
                locked.push(ActionCallRequestRecord {
                    vm_id: key.vm_id,
                    promise_state_id: key.promise_state_id,
                    effect_number: row.effect_number,
                    request: row.request.clone(),
                });
            } else {
                held_elsewhere.push(*key);
            }
        }

        Ok(VmLockOutcome {
            locked,
            held_elsewhere,
        })
    }
}

#[derive(Debug, thiserror::Error)]
#[error("mock renew failure")]
pub(crate) struct MockRenewError;

impl waymark_action_effect_reconciler_backend::RenewActionCallRequestLocks for MockBackend {
    type Error = MockRenewError;

    async fn renew_action_call_request_locks(
        &self,
        lock: TestLock,
        keys: NESlice<'_, TestKey>,
    ) -> Result<NEVec<RequestLockRenewal<TestVmId>>, Self::Error> {
        *self.renew_calls.lock().unwrap() += 1;
        if *self.fail_renewals.lock().unwrap() {
            return Err(MockRenewError);
        }

        let mut rows = self.rows.lock().unwrap();
        let renewals = keys
            .iter()
            .map(|key| {
                let status = match rows.get_mut(key) {
                    None => RenewalStatus::Missing,
                    Some(row) if row.locked_by == Some(lock.owner) => {
                        row.lock_expires_at = Some(lock.expires_at);
                        RenewalStatus::Renewed
                    }
                    Some(_) => RenewalStatus::HeldElsewhere,
                };
                RequestLockRenewal { key: *key, status }
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
        runtime: waymark_action_core::ActionRuntime::Python,
        action_name: name.to_owned(),
        module_name: None,
        call_args: vec!["value".to_owned()],
        timeout_seconds: 300,
        max_retries: 0,
        exception_types: Vec::new(),
    }
}
