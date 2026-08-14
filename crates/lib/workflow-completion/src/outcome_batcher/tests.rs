//! Flush-behavior tests for the shared outcome batcher: coalescing,
//! per-row conflict isolation, duplicate dedupe, and triaged, bounded
//! retry of recording failures.

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;

use nonempty_collections::{NESlice, NEVec};
use waymark_nonzero_duration::NonZeroDuration;
use waymark_workflow_completion_backend::record_outcomes::ErrorKind;
use waymark_workflow_completion_backend::{Outcome, RecordOutcomesItem, RecordingSuccess};

use crate::outcome_batcher::{
    OutcomeRecorderHandle, RETRY_MAX_ATTEMPTS, RecordError, outcome_batcher,
};

/// In-memory backend replicating the first-write-wins per-row semantics.
struct MockBackend {
    rows: Mutex<HashMap<u64, Outcome>>,
    fail_records: Mutex<usize>,
    fail_kind: Mutex<ErrorKind>,
    /// The vm_ids of every batch statement received, in call order.
    statements: Mutex<Vec<Vec<u64>>>,
}

impl Default for MockBackend {
    fn default() -> Self {
        Self {
            rows: Mutex::default(),
            fail_records: Mutex::default(),
            fail_kind: Mutex::new(ErrorKind::Internal),
            statements: Mutex::default(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("mock failure")]
struct MockError(ErrorKind);

impl waymark_workflow_completion_backend::record_outcomes::Error for MockError {
    fn kind(&self) -> ErrorKind {
        self.0
    }
}

impl waymark_workflow_completion_backend::HasVmId for MockBackend {
    type VmId = u64;
}

impl waymark_workflow_completion_backend::RecordOutcomes for MockBackend {
    type Error = MockError;

    async fn record_outcomes<'a>(
        &'a self,
        outcomes: NESlice<'a, RecordOutcomesItem<'a, u64>>,
    ) -> Result<RecordingSuccess<u64>, MockError> {
        self.statements
            .lock()
            .unwrap()
            .push(outcomes.iter().map(|item| *item.vm_id).collect());
        {
            let mut fail_records = self.fail_records.lock().unwrap();
            if *fail_records > 0 {
                *fail_records -= 1;
                return Err(MockError(*self.fail_kind.lock().unwrap()));
            }
        }

        let mut rows = self.rows.lock().unwrap();
        let mut conflicted = Vec::new();
        for item in outcomes.iter() {
            match rows.get(item.vm_id) {
                None => {
                    rows.insert(*item.vm_id, item.outcome.clone());
                }
                Some(stored) if stored == item.outcome => {}
                Some(_) => conflicted.push(*item.vm_id),
            }
        }
        match NEVec::try_from_vec(conflicted) {
            None => Ok(RecordingSuccess::AllRecorded),
            Some(keys) => Ok(RecordingSuccess::SomeConflicted(keys)),
        }
    }
}

/// A recorder whose batcher only ever flushes on the size trigger, so a
/// test that completes proves its submissions shared one batch.
fn spawn_size_gated_recorder(
    backend: &Arc<MockBackend>,
    max_batch: usize,
) -> OutcomeRecorderHandle<u64> {
    let (recorder, batcher) = outcome_batcher(
        Arc::clone(backend),
        waymark_batcher::Policy {
            max_batch: NonZeroUsize::new(max_batch).expect("non-zero"),
            max_delay: NonZeroDuration::from_secs(3600).expect("non-zero"),
        },
        std::future::pending(),
    );
    tokio::spawn(batcher);
    recorder
}

#[tokio::test]
async fn coalesces_submissions_into_one_statement() {
    let backend = Arc::new(MockBackend::default());
    let recorder = spawn_size_gated_recorder(&backend, 2);

    // The delay trigger is effectively off, so these can only resolve if
    // both outcomes left in the same batch.
    let a = {
        let recorder = recorder.clone();
        tokio::spawn(async move {
            recorder
                .submit((1, Outcome::Completion(b"a".to_vec())))
                .await
        })
    };
    let b = {
        let recorder = recorder.clone();
        tokio::spawn(async move {
            recorder
                .submit((2, Outcome::Exception(b"b".to_vec())))
                .await
        })
    };

    for submission in [a, b] {
        let outcome = submission.await.expect("join").expect("not closed");
        outcome.expect("recorded");
    }

    let rows = backend.rows.lock().unwrap();
    assert_eq!(rows.get(&1), Some(&Outcome::Completion(b"a".to_vec())));
    assert_eq!(rows.get(&2), Some(&Outcome::Exception(b"b".to_vec())));
}

#[tokio::test]
async fn conflicts_are_isolated_per_row() {
    let backend = Arc::new(MockBackend::default());

    // Seed VM 1 so a different resubmission conflicts.
    let seeder = spawn_size_gated_recorder(&backend, 1);
    seeder
        .submit((1, Outcome::Completion(b"original".to_vec())))
        .await
        .expect("not closed")
        .expect("recorded");

    // One batch holding a conflicting rewrite and an innocent fresh
    // outcome: only the conflicted waiter errors.
    let recorder = spawn_size_gated_recorder(&backend, 2);
    let conflicting = {
        let recorder = recorder.clone();
        tokio::spawn(async move {
            recorder
                .submit((1, Outcome::Completion(b"DIFFERENT".to_vec())))
                .await
        })
    };
    let innocent = {
        let recorder = recorder.clone();
        tokio::spawn(async move {
            recorder
                .submit((2, Outcome::Completion(b"fresh".to_vec())))
                .await
        })
    };

    let conflicting = conflicting.await.expect("join").expect("not closed");
    assert!(matches!(conflicting, Err(RecordError::Conflict)));

    let innocent = innocent.await.expect("join").expect("not closed");
    innocent.expect("the innocent outcome is recorded");
    assert_eq!(
        backend.rows.lock().unwrap().get(&2),
        Some(&Outcome::Completion(b"fresh".to_vec())),
    );
}

#[tokio::test]
async fn duplicate_vm_ids_are_deduped_first_wins() {
    let backend = Arc::new(MockBackend::default());
    let recorder = spawn_size_gated_recorder(&backend, 2);

    // Submitted from one task in listing order, so the batch holds the
    // "first" outcome ahead of the differing duplicate.
    let (first, duplicate) = tokio::join!(
        recorder.submit((1, Outcome::Completion(b"first".to_vec()))),
        recorder.submit((1, Outcome::Completion(b"DIFFERENT".to_vec()))),
    );

    first.expect("not closed").expect("recorded");
    let duplicate = duplicate.expect("not closed");
    assert!(matches!(duplicate, Err(RecordError::Conflict)));

    // Only the first occurrence went into the statement...
    assert_eq!(*backend.statements.lock().unwrap(), vec![vec![1]]);
    // ...and its outcome is the stored one.
    assert_eq!(
        backend.rows.lock().unwrap().get(&1),
        Some(&Outcome::Completion(b"first".to_vec())),
    );
}

#[tokio::test]
async fn an_identical_duplicate_shares_the_first_verdict() {
    let backend = Arc::new(MockBackend::default());
    let recorder = spawn_size_gated_recorder(&backend, 2);

    let (first, duplicate) = tokio::join!(
        recorder.submit((1, Outcome::Completion(b"same".to_vec()))),
        recorder.submit((1, Outcome::Completion(b"same".to_vec()))),
    );

    first.expect("not closed").expect("recorded");
    duplicate
        .expect("not closed")
        .expect("an identical duplicate shares the first's verdict");
    assert_eq!(*backend.statements.lock().unwrap(), vec![vec![1]]);
}

#[tokio::test(start_paused = true)]
async fn a_failed_batch_fails_folded_duplicates_too() {
    let backend = Arc::new(MockBackend::default());
    *backend.fail_records.lock().unwrap() = usize::MAX;
    let recorder = spawn_size_gated_recorder(&backend, 2);

    // An identical duplicate is folded out; its provisional fate would
    // be the winner's verdict — but the batch write never lands, so
    // settling must fan `Failed` to it as well, never `Ok`.
    let (first, duplicate) = tokio::join!(
        recorder.submit((1, Outcome::Completion(b"same".to_vec()))),
        recorder.submit((1, Outcome::Completion(b"same".to_vec()))),
    );
    assert!(matches!(
        first.expect("not closed"),
        Err(RecordError::Failed)
    ));
    assert!(matches!(
        duplicate.expect("not closed"),
        Err(RecordError::Failed)
    ));
}

#[tokio::test]
async fn a_conflicted_winner_conflicts_its_identical_duplicate() {
    let backend = Arc::new(MockBackend::default());

    // Seed a different stored outcome for the VM.
    let seeder = spawn_size_gated_recorder(&backend, 1);
    seeder
        .submit((1, Outcome::Completion(b"original".to_vec())))
        .await
        .expect("not closed")
        .expect("recorded");

    // The winner hits first-write-wins against the stored outcome; its
    // identical duplicate must inherit that `Conflict`, not report `Ok`
    // for a write that kept nothing.
    let recorder = spawn_size_gated_recorder(&backend, 2);
    let (winner, duplicate) = tokio::join!(
        recorder.submit((1, Outcome::Completion(b"DIFFERENT".to_vec()))),
        recorder.submit((1, Outcome::Completion(b"DIFFERENT".to_vec()))),
    );
    assert!(matches!(
        winner.expect("not closed"),
        Err(RecordError::Conflict)
    ));
    assert!(matches!(
        duplicate.expect("not closed"),
        Err(RecordError::Conflict)
    ));
}

#[tokio::test(start_paused = true)]
async fn retries_are_bounded() {
    let backend = Arc::new(MockBackend::default());
    *backend.fail_records.lock().unwrap() = usize::MAX;
    let recorder = spawn_size_gated_recorder(&backend, 1);

    let outcome = recorder
        .submit((1, Outcome::Completion(b"a".to_vec())))
        .await
        .expect("not closed");
    assert!(matches!(outcome, Err(RecordError::Failed)));
    assert_eq!(backend.statements.lock().unwrap().len(), RETRY_MAX_ATTEMPTS);
}

#[tokio::test]
async fn an_invalid_batch_fails_without_retry() {
    let backend = Arc::new(MockBackend::default());
    *backend.fail_records.lock().unwrap() = 1;
    *backend.fail_kind.lock().unwrap() = ErrorKind::InvalidBatch;
    let recorder = spawn_size_gated_recorder(&backend, 1);

    let outcome = recorder
        .submit((1, Outcome::Completion(b"a".to_vec())))
        .await
        .expect("not closed");
    assert!(matches!(outcome, Err(RecordError::Failed)));
    // One statement, no retry — a second attempt would have succeeded.
    assert_eq!(backend.statements.lock().unwrap().len(), 1);
    assert!(backend.rows.lock().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn recording_failures_are_retried() {
    let backend = Arc::new(MockBackend::default());
    *backend.fail_records.lock().unwrap() = 2;
    let recorder = spawn_size_gated_recorder(&backend, 1);

    recorder
        .submit((1, Outcome::Completion(b"a".to_vec())))
        .await
        .expect("not closed")
        .expect("retried to success");

    assert_eq!(
        backend.rows.lock().unwrap().get(&1),
        Some(&Outcome::Completion(b"a".to_vec())),
    );
}
