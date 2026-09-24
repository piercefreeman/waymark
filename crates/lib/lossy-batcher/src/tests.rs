use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use waymark_nonzero_duration::NonZeroDuration;

use super::*;

fn policy(buffers: usize, max_batch: usize, max_delay: Duration, flushers: usize) -> Policy {
    Policy {
        buffers: NonZeroUsize::new(buffers).expect("buffers must be non-zero"),
        max_batch: NonZeroUsize::new(max_batch).expect("max_batch must be non-zero"),
        max_delay: NonZeroDuration::new(max_delay).expect("max_delay must be non-zero"),
        flushers: NonZeroUsize::new(flushers).expect("flushers must be non-zero"),
    }
}

fn valid_policy(
    buffers: usize,
    max_batch: usize,
    max_delay: Duration,
    flushers: usize,
) -> ValidPolicy {
    policy(buffers, max_batch, max_delay, flushers)
        .validate()
        .expect("policy is valid")
}

/// The counters accumulate across the instance's lifetime, but a
/// [`Snapshotter`] snapshot reports deltas since the previous snapshot —
/// every reading below is therefore "since the last call".
fn counters(snapshotter: &Snapshotter) -> CountersSnapshot {
    let mut counters = CountersSnapshot::default();
    for (key, _, _, value) in snapshotter.snapshot().into_vec() {
        let DebugValue::Counter(value) = value else {
            panic!("only counters are registered, got {value:?}");
        };
        let key = key.key();
        let slot = match key.name() {
            "waymark_lossy_batcher_flushed_total" => &mut counters.flushed,
            "waymark_lossy_batcher_dropped_total" => {
                let reason = key
                    .labels()
                    .find(|label| label.key() == "reason")
                    .expect("dropped_total must carry a reason label");
                match reason.value() {
                    "full" => &mut counters.dropped_full,
                    "closed" => &mut counters.dropped_closed,
                    "flush_failed" => &mut counters.dropped_flush_failed,
                    other => panic!("unexpected reason label {other:?}"),
                }
            }
            other => panic!("unexpected metric {other:?}"),
        };
        *slot += value;
    }
    counters
}

#[derive(Debug, Default, PartialEq, Eq)]
struct CountersSnapshot {
    flushed: u64,
    dropped_full: u64,
    dropped_closed: u64,
    dropped_flush_failed: u64,
}

/// Run every task that is ready, repeatedly, so pushes propagate through
/// the flushers without advancing the (paused) clock.
async fn settle() {
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
}

/// A flush that appends each batch to `seen` and answers with the next
/// verdict from `verdicts` (`Ok` once `verdicts` runs out).
struct RecordingFlusher {
    seen: Arc<Mutex<Vec<Vec<u32>>>>,
    verdicts: Arc<Mutex<Vec<Result<(), String>>>>,
}

impl Flusher<u32> for RecordingFlusher {
    type Error = String;

    async fn flush(&self, batch: NESlice<'_, u32>) -> Result<(), String> {
        self.seen
            .lock()
            .unwrap()
            .push(batch.iter().copied().collect());
        let verdict = self.verdicts.lock().unwrap().pop();
        verdict.unwrap_or(Ok(()))
    }
}

fn recording_flusher(
    seen: &Arc<Mutex<Vec<Vec<u32>>>>,
    verdicts: Vec<Result<(), String>>,
) -> RecordingFlusher {
    RecordingFlusher {
        seen: Arc::clone(seen),
        verdicts: Arc::new(Mutex::new(verdicts)),
    }
}

/// A flusher that never completes a flush.
struct PendingFlusher;

impl Flusher<u32> for PendingFlusher {
    type Error = String;

    async fn flush(&self, _batch: NESlice<'_, u32>) -> Result<(), String> {
        std::future::pending::<()>().await;
        Ok(())
    }
}

/// A flush that records its batch on entry, then completes only once the
/// test hands it a permit through `release` — so a test can observe
/// flushes in flight and let them finish one at a time.
struct GatedFlusher {
    seen: Arc<Mutex<Vec<Vec<u32>>>>,
    release: Arc<tokio::sync::Semaphore>,
}

impl Flusher<u32> for GatedFlusher {
    type Error = String;

    async fn flush(&self, batch: NESlice<'_, u32>) -> Result<(), String> {
        self.seen
            .lock()
            .unwrap()
            .push(batch.iter().copied().collect());
        let permit = self
            .release
            .acquire()
            .await
            .expect("semaphore is never closed");
        permit.forget();
        Ok(())
    }
}

fn gated_flusher(
    seen: &Arc<Mutex<Vec<Vec<u32>>>>,
    release: &Arc<tokio::sync::Semaphore>,
) -> GatedFlusher {
    GatedFlusher {
        seen: Arc::clone(seen),
        release: Arc::clone(release),
    }
}

#[tokio::test(start_paused = true)]
async fn overfill_drops_exactly_the_unbuffered_items_as_full() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let (handle, task) = metrics::with_local_recorder(&recorder, || {
        lossy_batcher(
            "test",
            valid_policy(2, 2, Duration::from_secs(60), 1),
            // Flushes never complete: the one standby buffer goes out and
            // stays out. Arc also covers the blanket `Flusher` impl.
            Arc::new(PendingFlusher),
            std::future::pending(),
        )
    });
    tokio::spawn(task);

    for item in 0..6 {
        handle.push(item);
    }
    settle().await;

    // Items 0-1 took the standby buffer; 2-3 and 4-5 found none free.
    assert_eq!(
        counters(&snapshotter),
        CountersSnapshot {
            dropped_full: 4,
            ..Default::default()
        }
    );
}

#[tokio::test(start_paused = true)]
async fn push_many_fills_across_batch_boundaries() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (handle, task) = metrics::with_local_recorder(&recorder, || {
        lossy_batcher(
            "test",
            valid_policy(3, 2, Duration::from_secs(60), 2),
            recording_flusher(&seen, Vec::new()),
            std::future::pending(),
        )
    });
    tokio::spawn(task);

    handle.push_many([0, 1, 2, 3, 4]);
    settle().await;

    // Two full batches went out; the fifth item is still filling.
    assert_eq!(
        counters(&snapshotter),
        CountersSnapshot {
            flushed: 4,
            ..Default::default()
        }
    );
    assert_eq!(*seen.lock().unwrap(), vec![vec![0, 1], vec![2, 3]]);
}

#[tokio::test(start_paused = true)]
async fn flush_error_drops_that_batch_and_the_buffer_is_reused() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (handle, task) = metrics::with_local_recorder(&recorder, || {
        lossy_batcher(
            "test",
            valid_policy(2, 2, Duration::from_secs(60), 1),
            recording_flusher(&seen, vec![Err("boom".to_string())]),
            std::future::pending(),
        )
    });
    tokio::spawn(task);

    handle.push(1);
    handle.push(2);
    settle().await;
    assert_eq!(
        counters(&snapshotter),
        CountersSnapshot {
            dropped_flush_failed: 2,
            ..Default::default()
        }
    );

    // The failed batch's buffer went back to the free buffers pool; the
    // next batch flows through it successfully.
    handle.push(3);
    handle.push(4);
    settle().await;
    assert_eq!(
        counters(&snapshotter),
        CountersSnapshot {
            flushed: 2,
            ..Default::default()
        }
    );
    assert_eq!(*seen.lock().unwrap(), vec![vec![1, 2], vec![3, 4]]);
}

#[tokio::test(start_paused = true)]
async fn full_batch_flushes_without_waiting_for_the_delay() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (handle, task) = metrics::with_local_recorder(&recorder, || {
        lossy_batcher(
            "test",
            valid_policy(3, 2, Duration::from_secs(60), 2),
            recording_flusher(&seen, Vec::new()),
            std::future::pending(),
        )
    });
    tokio::spawn(task);

    handle.push(1);
    handle.push(2);
    // Only yields — the paused clock never comes near max_delay.
    settle().await;

    assert_eq!(
        counters(&snapshotter),
        CountersSnapshot {
            flushed: 2,
            ..Default::default()
        }
    );
}

#[tokio::test(start_paused = true)]
async fn partial_batch_flushes_after_max_delay() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (handle, task) = metrics::with_local_recorder(&recorder, || {
        lossy_batcher(
            "test",
            valid_policy(2, 10, Duration::from_millis(100), 1),
            recording_flusher(&seen, Vec::new()),
            std::future::pending(),
        )
    });
    tokio::spawn(task);

    handle.push(7);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        counters(&snapshotter),
        CountersSnapshot::default(),
        "not yet due"
    );

    tokio::time::sleep(Duration::from_millis(60)).await;
    settle().await;
    assert_eq!(
        counters(&snapshotter),
        CountersSnapshot {
            flushed: 1,
            ..Default::default()
        }
    );
    assert_eq!(*seen.lock().unwrap(), vec![vec![7]]);
}

#[tokio::test(start_paused = true)]
async fn shutdown_flushes_the_partial_batch_and_later_pushes_count_closed() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let (handle, task) = metrics::with_local_recorder(&recorder, || {
        lossy_batcher(
            "test",
            valid_policy(2, 10, Duration::from_secs(60), 1),
            recording_flusher(&seen, Vec::new()),
            async move {
                let _ = shutdown_rx.await;
            },
        )
    });
    let task = tokio::spawn(task);

    handle.push(1);
    // Let the task run first: a flush loop is parked on the channel, which
    // is the state shutdown must be able to unwind.
    settle().await;
    shutdown_tx.send(()).expect("task is alive");
    task.await.expect("batcher task must not panic");

    assert_eq!(
        counters(&snapshotter),
        CountersSnapshot {
            flushed: 1,
            ..Default::default()
        }
    );
    assert_eq!(*seen.lock().unwrap(), vec![vec![1]]);

    handle.push(2);
    handle.push_many([3, 4]);
    assert_eq!(
        counters(&snapshotter),
        CountersSnapshot {
            dropped_closed: 3,
            ..Default::default()
        }
    );
}

#[tokio::test(start_paused = true)]
async fn flush_loops_overlap_up_to_the_flusher_count() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let (handle, task) = metrics::with_local_recorder(&recorder, || {
        lossy_batcher(
            "test",
            valid_policy(3, 2, Duration::from_secs(60), 2),
            gated_flusher(&seen, &release),
            std::future::pending(),
        )
    });
    tokio::spawn(task);

    handle.push_many([0, 1, 2, 3]);
    settle().await;

    // Both batches entered a flush while neither has completed: the second
    // loop did not wait for the first one's flush.
    assert_eq!(*seen.lock().unwrap(), vec![vec![0, 1], vec![2, 3]]);
    assert_eq!(
        counters(&snapshotter),
        CountersSnapshot::default(),
        "nothing completed yet"
    );

    release.add_permits(2);
    settle().await;
    assert_eq!(
        counters(&snapshotter),
        CountersSnapshot {
            flushed: 4,
            ..Default::default()
        }
    );
}

#[tokio::test(start_paused = true)]
async fn an_overdue_partial_batch_waits_for_a_free_buffer_instead_of_dropping() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let (handle, task) = metrics::with_local_recorder(&recorder, || {
        lossy_batcher(
            "test",
            valid_policy(2, 10, Duration::from_millis(100), 1),
            gated_flusher(&seen, &release),
            std::future::pending(),
        )
    });
    tokio::spawn(task);

    // A full batch takes the only standby buffer out; its flush is gated.
    handle.push_many([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    settle().await;
    assert_eq!(*seen.lock().unwrap(), vec![(0..10).collect::<Vec<_>>()]);

    // A partial batch turns overdue while the free buffers pool is dry:
    // held, not lost.
    handle.push(10);
    tokio::time::sleep(Duration::from_millis(150)).await;
    settle().await;
    assert_eq!(
        counters(&snapshotter),
        CountersSnapshot::default(),
        "nothing dropped"
    );

    // Pushes keep landing in the held buffer.
    handle.push(11);

    // The flush completes, the buffer returns, and the held batch goes out
    // without waiting for another delay.
    release.add_permits(1);
    settle().await;
    assert_eq!(
        counters(&snapshotter),
        CountersSnapshot {
            flushed: 10,
            ..Default::default()
        }
    );
    assert_eq!(
        *seen.lock().unwrap(),
        vec![(0..10).collect::<Vec<_>>(), vec![10, 11]]
    );
}

#[tokio::test(start_paused = true)]
async fn dropping_the_last_handle_flushes_the_partial_batch_and_ends_the_task() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (handle, task) = metrics::with_local_recorder(&recorder, || {
        lossy_batcher(
            "test",
            valid_policy(2, 10, Duration::from_secs(60), 1),
            recording_flusher(&seen, Vec::new()),
            std::future::pending(),
        )
    });
    let task = tokio::spawn(task);

    let clone = handle.clone();
    handle.push(1);
    settle().await;
    drop(handle);
    settle().await;
    assert!(!task.is_finished(), "a live handle keeps the task running");

    drop(clone);
    task.await.expect("batcher task must not panic");

    assert_eq!(
        counters(&snapshotter),
        CountersSnapshot {
            flushed: 1,
            ..Default::default()
        }
    );
    assert_eq!(*seen.lock().unwrap(), vec![vec![1]]);
}

#[tokio::test(start_paused = true)]
async fn shutdown_with_no_free_buffer_drops_the_final_batch_as_full() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let (handle, task) = metrics::with_local_recorder(&recorder, || {
        lossy_batcher(
            "test",
            valid_policy(2, 10, Duration::from_secs(60), 1),
            // The one standby buffer goes out and never comes back.
            PendingFlusher,
            async move {
                let _ = shutdown_rx.await;
            },
        )
    });
    // Never joined: the hung flush keeps the task from ending, by contract.
    tokio::spawn(task);

    handle.push_many([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    handle.push(10);
    settle().await;
    assert_eq!(counters(&snapshotter), CountersSnapshot::default());

    // Shutdown does not wait for a buffer: the final batch is counted full
    // and the intake closes regardless of the hung flush.
    shutdown_tx.send(()).expect("task is alive");
    settle().await;
    assert_eq!(
        counters(&snapshotter),
        CountersSnapshot {
            dropped_full: 1,
            ..Default::default()
        }
    );
    handle.push(11);
    assert_eq!(
        counters(&snapshotter),
        CountersSnapshot {
            dropped_closed: 1,
            ..Default::default()
        }
    );
}

#[tokio::test(start_paused = true)]
async fn the_last_handle_close_waits_for_a_free_buffer() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let (handle, task) = metrics::with_local_recorder(&recorder, || {
        lossy_batcher(
            "test",
            valid_policy(2, 10, Duration::from_secs(60), 1),
            // The one standby buffer goes out into a held flush.
            gated_flusher(&seen, &release),
            std::future::pending(),
        )
    });
    let task = tokio::spawn(task);

    handle.push_many([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    handle.push(10);
    settle().await;
    assert_eq!(
        *seen.lock().unwrap(),
        vec![vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]]
    );
    assert_eq!(counters(&snapshotter), CountersSnapshot::default());

    // The last handle drops with no buffer free: the close waits for the
    // held flush instead of discarding the final batch.
    drop(handle);
    settle().await;
    assert!(!task.is_finished());
    assert_eq!(counters(&snapshotter), CountersSnapshot::default());

    // The held flush ends, its buffer comes back, and the final batch goes
    // out through it.
    release.add_permits(2);
    task.await.expect("task ends");
    assert_eq!(
        *seen.lock().unwrap(),
        vec![vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9], vec![10]]
    );
    assert_eq!(
        counters(&snapshotter),
        CountersSnapshot {
            flushed: 11,
            ..Default::default()
        }
    );
}

#[tokio::test(start_paused = true)]
async fn a_dropped_task_closes_the_intake_at_the_next_swap_without_panicking() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (handle, task) = metrics::with_local_recorder(&recorder, || {
        lossy_batcher(
            "test",
            valid_policy(3, 2, Duration::from_secs(60), 1),
            recording_flusher(&seen, Vec::new()),
            std::future::pending(),
        )
    });
    let task = tokio::spawn(task);
    settle().await;

    // The task goes away before its `shutdown` future resolves, taking the
    // receiver with it.
    task.abort();
    let joined = task.await;
    assert!(joined.is_err_and(|error| error.is_cancelled()));

    // The first swap finds the receiver gone: its batch is counted closed,
    // the intake closes itself, and the rest of that push_many is refused
    // instead of filling the next buffer; every later push is refused too.
    handle.push_many([0, 1, 2, 3]);
    handle.push(5);
    handle.push_many([6, 7]);
    assert_eq!(
        counters(&snapshotter),
        CountersSnapshot {
            dropped_closed: 7,
            ..Default::default()
        }
    );
    assert!(seen.lock().unwrap().is_empty());
}

#[test]
fn too_many_flushers_is_a_setup_error() {
    let result = policy(3, 2, Duration::from_secs(1), 3).validate();
    let Err(error) = result else {
        panic!("three flushers over three buffers must be refused");
    };
    assert_eq!(
        error,
        ValidateError::TooManyFlushers {
            flushers: NonZeroUsize::new(3).unwrap(),
            buffers: NonZeroUsize::new(3).unwrap(),
            max_flushers: NonZeroUsize::new(2).unwrap(),
        }
    );
    assert_eq!(
        error.to_string(),
        "flushers (3) must be at most 2 (buffers - 1, with 3 buffers)"
    );
}

#[test]
fn a_single_buffer_is_a_setup_error() {
    let result = policy(1, 2, Duration::from_secs(1), 1).validate();
    let Err(error) = result else {
        panic!("one buffer must be refused");
    };
    assert_eq!(
        error,
        ValidateError::TooFewBuffers {
            buffers: NonZeroUsize::new(1).unwrap(),
        }
    );
    assert_eq!(
        error.to_string(),
        "buffers (1) must be at least 2: one fills while the others stand by"
    );
}
