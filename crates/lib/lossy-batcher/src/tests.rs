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
fn counters(snapshotter: &Snapshotter) -> Counters {
    let mut counters = Counters::default();
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
struct Counters {
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
        Counters {
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

    handle.push_many(0..5);
    settle().await;

    // Two full batches went out; the fifth item is still filling.
    assert_eq!(
        counters(&snapshotter),
        Counters {
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
        Counters {
            dropped_flush_failed: 2,
            ..Default::default()
        }
    );

    // The failed batch's buffer went back to the pool; the next batch flows
    // through it successfully.
    handle.push(3);
    handle.push(4);
    settle().await;
    assert_eq!(
        counters(&snapshotter),
        Counters {
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
        Counters {
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
    assert_eq!(counters(&snapshotter), Counters::default(), "not yet due");

    tokio::time::sleep(Duration::from_millis(60)).await;
    settle().await;
    assert_eq!(
        counters(&snapshotter),
        Counters {
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
    shutdown_tx.send(()).expect("task is alive");
    task.await.expect("batcher task must not panic");

    assert_eq!(
        counters(&snapshotter),
        Counters {
            flushed: 1,
            ..Default::default()
        }
    );
    assert_eq!(*seen.lock().unwrap(), vec![vec![1]]);

    handle.push(2);
    handle.push_many([3, 4]);
    assert_eq!(
        counters(&snapshotter),
        Counters {
            dropped_closed: 3,
            ..Default::default()
        }
    );
}

#[tokio::test(start_paused = true)]
async fn buffers_never_grow_past_their_construction_capacity() {
    let recorder = DebuggingRecorder::new();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let max_batch = 4;
    let (handle, task) = metrics::with_local_recorder(&recorder, || {
        lossy_batcher(
            "test",
            valid_policy(3, max_batch, Duration::from_millis(100), 1),
            recording_flusher(&seen, vec![Err("boom".to_string())]),
            std::future::pending(),
        )
    });
    tokio::spawn(task);

    // Exercise every path: full swaps, a failed flush, a timer swap.
    for item in 0..20 {
        handle.push(item);
    }
    settle().await;
    handle.push(20);
    tokio::time::sleep(Duration::from_millis(150)).await;
    settle().await;

    assert_eq!(
        handle.shared.swapchain.filling_len(),
        0,
        "everything flushed"
    );
    assert_eq!(handle.shared.swapchain.filling_capacity(), max_batch);
    let free_capacities = handle.shared.swapchain.free_capacities();
    assert_eq!(
        free_capacities,
        vec![max_batch; 2],
        "all standby buffers back in the pool, capacity intact"
    );
}

#[test]
fn too_many_flushers_is_a_setup_error() {
    let result = policy(2, 2, Duration::from_secs(1), 2).validate();
    let Err(error) = result else {
        panic!("two flushers over two buffers must be refused");
    };
    assert_eq!(
        error,
        TooManyFlushers {
            flushers: NonZeroUsize::new(2).unwrap(),
            buffers: NonZeroUsize::new(2).unwrap(),
        }
    );
}
