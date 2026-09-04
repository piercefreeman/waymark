use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use nonempty_collections::NESlice;
use waymark_nonzero_duration::NonZeroDuration;
use waymark_observability_events_core::Event;

use super::*;

/// A payload for the tests: the emitter neither reads nor bounds it.
#[derive(Debug)]
struct TestPayload(u32);

/// One flushed event, as much of it as the assertions look at.
type Seen = (u8, u64, chrono::DateTime<chrono::Utc>, u32);

/// A flush that appends each event of the batch to `seen`.
struct RecordingFlusher {
    seen: Arc<std::sync::Mutex<Vec<Seen>>>,
}

impl waymark_lossy_batcher::Flusher<Event<u8, TestPayload>> for RecordingFlusher {
    type Error = String;

    async fn flush(&self, batch: NESlice<'_, Event<u8, TestPayload>>) -> Result<(), String> {
        self.seen.lock().unwrap().extend(batch.iter().map(|event| {
            (
                event.node_id,
                event.node_sequence.get(),
                event.at,
                event.payload.0,
            )
        }));
        Ok(())
    }
}

fn policy() -> waymark_lossy_batcher::ValidPolicy {
    waymark_lossy_batcher::Policy {
        buffers: NonZeroUsize::new(2).expect("non-zero"),
        max_batch: NonZeroUsize::new(8).expect("non-zero"),
        max_delay: NonZeroDuration::new(Duration::from_secs(60)).expect("non-zero"),
        flushers: NonZeroUsize::new(1).expect("non-zero"),
    }
    .validate()
    .expect("policy is valid")
}

/// A batcher recording into `seen`, and the sender whose drop shuts it
/// down — which flushes the filling buffer.
fn recording_batcher(
    seen: &Arc<std::sync::Mutex<Vec<Seen>>>,
) -> (
    waymark_lossy_batcher::BatcherHandle<Event<u8, TestPayload>>,
    tokio::task::JoinHandle<()>,
    tokio::sync::oneshot::Sender<()>,
) {
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let (batcher, task) = waymark_lossy_batcher::lossy_batcher(
        "test",
        policy(),
        RecordingFlusher {
            seen: Arc::clone(seen),
        },
        async move {
            let _ = shutdown_rx.await;
        },
    );
    (batcher, tokio::spawn(task), shutdown_tx)
}

#[tokio::test]
async fn emit_stamps_consecutive_positions_and_flushes_in_order() {
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (batcher, task, shutdown_tx) = recording_batcher(&seen);

    let emitter = Emitter::new(7u8, batcher);
    let before = chrono::Utc::now();
    emitter.emit(TestPayload(10));
    emitter.emit(TestPayload(20));
    emitter.emit(TestPayload(30));

    drop(shutdown_tx);
    task.await.expect("batcher task runs to completion");

    let seen = seen.lock().unwrap();
    let stamped: Vec<_> = seen
        .iter()
        .map(|(node_id, position, _, payload)| (*node_id, *position, *payload))
        .collect();
    assert_eq!(stamped, vec![(7, 0, 10), (7, 1, 20), (7, 2, 30)]);
    assert!(
        seen.iter().all(|(_, _, at, _)| *at >= before),
        "events are stamped with the time of emission"
    );
}

#[tokio::test]
async fn producers_sharing_the_emitter_emit_into_one_stream() {
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (batcher, task, shutdown_tx) = recording_batcher(&seen);

    let emitter = Arc::new(Emitter::new(7u8, batcher));
    let one = Arc::clone(&emitter);
    let other = Arc::clone(&emitter);
    one.emit(TestPayload(10));
    other.emit(TestPayload(20));
    one.emit(TestPayload(30));

    drop(shutdown_tx);
    task.await.expect("batcher task runs to completion");

    let positions: Vec<_> = seen
        .lock()
        .unwrap()
        .iter()
        .map(|(_, position, _, payload)| (*position, *payload))
        .collect();
    assert_eq!(
        positions,
        vec![(0, 10), (1, 20), (2, 30)],
        "one emitter, one counter: positions never collide across producers"
    );
}
