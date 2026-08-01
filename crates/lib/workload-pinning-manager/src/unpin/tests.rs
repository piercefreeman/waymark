use std::sync::Arc;
use std::time::Duration;

use mockall::predicate;
use nonempty_collections::nev;
use tokio::sync::mpsc;
use waymark_nonzero_duration::NonZeroDuration;
use waymark_workload_pinning_core::UnpinMode;

use super::{UnpinParams, run_unpin_loop};
use crate::test_utils::helpers::{short_heartbeat, test_node_id};
use crate::test_utils::mock::{MockBackend, MockError};

/// With nothing ever sent, closing the input exits the loop cleanly.
#[tokio::test]
async fn exits_cleanly_when_the_input_closes_empty() {
    let backend = Arc::new(MockBackend::new());
    let (unpin_tx, unpin_rx) = mpsc::unbounded_channel::<(u64, UnpinMode)>();
    drop(unpin_tx);

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        run_unpin_loop(UnpinParams {
            backend,
            node_id: test_node_id(),
            unpin_rx,
            retry_interval: short_heartbeat(),
        }),
    )
    .await
    .expect("the loop should exit promptly");

    assert!(result.is_ok(), "expected Ok, got {result:?}");
}

/// Evictions are applied, and the mode each carries is preserved.
#[tokio::test(flavor = "multi_thread")]
async fn applies_evictions_with_their_modes() {
    let node_id = test_node_id();

    let mut backend = MockBackend::new();
    backend
        .expect_unpin_workloads()
        .times(1)
        .with(
            predicate::eq(node_id),
            predicate::eq(nev![(1u64, UnpinMode::Park)]),
        )
        .returning(move |_, _| Box::pin(std::future::ready(Ok(()))));

    let backend = Arc::new(backend);
    let (unpin_tx, unpin_rx) = mpsc::unbounded_channel::<(u64, UnpinMode)>();
    unpin_tx.send((1u64, UnpinMode::Park)).expect("send");
    drop(unpin_tx);

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        run_unpin_loop(UnpinParams {
            backend: Arc::clone(&backend),
            node_id,
            unpin_rx,
            retry_interval: short_heartbeat(),
        }),
    )
    .await
    .expect("the loop should drain and exit");

    assert!(result.is_ok(), "expected Ok, got {result:?}");

    let mut mock = Arc::into_inner(backend).expect("exclusively owned after the loop exits");
    mock.checkpoint();
}

/// A transient failure keeps the batch queued and retries it, rather
/// than abandoning the pinnings.
#[tokio::test(flavor = "multi_thread")]
async fn retries_a_failed_unpin_until_it_lands() {
    let node_id = test_node_id();

    let mut backend = MockBackend::new();
    let mut attempts = 0;
    backend
        .expect_unpin_workloads()
        .times(2)
        .returning(move |_, _| {
            attempts += 1;
            if attempts == 1 {
                return Box::pin(std::future::ready(Err(MockError)));
            }
            Box::pin(std::future::ready(Ok(())))
        });

    let backend = Arc::new(backend);
    let (unpin_tx, unpin_rx) = mpsc::unbounded_channel::<(u64, UnpinMode)>();
    unpin_tx.send((1u64, UnpinMode::Release)).expect("send");
    drop(unpin_tx);

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        run_unpin_loop(UnpinParams {
            backend: Arc::clone(&backend),
            node_id,
            unpin_rx,
            retry_interval: short_heartbeat(),
        }),
    )
    .await
    .expect("the loop should drain once the retry lands");

    assert!(result.is_ok(), "expected Ok, got {result:?}");

    // Explicit: the failed attempt and the successful retry both ran.
    let mut mock = Arc::into_inner(backend).expect("exclusively owned after the loop exits");
    mock.checkpoint();
}

/// After `MAX_UNPIN_FAILURES` consecutive failures the loop gives up and
/// surfaces the error.
#[tokio::test(flavor = "multi_thread")]
async fn gives_up_after_repeated_failures() {
    let node_id = test_node_id();

    let mut backend = MockBackend::new();
    backend
        .expect_unpin_workloads()
        .times(super::MAX_UNPIN_FAILURES)
        .returning(move |_, _| Box::pin(std::future::ready(Err(MockError))));

    let backend = Arc::new(backend);
    let (unpin_tx, unpin_rx) = mpsc::unbounded_channel::<(u64, UnpinMode)>();
    unpin_tx.send((1u64, UnpinMode::Release)).expect("send");
    drop(unpin_tx);

    let result = tokio::time::timeout(
        Duration::from_secs(10),
        run_unpin_loop(UnpinParams {
            backend: Arc::clone(&backend),
            node_id,
            unpin_rx,
            retry_interval: short_heartbeat(),
        }),
    )
    .await
    .expect("the loop should give up");

    assert!(result.is_err(), "expected the error to surface");

    let mut mock = Arc::into_inner(backend).expect("exclusively owned after the loop exits");
    mock.checkpoint();
}

/// Evictions queued together are coalesced into one call, and a repeat
/// eviction of the same workload supersedes the earlier mode.
#[tokio::test(flavor = "multi_thread")]
async fn coalesces_queued_evictions_and_supersedes_repeats() {
    let node_id = test_node_id();

    let mut backend = MockBackend::new();
    backend
        .expect_unpin_workloads()
        .times(1)
        .withf(move |_, workloads| {
            let mut got: Vec<_> = workloads.iter().copied().collect();
            got.sort_by_key(|(id, _mode)| *id);
            got == vec![(1u64, UnpinMode::Park), (2u64, UnpinMode::Release)]
        })
        .returning(move |_, _| Box::pin(std::future::ready(Ok(()))));

    let backend = Arc::new(backend);
    let (unpin_tx, unpin_rx) = mpsc::unbounded_channel::<(u64, UnpinMode)>();
    // Queue everything before the loop runs so it coalesces in one pass.
    unpin_tx.send((1u64, UnpinMode::Release)).expect("send");
    unpin_tx.send((2u64, UnpinMode::Release)).expect("send");
    unpin_tx.send((1u64, UnpinMode::Park)).expect("send");
    drop(unpin_tx);

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        run_unpin_loop(UnpinParams {
            backend: Arc::clone(&backend),
            node_id,
            unpin_rx,
            retry_interval: NonZeroDuration::new(Duration::from_millis(50)).unwrap(),
        }),
    )
    .await
    .expect("the loop should drain and exit");

    assert!(result.is_ok(), "expected Ok, got {result:?}");

    let mut mock = Arc::into_inner(backend).expect("exclusively owned after the loop exits");
    mock.checkpoint();
}
