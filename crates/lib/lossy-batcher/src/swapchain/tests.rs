use std::num::NonZeroUsize;
use std::time::Duration;

use waymark_nonzero_duration::NonZeroDuration;

use super::*;

fn swapchain(buffers: usize, max_batch: usize) -> (Swapchain<u32>, mpsc::Receiver<NEVec<u32>>) {
    Swapchain::new(
        NonZeroUsize::new(buffers).expect("buffers must be non-zero"),
        NonZeroUsize::new(max_batch).expect("max_batch must be non-zero"),
    )
}

fn max_delay(millis: u64) -> NonZeroDuration {
    NonZeroDuration::new(Duration::from_millis(millis)).expect("max_delay must be non-zero")
}

/// Every batch waiting on the full channel, in order.
fn received(full_rx: &mut mpsc::Receiver<NEVec<u32>>) -> Vec<Vec<u32>> {
    let mut batches = Vec::new();
    while let Ok(batch) = full_rx.try_recv() {
        batches.push(Vec::from(batch));
    }
    batches
}

fn filling_len(swapchain: &Swapchain<u32>) -> usize {
    swapchain.filling.lock().unwrap().buf.len()
}

fn free_len(swapchain: &Swapchain<u32>) -> usize {
    swapchain.free.lock().unwrap().len()
}

#[tokio::test(start_paused = true)]
async fn a_full_push_swaps_the_buffer_out() {
    let (swapchain, mut full_rx) = swapchain(2, 2);

    let outcome = swapchain.push(1);
    assert!(outcome.first_push);
    assert_eq!(outcome.discarded_full, 0);
    assert_eq!(outcome.discarded_closed, 0);
    assert!(swapchain.first_push_at().is_some());

    let outcome = swapchain.push(2);
    assert!(!outcome.first_push);
    assert_eq!(outcome.discarded_full, 0);
    assert_eq!(received(&mut full_rx), vec![vec![1, 2]]);
    assert_eq!(filling_len(&swapchain), 0);
    assert_eq!(free_len(&swapchain), 0, "the standby buffer is now filling");
    assert!(swapchain.first_push_at().is_none());
}

#[tokio::test(start_paused = true)]
async fn a_full_push_with_no_free_buffer_discards_as_full() {
    let (swapchain, mut full_rx) = swapchain(2, 2);

    swapchain.push(1);
    swapchain.push(2);
    swapchain.push(3);
    let outcome = swapchain.push(4);
    assert_eq!(outcome.discarded_full, 2, "the whole full buffer");
    assert_eq!(filling_len(&swapchain), 0);
    assert!(swapchain.first_push_at().is_none());
    assert_eq!(received(&mut full_rx), vec![vec![1, 2]]);
}

#[tokio::test(start_paused = true)]
async fn push_many_splits_across_batch_boundaries() {
    let (swapchain, mut full_rx) = swapchain(3, 2);

    let outcome = swapchain.push_many([0, 1, 2, 3, 4]);
    assert!(outcome.first_push);
    assert_eq!(outcome.discarded_full, 0);
    assert_eq!(received(&mut full_rx), vec![vec![0, 1], vec![2, 3]]);
    assert_eq!(filling_len(&swapchain), 1);
}

#[tokio::test(start_paused = true)]
async fn swap_overdue_swaps_only_once_the_first_item_is_old_enough() {
    let (swapchain, mut full_rx) = swapchain(2, 10);

    assert!(swapchain.swap_overdue(max_delay(100)).is_none(), "empty");

    swapchain.push(7);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        swapchain.swap_overdue(max_delay(100)).is_none(),
        "too young"
    );

    tokio::time::sleep(Duration::from_millis(50)).await;
    let outcome = swapchain.swap_overdue(max_delay(100));
    assert!(matches!(
        outcome,
        Some(OverdueOutcome::Swapped(SwapOutcome::Sent))
    ));
    assert_eq!(received(&mut full_rx), vec![vec![7]]);
    assert_eq!(filling_len(&swapchain), 0);
}

#[tokio::test(start_paused = true)]
async fn an_overdue_swap_with_no_free_buffer_is_held() {
    let (swapchain, mut full_rx) = swapchain(2, 10);

    // The standby buffer goes out and stays out.
    swapchain.push_many([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    let batches = received(&mut full_rx);
    assert_eq!(batches, vec![(0..10).collect::<Vec<_>>()]);

    swapchain.push(10);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let outcome = swapchain.swap_overdue(max_delay(100));
    assert!(matches!(outcome, Some(OverdueOutcome::Held)));
    assert_eq!(filling_len(&swapchain), 1, "nothing discarded");
    assert!(swapchain.first_push_at().is_some(), "still overdue");

    // The buffer returns: the held swap goes through.
    let outcome = swapchain.recycle(batches.into_iter().next().unwrap());
    assert!(outcome.free_was_empty);
    let outcome = swapchain.swap_overdue(max_delay(100));
    assert!(matches!(
        outcome,
        Some(OverdueOutcome::Swapped(SwapOutcome::Sent))
    ));
    assert_eq!(received(&mut full_rx), vec![vec![10]]);
}

#[tokio::test(start_paused = true)]
async fn close_swaps_the_final_buffer_refuses_pushes_and_ends_the_channel() {
    let (swapchain, mut full_rx) = swapchain(2, 10);

    swapchain.push(1);
    assert!(matches!(swapchain.close(), Some(SwapOutcome::Sent)));

    assert_eq!(swapchain.push(2).discarded_closed, 1);
    assert_eq!(swapchain.push_many([3, 4]).discarded_closed, 2);
    assert_eq!(filling_len(&swapchain), 0);

    // The final batch is delivered; then the sender is gone.
    assert_eq!(full_rx.recv().await.map(Vec::from), Some(vec![1]));
    assert!(full_rx.recv().await.is_none());

    assert!(swapchain.close().is_none(), "idempotent");
}

#[tokio::test(start_paused = true)]
async fn close_with_no_free_buffer_discards_the_final_buffer_as_full() {
    let (swapchain, mut full_rx) = swapchain(2, 10);

    swapchain.push_many([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    swapchain.push(10);
    assert!(matches!(
        swapchain.close(),
        Some(SwapOutcome::DroppedFull(1))
    ));
    assert_eq!(received(&mut full_rx), vec![(0..10).collect::<Vec<_>>()]);
    assert!(full_rx.recv().await.is_none());
}

#[tokio::test(start_paused = true)]
async fn a_dropped_receiver_closes_the_intake_at_the_next_swap() {
    let (swapchain, full_rx) = swapchain(2, 2);
    drop(full_rx);

    // Nothing notices until a swap: the first item just fills.
    let outcome = swapchain.push(1);
    assert_eq!(outcome.discarded_closed, 0);

    // The swap finds the receiver gone: its batch is discarded as closed,
    // and the intake is closed from here on.
    let outcome = swapchain.push(2);
    assert_eq!(outcome.discarded_closed, 2);
    assert_eq!(filling_len(&swapchain), 0);
    assert_eq!(swapchain.push(3).discarded_closed, 1);
    assert!(swapchain.close().is_none());
}

#[tokio::test(start_paused = true)]
async fn push_many_that_closes_the_intake_refuses_the_rest_without_panicking() {
    let (swapchain, full_rx) = swapchain(3, 2);
    drop(full_rx);

    // Items 0-1 swap and close the intake; 2-3 must not fill the fresh
    // buffer and swap again on a closed swapchain.
    let outcome = swapchain.push_many([0, 1, 2, 3]);
    assert_eq!(outcome.discarded_closed, 4);
    assert_eq!(outcome.discarded_full, 0);
    assert_eq!(filling_len(&swapchain), 0);
    assert_eq!(swapchain.push(4).discarded_closed, 1);
}

#[tokio::test(start_paused = true)]
async fn push_many_that_closes_the_intake_counts_the_rest_as_closed_not_full() {
    let (swapchain, full_rx) = swapchain(2, 2);
    drop(full_rx);

    // With no free buffer, 2-3 would otherwise discard as `full`.
    let outcome = swapchain.push_many([0, 1, 2, 3]);
    assert_eq!(outcome.discarded_closed, 4);
    assert_eq!(outcome.discarded_full, 0);
}

#[tokio::test(start_paused = true)]
async fn push_many_that_closes_the_intake_strands_no_trailing_partial() {
    let (swapchain, full_rx) = swapchain(2, 2);
    drop(full_rx);

    // Item 2 would otherwise sit unreported in a closed swapchain's buffer.
    let outcome = swapchain.push_many([0, 1, 2]);
    assert_eq!(outcome.discarded_closed, 3);
    assert_eq!(filling_len(&swapchain), 0);
}

#[tokio::test(start_paused = true)]
async fn recycle_reports_whether_the_free_buffers_pool_was_empty() {
    let (swapchain, mut full_rx) = swapchain(3, 2);

    swapchain.push_many([0, 1, 2, 3]);
    let batches = received(&mut full_rx);
    assert_eq!(batches.len(), 2);
    assert_eq!(free_len(&swapchain), 0);

    let mut batches = batches.into_iter();
    assert!(swapchain.recycle(batches.next().unwrap()).free_was_empty);
    assert!(!swapchain.recycle(batches.next().unwrap()).free_was_empty);
    assert_eq!(free_len(&swapchain), 2);
}

#[tokio::test(start_paused = true)]
async fn buffers_never_grow_past_their_construction_capacity() {
    let max_batch = 4;
    let (swapchain, mut full_rx) = swapchain(2, max_batch);

    // Every path: a send, a discard on a dry free buffers pool, a recycle,
    // an overdue swap.
    swapchain.push_many([0, 1, 2, 3]);
    assert_eq!(swapchain.push_many([4, 5, 6, 7]).discarded_full, 4);
    for batch in received(&mut full_rx) {
        swapchain.recycle(batch);
    }
    swapchain.push(8);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(matches!(
        swapchain.swap_overdue(max_delay(100)),
        Some(OverdueOutcome::Swapped(SwapOutcome::Sent))
    ));
    for batch in received(&mut full_rx) {
        swapchain.recycle(batch);
    }

    assert_eq!(filling_len(&swapchain), 0, "everything went out");
    assert_eq!(swapchain.filling.lock().unwrap().buf.capacity(), max_batch);
    let free_capacities: Vec<usize> = swapchain
        .free
        .lock()
        .unwrap()
        .iter()
        .map(Vec::capacity)
        .collect();
    assert_eq!(
        free_capacities,
        vec![max_batch],
        "the standby buffer is back in the free buffers pool, capacity intact"
    );
}
