use std::cell::Cell;
use std::convert::Infallible;
use std::time::Duration;

use waymark_nonzero_duration::NonZeroDuration;

use super::*;

/// A sweep counting its calls and deleting nothing.
fn counting_sweep(
    calls: &Cell<u32>,
) -> impl Fn(chrono::DateTime<chrono::Utc>) -> std::future::Ready<Result<u64, Infallible>> {
    move |_cutoff| {
        calls.set(calls.get() + 1);
        std::future::ready(Ok(0))
    }
}

#[tokio::test(start_paused = true)]
async fn sweeps_once_per_interval_until_shutdown() {
    let calls = Cell::new(0);

    run(
        "test",
        NonZeroDuration::from_secs(3600).unwrap(),
        NonZeroDuration::from_secs(60).unwrap(),
        counting_sweep(&calls),
        tokio::time::sleep(Duration::from_secs(150)),
    )
    .await;

    assert_eq!(calls.get(), 3, "the ticks at 0, 60 and 120 seconds");
}

#[tokio::test(start_paused = true)]
async fn shutdown_wins_over_a_ready_tick() {
    let calls = Cell::new(0);

    // The first tick is ready at once, and so is the shutdown.
    run(
        "test",
        NonZeroDuration::from_secs(3600).unwrap(),
        NonZeroDuration::from_secs(60).unwrap(),
        counting_sweep(&calls),
        std::future::ready(()),
    )
    .await;

    assert_eq!(calls.get(), 0);
}

#[tokio::test(start_paused = true)]
async fn retention_past_the_date_range_keeps_everything() {
    let calls = Cell::new(0);

    run(
        "test",
        NonZeroDuration::from_secs(u64::MAX).unwrap(),
        NonZeroDuration::from_secs(60).unwrap(),
        counting_sweep(&calls),
        tokio::time::sleep(Duration::from_secs(150)),
    )
    .await;

    assert_eq!(calls.get(), 0, "no cutoff exists, so nothing is swept");
}
