use std::cell::{Cell, RefCell};
use std::convert::Infallible;
use std::time::Duration;

use waymark_nonzero_duration::NonZeroDuration;

use super::*;

/// A sweep recording every cutoff it is given and deleting nothing.
fn recording_sweep(
    cutoffs: &RefCell<Vec<chrono::DateTime<chrono::Utc>>>,
) -> impl Fn(chrono::DateTime<chrono::Utc>) -> std::future::Ready<Result<u64, Infallible>> {
    move |cutoff| {
        cutoffs.borrow_mut().push(cutoff);
        std::future::ready(Ok(0))
    }
}

/// A sweep failing its first call and succeeding after, counting them all.
fn failing_once_sweep(
    calls: &Cell<u32>,
) -> impl Fn(chrono::DateTime<chrono::Utc>) -> std::future::Ready<Result<u64, &'static str>> {
    move |_cutoff| {
        calls.set(calls.get() + 1);
        if calls.get() == 1 {
            std::future::ready(Err("the database is away"))
        } else {
            std::future::ready(Ok(1))
        }
    }
}

#[tokio::test(start_paused = true)]
async fn sweeps_once_per_interval_until_shutdown_with_the_cutoff_retention_ago() {
    let cutoffs = RefCell::new(Vec::new());
    let retention = chrono::TimeDelta::hours(1);

    let before = chrono::Utc::now();
    run(
        "test",
        NonZeroDuration::from_secs(3600).unwrap(),
        NonZeroDuration::from_secs(60).unwrap(),
        recording_sweep(&cutoffs),
        tokio::time::sleep(Duration::from_secs(150)),
    )
    .await;
    let after = chrono::Utc::now();

    let cutoffs = cutoffs.into_inner();
    assert_eq!(cutoffs.len(), 3, "the ticks at 0, 60 and 120 seconds");
    // The paused clock makes the run take microseconds of real time, so
    // the bracket is tight: every cutoff is `retention` before a `now`
    // taken during the run.
    for cutoff in cutoffs {
        assert!(cutoff >= before - retention, "{cutoff} vs {before}");
        assert!(cutoff <= after - retention, "{cutoff} vs {after}");
    }
}

#[tokio::test(start_paused = true)]
async fn shutdown_wins_over_a_ready_tick() {
    let cutoffs = RefCell::new(Vec::new());

    // The first tick is ready at once, and so is the shutdown.
    run(
        "test",
        NonZeroDuration::from_secs(3600).unwrap(),
        NonZeroDuration::from_secs(60).unwrap(),
        recording_sweep(&cutoffs),
        std::future::ready(()),
    )
    .await;

    assert!(cutoffs.into_inner().is_empty());
}

#[tokio::test(start_paused = true)]
async fn retention_past_the_date_range_keeps_everything() {
    let cutoffs = RefCell::new(Vec::new());

    run(
        "test",
        NonZeroDuration::from_secs(u64::MAX).unwrap(),
        NonZeroDuration::from_secs(60).unwrap(),
        recording_sweep(&cutoffs),
        tokio::time::sleep(Duration::from_secs(150)),
    )
    .await;

    assert!(
        cutoffs.into_inner().is_empty(),
        "no cutoff exists, so nothing is swept"
    );
}

#[tokio::test(start_paused = true)]
async fn a_failed_sweep_is_retried_at_the_next_tick() {
    let calls = Cell::new(0);

    run(
        "test",
        NonZeroDuration::from_secs(3600).unwrap(),
        NonZeroDuration::from_secs(60).unwrap(),
        failing_once_sweep(&calls),
        tokio::time::sleep(Duration::from_secs(150)),
    )
    .await;

    assert_eq!(calls.get(), 3, "the failure at 0 seconds, then 60 and 120");
}
