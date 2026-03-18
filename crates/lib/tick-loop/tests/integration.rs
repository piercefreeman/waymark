use std::ops::ControlFlow;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use waymark_tick_loop::{Params, run};

#[tokio::test]
async fn exits_on_break() {
    let count = Arc::new(AtomicUsize::new(0));
    let count2 = count.clone();
    run(Params {
        cancellation_token: CancellationToken::new(),
        tick_interval: Duration::ZERO,
        tick_fn: move || {
            let count = count2.clone();
            async move {
                count.fetch_add(1, Ordering::Relaxed);
                ControlFlow::Break(())
            }
        },
    })
    .await;
    assert_eq!(count.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn tick_fn_called_multiple_times_before_break() {
    let count = Arc::new(AtomicUsize::new(0));
    let count2 = count.clone();
    run(Params {
        cancellation_token: CancellationToken::new(),
        tick_interval: Duration::ZERO,
        tick_fn: move || {
            let count = count2.clone();
            async move {
                let prev = count.fetch_add(1, Ordering::Relaxed);
                if prev < 4 {
                    ControlFlow::Continue(())
                } else {
                    ControlFlow::Break(())
                }
            }
        },
    })
    .await;
    assert_eq!(count.load(Ordering::Relaxed), 5);
}

/// With a very long tick interval, the first tick must still run immediately
/// without waiting. If the loop delayed before the first tick it would
/// exceed the timeout and the test would fail.
#[tokio::test]
async fn first_tick_runs_without_delay() {
    tokio::time::timeout(
        Duration::from_millis(100),
        run(Params {
            cancellation_token: CancellationToken::new(),
            tick_interval: Duration::from_secs(3600),
            tick_fn: || async { ControlFlow::Break(()) },
        }),
    )
    .await
    .expect("first tick should run immediately, not after tick_interval");
}

/// A non-zero tick_interval must delay subsequent ticks. We run exactly two
/// ticks and assert that total elapsed time is at least one full interval.
#[tokio::test]
async fn tick_interval_delays_subsequent_ticks() {
    let interval = Duration::from_millis(10);
    let start = std::time::Instant::now();
    let count = Arc::new(AtomicUsize::new(0));
    let count2 = count.clone();

    run(Params {
        cancellation_token: CancellationToken::new(),
        tick_interval: interval,
        tick_fn: move || {
            let count = count2.clone();
            async move {
                let prev = count.fetch_add(1, Ordering::Relaxed);
                if prev < 1 {
                    ControlFlow::Continue(())
                } else {
                    ControlFlow::Break(())
                }
            }
        },
    })
    .await;

    assert_eq!(count.load(Ordering::Relaxed), 2);
    assert!(
        start.elapsed() >= interval,
        "second tick should be delayed by at least {interval:?}"
    );
}

/// Cancelling the token while the loop is waiting for its tick interval
/// must cause the loop to exit promptly without waiting out the interval.
#[tokio::test]
async fn cancellation_stops_loop_during_interval_wait() {
    let token = CancellationToken::new();
    let count = Arc::new(AtomicUsize::new(0));
    let count2 = count.clone();

    let loop_task = tokio::spawn(run(Params {
        cancellation_token: token.clone(),
        // Long interval so the loop would stall for 60 s without cancellation.
        tick_interval: Duration::from_secs(60),
        tick_fn: move || {
            let count = count2.clone();
            async move {
                count.fetch_add(1, Ordering::Relaxed);
                ControlFlow::Continue(())
            }
        },
    }));

    // Give the first tick time to complete before we cancel.
    tokio::time::sleep(Duration::from_millis(10)).await;
    token.cancel();

    tokio::time::timeout(Duration::from_millis(200), loop_task)
        .await
        .expect("loop should exit promptly after cancellation")
        .expect("loop task");

    assert_eq!(count.load(Ordering::Relaxed), 1);
}
