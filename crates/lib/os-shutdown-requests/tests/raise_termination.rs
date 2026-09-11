//! Raises SIGTERM at the test process itself and checks the latching
//! contract against a held receiver.
//!
//! One test function: raises are process-wide, so a second test in this
//! binary would deliver into this one's receiver.

#![cfg(unix)]

fn raise() {
    nix::sys::signal::raise(nix::sys::signal::Signal::SIGTERM).unwrap();
}

async fn expect_request(receiver: &mut waymark_os_shutdown_requests::termination::Receiver) {
    tokio::time::timeout(std::time::Duration::from_secs(5), receiver.recv())
        .await
        .expect("request not delivered in time");
}

async fn expect_quiet(receiver: &mut waymark_os_shutdown_requests::termination::Receiver) {
    let pending =
        tokio::time::timeout(std::time::Duration::from_millis(100), receiver.recv()).await;
    assert!(pending.is_err(), "an unraised request was delivered");
}

#[tokio::test]
async fn requests_between_waits_are_latched() {
    let mut receiver = waymark_os_shutdown_requests::termination::install().unwrap();
    expect_quiet(&mut receiver).await;

    // A request with nobody waiting is latched for the next wait.
    raise();
    expect_request(&mut receiver).await;
    expect_quiet(&mut receiver).await;

    // A request landing after a completion and well before the next wait,
    // long enough for the driver to have broadcast it, is not lost. This
    // is the window a fresh per-wait subscription would miss.
    raise();
    expect_request(&mut receiver).await;
    raise();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    expect_request(&mut receiver).await;
    expect_quiet(&mut receiver).await;

    // A burst with nobody waiting coalesces: at least one completion,
    // never more than the requests, then quiet.
    raise();
    raise();
    raise();
    expect_request(&mut receiver).await;
    let mut extra = 0;
    while tokio::time::timeout(std::time::Duration::from_millis(100), receiver.recv())
        .await
        .is_ok()
    {
        extra += 1;
    }
    assert!(extra <= 2, "more completions than requests: {}", extra + 1);
    expect_quiet(&mut receiver).await;

    // A request with a receiver waiting wakes it.
    let waiter = tokio::spawn(async move {
        expect_request(&mut receiver).await;
        receiver
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    raise();
    let mut receiver = waiter.await.unwrap();
    expect_quiet(&mut receiver).await;

    // A second, independent receiver sees requests too.
    let mut second = waymark_os_shutdown_requests::termination::install().unwrap();
    raise();
    expect_request(&mut receiver).await;
    expect_request(&mut second).await;
}
