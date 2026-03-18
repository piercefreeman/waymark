//! Generic tick loop.

pub struct Params<TickFn> {
    pub cancellation_token: tokio_util::sync::CancellationToken,
    pub tick_interval: std::time::Duration,
    pub tick_fn: TickFn,
}

pub async fn run<TickFn, TickFut>(params: Params<TickFn>)
where
    TickFn: FnMut() -> TickFut,
    TickFut: Future<Output = std::ops::ControlFlow<()>>,
{
    let Params {
        cancellation_token,
        tick_interval,
        mut tick_fn,
    } = params;

    tracing::debug!("tick loop starting");

    let mut first_attempt = true;

    loop {
        // On the first iteration skip the wait entirely; subsequent iterations
        // respect the configured interval. Compute this BEFORE the async block
        // so that `async move` captures the correct per-iteration boolean (a
        // plain `bool` copy rather than a mutable borrow that would be aliased
        // across futures).
        let should_wait = !first_attempt;
        first_attempt = false;

        let wait_fut = async move {
            if should_wait {
                if tick_interval > std::time::Duration::ZERO {
                    tokio::time::sleep(tick_interval).await;
                } else {
                    tokio::task::yield_now().await;
                }
            }
        };

        let Some(()) = cancellation_token.run_until_cancelled(wait_fut).await else {
            tracing::info!("tick loop cancelled");
            break;
        };

        match tick_fn().await {
            std::ops::ControlFlow::Continue(()) => continue,
            std::ops::ControlFlow::Break(outcome) => break outcome,
        }
    }

    tracing::debug!("tick loop exiting");
}
