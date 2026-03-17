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

    let mut first_attempt = true;
    let wait_if_needed = {
        || async move {
            let first_attempt = &mut first_attempt;
            if !*first_attempt {
                if tick_interval > std::time::Duration::ZERO {
                    tokio::time::sleep(tick_interval).await;
                } else {
                    tokio::task::yield_now().await;
                }
            }
            *first_attempt = false;
        }
    };

    tracing::debug!("tick loop starting");

    loop {
        let wait_fut = wait_if_needed();
        let wait_fut = std::pin::pin!(wait_fut);
        let wait_fut =
            waymark_utils_futures::with_cancellation(wait_fut, cancellation_token.cancelled());

        let wait_result = wait_fut.await;

        if wait_result.is_err() || cancellation_token.is_cancelled() {
            tracing::info!("tick loop cancelled");
            break;
        }

        match tick_fn().await {
            std::ops::ControlFlow::Continue(()) => continue,
            std::ops::ControlFlow::Break(outcome) => break outcome,
        }
    }

    tracing::debug!("tick loop exiting");
}
