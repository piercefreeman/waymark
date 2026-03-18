//! Generic tick loop.

pub struct Params<TickFn> {
    pub cancellation_token: tokio_util::sync::CancellationToken,
    pub tick_interval: Option<tokio::time::Interval>,
    pub tick_fn: TickFn,
}

pub async fn run<TickFn, TickFut>(params: Params<TickFn>)
where
    TickFn: FnMut() -> TickFut,
    TickFut: Future<Output = std::ops::ControlFlow<()>>,
{
    let Params {
        cancellation_token,
        mut tick_interval,
        mut tick_fn,
    } = params;

    tracing::debug!("tick loop starting");

    loop {
        let wait_fut = {
            let tick_interval = tick_interval.as_mut();
            async move {
                if let Some(tick_interval) = tick_interval {
                    tick_interval.tick().await;
                }
            }
        };

        let Some(_) = cancellation_token.run_until_cancelled(wait_fut).await else {
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
