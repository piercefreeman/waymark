//! Generic tick loop.

#![no_std]

#[cfg(feature = "builder")]
mod builder;

#[cfg(feature = "builder")]
pub use self::builder::{Builder, new};

/// Tick loop params.
pub struct Params<TickFn> {
    pub cancellation_token: tokio_util::sync::CancellationToken,
    pub tick_interval: Option<tokio::time::Interval>,
    pub tick_fn: TickFn,
}

/// An error that can originate from the tick loop.
#[derive(Debug, thiserror::Error)]
pub enum Error<TickError> {
    #[error("tick loop cancelled")]
    Cancelled,

    #[error("tick failed: {0}")]
    Tick(TickError),
}

pub async fn run<'f, TickFn, TickError>(
    params: Params<TickFn>,
) -> Result<core::convert::Infallible, Error<TickError>>
where
    TickFn: AsyncFnMut() -> Result<(), TickError> + 'f,
    TickError: core::fmt::Debug,
{
    let Params {
        cancellation_token,
        mut tick_interval,
        mut tick_fn,
    } = params;

    tracing::debug!("tick loop starting");

    let error = loop {
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
            break Error::Cancelled;
        };

        match tick_fn().await {
            Ok(()) => continue,
            Err(err) => break Error::Tick(err),
        }
    };

    tracing::debug!(?error, "tick loop exiting");

    Err(error)
}
