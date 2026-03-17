use futures_util::future::Either;

pub async fn with_cancellation<PayloadFut, CancellationSignalFut>(
    payload_fut: PayloadFut,
    cancellation_signal_fut: CancellationSignalFut,
) -> Result<PayloadFut::Output, CancelledError<PayloadFut>>
where
    PayloadFut: Future + Unpin,
    CancellationSignalFut: Future<Output = ()>,
{
    let cancellation_signal_fut = std::pin::pin!(cancellation_signal_fut);

    let either = futures_util::future::select(payload_fut, cancellation_signal_fut).await;
    match either {
        Either::Left((payload_outcome, _)) => Ok(payload_outcome),
        Either::Right(((), payload_fut)) => Err(CancelledError { payload_fut }),
    }
}

#[derive(Debug, thiserror::Error)]
#[error("cancelled")]
pub struct CancelledError<PayloadFut> {
    /// The payload future provided in the error in case the caller decides
    /// to resume polling it.
    /// Can also be dropped for actual cancellation.
    pub payload_fut: PayloadFut,
}
