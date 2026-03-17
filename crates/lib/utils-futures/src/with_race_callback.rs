use futures_util::future::Either;

pub async fn with_race_callback<PayloadFut, RaceFut>(
    payload_fut: PayloadFut,
    race_fut: RaceFut,
    callback_fn: impl FnOnce(),
) -> PayloadFut::Output
where
    PayloadFut: Future + Unpin,
    RaceFut: Future<Output = ()>,
{
    let race_fut = std::pin::pin!(race_fut);

    let either = futures_util::future::select(payload_fut, race_fut).await;
    match either {
        Either::Left((payload_outcome, _)) => payload_outcome,
        Either::Right(((), payload_fut)) => {
            callback_fn();
            payload_fut.await
        }
    }
}
