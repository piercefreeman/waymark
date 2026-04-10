use either::Either;

pub fn with_vcr_recorder<Pool>(
    pool: Pool,
    recorder: Option<waymark_vcr_recorder::pool::Handle>,
) -> Either<Pool, waymark_vcr_recorder_worker_pool::Pool<Pool>> {
    if let Some(recorder) = recorder {
        Either::Right(waymark_vcr_recorder_worker_pool::Pool {
            inner: pool,
            recorder,
        })
    } else {
        Either::Left(pool)
    }
}
