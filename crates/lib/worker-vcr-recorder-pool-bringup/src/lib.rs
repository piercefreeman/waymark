use either::Either;

pub fn with_vcr_recorder<Pool>(
    pool: Pool,
    recorder: Option<waymark_worker_vcr_recorder::Handle>,
) -> Either<Pool, waymark_worker_vcr_recorder_pool::Pool<Pool>> {
    if let Some(recorder) = recorder {
        Either::Right(waymark_worker_vcr_recorder_pool::Pool {
            wrapped: pool,
            recorder,
        })
    } else {
        Either::Left(pool)
    }
}
