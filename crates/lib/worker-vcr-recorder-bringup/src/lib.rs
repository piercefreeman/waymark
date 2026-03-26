use either::Either;

pub fn with_vcr_recorder_pool<Pool>(
    pool: Pool,
    enabled: bool,
) -> Either<Pool, waymark_worker_vcr_recorder::Recorder<Pool>> {
    if enabled {
        Either::Right(waymark_worker_vcr_recorder::Recorder { pool })
    } else {
        Either::Left(pool)
    }
}
