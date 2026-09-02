//! A periodic retention sweep: on a fixed cadence, delete what is past
//! its retention and account for the outcome.

#![warn(missing_docs)]

use waymark_nonzero_duration::NonZeroDuration;

/// The retention sweep task: every `sweep_interval` it calls `sweep`
/// with the cutoff `retention` ago, until `shutdown` resolves. `sweep`
/// returns how many items it deleted; `name` labels this instance's log
/// records.
///
/// Failures are logged and retried at the next sweep; a `retention` too
/// large for the cutoff arithmetic keeps everything.
pub async fn run<Sweep, SweepFuture, SweepError, Shutdown>(
    name: &'static str,
    retention: NonZeroDuration,
    sweep_interval: NonZeroDuration,
    sweep: Sweep,
    shutdown: Shutdown,
) where
    Sweep: Fn(chrono::DateTime<chrono::Utc>) -> SweepFuture,
    SweepFuture: Future<Output = Result<u64, SweepError>>,
    SweepError: std::fmt::Debug,
    Shutdown: Future<Output = ()>,
{
    let retention = chrono::TimeDelta::from_std(retention.get()).unwrap_or(chrono::TimeDelta::MAX);
    let mut ticks = tokio::time::interval(sweep_interval.get());
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut shutdown = std::pin::pin!(shutdown);
    loop {
        tokio::select! {
            biased;
            () = &mut shutdown => break,
            _ = ticks.tick() => {
                let Some(cutoff) = chrono::Utc::now().checked_sub_signed(retention) else {
                    continue;
                };
                match sweep(cutoff).await {
                    Ok(0) => {}
                    Ok(deleted) => tracing::debug!(name, deleted, "retention swept"),
                    Err(error) => tracing::warn!(name, ?error, "retention sweep failed"),
                }
            }
        }
    }
}
