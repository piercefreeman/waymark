//! Bringup for the scheduler subsystem.
//!
//! Spawns the firing loop as a background task, pinning the concrete
//! wiring: the definition-blob codec is the snapshot-plane rmp codec,
//! and spawned VM ids are freshly minted [`waymark_ids::InstanceId`]s.
//! The scheduler is its own subsystem — it shares nothing with the
//! execution bringup beyond the backend handle.

#![warn(missing_docs)]

use std::num::NonZeroUsize;
use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use waymark_nonzero_duration::NonZeroDuration;

/// Configuration for the scheduler subsystem.
#[derive(Debug, Clone, Copy)]
pub struct Config {
    /// How long the firing loop waits between polls that find less
    /// than a full batch.
    pub poll_interval: NonZeroDuration,

    /// The most due schedules a single poll fetches (and a single
    /// registration statement carries).
    pub max_items: NonZeroUsize,
}

/// Spawn the scheduler's firing loop.
///
/// The task runs until `shutdown_token` is cancelled or the loop stops
/// on persistent backend failure (isolated failures are retried inside
/// the loop; the stop is logged as an error). Any exit of the task
/// cancels `shutdown_token` via a drop guard, so how far the death
/// reaches — nothing, a subsystem, the whole process — is decided by
/// the token the caller passes.
pub fn start<Backend>(
    config: Config,
    backend: Arc<Backend>,
    shutdown_token: CancellationToken,
) -> tokio::task::JoinHandle<()>
where
    Backend: waymark_scheduler_backend::PollDueSchedules,
    Backend: waymark_scheduler_backend::RegisterScheduledVmRuntimes,
    Backend: waymark_scheduler_backend::HasVmId<VmId = waymark_ids::InstanceId>,
    Backend: waymark_scheduler_backend::HasTimestamp<Timestamp = chrono::DateTime<chrono::Utc>>,
    Backend: Send + Sync + 'static,
    <Backend as waymark_scheduler_backend::PollDueSchedules>::Error: std::fmt::Display,
    <Backend as waymark_scheduler_backend::RegisterScheduledVmRuntimes>::Error: std::fmt::Display,
{
    tokio::spawn(async move {
        // Any exit of this task — the loop stopping, a panic, an abort
        // — cancels the token it was given (a no-op when shutdown was
        // already requested). How far that reaches is the caller's
        // choice of token.
        let _shutdown_guard = shutdown_token.clone().drop_guard();
        let params = waymark_scheduler::Params {
            backend,
            codec: Arc::new(waymark_vm_codec_rmp::RmpCodec),
            mint_vm_id_fn: waymark_ids::InstanceId::new_uuid_v4,
            poll_interval: config.poll_interval,
            max_items: config.max_items,
        };
        tokio::select! {
            result = waymark_scheduler::run(params) => {
                // The loop's success type is uninhabited: finishing
                // means failing persistently.
                let Err(err) = result;
                tracing::error!(%err, "scheduler loop stopped on persistent backend failure");
            }
            () = shutdown_token.cancelled() => {
                tracing::info!("scheduler stopped");
            }
        }
    })
}
