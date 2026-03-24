use std::sync::Arc;

use chrono::{Duration as ChronoDuration, Utc};

use tracing::{debug, info, warn};
use uuid::Uuid;
use waymark_core_backend::LockClaim;
use waymark_nonzero_duration::NonZeroDuration;

pub struct Params<CoreBackend>
where
    CoreBackend: ?Sized,
{
    pub shutdown_signal: tokio_util::sync::WaitForCancellationFutureOwned,
    pub core_backend: Arc<CoreBackend>,
    pub tracker: super::Tracker,
    pub heartbeat_interval: NonZeroDuration,
    pub lock_uuid: Uuid,
    pub lock_ttl: NonZeroDuration,
}

pub async fn run<CoreBackend>(params: Params<CoreBackend>)
where
    CoreBackend: ?Sized,
    CoreBackend: waymark_core_backend::CoreBackend,
{
    let Params {
        shutdown_signal,
        core_backend,
        tracker,
        heartbeat_interval,
        lock_ttl,
        lock_uuid,
    } = params;

    let mut shutdown_signal = std::pin::pin!(shutdown_signal);
    loop {
        tokio::select! {
            _ = &mut shutdown_signal => {
                info!("lock heartbeat shutdown_signal notified");
                break;
            }
            _ = tokio::time::sleep(heartbeat_interval.get()) => {}
        };

        let instance_ids = tracker.snapshot();
        debug!(count = instance_ids.len(), "lock heartbeat tick");
        if instance_ids.is_empty() {
            continue;
        }

        let lock_expires_at = Utc::now()
            + ChronoDuration::from_std(lock_ttl.get())
                .unwrap_or_else(|_| ChronoDuration::seconds(0));
        debug!(count = instance_ids.len(), "refreshing instance locks");

        if let Err(err) = core_backend
            .refresh_instance_locks(
                LockClaim {
                    lock_uuid,
                    lock_expires_at,
                },
                &instance_ids,
            )
            .await
        {
            warn!(error = %err, "failed to refresh instance locks");
        }
    }
    info!("lock heartbeat exiting");
}
