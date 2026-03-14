use std::sync::Arc;
use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};

use tracing::{debug, info, warn};
use waymark_core_backend::LockClaim;

pub struct Params<CoreBackend>
where
    CoreBackend: ?Sized,
{
    pub shutdown_signal: tokio_util::sync::WaitForCancellationFutureOwned,
    pub core_backend: Arc<CoreBackend>,
    pub tracker: super::Tracker,
    pub heartbeat_interval: Duration,
    pub lock_ttl: Duration,
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
    } = params;

    let mut shutdown_signal = std::pin::pin!(shutdown_signal);
    loop {
        tokio::select! {
            _ = &mut shutdown_signal => {
                info!("lock heartbeat shutdown_signal notified");
                break;
            }
            _ = tokio::time::sleep(heartbeat_interval) => {}
        };

        let instance_ids = tracker.snapshot();
        debug!(count = instance_ids.len(), "lock heartbeat tick");
        if instance_ids.is_empty() {
            continue;
        }

        let lock_expires_at = Utc::now()
            + ChronoDuration::from_std(lock_ttl).unwrap_or_else(|_| ChronoDuration::seconds(0));
        debug!(count = instance_ids.len(), "refreshing instance locks");

        if let Err(err) = core_backend
            .refresh_instance_locks(
                LockClaim {
                    lock_uuid: tracker.lock_uuid(),
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
