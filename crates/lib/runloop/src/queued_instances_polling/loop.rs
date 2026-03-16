use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tracing::{debug, info};
use uuid::Uuid;
use waymark_core_backend::LockClaim;
use waymark_utils_tokio_channel::send_with_stop;

use crate::available_instance_slots;

pub struct Params<CoreBackend, BackendError>
where
    CoreBackend: ?Sized,
{
    pub shutdown_token: tokio_util::sync::CancellationToken,
    pub core_backend: Arc<CoreBackend>,
    pub available_instance_slots_tracker: Arc<available_instance_slots::Tracker>,
    pub poll_interval: Duration,
    pub lock_uuid: Uuid,
    pub lock_ttl: Duration,
    pub queued_instance_tx: tokio::sync::mpsc::Sender<super::Message<BackendError>>,
}

pub type BackendErrorFor<T> = <T as waymark_core_backend::CoreBackend>::PollQueuedInstancesError;

pub async fn run<CoreBackend>(params: Params<CoreBackend, BackendErrorFor<CoreBackend>>)
where
    CoreBackend: ?Sized,
    CoreBackend: waymark_core_backend::CoreBackend,
    CoreBackend::PollQueuedInstancesError: core::fmt::Debug,
{
    let Params {
        shutdown_token,
        core_backend,
        available_instance_slots_tracker,
        poll_interval,
        lock_ttl,
        lock_uuid,
        queued_instance_tx,
    } = params;

    loop {
        if shutdown_token.is_cancelled() {
            info!("instance poller stop flag set");
            break;
        }
        let available_slots = available_instance_slots_tracker.get();
        let Some(batch_size) = std::num::NonZeroUsize::new(available_slots) else {
            if poll_interval > Duration::ZERO {
                tokio::time::sleep(poll_interval).await;
            } else {
                tokio::time::sleep(Duration::from_millis(0)).await;
            }
            continue;
        };

        let lock_expires_at = Utc::now()
            + chrono::Duration::from_std(lock_ttl).unwrap_or_else(|_| chrono::Duration::seconds(0));
        let batch = core_backend
            .poll_queued_instances(
                batch_size,
                LockClaim {
                    lock_uuid,
                    lock_expires_at,
                },
            )
            .await;

        let message = match batch {
            Ok(instances) => {
                let count = instances.len();
                debug!(count, "polled queued instances");
                super::Message::Batch { instances }
            }
            Err(waymark_core_backend::PollQueuedInstancesError::Internal(err)) => {
                super::Message::Error(err)
            }
            Err(waymark_core_backend::PollQueuedInstancesError::NoInstances(error)) => {
                // No instances were obtained.
                tracing::trace!(?error, "no instances from core backend");
                super::Message::Pending
            }
        };

        let send_result = send_with_stop(
            &queued_instance_tx,
            message,
            shutdown_token.cancelled(),
            "instance message",
        )
        .await;
        if send_result.is_err() {
            break;
        }

        if poll_interval > Duration::ZERO {
            tokio::time::sleep(poll_interval).await;
        } else {
            tokio::time::sleep(Duration::from_millis(0)).await;
        }
    }
    info!("instance poller exiting");
}
