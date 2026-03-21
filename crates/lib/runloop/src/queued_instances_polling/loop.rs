use std::sync::Arc;

use chrono::Utc;
use uuid::Uuid;
use waymark_core_backend::{LockClaim, poll_queued_instances::Error as _};
use waymark_nonzero_duration::NonZeroDuration;
use waymark_utils_tokio_channel::send_with_stop;

use crate::available_instance_slots;

pub struct Params<CoreBackend, BackendError>
where
    CoreBackend: ?Sized,
{
    pub shutdown_token: tokio_util::sync::CancellationToken,
    pub core_backend: Arc<CoreBackend>,
    pub available_instance_slots_tracker: Arc<available_instance_slots::Tracker>,
    pub poll_interval: Option<NonZeroDuration>,
    pub lock_uuid: Uuid,
    pub lock_ttl: NonZeroDuration,
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

    let tick_fn = {
        let shutdown_token = shutdown_token.clone();
        async move || {
            let available_slots = available_instance_slots_tracker.get();
            let Some(batch_size) = std::num::NonZeroUsize::new(available_slots) else {
                return Ok(());
            };

            let lock_expires_at = Utc::now()
                + chrono::Duration::from_std(lock_ttl.get())
                    .unwrap_or_else(|_| chrono::Duration::seconds(0));
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
                    tracing::debug!(count, "polled queued instances");
                    super::Message::Batch { instances }
                }
                Err(error) => match error.kind() {
                    waymark_core_backend::poll_queued_instances::ErrorKind::NoInstances => {
                        // No instances were obtained.
                        tracing::trace!(?error, "no instances from core backend");
                        super::Message::Pending
                    }
                    waymark_core_backend::poll_queued_instances::ErrorKind::Internal => {
                        super::Message::Error(error)
                    }
                },
            };

            send_with_stop(
                &queued_instance_tx,
                message,
                shutdown_token.cancelled(),
                "instance message",
            )
            .await
        }
    };

    let tick_interval = poll_interval.map(|poll_interval| {
        let mut tick_interval = tokio::time::interval(poll_interval.get());

        tick_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        tick_interval
    });

    let Err(error) = waymark_tick_loop::run(waymark_tick_loop::Params {
        cancellation_token: shutdown_token.clone(),
        tick_fn,
        tick_interval,
    })
    .await;

    tracing::error!(?error, "queued instances loop terminated");
}
