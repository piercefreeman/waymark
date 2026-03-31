use std::sync::Arc;

use chrono::Utc;

use waymark_core_backend::{LockClaim, poll_queued_instances::Error as _};
use waymark_ids::LockId;
use waymark_nonzero_duration::NonZeroDuration;
use waymark_utils_tokio_channel::send_with_stop;

use crate::available_instance_slots;

pub struct Params<CoreBackend, BackendError>
where
    CoreBackend: ?Sized,
{
    pub shutdown_token: tokio_util::sync::CancellationToken,
    pub core_backend: Arc<CoreBackend>,
    pub available_instance_slots_reader: available_instance_slots::Reader,
    pub poll_interval: Option<NonZeroDuration>,
    pub lock_uuid: LockId,
    pub lock_ttl: NonZeroDuration,
    pub queued_instance_tx: tokio::sync::mpsc::Sender<super::Message<BackendError>>,
}

pub type BackendErrorFor<T> = <T as waymark_core_backend::CoreBackend>::PollQueuedInstancesError;

#[derive(thiserror::Error)]
enum Error<T> {
    #[error("waiting for available instance slots: {0}")]
    WaitForAvailableInstanceSlots(available_instance_slots::TrackerGoneError),

    #[error("sending message: {0}")]
    SendWithStop(send_with_stop::Error<T>),
}

pub async fn run<CoreBackend>(params: Params<CoreBackend, BackendErrorFor<CoreBackend>>)
where
    CoreBackend: ?Sized,
    CoreBackend: waymark_core_backend::CoreBackend,
    CoreBackend::PollQueuedInstancesError: core::fmt::Debug,
{
    let Params {
        shutdown_token,
        core_backend,
        mut available_instance_slots_reader,
        poll_interval,
        lock_ttl,
        lock_uuid,
        queued_instance_tx,
    } = params;

    let tick_fn = {
        let shutdown_token = shutdown_token.clone();
        async move || {
            let batch_size = available_instance_slots_reader
                .wait_available()
                .await
                .map_err(Error::WaitForAvailableInstanceSlots)?;

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
            .map_err(Error::SendWithStop)
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

impl<T> core::fmt::Debug for Error<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WaitForAvailableInstanceSlots(arg0) => f
                .debug_tuple("WaitForAvailableInstanceSlots")
                .field(arg0)
                .finish(),
            Self::SendWithStop(arg0) => f.debug_tuple("SendWithStop").field(arg0).finish(),
        }
    }
}
