use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use uuid::Uuid;
use waymark_core_backend::{LockClaim, QueuedInstanceBatch};
use waymark_utils_tokio_channel::send_with_stop;

use crate::available_instance_slots;

pub struct Params<CoreBackend>
where
    CoreBackend: ?Sized,
{
    pub shutdown_token: tokio_util::sync::CancellationToken,
    pub core_backend: Arc<CoreBackend>,
    pub available_instance_slots_tracker: Arc<available_instance_slots::Tracker>,
    pub poll_interval: Duration,
    pub lock_uuid: Uuid,
    pub lock_ttl: Duration,
    pub queued_instance_tx: tokio::sync::mpsc::Sender<super::Message>,
}

pub async fn run<CoreBackend>(params: Params<CoreBackend>)
where
    CoreBackend: ?Sized,
    CoreBackend: waymark_core_backend::CoreBackend,
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

    let available_instance_slots_tracker = available_instance_slots_tracker.as_ref();
    let core_backend = core_backend.as_ref();
    let queued_instance_tx = &queued_instance_tx;
    let shutdown_token = &shutdown_token;

    let tick_fn = || async move {
        let available_slots = available_instance_slots_tracker.get();
        let Some(batch_size) = std::num::NonZeroUsize::new(available_slots) else {
            return std::ops::ControlFlow::Continue(());
        };

        let lock_expires_at = Utc::now()
            + chrono::Duration::from_std(lock_ttl).unwrap_or_else(|_| chrono::Duration::seconds(0));
        let batch = core_backend
            .get_queued_instances(
                batch_size.get(), // TODO: switch `get_queued_instances` to `NonZeroUsize`
                LockClaim {
                    lock_uuid,
                    lock_expires_at,
                },
            )
            .await;
        let message = match batch {
            Ok(QueuedInstanceBatch { instances }) => {
                let count = instances.len();
                tracing::debug!(count, "polled queued instances");
                super::Message::Batch { instances }
            }
            Err(err) => super::Message::Error(err),
        };

        let send_result = send_with_stop(
            queued_instance_tx,
            message,
            shutdown_token.cancelled(),
            "instance message",
        )
        .await;
        if send_result.is_err() {
            return std::ops::ControlFlow::Break(());
        }

        std::ops::ControlFlow::Continue(())
    };

    let tick_interval = (!poll_interval.is_zero()).then(|| {
        let mut tick_interval = tokio::time::interval(poll_interval);

        tick_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        tick_interval
    });

    waymark_tick_loop::run(waymark_tick_loop::Params {
        cancellation_token: shutdown_token.clone(),
        tick_fn,
        tick_interval,
    })
    .await
}
