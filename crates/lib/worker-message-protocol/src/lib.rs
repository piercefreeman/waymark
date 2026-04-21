//! Typed protocol wrapper for request/response messaging with a connected worker.

#![warn(missing_docs)]

use std::{
    collections::HashMap,
    sync::{Arc, atomic::AtomicU64},
};

use prost::Message as _;
use uuid::Uuid;
use waymark_proto::messages as proto;
use waymark_worker_metrics::RoundTripMetrics;

/// Channels for communicating with a connected worker.
pub struct Channels {
    /// Send actions to the worker
    pub to_worker: tokio::sync::mpsc::Sender<proto::Envelope>,

    /// Receive results from the worker
    pub from_worker: tokio::sync::mpsc::Receiver<proto::Envelope>,
}

/// Internal state shared between worker sender and reader tasks.
#[derive(Debug, Default)]
struct SharedState {
    /// Pending ACK receivers, keyed by delivery_id
    pub pending_acks: HashMap<u64, tokio::sync::oneshot::Sender<std::time::Instant>>,

    /// Pending result receivers, keyed by delivery_id
    pub pending_responses:
        HashMap<u64, tokio::sync::oneshot::Sender<(proto::ActionResult, std::time::Instant)>>,
}

/// Protocol sender for action dispatch requests.
///
/// A `Sender` is cheap to hold and can be reused for many dispatches over the
/// same worker channel pair initialized by [`setup`].
pub struct Sender {
    to_worker: tokio::sync::mpsc::Sender<proto::Envelope>,
    next_delivery: AtomicU64,
    shared: Arc<tokio::sync::Mutex<SharedState>>,
}

/// Builds protocol state from raw worker channels.
///
/// Returns:
/// - A [`Sender`] used by callers to dispatch actions.
/// - A future that must be continuously polled (typically via `tokio::spawn`) to
///   process incoming worker messages and resolve pending requests.
///
/// The returned future exits when the worker receive channel closes.
pub fn setup(channels: Channels) -> (Sender, impl Future<Output = ()>) {
    let Channels {
        to_worker,
        from_worker,
    } = channels;

    // Set up shared state and spawn reader task
    let shared = Arc::new(tokio::sync::Mutex::new(SharedState::default()));
    let loop_fut = {
        let shared = Arc::clone(&shared);
        async move {
            if let Err(err) = r#loop(from_worker, shared).await {
                tracing::error!(?err, "worker message protocol loop exited");
            }
        }
    };

    let sender = Sender {
        to_worker,
        shared,
        next_delivery: AtomicU64::new(1),
    };

    (sender, loop_fut)
}

/// Transport-level errors produced by low-level envelope handling.
#[derive(Debug, thiserror::Error)]
pub enum MessageError {
    /// Underlying I/O failure while reading or writing protocol data.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Failed to decode a protobuf payload from an envelope.
    #[error("failed to decode message: {0}")]
    Decode(#[from] prost::DecodeError),

    /// Failed to encode a protobuf message into an envelope payload.
    #[error("failed to encode message: {0}")]
    Encode(#[from] prost::EncodeError),

    /// Worker transport channel closed before completion.
    #[error("channel closed")]
    ChannelClosed,
}

/// Errors returned by [`Sender::send_action`].
#[derive(Debug, thiserror::Error)]
pub enum SendActionError {
    /// Worker transport channel closed before ACK or result was received.
    #[error("channel closed")]
    ChannelClosed,
}

/// Background task that reads messages from the worker.
async fn r#loop(
    mut from_worker: tokio::sync::mpsc::Receiver<proto::Envelope>,
    shared: Arc<tokio::sync::Mutex<SharedState>>,
) -> Result<(), MessageError> {
    while let Some(envelope) = from_worker.recv().await {
        let kind =
            proto::MessageKind::try_from(envelope.kind).unwrap_or(proto::MessageKind::Unspecified);

        match kind {
            proto::MessageKind::Ack => {
                let ack = proto::Ack::decode(envelope.payload.as_slice())?;
                let mut guard = shared.lock().await;
                if let Some(sender) = guard.pending_acks.remove(&ack.acked_delivery_id) {
                    let _ = sender.send(std::time::Instant::now());
                } else {
                    tracing::warn!(delivery = ack.acked_delivery_id, "unexpected ACK");
                }
            }
            proto::MessageKind::ActionResult => {
                let response = proto::ActionResult::decode(envelope.payload.as_slice())?;
                let mut guard = shared.lock().await;
                if let Some(sender) = guard.pending_responses.remove(&envelope.delivery_id) {
                    let _ = sender.send((response, std::time::Instant::now()));
                } else {
                    tracing::warn!(delivery = envelope.delivery_id, "orphan response");
                }
            }
            proto::MessageKind::Heartbeat => {
                tracing::trace!(delivery = envelope.delivery_id, "heartbeat");
            }
            other => {
                tracing::warn!(?other, "unhandled message kind");
            }
        }
    }

    Ok(())
}

/// Payload for dispatching an action to a worker.
#[derive(Debug, Clone)]
pub struct ActionDispatchPayload {
    /// Unique action identifier
    pub action_id: String,

    /// Workflow instance this action belongs to
    pub instance_id: String,

    /// Sequence number within the instance
    pub sequence: u32,

    /// Name of the action function to call
    pub action_name: String,

    /// Python module containing the action
    pub module_name: String,

    /// Keyword arguments for the action
    pub kwargs: proto::WorkflowArguments,

    /// Timeout in seconds (0 = no timeout)
    pub timeout_seconds: u32,

    /// Maximum retry attempts
    pub max_retries: u32,

    /// Current attempt number
    pub attempt_number: u32,

    /// Dispatch token for correlation
    pub dispatch_token: Uuid,
}

impl Sender {
    /// Send an action to the worker and wait for the result.
    ///
    /// This method:
    /// 1. Allocates a delivery ID
    /// 2. Creates channels for ACK and response
    /// 3. Sends the action dispatch
    /// 4. Waits for ACK (immediate)
    /// 5. Waits for result (after execution)
    /// 6. Returns metrics including latencies
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The worker channel is closed (worker crashed)
    /// - Response decoding fails
    pub async fn send_action(
        &self,
        dispatch: ActionDispatchPayload,
    ) -> Result<RoundTripMetrics, SendActionError> {
        let delivery_id = self
            .next_delivery
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let send_instant = std::time::Instant::now();

        tracing::trace!(
            action_id = %dispatch.action_id,
            instance_id = %dispatch.instance_id,
            sequence = dispatch.sequence,
            module = %dispatch.module_name,
            function = %dispatch.action_name,
            delivery_id,
            "sending action to worker"
        );

        // Create channels for receiving ACK and response
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();

        // Register pending requests
        {
            let mut shared = self.shared.lock().await;
            shared.pending_acks.insert(delivery_id, ack_tx);
            shared.pending_responses.insert(delivery_id, response_tx);
        }

        // Build and send the dispatch envelope
        let command = proto::ActionDispatch {
            action_id: dispatch.action_id.clone(),
            instance_id: dispatch.instance_id.clone(),
            sequence: dispatch.sequence,
            action_name: dispatch.action_name.clone(),
            module_name: dispatch.module_name.clone(),
            kwargs: Some(dispatch.kwargs.clone()),
            timeout_seconds: Some(dispatch.timeout_seconds),
            max_retries: Some(dispatch.max_retries),
            attempt_number: Some(dispatch.attempt_number),
            dispatch_token: Some(dispatch.dispatch_token.to_string()),
        };

        let envelope = proto::Envelope {
            delivery_id,
            partition_id: 0,
            kind: proto::MessageKind::ActionDispatch as i32,
            payload: command.encode_to_vec(),
        };

        self.send_envelope(envelope)
            .await
            .map_err(|_| SendActionError::ChannelClosed)?;

        // Wait for ACK (should be immediate)
        let ack_instant = ack_rx.await.map_err(|_| SendActionError::ChannelClosed)?;

        // Wait for the actual response (after execution)
        let (response, response_instant) = response_rx
            .await
            .map_err(|_| SendActionError::ChannelClosed)?;

        // Calculate metrics
        let ack_latency = ack_instant
            .checked_duration_since(send_instant)
            .unwrap_or_default();
        let round_trip = response_instant
            .checked_duration_since(send_instant)
            .unwrap_or_default();
        let worker_duration = std::time::Duration::from_nanos(
            response
                .worker_end_ns
                .saturating_sub(response.worker_start_ns),
        );

        tracing::trace!(
            action_id = %dispatch.action_id,
            ack_latency_us = ack_latency.as_micros(),
            round_trip_ms = round_trip.as_millis(),
            worker_duration_ms = worker_duration.as_millis(),
            success = response.success,
            "action completed"
        );

        Ok(RoundTripMetrics {
            action_id: dispatch.action_id,
            instance_id: dispatch.instance_id,
            delivery_id,
            sequence: dispatch.sequence,
            ack_latency,
            round_trip,
            worker_duration,
            response_payload: response
                .payload
                .as_ref()
                .map(|payload| payload.encode_to_vec())
                .unwrap_or_default(),
            success: response.success,
            dispatch_token: response
                .dispatch_token
                .as_ref()
                .and_then(|token| Uuid::parse_str(token).ok()),
            error_type: response.error_type,
            error_message: response.error_message,
        })
    }

    /// Send an envelope to the worker.
    async fn send_envelope(
        &self,
        envelope: proto::Envelope,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<proto::Envelope>> {
        self.to_worker.send(envelope).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixed_dispatch_token() -> Uuid {
        Uuid::parse_str("11111111-2222-4333-8444-555555555555").expect("valid fixed UUID for tests")
    }

    fn sample_dispatch(dispatch_token: Uuid) -> ActionDispatchPayload {
        ActionDispatchPayload {
            action_id: "action-1".to_string(),
            instance_id: "instance-1".to_string(),
            sequence: 42,
            action_name: "do_work".to_string(),
            module_name: "workers.demo".to_string(),
            kwargs: proto::WorkflowArguments::default(),
            timeout_seconds: 30,
            max_retries: 3,
            attempt_number: 1,
            dispatch_token,
        }
    }

    #[tokio::test]
    async fn send_action_round_trip_returns_metrics() {
        let (to_worker_tx, mut to_worker_rx) = tokio::sync::mpsc::channel(4);
        let (from_worker_tx, from_worker_rx) = tokio::sync::mpsc::channel(4);

        let (sender, protocol_loop) = setup(Channels {
            to_worker: to_worker_tx,
            from_worker: from_worker_rx,
        });
        let protocol_loop_handle = tokio::spawn(protocol_loop);

        let dispatch_token = fixed_dispatch_token();
        let expected_dispatch = sample_dispatch(dispatch_token);

        let worker_handle = tokio::spawn(async move {
            let envelope = to_worker_rx
                .recv()
                .await
                .expect("receive action dispatch envelope");
            assert_eq!(
                envelope.kind,
                proto::MessageKind::ActionDispatch as i32,
                "must send action-dispatch envelope"
            );

            let command = proto::ActionDispatch::decode(envelope.payload.as_slice())
                .expect("decode action dispatch payload");
            assert_eq!(command.action_id, "action-1");
            assert_eq!(command.instance_id, "instance-1");
            assert_eq!(command.sequence, 42);

            let ack = proto::Ack {
                acked_delivery_id: envelope.delivery_id,
            };
            from_worker_tx
                .send(proto::Envelope {
                    delivery_id: envelope.delivery_id,
                    partition_id: 0,
                    kind: proto::MessageKind::Ack as i32,
                    payload: ack.encode_to_vec(),
                })
                .await
                .expect("send ack envelope");

            let result = proto::ActionResult {
                success: true,
                worker_start_ns: 1_000,
                worker_end_ns: 4_000,
                dispatch_token: Some(dispatch_token.to_string()),
                ..Default::default()
            };
            from_worker_tx
                .send(proto::Envelope {
                    delivery_id: envelope.delivery_id,
                    partition_id: 0,
                    kind: proto::MessageKind::ActionResult as i32,
                    payload: result.encode_to_vec(),
                })
                .await
                .expect("send action result envelope");
        });

        let metrics = sender
            .send_action(expected_dispatch)
            .await
            .expect("send_action should succeed");

        assert_eq!(metrics.action_id, "action-1");
        assert_eq!(metrics.instance_id, "instance-1");
        assert_eq!(metrics.sequence, 42);
        assert!(metrics.success);
        assert_eq!(metrics.worker_duration.as_nanos(), 3_000);
        assert_eq!(metrics.dispatch_token, Some(dispatch_token));

        worker_handle.await.expect("worker task should finish");
        drop(sender);
        protocol_loop_handle
            .await
            .expect("protocol loop task should finish");
    }

    #[tokio::test]
    async fn send_action_fails_when_worker_channel_closed() {
        let (to_worker_tx, to_worker_rx) = tokio::sync::mpsc::channel(1);
        let (from_worker_tx, from_worker_rx) = tokio::sync::mpsc::channel(1);
        drop(to_worker_rx);

        let (sender, protocol_loop) = setup(Channels {
            to_worker: to_worker_tx,
            from_worker: from_worker_rx,
        });
        let protocol_loop_handle = tokio::spawn(protocol_loop);

        let err = sender
            .send_action(sample_dispatch(fixed_dispatch_token()))
            .await
            .expect_err("send_action should fail when worker channel is closed");

        assert!(matches!(err, SendActionError::ChannelClosed));

        drop(sender);
        drop(from_worker_tx);
        protocol_loop_handle
            .await
            .expect("protocol loop task should finish");
    }
}
