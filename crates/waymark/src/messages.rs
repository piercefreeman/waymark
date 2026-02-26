//! Protocol buffer message types and encoding/decoding utilities.
//!
//! This module wraps the generated protobuf types and provides helper functions
//! for encoding and decoding messages in the worker bridge protocol.

use std::time::Instant;

use once_cell::sync::Lazy;
use prost::Message;
use thiserror::Error;

use waymark_proto::messages as proto;

/// Errors that can occur during message encoding/decoding
#[derive(Debug, Error)]
pub enum MessageError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Failed to decode message: {0}")]
    Decode(#[from] prost::DecodeError),
    #[error("Failed to encode message: {0}")]
    Encode(#[from] prost::EncodeError),
    #[error("Channel closed")]
    ChannelClosed,
}

/// Encode a protobuf message to bytes
pub fn encode_message<M: Message>(msg: &M) -> Vec<u8> {
    msg.encode_to_vec()
}

/// Decode a protobuf message from bytes
pub fn decode_message<M>(bytes: &[u8]) -> Result<M, MessageError>
where
    M: Message + Default,
{
    M::decode(bytes).map_err(MessageError::from)
}

/// Create an ACK envelope for the given delivery_id
pub fn ack_envelope(partition_id: u32, delivery_id: u64) -> proto::Envelope {
    let ack = proto::Ack {
        acked_delivery_id: delivery_id,
    };
    proto::Envelope {
        delivery_id,
        partition_id,
        kind: proto::MessageKind::Ack as i32,
        payload: encode_message(&ack),
    }
}

/// Get monotonic time in nanoseconds since process start.
/// Used for performance measurement.
pub fn now_monotonic_ns() -> u64 {
    static START: Lazy<Instant> = Lazy::new(Instant::now);
    START.elapsed().as_nanos() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_roundtrip() {
        let original = proto::Ack {
            acked_delivery_id: 42,
        };
        let bytes = encode_message(&original);
        let decoded: proto::Ack = decode_message(&bytes).expect("decode");
        assert_eq!(decoded.acked_delivery_id, 42);
    }

    #[test]
    fn test_ack_envelope() {
        let envelope = ack_envelope(1, 100);
        assert_eq!(envelope.delivery_id, 100);
        assert_eq!(envelope.partition_id, 1);
        assert_eq!(envelope.kind, proto::MessageKind::Ack as i32);

        let ack: proto::Ack = decode_message(&envelope.payload).expect("decode ack");
        assert_eq!(ack.acked_delivery_id, 100);
    }

    #[test]
    fn test_monotonic_ns_increases() {
        let t1 = now_monotonic_ns();
        std::thread::sleep(std::time::Duration::from_millis(1));
        let t2 = now_monotonic_ns();
        assert!(t2 > t1);
    }
}
