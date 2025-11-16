use std::time::Instant;

use bytes::BytesMut;
use once_cell::sync::Lazy;
use prost::Message;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/carabiner.messages.rs"));
}

pub const FRAME_HEADER_LEN: usize = 4;

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

pub async fn read_envelope<R>(reader: &mut R) -> Result<proto::Envelope, MessageError>
where
    R: AsyncRead + Unpin,
{
    let mut header = [0u8; FRAME_HEADER_LEN];
    reader.read_exact(&mut header).await?;
    let len = u32::from_le_bytes(header) as usize;
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).await?;
    proto::Envelope::decode(&*buf).map_err(MessageError::from)
}

pub async fn write_envelope<W>(
    writer: &mut W,
    envelope: &proto::Envelope,
) -> Result<(), MessageError>
where
    W: AsyncWrite + Unpin,
{
    let mut buf = BytesMut::with_capacity(256);
    envelope.encode(&mut buf)?;
    let len = (buf.len() as u32).to_le_bytes();
    writer.write_all(&len).await?;
    writer.write_all(&buf).await?;
    writer.flush().await?;
    Ok(())
}

pub fn encode_message<M: Message>(msg: &M) -> Vec<u8> {
    msg.encode_to_vec()
}

pub fn decode_message<M>(bytes: &[u8]) -> Result<M, MessageError>
where
    M: Message + Default,
{
    M::decode(bytes).map_err(MessageError::from)
}

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

pub fn now_monotonic_ns() -> u64 {
    static START: Lazy<Instant> = Lazy::new(Instant::now);
    START.elapsed().as_nanos() as u64
}
