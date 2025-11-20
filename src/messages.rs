use std::{io::ErrorKind, time::Instant};

use bytes::BytesMut;
use once_cell::sync::Lazy;
use prost::Message;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub mod proto {
    tonic::include_proto!("rappel.messages");
}

pub const FRAME_HEADER_LEN: usize = 4;
pub const MAX_FRAME_LEN: usize = 8 * 1024 * 1024; // 8 MiB
pub const MAX_LOG_LINE_LEN: usize = 8 * 1024; // 8 KiB

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
    loop {
        reader.read_exact(&mut header).await?;
        let len = u32::from_le_bytes(header) as usize;

        if len == 0 || len > MAX_FRAME_LEN {
            discard_until_newline(reader).await?;
            continue;
        }

        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf).await?;
        return proto::Envelope::decode(&*buf).map_err(MessageError::from);
    }
}

async fn discard_until_newline<R>(reader: &mut R) -> Result<(), MessageError>
where
    R: AsyncRead + Unpin,
{
    for _ in 0..MAX_LOG_LINE_LEN {
        match reader.read_u8().await {
            Ok(byte) => {
                if byte == b'\n' {
                    break;
                }
            }
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
            Err(err) => return Err(err.into()),
        }
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;
    use tokio::io::{self, AsyncWriteExt, BufReader};

    fn sample_envelope(delivery_id: u64) -> proto::Envelope {
        let ack = proto::Ack {
            acked_delivery_id: delivery_id,
        };
        proto::Envelope {
            delivery_id,
            partition_id: 0,
            kind: proto::MessageKind::Ack as i32,
            payload: encode_message(&ack),
        }
    }

    fn encode_frame(envelope: &proto::Envelope) -> Vec<u8> {
        let mut payload = Vec::new();
        envelope.encode(&mut payload).expect("encode envelope");
        let mut frame = Vec::with_capacity(FRAME_HEADER_LEN + payload.len());
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(&payload);
        frame
    }

    async fn write_sequence(
        chunks: Vec<Vec<u8>>,
    ) -> (BufReader<io::DuplexStream>, tokio::task::JoinHandle<()>) {
        let (mut writer, reader) = tokio::io::duplex(16 * 1024);
        let handle = tokio::spawn(async move {
            for chunk in chunks {
                writer.write_all(&chunk).await.unwrap();
            }
            writer.shutdown().await.unwrap();
        });
        (BufReader::new(reader), handle)
    }

    #[tokio::test]
    async fn read_envelope_parses_clean_frames() {
        let frame = encode_frame(&sample_envelope(1));
        let (mut reader, handle) = write_sequence(vec![frame]).await;
        let env = read_envelope(&mut reader).await.expect("frame");
        assert_eq!(env.delivery_id, 1);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn read_envelope_skips_leading_logs() {
        let frame = encode_frame(&sample_envelope(2));
        let (mut reader, handle) =
            write_sequence(vec![b"[worker] booting\n".to_vec(), frame]).await;
        let env = read_envelope(&mut reader).await.expect("frame after logs");
        assert_eq!(env.delivery_id, 2);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn read_envelope_skips_interleaved_logs() {
        let frame1 = encode_frame(&sample_envelope(3));
        let frame2 = encode_frame(&sample_envelope(4));
        let (mut reader, handle) =
            write_sequence(vec![frame1, b"log line\n".to_vec(), frame2]).await;
        let env1 = read_envelope(&mut reader).await.expect("frame 1");
        assert_eq!(env1.delivery_id, 3);
        let env2 = read_envelope(&mut reader).await.expect("frame 2");
        assert_eq!(env2.delivery_id, 4);
        handle.await.unwrap();
    }
}
