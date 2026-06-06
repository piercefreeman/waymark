//! MessagePack codec.

#![warn(missing_docs)]

use rmp_serde::config::DefaultConfig;
use waymark_vm_codec_core::{DeserializerProvider, SerializerProvider};

/// MessagePack codec backed by [`rmp_serde`].
#[derive(Debug, Clone, Copy, Default)]
pub struct RmpCodec;

impl SerializerProvider for RmpCodec {
    type Serializer<'buf>
        = &'buf mut rmp_serde::Serializer<&'buf mut Vec<u8>, DefaultConfig>
    where
        Self: 'buf;

    type Ok = ();

    type Error = rmp_serde::encode::Error;

    fn with_serializer<F, R>(&self, buffer: &mut Vec<u8>, f: F) -> R
    where
        for<'buf> F: FnOnce(Self::Serializer<'buf>) -> R,
    {
        let mut ser = rmp_serde::Serializer::new(&mut *buffer);
        f(&mut ser)
    }
}

impl DeserializerProvider for RmpCodec {
    type Deserializer<'de>
        = &'de mut rmp_serde::Deserializer<rmp_serde::decode::ReadReader<&'de [u8]>>
    where
        Self: 'de;

    type Error = rmp_serde::decode::Error;

    fn with_deserializer<F, T>(&self, data: &[u8], f: F) -> Result<T, Self::Error>
    where
        for<'de> F: FnOnce(Self::Deserializer<'de>) -> Result<T, Self::Error>,
    {
        let mut de = rmp_serde::Deserializer::new(data);
        f(&mut de)
    }
}
