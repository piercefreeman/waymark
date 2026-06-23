//! Blanket `Arc<T>` implementations for codec traits.

use std::sync::Arc;

use crate::{DeserializerProvider, SerializerProvider};

impl<P: SerializerProvider + 'static> SerializerProvider for Arc<P> {
    type Serializer<'buf>
        = P::Serializer<'buf>
    where
        Self: 'buf,
        P: 'buf;

    type Ok = P::Ok;

    type Error = P::Error;

    fn with_serializer<F, R>(&self, buffer: &mut Vec<u8>, f: F) -> R
    where
        for<'buf> F: FnOnce(Self::Serializer<'buf>) -> R,
    {
        P::with_serializer(self, buffer, f)
    }
}

impl<P: DeserializerProvider + 'static> DeserializerProvider for Arc<P> {
    type Deserializer<'de>
        = P::Deserializer<'de>
    where
        Self: 'de,
        P: 'de;

    type Error = P::Error;

    fn with_deserializer<F, T>(&self, data: &[u8], f: F) -> Result<T, Self::Error>
    where
        for<'de> F: FnOnce(Self::Deserializer<'de>) -> Result<T, Self::Error>,
    {
        P::with_deserializer(self, data, f)
    }
}
