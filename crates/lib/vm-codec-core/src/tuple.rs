//! Blanket implementations for tuples of codec traits.

use crate::{DeserializerProvider, SerializerProvider};

impl<A: SerializerProvider + 'static, B: 'static> SerializerProvider for (A, B) {
    type Serializer<'buf>
        = A::Serializer<'buf>
    where
        Self: 'buf;

    type Ok = A::Ok;

    type Error = A::Error;

    fn with_serializer<F, R>(&self, buffer: &mut Vec<u8>, f: F) -> R
    where
        for<'buf> F: FnOnce(Self::Serializer<'buf>) -> R,
    {
        A::with_serializer(&self.0, buffer, f)
    }
}

impl<A: 'static, B: DeserializerProvider + 'static> DeserializerProvider for (A, B) {
    type Deserializer<'de>
        = B::Deserializer<'de>
    where
        Self: 'de;

    type Error = B::Error;

    fn with_deserializer<F, T>(&self, data: &[u8], f: F) -> Result<T, Self::Error>
    where
        for<'de> F: FnOnce(Self::Deserializer<'de>) -> Result<T, Self::Error>,
    {
        B::with_deserializer(&self.1, data, f)
    }
}
