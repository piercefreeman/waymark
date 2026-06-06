//! Blanket `Arc<T>` implementations for codec traits.

use std::sync::Arc;

use crate::{SnapshotDeserializer, SnapshotSerializer};

impl<T: SnapshotSerializer + 'static> SnapshotSerializer for Arc<T> {
    type Serializer<'buf>
        = T::Serializer<'buf>
    where
        Self: 'buf,
        T: 'buf;
    type Error = T::Error;

    fn with_serializer<F, R>(&self, buffer: &mut Vec<u8>, f: F) -> Result<R, Self::Error>
    where
        for<'buf> F: FnOnce(Self::Serializer<'buf>) -> Result<R, Self::Error>,
    {
        T::with_serializer(self, buffer, f)
    }
}

impl<T: SnapshotDeserializer + 'static> SnapshotDeserializer for Arc<T> {
    type Deserializer<'de>
        = T::Deserializer<'de>
    where
        Self: 'de,
        T: 'de;
    type Error = T::Error;

    fn with_deserializer<F, R>(&self, data: &[u8], f: F) -> Result<R, Self::Error>
    where
        for<'de> F: FnOnce(Self::Deserializer<'de>) -> Result<R, Self::Error>,
    {
        T::with_deserializer(self, data, f)
    }
}
