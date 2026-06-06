//! Blanket implementations for tuples of codec traits.

use crate::{SnapshotDeserializer, SnapshotSerializer};

impl<A: SnapshotSerializer + 'static, B: 'static> SnapshotSerializer for (A, B) {
    type Serializer<'buf>
        = A::Serializer<'buf>
    where
        Self: 'buf,
        A: 'buf;
    type Error = A::Error;

    fn with_serializer<F, R>(&self, buffer: &mut Vec<u8>, f: F) -> Result<R, Self::Error>
    where
        for<'buf> F: FnOnce(Self::Serializer<'buf>) -> Result<R, Self::Error>,
    {
        A::with_serializer(&self.0, buffer, f)
    }
}

impl<A: 'static, B: SnapshotDeserializer + 'static> SnapshotDeserializer for (A, B) {
    type Deserializer<'de>
        = B::Deserializer<'de>
    where
        Self: 'de,
        B: 'de;
    type Error = B::Error;

    fn with_deserializer<F, R>(&self, data: &[u8], f: F) -> Result<R, Self::Error>
    where
        for<'de> F: FnOnce(Self::Deserializer<'de>) -> Result<R, Self::Error>,
    {
        B::with_deserializer(&self.1, data, f)
    }
}
