/// Persists raw snapshot bytes.
pub trait SnapshotPersister {
    /// The error type returned by [`persist_snapshot`](SnapshotPersister::persist_snapshot).
    type Error: std::fmt::Debug;

    /// Persist the given snapshot bytes.
    fn persist_snapshot<'a>(
        &'a self,
        data: &'a [u8],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}

impl SnapshotPersister for () {
    type Error = core::convert::Infallible;

    async fn persist_snapshot<'a>(&'a self, _data: &'a [u8]) -> Result<(), Self::Error> {
        Ok(())
    }
}
