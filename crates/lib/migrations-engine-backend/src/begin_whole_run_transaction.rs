//! Whole-run transaction beginning trait.

/// Backend capability for beginning the transaction a whole migrations run
/// goes in.
pub trait BeginWholeRunTransaction {
    /// An error that can occur while beginning.
    type Error;

    /// Begin a transaction on the connection the backend holds.
    fn begin_whole_run_transaction(
        &mut self,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + '_;
}
