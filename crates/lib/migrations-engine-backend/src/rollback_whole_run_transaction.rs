//! Whole-run transaction rollback trait.

/// Backend capability for rolling back the transaction a whole migrations
/// run went in.
pub trait RollbackWholeRunTransaction {
    /// An error that can occur while rolling back.
    type Error;

    /// Roll back the transaction begun on the connection the backend
    /// holds.
    fn rollback_whole_run_transaction(
        &mut self,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + '_;
}
