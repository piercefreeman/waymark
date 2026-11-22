//! Whole-run transaction commit trait.

/// Backend capability for committing the transaction a whole migrations
/// run went in.
pub trait CommitWholeRunTransaction {
    /// An error that can occur while committing.
    type Error;

    /// Commit the transaction begun on the connection the backend holds.
    fn commit_whole_run_transaction(
        &mut self,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + '_;
}
