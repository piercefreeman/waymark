//! Cancellation token helpers.

/// Cancel `token`, once something else has happened:
/// `cancel(token).after(that).await`.
pub fn cancel(token: &tokio_util::sync::CancellationToken) -> Cancel<'_> {
    Cancel(token)
}

/// A cancellation of a token, waiting for its moment; see [`cancel`].
#[derive(Debug)]
pub struct Cancel<'a>(&'a tokio_util::sync::CancellationToken);

impl Cancel<'_> {
    /// Cancel the token after `after` resolves.
    ///
    /// Ends as soon as the token is cancelled, by this or by anyone else,
    /// so it never waits on `after` for longer than the token is worth
    /// waiting for.
    pub async fn after(self, after: impl Future<Output = ()>) {
        self.0.run_until_cancelled(after).await;
        self.0.cancel();
    }
}

#[cfg(test)]
mod tests;
