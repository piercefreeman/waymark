//! Background acker — batch-deletes durably-applied settlement keys.

use std::time::Duration;

use nonempty_collections::NESlice;

/// Initial delay between retries of a failed ack batch.
const RETRY_INITIAL_BACKOFF: Duration = Duration::from_millis(25);

/// Cap on the retry delay.
const RETRY_MAX_BACKOFF: Duration = Duration::from_secs(1);

/// Upper bound on how many keys are deleted per batch.
const ACK_BATCH_LIMIT: usize = 1024;

/// The delete operation the acker drains into.
///
/// Domain crates adapt their backend's ack capability into this trait;
/// the deletion must be idempotent, as the acker retries failed batches
/// indefinitely.
pub trait AckKeys {
    /// The settlement key type being acknowledged.
    type Key;

    /// The error type for ack operations.
    type Error: core::fmt::Debug;

    /// Remove the rows identified by `keys`.
    fn ack_keys<'a>(
        &'a self,
        keys: NESlice<'a, Self::Key>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}

/// Parameters for [`run`].
pub struct Params<Acker>
where
    Acker: AckKeys,
{
    /// The delete operation to drain acked keys into.
    pub acker: Acker,

    /// The receiving half of the ack channel.
    pub ack_rx: tokio::sync::mpsc::UnboundedReceiver<Acker::Key>,
}

/// Drain acknowledged settlement keys and batch-delete their rows.
///
/// Drive this in a background task.  Ack failures are retried indefinitely
/// with backoff — deletion is idempotent, and an unacked row is re-fetched,
/// re-settled, and re-acked after a crash, so retrying is always safe.
/// The loop ends normally when every sender of the ack channel has been
/// dropped.
pub async fn run<Acker>(params: Params<Acker>)
where
    Acker: AckKeys,
{
    let Params { acker, mut ack_rx } = params;

    // Reused across batches; borrowed (never moved out), so the
    // allocation is kept.
    let mut keys = Vec::new();
    loop {
        keys.clear();
        let received = ack_rx.recv_many(&mut keys, ACK_BATCH_LIMIT).await;
        if received == 0 {
            // Every sender is gone; nothing more will be acked.
            return;
        }

        let mut backoff = RETRY_INITIAL_BACKOFF;
        loop {
            let batch =
                NESlice::try_from_slice(&keys).expect("recv_many returned a non-zero count");
            match acker.ack_keys(batch).await {
                Ok(()) => break,
                Err(error) => {
                    tracing::error!(?error, ?backoff, "acking settlements failed; retrying");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(RETRY_MAX_BACKOFF);
                }
            }
        }
    }
}
