//! Background acker — batch-deletes durably-applied completions.

#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::time::Duration;

use nonempty_collections::NESlice;
use waymark_action_completions_reconciler_backend::{AckCompletions, CompletionKey};

/// Initial delay between retries of a failed ack batch.
const RETRY_INITIAL_BACKOFF: Duration = Duration::from_millis(25);

/// Cap on the retry delay.
const RETRY_MAX_BACKOFF: Duration = Duration::from_secs(1);

/// Upper bound on how many keys are deleted per batch.
const ACK_BATCH_LIMIT: usize = 1024;

/// Parameters for [`run`].
pub struct Params<Backend>
where
    Backend: AckCompletions,
{
    /// The durable completions backend to delete acked rows from.
    pub backend: Arc<Backend>,

    /// The receiving half of the ack channel.
    pub ack_rx: tokio::sync::mpsc::UnboundedReceiver<CompletionKey<Backend::VmId>>,
}

/// Drain acknowledged completion keys and batch-delete their rows.
///
/// Drive this in a background task.  Ack failures are retried indefinitely
/// with backoff — deletion is idempotent, and an unacked row is re-fetched,
/// re-settled as already-resolved, and re-acked after a crash, so retrying
/// is always safe.  The loop ends normally when every sender of the ack
/// channel has been dropped.
pub async fn run<Backend>(params: Params<Backend>)
where
    Backend: AckCompletions,
    Backend::Error: core::fmt::Debug,
{
    let Params {
        backend,
        mut ack_rx,
    } = params;

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
            match backend.ack_completions(batch).await {
                Ok(()) => break,
                Err(error) => {
                    tracing::error!(?error, ?backoff, "acking completions failed; retrying");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(RETRY_MAX_BACKOFF);
                }
            }
        }
    }
}
