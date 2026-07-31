//! Background acker — batch-deletes durably-applied sleep settlements.
//!
//! A thin adapter over
//! [`waymark_promise_settlement_demand_registry::acker`], which owns the
//! batching and retry behavior.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use nonempty_collections::NESlice;
use waymark_promise_settlement_demand_registry::acker;
use waymark_sleep_reconciler_backend::{AckSleeps, SleepKey};

/// Parameters for [`run`].
pub struct Params<Backend>
where
    Backend: AckSleeps,
{
    /// The durable sleeps backend to delete acked rows from.
    pub backend: Arc<Backend>,

    /// The receiving half of the ack channel.
    pub ack_rx: tokio::sync::mpsc::UnboundedReceiver<SleepKey<Backend::VmId>>,
}

/// Adapts the backend's ack capability to the shared acker.
struct BackendAcker<Backend>(Arc<Backend>);

impl<Backend> acker::AckKeys for BackendAcker<Backend>
where
    Backend: AckSleeps,
    Backend::Error: core::fmt::Debug,
{
    type Key = SleepKey<Backend::VmId>;
    type Error = Backend::Error;

    fn ack_keys<'a>(
        &'a self,
        keys: NESlice<'a, Self::Key>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.0.ack_sleeps(keys)
    }
}

/// Drain acknowledged sleep keys and batch-delete their rows.
///
/// Drive this in a background task.  Ack failures are retried indefinitely
/// with backoff — deletion is idempotent, and an unacked row is re-fetched,
/// re-settled as already-due, and re-acked after a crash, so retrying is
/// always safe.  The loop ends normally when every sender of the ack
/// channel has been dropped.
pub async fn run<Backend>(params: Params<Backend>)
where
    Backend: AckSleeps,
    Backend::Error: core::fmt::Debug,
{
    let Params { backend, ack_rx } = params;
    acker::run(acker::Params {
        acker: BackendAcker(backend),
        ack_rx,
    })
    .await
}
