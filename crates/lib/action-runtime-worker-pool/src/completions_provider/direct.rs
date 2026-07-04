use nonempty_collections::{IntoNonEmptyIterator as _, NEVec, NonEmptyIterator as _};
use waymark_action_runtime_core::ActionCallCompletionFor;

use super::shared::{ResolveError, resolve_completion};

/// Errors that can occur when waiting for action completions from
/// the worker pool.
#[derive(Debug, thiserror::Error)]
pub enum DirectCompletionsError {
    /// The worker pool has shut down and can no longer provide
    /// completions.
    #[error("worker pool gone")]
    WorkerPoolGone,

    /// Failed to resolve a raw completion.
    #[error(transparent)]
    Resolve(#[from] ResolveError),
}

/// Provides action outcomes by directly polling a [`waymark_worker_core::BaseWorkerPool`].
///
/// This polls the global worker pool for ALL completions — there is no
/// per-VM filtering.  Prefer [`super::routed::RoutedCompletionsProvider`]
/// for multi-VM deployments where completions must be delivered to the
/// correct VM instance.
pub struct DirectCompletionsProvider<Pool, Metadata> {
    /// The worker pool to poll for completed actions.
    pub pool: Pool,

    /// The associated phantom data.
    pub phantom_data: std::marker::PhantomData<Metadata>,
}

impl<Pool, Metadata> waymark_action_runtime_core::WithActionCallMetadata
    for DirectCompletionsProvider<Pool, Metadata>
{
    type ActionCallMetadata = Metadata;
}

impl<Pool, Metadata> waymark_action_runtime_core::ActionCallCompletionsProvider
    for DirectCompletionsProvider<Pool, Metadata>
where
    Pool: waymark_worker_core::BaseWorkerPool + Send + Sync + 'static,
    Metadata: for<'a> From<&'a waymark_worker_core::ActionCompletion> + Send,
{
    type Value = waymark_vm_value::ReadyValue;
    type Error = DirectCompletionsError;

    async fn wait_for_completions(
        &mut self,
    ) -> Result<NEVec<ActionCallCompletionFor<Self>>, Self::Error> {
        let maybe_completions = self.pool.poll_complete().await;
        let completions = maybe_completions.ok_or(DirectCompletionsError::WorkerPoolGone)?;

        completions
            .into_nonempty_iter()
            .map(|completion| {
                let metadata = Metadata::from(&completion);
                resolve_completion(completion, metadata).map_err(Into::into)
            })
            .collect()
    }
}
