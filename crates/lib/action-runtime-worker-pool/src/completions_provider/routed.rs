//! Routed completions provider — polls the global worker pool and routes
//! completions to the correct VM based on [`waymark_worker_core::ActionCompletion::executor_id`].
//!
//! # Architecture
//!
//! [`RoutedCompletionsProvider`] owns the shared worker pool and a route
//! table mapping VM identifiers to per-VM channels.  Callers register
//! each VM via [`register`](RoutedCompletionsProvider::register), which
//! returns a [`RoutedCompletionsHandle`].  The handle implements
//! [`waymark_action_runtime_core::ActionCallCompletionsProvider`] — each
//! VM drives its own handle, receiving only its own completions.
//!
//! A background task (or the execution driver) calls
//! [`poll_and_route`](RoutedCompletionsProvider::poll_and_route) in a
//! loop.  This polls the worker pool and pushes each completion into the
//! channel belonging to the VM identified by the completion's
//! [`executor_id`](waymark_worker_core::ActionCompletion::executor_id).
//!
//! When a [`RoutedCompletionsHandle`] is dropped (e.g. the VM is evicted),
//! its route is automatically removed from the route table so that no
//! further completions are sent to a defunct channel.

#![warn(missing_docs)]

use std::sync::Arc;

use dashmap::DashMap;
use nonempty_collections::NEVec;
use tokio::sync::mpsc;
use waymark_action_runtime_core::ActionCallCompletionFor;

use super::shared::{ResolveError, resolve_completion};

/// Errors that can occur when waiting for action completions via the
/// routed provider.
#[derive(Debug, thiserror::Error)]
pub enum RoutedCompletionsError {
    /// The completion channel has been closed.  This typically means
    /// the central provider has been dropped or the VM was evicted.
    #[error("completion channel closed")]
    ChannelClosed,

    /// Failed to resolve a raw completion.
    #[error(transparent)]
    Resolve(#[from] ResolveError),
}

/// Errors that can occur when polling and routing completions.
#[derive(Debug, thiserror::Error)]
pub enum PollRouteError {
    /// The worker pool has shut down and can no longer provide
    /// completions.
    #[error("worker pool gone")]
    WorkerPoolGone,
}

// ---------------------------------------------------------------------------
// Route table
// ---------------------------------------------------------------------------

type CompletionTx<Metadata> =
    mpsc::UnboundedSender<(waymark_worker_core::ActionCompletion, Metadata)>;

type CompletionRx<Metadata> =
    mpsc::UnboundedReceiver<(waymark_worker_core::ActionCompletion, Metadata)>;

type RouteTable<Metadata> =
    Arc<DashMap<<Metadata as ToRoutingKey>::RoutingKey, CompletionTx<Metadata>>>;

/// Metadata that can be converted into a routing key for the routed
/// completions provider.
pub trait ToRoutingKey {
    /// The raw key used to route completions to a specific VM.
    type RoutingKey: core::hash::Hash + Eq + Clone;

    /// Convert the metadata into its routing key.
    fn to_routing_key(&self) -> Self::RoutingKey;
}

// ---------------------------------------------------------------------------
// RoutedCompletionsProvider
// ---------------------------------------------------------------------------

/// Central provider that polls the worker pool and routes completions to
/// registered per-VM handles.
///
/// Create one instance per worker pool, then call [`register`](Self::register)
/// for each VM.  The returned [`RoutedCompletionsHandle`] satisfies
/// [`waymark_action_runtime_core::ActionCallCompletionsProvider`] and
/// automatically cleans up its route on drop.
pub struct RoutedCompletionsProvider<Pool, Metadata>
where
    Metadata: ToRoutingKey,
{
    /// The worker pool to poll for completed actions.
    pub pool: Pool,

    /// Table mapping VM identifiers to per-VM completion channels.
    pub routes: RouteTable<Metadata>,
}

impl<Pool, Metadata> RoutedCompletionsProvider<Pool, Metadata>
where
    Metadata: ToRoutingKey,
{
    /// Create a new routed completions provider backed by the given pool.
    pub fn new(pool: Pool) -> Self {
        Self {
            pool,
            routes: Arc::new(DashMap::new()),
        }
    }

    /// Register a VM and return a handle that receives its completions.
    ///
    /// The handle implements [`waymark_action_runtime_core::ActionCallCompletionsProvider`].
    /// When the handle is dropped the route is automatically removed — no
    /// explicit unregistration is needed.
    pub fn register(&self, routing_key: Metadata::RoutingKey) -> RoutedCompletionsHandle<Metadata> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.routes.insert(routing_key.clone(), tx);
        RoutedCompletionsHandle {
            completion_rx: rx,
            routing_key,
            routes: Arc::clone(&self.routes),
        }
    }
}

impl<Pool, Metadata> RoutedCompletionsProvider<Pool, Metadata>
where
    Pool: waymark_worker_core::BaseWorkerPool + Send + Sync,
    Metadata: ToRoutingKey<RoutingKey: std::fmt::Debug>,
    Metadata: for<'a> From<&'a waymark_worker_core::ActionCompletion> + Send,
{
    /// Poll the worker pool once and route every completion to its target VM.
    ///
    /// Call this in a background loop.  Completions destined for a VM that
    /// has not been registered (or has already been dropped) are silently
    /// discarded with a warning.
    pub async fn poll_and_route(&self) -> Result<(), PollRouteError> {
        let maybe_completions = self.pool.poll_complete().await;
        let completions = maybe_completions.ok_or(PollRouteError::WorkerPoolGone)?;

        let routes = &self.routes;
        for completion in completions {
            let metadata = Metadata::from(&completion);
            let routing_key = metadata.to_routing_key();
            match routes.get(&routing_key) {
                Some(tx) => {
                    if let Err(error) = tx.send((completion, metadata)) {
                        tracing::warn!(
                            ?error,
                            ?routing_key,
                            "failed to push completion to routed channel"
                        );
                    }
                }
                None => {
                    tracing::warn!(
                        ?routing_key,
                        "completion for unregistered routing key — discarding"
                    );
                }
            }
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// RoutedCompletionsHandle
// ---------------------------------------------------------------------------

/// Per-VM handle that receives completions via a dedicated channel.
///
/// Created by [`RoutedCompletionsProvider::register`].  Implements
/// [`waymark_action_runtime_core::ActionCallCompletionsProvider`] so it
/// can be used directly as the completions provider for a VM driver.
///
/// When dropped, the handle removes its route from the central provider's
/// route table, ensuring no further completions are sent to it.
pub struct RoutedCompletionsHandle<Metadata>
where
    Metadata: ToRoutingKey,
{
    completion_rx: CompletionRx<Metadata>,
    routing_key: Metadata::RoutingKey,
    routes: RouteTable<Metadata>,
}

impl<Metadata> Drop for RoutedCompletionsHandle<Metadata>
where
    Metadata: ToRoutingKey,
{
    fn drop(&mut self) {
        self.routes.remove(&self.routing_key);
    }
}

impl<Metadata> waymark_action_runtime_core::WithActionCallMetadata
    for RoutedCompletionsHandle<Metadata>
where
    Metadata: ToRoutingKey,
{
    type ActionCallMetadata = Metadata;
}

impl<Metadata> waymark_action_runtime_core::ActionCallCompletionsProvider
    for RoutedCompletionsHandle<Metadata>
where
    Metadata: ToRoutingKey<RoutingKey: Send + Sync>,
    Metadata: for<'a> From<&'a waymark_worker_core::ActionCompletion> + Send,
{
    type Value = waymark_vm_value::ReadyValue;
    type Error = RoutedCompletionsError;

    async fn wait_for_completions(
        &mut self,
    ) -> Result<NEVec<ActionCallCompletionFor<Self>>, Self::Error> {
        // Block until at least one completion arrives, then drain any
        // additional ones that are immediately available.
        let (first_completion, first_metadata) = self
            .completion_rx
            .recv()
            .await
            .ok_or(RoutedCompletionsError::ChannelClosed)?;

        let mut batch = NEVec::new(resolve_completion(first_completion, first_metadata)?);
        while let Ok((completion, metadata)) = self.completion_rx.try_recv() {
            batch.push(resolve_completion(completion, metadata)?);
        }

        Ok(batch)
    }
}
