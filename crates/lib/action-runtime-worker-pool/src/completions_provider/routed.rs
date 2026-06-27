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
use waymark_action_runtime_core::ActionCallCompletion;

use super::shared::{ResolveError, resolve_completion};
use crate::DispatchCorrelationMap;

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

type CompletionTx = mpsc::UnboundedSender<waymark_worker_core::ActionCompletion>;
type CompletionRx = mpsc::UnboundedReceiver<waymark_worker_core::ActionCompletion>;
type RouteTable = Arc<DashMap<waymark_ids::InstanceId, CompletionTx>>;

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
pub struct RoutedCompletionsProvider<Pool> {
    /// The worker pool to poll for completed actions.
    pub pool: Pool,

    /// Table mapping VM identifiers to per-VM completion channels.
    pub routes: RouteTable,
}

impl<Pool> RoutedCompletionsProvider<Pool> {
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
    pub fn register(
        &self,
        vm_id: waymark_ids::InstanceId,
        correlation_map: DispatchCorrelationMap,
    ) -> RoutedCompletionsHandle {
        let (tx, rx) = mpsc::unbounded_channel();
        self.routes.insert(vm_id, tx);
        RoutedCompletionsHandle {
            completion_rx: rx,
            vm_id,
            correlation_map,
            routes: Arc::clone(&self.routes),
        }
    }
}

impl<Pool> RoutedCompletionsProvider<Pool>
where
    Pool: waymark_worker_core::BaseWorkerPool + Send + Sync,
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
            let executor_id = completion.executor_id;
            match routes.get(&executor_id) {
                Some(tx) => {
                    if let Err(err) = tx.send(completion) {
                        tracing::warn!(
                            ?err,
                            %executor_id,
                            "failed to push completion to VM channel"
                        );
                    }
                }
                None => {
                    tracing::warn!(
                        %executor_id,
                        "completion for unregistered VM — discarding"
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
pub struct RoutedCompletionsHandle {
    completion_rx: CompletionRx,
    vm_id: waymark_ids::InstanceId,
    correlation_map: DispatchCorrelationMap,
    routes: RouteTable,
}

impl Drop for RoutedCompletionsHandle {
    fn drop(&mut self) {
        self.routes.remove(&self.vm_id);
    }
}

impl waymark_action_runtime_core::ActionCallCompletionsProvider for RoutedCompletionsHandle {
    type Value = waymark_vm_value::ReadyValue;
    type Error = RoutedCompletionsError;

    async fn wait_for_completions(
        &mut self,
    ) -> Result<NEVec<ActionCallCompletion<Self::Value>>, Self::Error> {
        // Block until at least one completion arrives, then drain any
        // additional ones that are immediately available.
        let first = self
            .completion_rx
            .recv()
            .await
            .ok_or(RoutedCompletionsError::ChannelClosed)?;

        let mut batch = NEVec::new(resolve_completion(&self.correlation_map, first)?);
        while let Ok(completion) = self.completion_rx.try_recv() {
            batch.push(resolve_completion(&self.correlation_map, completion)?);
        }

        Ok(batch)
    }
}
