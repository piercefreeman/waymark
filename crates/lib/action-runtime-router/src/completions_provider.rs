use std::sync::Arc;

use dashmap::DashMap;
use nonempty_collections::NEVec;
use tokio::sync::mpsc;
use waymark_action_runtime_core::ActionCallCompletionFor;
use waymark_action_runtime_router_core::ToRoutingKey;

use crate::RoutingKeyForInner;

type CompletionTx<Inner> =
    mpsc::UnboundedSender<waymark_action_runtime_core::ActionCallCompletionFor<Inner>>;

type CompletionRx<Inner> =
    mpsc::UnboundedReceiver<waymark_action_runtime_core::ActionCallCompletionFor<Inner>>;

type RouteTable<Inner> = DashMap<RoutingKeyForInner<Inner>, CompletionTx<Inner>>;

pub struct RoutedCompletionsProvider<Inner>
where
    Inner: waymark_action_runtime_core::ActionCallCompletionsProvider,
    Inner::ActionCallMetadata: ToRoutingKey,
{
    /// The (unrouted) completions provider.
    pub inner: Inner,

    /// Table mapping VM identifiers to per-VM completion channels.
    pub routes: Arc<RouteTable<Inner>>,
}

impl<Inner> RoutedCompletionsProvider<Inner>
where
    Inner: waymark_action_runtime_core::ActionCallCompletionsProvider,
    Inner::ActionCallMetadata: ToRoutingKey,
{
    /// Create a new routed completions provider backed by the given pool.
    pub fn new(inner: Inner) -> Self {
        Self {
            inner,
            routes: Arc::new(DashMap::new()),
        }
    }

    /// Register a VM and return a handle that receives its completions.
    ///
    /// The handle implements [`waymark_action_runtime_core::ActionCallCompletionsProvider`].
    /// When the handle is dropped the route is automatically removed — no
    /// explicit unregistration is needed.
    pub fn register(&self, routing_key: RoutingKeyForInner<Inner>) -> RoutedCompletionsHandle<Inner>
    where
        RoutingKeyForInner<Inner>: Clone,
    {
        let (tx, rx) = mpsc::unbounded_channel();
        self.routes.insert(routing_key.clone(), tx);
        RoutedCompletionsHandle {
            completion_rx: rx,
            routing_key,
            routes: Arc::clone(&self.routes),
        }
    }
}

impl<Inner> RoutedCompletionsProvider<Inner>
where
    Inner: waymark_action_runtime_core::ActionCallCompletionsProvider,
    Inner::ActionCallMetadata: ToRoutingKey<RoutingKey: std::fmt::Debug>,
{
    /// Poll the worker pool once and route every completion to its target VM.
    ///
    /// Call this in a background loop.  Completions destined for a VM that
    /// has not been registered (or has already been dropped) are silently
    /// discarded with a warning.
    pub async fn wait_and_route(&mut self) -> Result<(), Inner::Error> {
        let completions = self.inner.wait_for_completions().await?;

        let routes = &self.routes;
        for completion in completions {
            let routing_key = completion.metadata.to_routing_key();
            match routes.get(&routing_key) {
                Some(tx) => {
                    if let Err(error) = tx.send(completion) {
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
pub struct RoutedCompletionsHandle<Inner>
where
    Inner: waymark_action_runtime_core::ActionCallCompletionsProvider,
    Inner::ActionCallMetadata: ToRoutingKey,
{
    completion_rx: CompletionRx<Inner>,
    routing_key: RoutingKeyForInner<Inner>,
    routes: Arc<RouteTable<Inner>>,
}

impl<Inner> Drop for RoutedCompletionsHandle<Inner>
where
    Inner: waymark_action_runtime_core::ActionCallCompletionsProvider,
    Inner::ActionCallMetadata: ToRoutingKey,
{
    fn drop(&mut self) {
        self.routes.remove(&self.routing_key);
    }
}

impl<Inner> waymark_action_runtime_core::WithActionCallMetadata for RoutedCompletionsHandle<Inner>
where
    Inner: waymark_action_runtime_core::ActionCallCompletionsProvider,
    Inner::ActionCallMetadata: ToRoutingKey,
{
    type ActionCallMetadata = Inner::ActionCallMetadata;
}

/// Errors that can occur when waiting for action completions via the
/// routed provider.
#[derive(Debug, thiserror::Error)]
pub enum RoutedCompletionsError {
    /// The completion channel has been closed.  This typically means
    /// the central provider has been dropped or the VM was evicted.
    #[error("completion channel closed")]
    ChannelClosed,
}

impl<Inner> waymark_action_runtime_core::ActionCallCompletionsProvider
    for RoutedCompletionsHandle<Inner>
where
    Inner: waymark_action_runtime_core::ActionCallCompletionsProvider,
    Inner::ActionCallMetadata: ToRoutingKey + Send,
    Inner::Value: Send,
    RoutingKeyForInner<Inner>: Send + Sync,
{
    type Value = Inner::Value;
    type Error = RoutedCompletionsError;

    async fn wait_for_completions(
        &mut self,
    ) -> Result<NEVec<ActionCallCompletionFor<Self>>, Self::Error> {
        // Block until at least one completion arrives, then drain any
        // additional ones that are immediately available.
        let completion = self
            .completion_rx
            .recv()
            .await
            .ok_or(RoutedCompletionsError::ChannelClosed)?;

        let mut batch = NEVec::new(completion);
        while let Ok(completion) = self.completion_rx.try_recv() {
            batch.push(completion);
        }

        Ok(batch)
    }
}
