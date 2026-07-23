//! Routed completions provider — polls an inner completions provider and
//! demultiplexes each completion to the VM that owns it.
//!
//! # Architecture
//!
//! [`RoutedCompletionsProvider`] owns an inner
//! [`waymark_action_runtime_core::ActionCallCompletionsProvider`] whose
//! metadata is [`Routed`] (i.e. carries a routing key), plus a route
//! table mapping keys to per-destination channels.
//!
//! A single driver task calls
//! [`poll_and_route`](RoutedCompletionsProvider::poll_and_route) in a loop.
//! Each call polls the inner provider once and pushes every completion into
//! the channel belonging to the destination identified by the completion's
//! [`routing_key`](Routed::routing_key).
//!
//! Because polling the inner provider requires `&mut`, the poll loop owns the
//! provider exclusively.  Registration is a separate, shared concern: call
//! [`registrar`](RoutedCompletionsProvider::registrar) to obtain a cloneable
//! [`RouteRegistrar`] before handing the provider to the poll loop, then use
//! it to [`register`](RouteRegistrar::register) each VM.  The returned
//! [`RoutedCompletionsHandle`] implements
//! [`waymark_action_runtime_core::ActionCallCompletionsProvider`] — each VM
//! drives its own handle, receiving only its own completions.
//!
//! When a [`RoutedCompletionsHandle`] is dropped (e.g. the VM is evicted),
//! its route is automatically removed from the route table so that no
//! further completions are sent to a defunct channel.

#![warn(missing_docs)]

use std::sync::Arc;

use dashmap::DashMap;
use nonempty_collections::NEVec;
use tokio::sync::mpsc;
use waymark_action_runtime_completions_router_core::Routed;
use waymark_action_runtime_core::{
    ActionCallCompletion, ActionCallCompletionFor, ActionCallCompletionsProvider,
};

/// Errors that can occur when a [`RoutedCompletionsHandle`] waits for its
/// VM's completions.
#[derive(Debug, thiserror::Error)]
pub enum RoutedCompletionsError {
    /// The completion channel has been closed.  This typically means
    /// the central provider has been dropped or the VM was evicted.
    #[error("completion channel closed")]
    ChannelClosed,
}

// ---------------------------------------------------------------------------
// Route table
// ---------------------------------------------------------------------------

type CompletionTx<Value, Metadata> = mpsc::UnboundedSender<ActionCallCompletion<Value, Metadata>>;
type CompletionRx<Value, Metadata> = mpsc::UnboundedReceiver<ActionCallCompletion<Value, Metadata>>;
type WeakCompletionTx<Value, Metadata> =
    mpsc::WeakUnboundedSender<ActionCallCompletion<Value, Metadata>>;
type RouteTable<RoutingKey, Value, Metadata> = DashMap<RoutingKey, CompletionTx<Value, Metadata>>;

// ---------------------------------------------------------------------------
// RoutedCompletionsProvider
// ---------------------------------------------------------------------------

/// Central router that polls an inner completions provider and dispatches
/// each completion to the VM that owns it.
///
/// Construct one per inner provider with [`new`](Self::new), obtain a
/// [`registrar`](Self::registrar) for wiring up per-VM handles, then drive
/// [`poll_and_route`](Self::poll_and_route) in a loop.
pub struct RoutedCompletionsProvider<Inner, RoutingKey>
where
    Inner: ActionCallCompletionsProvider,
    RoutingKey: Eq + std::hash::Hash,
{
    /// The inner provider polled for completions across all VMs.
    inner: Inner,

    /// Table mapping routing keys to per-VM completion channels.
    routes: Arc<RouteTable<RoutingKey, Inner::Value, Inner::Metadata>>,
}

impl<Inner, RoutingKey> RoutedCompletionsProvider<Inner, RoutingKey>
where
    Inner: ActionCallCompletionsProvider,
    RoutingKey: Eq + std::hash::Hash,
{
    /// Create a new routed completions provider backed by the given inner
    /// provider.
    pub fn new(inner: Inner) -> Self {
        Self {
            inner,
            routes: Arc::new(DashMap::new()),
        }
    }

    /// Obtain a cloneable registrar for wiring up per-VM handles.
    ///
    /// The registrar shares this provider's route table, so handles it
    /// creates receive completions routed by [`poll_and_route`](Self::poll_and_route).
    /// Take a registrar before moving the provider into its poll loop.
    pub fn registrar(&self) -> RouteRegistrar<RoutingKey, Inner::Value, Inner::Metadata> {
        RouteRegistrar {
            routes: Arc::clone(&self.routes),
        }
    }
}

impl<Inner, RoutingKey> RoutedCompletionsProvider<Inner, RoutingKey>
where
    Inner: ActionCallCompletionsProvider,
    Inner::Metadata: Routed<RoutingKey>,
    RoutingKey: Eq + std::hash::Hash + std::fmt::Display,
{
    /// Poll the inner provider once and route every completion to its target
    /// VM.
    ///
    /// Call this in a background loop.  Completions destined for a VM that
    /// has not been registered (or has already been dropped) are silently
    /// discarded with a warning.  Errors from the inner provider (e.g. its
    /// source is exhausted) are propagated to the caller, which should stop
    /// the loop.
    pub async fn poll_and_route(&mut self) -> Result<(), Inner::Error> {
        let completions = self.inner.wait_for_completions().await?;

        for completion in completions {
            let routing_key = completion.metadata.routing_key();
            match self.routes.get(&routing_key) {
                Some(tx) => {
                    if tx.send(completion).is_err() {
                        tracing::warn!(%routing_key, "failed to push completion to VM channel");
                    }
                }
                None => {
                    tracing::warn!(%routing_key, "completion for unregistered VM — discarding");
                }
            }
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// RouteRegistrar
// ---------------------------------------------------------------------------

/// Cloneable handle for registering routing keys with a
/// [`RoutedCompletionsProvider`].
///
/// Shares the provider's route table, so it can hand out
/// [`RoutedCompletionsHandle`]s even after the provider has been moved into
/// its poll loop.
pub struct RouteRegistrar<RoutingKey, Value, Metadata>
where
    RoutingKey: Eq + std::hash::Hash,
{
    routes: Arc<RouteTable<RoutingKey, Value, Metadata>>,
}

impl<RoutingKey, Value, Metadata> Clone for RouteRegistrar<RoutingKey, Value, Metadata>
where
    RoutingKey: Eq + std::hash::Hash,
{
    fn clone(&self) -> Self {
        Self {
            routes: Arc::clone(&self.routes),
        }
    }
}

impl<RoutingKey, Value, Metadata> RouteRegistrar<RoutingKey, Value, Metadata>
where
    RoutingKey: Copy + Eq + std::hash::Hash,
{
    /// Register a routing key and return a handle that receives completions
    /// for it.
    ///
    /// The handle implements [`waymark_action_runtime_core::ActionCallCompletionsProvider`].
    /// When the handle is dropped the route is automatically removed — no
    /// explicit unregistration is needed.
    ///
    /// Registering a key that already has a route replaces the route: the
    /// previous handle's channel is closed and all further completions for
    /// the key go to the new handle.  The displaced handle's eventual drop
    /// does not disturb the new route.
    pub fn register(
        &self,
        routing_key: RoutingKey,
    ) -> RoutedCompletionsHandle<RoutingKey, Value, Metadata> {
        let (tx, rx) = mpsc::unbounded_channel();
        // Keep only a weak reference in the handle: the route table must hold
        // the sole strong sender so that replacing the route closes the
        // displaced handle's channel.
        let route_tx = tx.downgrade();
        self.routes.insert(routing_key, tx);
        RoutedCompletionsHandle {
            completion_rx: rx,
            routing_key,
            route_tx,
            routes: Arc::clone(&self.routes),
        }
    }
}

// ---------------------------------------------------------------------------
// RoutedCompletionsHandle
// ---------------------------------------------------------------------------

/// Per-key handle that receives completions via a dedicated channel.
///
/// Created by [`RouteRegistrar::register`].  Implements
/// [`waymark_action_runtime_core::ActionCallCompletionsProvider`] so it
/// can be used directly as the completions provider for a VM driver.
///
/// When dropped, the handle removes its route from the route table,
/// ensuring no further completions are sent to it.  If the key has since
/// been re-registered, the route belongs to the newer handle and is left
/// untouched.
pub struct RoutedCompletionsHandle<RoutingKey, Value, Metadata>
where
    RoutingKey: Eq + std::hash::Hash,
{
    completion_rx: CompletionRx<Value, Metadata>,
    routing_key: RoutingKey,
    /// Weak reference to this handle's own sender, used on drop to verify
    /// the route table entry still belongs to this handle.  Weak so that the
    /// route table remains the only strong sender.
    route_tx: WeakCompletionTx<Value, Metadata>,
    routes: Arc<RouteTable<RoutingKey, Value, Metadata>>,
}

impl<RoutingKey, Value, Metadata> Drop for RoutedCompletionsHandle<RoutingKey, Value, Metadata>
where
    RoutingKey: Eq + std::hash::Hash,
{
    fn drop(&mut self) {
        // Only remove the route if it is still ours.  A handle can outlive
        // its registration when the same key is re-registered (e.g. a VM is
        // reloaded while the previous instance is still winding down); an
        // unconditional remove would delete the new handle's live route.
        self.routes.remove_if(&self.routing_key, |_, tx| {
            self.route_tx
                .upgrade()
                .is_some_and(|own_tx| own_tx.same_channel(tx))
        });
    }
}

impl<RoutingKey, Value, Metadata> ActionCallCompletionsProvider
    for RoutedCompletionsHandle<RoutingKey, Value, Metadata>
where
    Value: Send,
    Metadata: Send,
    RoutingKey: Eq + std::hash::Hash + Send + Sync + 'static,
{
    type Value = Value;
    type Error = RoutedCompletionsError;
    type Metadata = Metadata;

    async fn wait_for_completions(
        &mut self,
    ) -> Result<NEVec<ActionCallCompletionFor<Self>>, Self::Error> {
        // Block until at least one completion arrives, then drain any
        // additional ones that are immediately available.  Completions are
        // already resolved by the inner provider, so no decoding happens here.
        let first = self
            .completion_rx
            .recv()
            .await
            .ok_or(RoutedCompletionsError::ChannelClosed)?;

        let mut batch = NEVec::new(first);

        while let Ok(completion) = self.completion_rx.try_recv() {
            batch.push(completion);
        }

        Ok(batch)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use waymark_action_runtime_core::ActionCallOutcome;
    use waymark_ids::InstanceId;

    use super::*;

    /// Minimal routed metadata for the routing tests.
    #[derive(Debug, Clone)]
    struct TestMeta {
        routing_key: InstanceId,
    }

    impl Routed<InstanceId> for TestMeta {
        fn routing_key(&self) -> InstanceId {
            self.routing_key
        }
    }

    #[derive(Debug)]
    struct MockExhausted;

    /// Inner provider that yields pre-canned batches, then reports exhaustion.
    struct MockInner {
        batches: VecDeque<Vec<ActionCallCompletion<u32, TestMeta>>>,
    }

    type TestRouter = RoutedCompletionsProvider<MockInner, InstanceId>;

    impl ActionCallCompletionsProvider for MockInner {
        type Value = u32;
        type Error = MockExhausted;
        type Metadata = TestMeta;

        async fn wait_for_completions(
            &mut self,
        ) -> Result<NEVec<ActionCallCompletionFor<Self>>, Self::Error> {
            let batch = self.batches.pop_front().ok_or(MockExhausted)?;
            Ok(NEVec::try_from_vec(batch).expect("test batches are non-empty"))
        }
    }

    fn completion(routing_key: InstanceId, value: u32) -> ActionCallCompletion<u32, TestMeta> {
        ActionCallCompletion {
            metadata: TestMeta { routing_key },
            outcome: ActionCallOutcome::Value(value),
        }
    }

    fn values(batch: NEVec<ActionCallCompletion<u32, TestMeta>>) -> Vec<u32> {
        batch
            .into_iter()
            .map(|completion| match completion.outcome {
                ActionCallOutcome::Value(value) => value,
                ActionCallOutcome::Exception(_) => panic!("unexpected exception outcome"),
            })
            .collect()
    }

    #[tokio::test]
    async fn routes_each_completion_to_its_owning_vm() {
        let vm_a = InstanceId::new_uuid_v4();
        let vm_b = InstanceId::new_uuid_v4();

        let inner = MockInner {
            batches: VecDeque::from([vec![
                completion(vm_a, 1),
                completion(vm_b, 2),
                completion(vm_a, 3),
            ]]),
        };

        let mut router = TestRouter::new(inner);
        let registrar = router.registrar();
        let mut handle_a = registrar.register(vm_a);
        let mut handle_b = registrar.register(vm_b);

        router.poll_and_route().await.expect("routing succeeds");

        assert_eq!(
            values(handle_a.wait_for_completions().await.unwrap()),
            [1, 3]
        );
        assert_eq!(values(handle_b.wait_for_completions().await.unwrap()), [2]);
    }

    #[tokio::test]
    async fn completions_for_unregistered_vms_are_dropped() {
        let vm_a = InstanceId::new_uuid_v4();
        let vm_unknown = InstanceId::new_uuid_v4();

        let inner = MockInner {
            batches: VecDeque::from([vec![completion(vm_unknown, 9), completion(vm_a, 1)]]),
        };

        let mut router = TestRouter::new(inner);
        let registrar = router.registrar();
        let mut handle_a = registrar.register(vm_a);

        // The completion for the unregistered VM is discarded, not an error.
        router.poll_and_route().await.expect("routing succeeds");

        assert_eq!(values(handle_a.wait_for_completions().await.unwrap()), [1]);
    }

    #[tokio::test]
    async fn stale_handle_drop_leaves_reregistered_route_intact() {
        let vm_a = InstanceId::new_uuid_v4();

        let inner = MockInner {
            batches: VecDeque::from([vec![completion(vm_a, 1)]]),
        };

        let mut router = TestRouter::new(inner);
        let registrar = router.registrar();

        // The VM is re-registered (e.g. evicted and reloaded) while the
        // previous handle is still alive; dropping the stale handle
        // afterwards must not delete the new handle's route.
        let stale_handle = registrar.register(vm_a);
        let mut fresh_handle = registrar.register(vm_a);
        drop(stale_handle);

        router.poll_and_route().await.expect("routing succeeds");

        assert_eq!(
            values(fresh_handle.wait_for_completions().await.unwrap()),
            [1]
        );
    }

    #[tokio::test]
    async fn reregistering_closes_the_displaced_handles_channel() {
        let vm_a = InstanceId::new_uuid_v4();

        let inner = MockInner {
            batches: VecDeque::new(),
        };

        let router = TestRouter::new(inner);
        let registrar = router.registrar();

        let mut stale_handle = registrar.register(vm_a);
        let _fresh_handle = registrar.register(vm_a);

        // Re-registration drops the stale route's sender, so the displaced
        // handle observes a closed channel instead of blocking forever.
        assert!(matches!(
            stale_handle.wait_for_completions().await,
            Err(RoutedCompletionsError::ChannelClosed)
        ));
    }

    #[tokio::test]
    async fn inner_error_propagates() {
        let inner = MockInner {
            batches: VecDeque::new(),
        };
        let mut router = TestRouter::new(inner);
        assert!(router.poll_and_route().await.is_err());
    }
}
