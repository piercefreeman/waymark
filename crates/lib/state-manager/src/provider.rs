//! The [`Provider`] trait - an abstraction over the state manager's
//! [`get`](State::get) semantics.
//!
//! Consumers can bound on `Provider<Key = ..., Value = ...>` without
//! knowing about [`Factory`](waymark_state_manager_core::Factory) or other
//! implementation details.

use core::hash::Hash;
use std::sync::Arc;

use crate::{Handle, State};

/// Abstract provider of key-value pairs that returns [`Handle`]s.
///
/// This trait lets consumers declare only their required `Key` and `Value`
/// types without knowing about [`Factory`](waymark_state_manager_core::Factory),
/// [`State`], or other implementation details.
pub trait Provider {
    /// The key type.
    type Key: Eq + Hash;

    /// The value type.
    type Value;

    /// The error returned when provisioning fails.
    type Error;

    /// Get or create a value for the given key, returning a [`Handle`]
    /// that keeps the entry alive.
    fn get(
        &self,
        key: Self::Key,
    ) -> impl Future<Output = Result<Handle<Self::Key, Self::Value>, Self::Error>> + Send + '_;
}

impl<Key, Value, Factory> Provider for State<Key, Value, Factory>
where
    Key: Eq + Hash + Clone + Send + Sync,
    Factory: waymark_state_manager_core::Factory<Key = Key, Value = Value> + Sync,
    Value: Clone + Send + Sync,
{
    type Key = Key;
    type Value = Value;
    type Error = Factory::Error;

    async fn get(&self, key: Self::Key) -> Result<Handle<Self::Key, Self::Value>, Self::Error> {
        State::get(self, key).await
    }
}

impl<T> Provider for Arc<T>
where
    T: Provider + Send + Sync,
    <T as Provider>::Key: Send,
{
    type Key = T::Key;
    type Value = T::Value;
    type Error = T::Error;

    async fn get(&self, key: Self::Key) -> Result<Handle<Self::Key, Self::Value>, Self::Error> {
        T::get(self, key).await
    }
}
