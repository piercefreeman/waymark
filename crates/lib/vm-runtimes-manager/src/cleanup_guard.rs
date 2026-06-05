//! RAII guard that removes an entry from a shared map on drop.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

/// Removes a key from a shared [`HashMap`] when dropped.
///
/// Useful for cleaning up entries from a registry map when a task
/// completes or panics, without manual cleanup code in every exit path.
pub struct CleanupGuard<K: Eq + Hash, V> {
    key: K,
    map: Arc<Mutex<HashMap<K, V>>>,
}

impl<K: Eq + Hash, V> CleanupGuard<K, V> {
    /// Create a new guard that will remove `key` from `map` on drop.
    pub fn new(map: Arc<Mutex<HashMap<K, V>>>, key: K) -> Self {
        Self { key, map }
    }
}

impl<K: Eq + Hash, V> Drop for CleanupGuard<K, V> {
    fn drop(&mut self) {
        self.map.lock().unwrap().remove(&self.key);
    }
}
