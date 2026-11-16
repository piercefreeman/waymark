use std::cell::RefCell;
use std::collections::HashMap;

use waymark_atomic_owners_counter::AtomicOwnersCounter;
use waymark_vm_runtime_promise_core::PromiseStateId;

thread_local! {
    /// The owners counters handed out while a restore is in progress on
    /// this thread, one per promise state id.
    static REGISTRY: RefCell<Option<HashMap<PromiseStateId, AtomicOwnersCounter>>> =
        const { RefCell::new(None) };
}

/// Why a promise state handle or a promise states table entry could not get its
/// owners counter while being deserialized.
#[derive(Debug, thiserror::Error)]
pub enum PromiseStateHandleRegistryError {
    /// The deserialization does not run inside a restore.
    #[error("a promise state handle was decoded outside a runtime state restore")]
    NotInScope,

    /// The registry of this thread cannot be reached.
    #[error("the promise state handle registry of this thread cannot be reached")]
    Unavailable,
}

/// A registry in scope on this thread until the value is dropped; the
/// registry that was in scope before is put back then.
pub struct PromiseStateHandleRegistryScope {
    /// The registry this scope replaced.
    previous: Option<HashMap<PromiseStateId, AtomicOwnersCounter>>,
}

impl PromiseStateHandleRegistryScope {
    /// Put a new, empty registry in scope on this thread.
    pub fn enter() -> Result<Self, PromiseStateHandleRegistryError> {
        let previous = REGISTRY
            .try_with(|registry| {
                let mut registry = registry
                    .try_borrow_mut()
                    .map_err(|_| PromiseStateHandleRegistryError::Unavailable)?;
                Ok(registry.replace(HashMap::new()))
            })
            .map_err(|_| PromiseStateHandleRegistryError::Unavailable)??;

        Ok(Self { previous })
    }
}

impl Drop for PromiseStateHandleRegistryScope {
    fn drop(&mut self) {
        let previous = self.previous.take();

        // The registry is only ever borrowed for the length of one call
        // below, so it can be reached here; if the thread is already
        // tearing its locals down there is nothing left to restore.
        let _restored = REGISTRY.try_with(|registry| {
            if let Ok(mut registry) = registry.try_borrow_mut() {
                *registry = previous;
            }
        });
    }
}

/// The owners counter for `id` from the registry in scope, created on the
/// first request for that id.
pub fn owners_counter(
    id: PromiseStateId,
) -> Result<AtomicOwnersCounter, PromiseStateHandleRegistryError> {
    REGISTRY
        .try_with(|registry| {
            let mut registry = registry
                .try_borrow_mut()
                .map_err(|_| PromiseStateHandleRegistryError::Unavailable)?;
            let registry = registry
                .as_mut()
                .ok_or(PromiseStateHandleRegistryError::NotInScope)?;
            Ok(registry.entry(id).or_default().clone())
        })
        .map_err(|_| PromiseStateHandleRegistryError::Unavailable)?
}
