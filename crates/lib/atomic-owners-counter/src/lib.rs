//! An atomic count of owners, with no data shared between them.

#![warn(missing_docs)]

use std::num::NonZeroUsize;
use std::sync::{Arc, Weak};

#[cfg(test)]
mod tests;

/// One owner of a shared count: cloning adds an owner, dropping removes one.
#[derive(Debug)]
pub struct AtomicOwnersCounter(Arc<()>);

/// A non-owning observer of an [`AtomicOwnersCounter`]'s count.
#[derive(Debug)]
pub struct WeakAtomicOwnersCounter(Weak<()>);

impl AtomicOwnersCounter {
    /// A new count with this one owner.
    pub fn new() -> Self {
        Self(Arc::new(()))
    }

    /// The number of owners, this one included.
    pub fn strong(&self) -> NonZeroUsize {
        // An owner always counts itself, so the count is at least one and
        // the saturating arithmetic below returns it exactly.
        let others = Arc::strong_count(&self.0).saturating_sub(1);
        NonZeroUsize::MIN.saturating_add(others)
    }

    /// The number of weak observers.
    pub fn weak(&self) -> usize {
        Arc::weak_count(&self.0)
    }

    /// A weak observer of this count.
    pub fn downgrade(&self) -> WeakAtomicOwnersCounter {
        WeakAtomicOwnersCounter(Arc::downgrade(&self.0))
    }
}

impl Default for AtomicOwnersCounter {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for AtomicOwnersCounter {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl WeakAtomicOwnersCounter {
    /// The number of owners; zero once the last one is dropped.
    pub fn strong(&self) -> usize {
        Weak::strong_count(&self.0)
    }

    /// The number of weak observers, this one included while any owner
    /// remains.
    pub fn weak(&self) -> usize {
        Weak::weak_count(&self.0)
    }

    /// A new owner of the count, if any owner remains.
    pub fn upgrade(&self) -> Option<AtomicOwnersCounter> {
        self.0.upgrade().map(AtomicOwnersCounter)
    }
}

impl Clone for WeakAtomicOwnersCounter {
    fn clone(&self) -> Self {
        Self(Weak::clone(&self.0))
    }
}
