//! Instance lock tracking.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use uuid::Uuid;

#[derive(Clone, Default)]
pub struct Tracker {
    owned: Arc<Mutex<HashSet<Uuid>>>,
}

impl Tracker {
    pub fn insert_all<I>(&self, ids: I)
    where
        I: IntoIterator<Item = Uuid>,
    {
        let mut guard = self.owned.lock().expect("lock tracker poisoned");
        for id in ids {
            guard.insert(id);
        }
    }

    pub fn remove_all<I>(&self, ids: I)
    where
        I: IntoIterator<Item = Uuid>,
    {
        let mut guard = self.owned.lock().expect("lock tracker poisoned");
        for id in ids {
            guard.remove(&id);
        }
    }

    pub fn snapshot(&self) -> Vec<Uuid> {
        let owned = self.owned.lock().expect("lock tracker poisoned");
        owned.iter().copied().collect()
    }
}
