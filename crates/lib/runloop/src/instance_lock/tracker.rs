//! Instance lock tracking.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use uuid::Uuid;

#[derive(Clone)]
pub struct Tracker {
    lock_uuid: Uuid,
    owned: Arc<Mutex<HashSet<Uuid>>>,
}

impl Tracker {
    pub fn new(lock_uuid: Uuid) -> Self {
        Self {
            lock_uuid,
            owned: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub fn lock_uuid(&self) -> Uuid {
        self.lock_uuid
    }

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
