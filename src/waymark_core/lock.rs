//! Instance lock tracking and heartbeat maintenance.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};
use tokio::sync::Notify;
use uuid::Uuid;

use tracing::{debug, info, warn};

use crate::backends::{CoreBackend, LockClaim};

#[derive(Clone)]
pub struct InstanceLockTracker {
    lock_uuid: Uuid,
    owned: Arc<Mutex<HashSet<Uuid>>>,
}

impl InstanceLockTracker {
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
        self.owned
            .lock()
            .expect("lock tracker poisoned")
            .iter()
            .copied()
            .collect()
    }
}

pub fn spawn_lock_heartbeat(
    backend: Arc<dyn CoreBackend>,
    tracker: InstanceLockTracker,
    heartbeat_interval: Duration,
    lock_ttl: Duration,
    stop: Arc<AtomicBool>,
    stop_notify: Arc<Notify>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            if stop.load(Ordering::SeqCst) {
                info!("lock heartbeat stop flag set");
                break;
            }
            tokio::select! {
                _ = stop_notify.notified() => {
                    info!("lock heartbeat stop notified");
                    break;
                }
                _ = tokio::time::sleep(heartbeat_interval) => {}
            };
            let instance_ids = tracker.snapshot();
            debug!(count = instance_ids.len(), "lock heartbeat tick");
            if instance_ids.is_empty() {
                continue;
            }
            let lock_expires_at = Utc::now()
                + ChronoDuration::from_std(lock_ttl).unwrap_or_else(|_| ChronoDuration::seconds(0));
            debug!(count = instance_ids.len(), "refreshing instance locks");
            if let Err(err) = backend
                .refresh_instance_locks(
                    LockClaim {
                        lock_uuid: tracker.lock_uuid(),
                        lock_expires_at,
                    },
                    &instance_ids,
                )
                .await
            {
                warn!(error = %err, "failed to refresh instance locks");
            }
        }
        info!("lock heartbeat exiting");
    })
}
