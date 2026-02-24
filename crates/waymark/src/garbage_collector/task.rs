//! Background garbage collector task.
//!
//! This task periodically deletes old finished instances and their action rows.

use std::time::Duration;

use chrono::Utc;
use tracing::{debug, error, info};

use crate::backends::{GarbageCollectionResult, GarbageCollectorBackend};

/// Configuration for the garbage collector task.
#[derive(Debug, Clone)]
pub struct GarbageCollectorConfig {
    /// How often to run a garbage collection sweep.
    pub interval: Duration,
    /// Maximum number of done instances to delete in one batch.
    pub batch_size: usize,
    /// Retention window for done instances.
    pub retention: Duration,
}

impl Default for GarbageCollectorConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(5 * 60),
            batch_size: 100,
            retention: Duration::from_secs(24 * 60 * 60),
        }
    }
}

/// Background garbage collector task.
pub struct GarbageCollectorTask<B> {
    pub backend: B,
    pub config: GarbageCollectorConfig,
}

impl<B> GarbageCollectorTask<B>
where
    B: GarbageCollectorBackend + Clone + Send + Sync + 'static,
{
    /// Run the garbage collector loop.
    pub async fn run(self, shutdown: tokio_util::sync::WaitForCancellationFutureOwned) {
        info!(
            interval_ms = self.config.interval.as_millis(),
            batch_size = self.config.batch_size,
            retention_secs = self.config.retention.as_secs(),
            "garbage collector task started"
        );

        let mut shutdown = std::pin::pin!(shutdown);

        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    info!("garbage collector task shutting down");
                    break;
                }
                _ = tokio::time::sleep(self.config.interval) => {
                    if let Err(err) = self.collect_until_drained().await {
                        error!(error = ?err, "garbage collector sweep failed");
                    }
                }
            }
        }
    }

    async fn collect_until_drained(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut total_deleted_instances = 0usize;
        let mut total_deleted_actions = 0usize;

        loop {
            let result = self.collect_batch().await?;
            total_deleted_instances += result.deleted_instances;
            total_deleted_actions += result.deleted_actions;

            if result.deleted_instances == 0 {
                break;
            }
            if result.deleted_instances < self.config.batch_size {
                break;
            }
            debug!(
                deleted_instances = result.deleted_instances,
                batch_size = self.config.batch_size,
                "garbage collector batch filled; continuing immediately"
            );
        }

        if total_deleted_instances > 0 || total_deleted_actions > 0 {
            info!(
                deleted_instances = total_deleted_instances,
                deleted_actions = total_deleted_actions,
                "garbage collector deleted old workflow data"
            );
        }

        Ok(())
    }

    async fn collect_batch(
        &self,
    ) -> Result<GarbageCollectionResult, Box<dyn std::error::Error + Send + Sync>> {
        let retention = chrono::Duration::from_std(self.config.retention)
            .map_err(|err| format!("invalid garbage collector retention: {err}"))?;
        let older_than = Utc::now() - retention;
        self.backend
            .collect_done_instances(older_than, self.config.batch_size)
            .await
            .map_err(|err| err.into())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use chrono::{Duration as ChronoDuration, Utc};
    use tonic::async_trait;

    use super::*;
    use crate::backends::{BackendResult, GarbageCollectorBackend};

    #[derive(Clone)]
    struct StubGarbageCollectorBackend {
        calls: Arc<AtomicUsize>,
        deleted_instance_batches: Arc<Mutex<VecDeque<usize>>>,
        observed_limits: Arc<Mutex<Vec<usize>>>,
        observed_cutoffs: Arc<Mutex<Vec<chrono::DateTime<Utc>>>>,
    }

    #[async_trait]
    impl GarbageCollectorBackend for StubGarbageCollectorBackend {
        async fn collect_done_instances(
            &self,
            older_than: chrono::DateTime<Utc>,
            limit: usize,
        ) -> BackendResult<GarbageCollectionResult> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.observed_limits
                .lock()
                .expect("limits poisoned")
                .push(limit);
            self.observed_cutoffs
                .lock()
                .expect("cutoffs poisoned")
                .push(older_than);

            let deleted_instances = self
                .deleted_instance_batches
                .lock()
                .expect("batches poisoned")
                .pop_front()
                .unwrap_or(0);

            Ok(GarbageCollectionResult {
                deleted_instances,
                deleted_actions: deleted_instances,
            })
        }
    }

    #[tokio::test]
    async fn garbage_collector_retries_immediately_when_batch_is_full() {
        let backend = StubGarbageCollectorBackend {
            calls: Arc::new(AtomicUsize::new(0)),
            deleted_instance_batches: Arc::new(Mutex::new(VecDeque::from(vec![2, 2, 1]))),
            observed_limits: Arc::new(Mutex::new(Vec::new())),
            observed_cutoffs: Arc::new(Mutex::new(Vec::new())),
        };
        let task = GarbageCollectorTask {
            backend: backend.clone(),
            config: GarbageCollectorConfig {
                interval: Duration::from_secs(60),
                batch_size: 2,
                retention: Duration::from_secs(24 * 60 * 60),
            },
        };

        task.collect_until_drained()
            .await
            .expect("collect until drained");

        assert_eq!(backend.calls.load(Ordering::Relaxed), 3);
        assert_eq!(
            backend.observed_limits.lock().expect("limits").as_slice(),
            &[2, 2, 2]
        );
    }

    #[tokio::test]
    async fn garbage_collector_uses_retention_to_compute_cutoff() {
        let backend = StubGarbageCollectorBackend {
            calls: Arc::new(AtomicUsize::new(0)),
            deleted_instance_batches: Arc::new(Mutex::new(VecDeque::from(vec![0]))),
            observed_limits: Arc::new(Mutex::new(Vec::new())),
            observed_cutoffs: Arc::new(Mutex::new(Vec::new())),
        };
        let task = GarbageCollectorTask {
            backend: backend.clone(),
            config: GarbageCollectorConfig {
                interval: Duration::from_secs(60),
                batch_size: 3,
                retention: Duration::from_secs(24 * 60 * 60),
            },
        };

        let before = Utc::now();
        task.collect_until_drained()
            .await
            .expect("collect until drained");
        let after = Utc::now();

        let cutoffs = backend.observed_cutoffs.lock().expect("cutoffs");
        assert_eq!(cutoffs.len(), 1);
        let cutoff = cutoffs[0];

        let lower = before - ChronoDuration::hours(24) - ChronoDuration::seconds(1);
        let upper = after - ChronoDuration::hours(24) + ChronoDuration::seconds(1);
        assert!(cutoff >= lower, "cutoff should be close to now - retention");
        assert!(cutoff <= upper, "cutoff should be close to now - retention");
    }
}
