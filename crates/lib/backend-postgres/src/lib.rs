//! Postgres backend for persisting runner state and action results.

mod codec;
mod core;
mod macros;
mod registry;
mod scheduler;
#[cfg(test)]
mod test_helpers;
mod webapp;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use sqlx::PgPool;
use waymark_backends_core::{BackendError, BackendResult};
use waymark_metrics_util::Val as MetricsVal;
use waymark_observability::obs;
use waymark_secret_string::SecretStr;
use waymark_timed_future::TimedFutureExt as _;

/// Persist runner state and action results in Postgres.
#[derive(Clone)]
pub struct PostgresBackend {
    pool: PgPool,
    query_counts: Arc<Mutex<HashMap<String, usize>>>,
    batch_size_counts: Arc<Mutex<HashMap<String, HashMap<usize, usize>>>>,
}

impl PostgresBackend {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            query_counts: Arc::new(Mutex::new(HashMap::new())),
            batch_size_counts: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    #[obs]
    pub async fn connect(dsn: &SecretStr) -> BackendResult<Self> {
        let pool = PgPool::connect(dsn.expose_secret()).await?;
        waymark_backend_postgres_migrations::run(&pool)
            .await
            .map_err(|err| BackendError::Message(err.to_string()))?;
        Ok(Self::new(pool))
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Delete all queued instances from the backing table.
    #[obs]
    #[function_name::named]
    pub async fn clear_queue(&self) -> BackendResult<()> {
        Self::count_query(&self.query_counts, "delete:queued_instances_all");
        sqlx::query("DELETE FROM queued_instances")
            .execute(&self.pool)
            .timed(crate::query_timing_histogram!(
                "delete:queued_instances_all"
            ))
            .await?;
        Ok(())
    }

    /// Delete all persisted runner data for a clean benchmark run.
    #[obs]
    #[function_name::named]
    pub async fn clear_all(&self) -> BackendResult<()> {
        Self::count_query(&self.query_counts, "truncate:runner_tables");
        sqlx::query(
            r#"
            TRUNCATE runner_actions_done,
                     runner_instances,
                     queued_instances
            RESTART IDENTITY
            "#,
        )
        .execute(&self.pool)
        .timed(crate::query_timing_histogram!("truncate:runner_tables"))
        .await?;
        Ok(())
    }

    pub fn query_counts(&self) -> HashMap<String, usize> {
        self.query_counts
            .lock()
            .expect("query counts poisoned")
            .clone()
    }

    pub fn batch_size_counts(&self) -> HashMap<String, HashMap<usize, usize>> {
        self.batch_size_counts
            .lock()
            .expect("batch size counts poisoned")
            .clone()
    }

    pub(crate) fn count_query(counts: &Arc<Mutex<HashMap<String, usize>>>, label: &'static str) {
        metrics::counter!("waymark_postgres_queries_total", "label" => label).increment(1);
        let mut guard = counts.lock().expect("query counts poisoned");
        *guard.entry(label.to_string()).or_insert(0) += 1;
    }

    pub(crate) fn count_batch_size(
        counts: &Arc<Mutex<HashMap<String, HashMap<usize, usize>>>>,
        label: &'static str,
        size: usize,
    ) {
        if size == 0 {
            return;
        }
        metrics::histogram!("waymark_postgres_queries_batch_size", "label" => label)
            .record(MetricsVal(size));
        let mut guard = counts.lock().expect("batch size counts poisoned");
        let entry = guard.entry(label.to_string()).or_default();
        *entry.entry(size).or_insert(0) += 1;
    }

    pub(crate) fn query_timing_histogram(
        fn_name: &'static str,
        label: &'static str,
    ) -> metrics::Histogram {
        metrics::histogram!(
            "waymark_postgres_query_seconds",
            "fn_name" => fn_name,
            "label" => label
        )
    }

    pub(crate) fn serialize<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, BackendError> {
        codec::serialize(value).map_err(|e| BackendError::Message(e.to_string()))
    }

    pub(crate) fn deserialize<T: serde::de::DeserializeOwned>(
        payload: &[u8],
    ) -> Result<T, BackendError> {
        codec::deserialize(payload).map_err(|e| BackendError::Message(e.to_string()))
    }
}

/// The common postgres backend error.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Sqlx(sqlx::Error),
}
