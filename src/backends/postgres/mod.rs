//! Postgres backend for persisting runner state and action results.

mod core;
mod registry;
mod scheduler;
#[cfg(test)]
mod test_helpers;
mod webapp;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use sqlx::PgPool;

use crate::db;
use crate::observability::obs;

use super::base::{BackendError, BackendResult};

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
    pub async fn connect(dsn: &str) -> BackendResult<Self> {
        let pool = PgPool::connect(dsn).await?;
        db::run_migrations(&pool).await?;
        Ok(Self::new(pool))
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Delete all queued instances from the backing table.
    #[obs]
    pub async fn clear_queue(&self) -> BackendResult<()> {
        Self::count_query(&self.query_counts, "delete:queued_instances_all");
        sqlx::query("DELETE FROM queued_instances")
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Delete all persisted runner data for a clean benchmark run.
    #[obs]
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

    pub(super) fn count_query(counts: &Arc<Mutex<HashMap<String, usize>>>, label: &str) {
        let mut guard = counts.lock().expect("query counts poisoned");
        *guard.entry(label.to_string()).or_insert(0) += 1;
    }

    pub(super) fn count_batch_size(
        counts: &Arc<Mutex<HashMap<String, HashMap<usize, usize>>>>,
        label: &str,
        size: usize,
    ) {
        if size == 0 {
            return;
        }
        let mut guard = counts.lock().expect("batch size counts poisoned");
        let entry = guard.entry(label.to_string()).or_default();
        *entry.entry(size).or_insert(0) += 1;
    }

    pub(super) fn serialize<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, BackendError> {
        rmp_serde::to_vec_named(value).map_err(|e| BackendError::Message(e.to_string()))
    }

    pub(super) fn deserialize<T: serde::de::DeserializeOwned>(
        payload: &[u8],
    ) -> Result<T, BackendError> {
        rmp_serde::from_slice(payload).map_err(|e| BackendError::Message(e.to_string()))
    }
}
