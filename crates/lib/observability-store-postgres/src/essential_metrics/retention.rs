//! The retention side of the essential-metrics subsystem.

use crate::Store;
use crate::common::{RETENTION_CHUNK, delete_before_in_chunks};

impl waymark_essential_metrics_retention_backend::ApplyRetention for Store {
    type Error = sqlx::Error;

    async fn apply_retention(
        &self,
        cutoff: chrono::DateTime<chrono::Utc>,
    ) -> Result<u64, sqlx::Error> {
        delete_before_in_chunks(
            &self.pool,
            "essential_metrics_node_samples",
            "sampled_at",
            cutoff,
            RETENTION_CHUNK,
        )
        .await
    }
}
