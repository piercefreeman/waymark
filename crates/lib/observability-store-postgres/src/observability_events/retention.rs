//! The retention side of the observability-events subsystem.

use crate::Store;
use crate::common::{RETENTION_CHUNK, delete_before_in_chunks};

impl waymark_observability_events_retention_backend::ApplyRetention for Store {
    type Error = sqlx::Error;

    async fn apply_retention(
        &self,
        cutoff: chrono::DateTime<chrono::Utc>,
    ) -> Result<u64, sqlx::Error> {
        let events = delete_before_in_chunks(
            &self.pool,
            "observability_events",
            "at",
            cutoff,
            RETENTION_CHUNK,
        )
        .await?;

        // The instances the events no longer speak for go with them; the
        // count stays the events'.
        delete_before_in_chunks(
            &self.pool,
            "observability_vm_instances",
            "last_at",
            cutoff,
            RETENTION_CHUNK,
        )
        .await?;

        Ok(events)
    }
}
