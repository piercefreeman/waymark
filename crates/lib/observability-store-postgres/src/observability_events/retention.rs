//! The retention side of the observability-events subsystem.

use crate::Store;

impl waymark_observability_events_retention_backend::ApplyRetention for Store {
    type Error = sqlx::Error;

    async fn apply_retention(
        &self,
        cutoff: chrono::DateTime<chrono::Utc>,
    ) -> Result<u64, sqlx::Error> {
        let result = sqlx::query(
            r#"
            DELETE FROM observability_events
            WHERE at < $1
            "#,
        )
        .bind(cutoff)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }
}
