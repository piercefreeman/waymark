//! The retention side of the essential-metrics family.

use crate::Store;

impl waymark_essential_metrics_retention_backend::ApplyRetention for Store {
    type Error = sqlx::Error;

    async fn apply_retention(
        &self,
        cutoff: chrono::DateTime<chrono::Utc>,
    ) -> Result<u64, sqlx::Error> {
        let result = sqlx::query(
            r#"
            DELETE FROM essential_metrics_node_samples
            WHERE sampled_at < $1
            "#,
        )
        .bind(cutoff)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }
}
