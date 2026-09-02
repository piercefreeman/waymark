//! The sink side: appending samples.

use nonempty_collections::NESlice;
use waymark_essential_metrics_core::NodeSample;

use super::common::NODE_SAMPLE_COLUMNS;
use crate::Store;
use crate::common::to_bigint_saturating;

/// Saturate histogram counts into the `bigint[]` column domain; sqlx
/// binds a slice, so the fixed-length counts become a `Vec` here.
fn to_bigint_array<const N: usize>(counts: [u64; N]) -> Vec<i64> {
    counts.into_iter().map(to_bigint_saturating).collect()
}

impl waymark_essential_metrics_sink_backend::HasNodeId for Store {
    type NodeId = waymark_ids::NodeId;
}

impl waymark_essential_metrics_sink_backend::AppendSamples for Store {
    type Error = sqlx::Error;

    async fn append_samples(
        &self,
        samples: NESlice<'_, NodeSample<waymark_ids::NodeId>>,
    ) -> Result<(), sqlx::Error> {
        let mut query = sqlx::QueryBuilder::new(format!(
            r#"
            INSERT INTO essential_metrics_node_samples ({NODE_SAMPLE_COLUMNS})
            "#,
        ));
        query.push_values(samples.iter(), |mut row, sample| {
            row.push_bind(sample.node_id)
                .push_bind(sample.sampled_at)
                .push_bind(to_bigint_saturating(sample.worker_pool_size))
                .push_bind(to_bigint_saturating(sample.max_in_flight_actions))
                .push_bind(to_bigint_saturating(sample.in_flight_actions))
                .push_bind(to_bigint_saturating(sample.queued_action_dispatches))
                .push_bind(to_bigint_saturating(sample.driven_vm_runtimes))
                .push_bind(to_bigint_saturating(sample.actions_completed_total))
                .push_bind(sample.last_action_completed_at)
                .push_bind(to_bigint_array(sample.action_dequeue_seconds.counts))
                .push_bind(sample.action_dequeue_seconds.sum)
                .push_bind(to_bigint_array(sample.action_handling_seconds.counts))
                .push_bind(sample.action_handling_seconds.sum)
                .push_bind(to_bigint_saturating(sample.essential_metrics_dropped_total));
        });
        query.build().execute(&self.pool).await?;
        Ok(())
    }
}
