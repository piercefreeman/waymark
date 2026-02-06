//! Background scheduler task.
//!
//! This task periodically polls for due schedules and queues new workflow instances.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tracing::{debug, error, info};
use uuid::Uuid;

use super::types::{ScheduleId, WorkflowSchedule};
use crate::backends::{CoreBackend, QueuedInstance, SchedulerBackend};
use crate::rappel_core::dag::DAG;

type DagResolver = Arc<dyn Fn(&str) -> Option<DAG> + Send + Sync>;

/// Configuration for the scheduler task.
#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    /// How often to poll for due schedules.
    pub poll_interval: Duration,
    /// Maximum number of schedules to process per poll.
    pub batch_size: i32,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(1),
            batch_size: 100,
        }
    }
}

/// Background scheduler task.
pub struct SchedulerTask<B> {
    backend: B,
    config: SchedulerConfig,
    shutdown_rx: watch::Receiver<bool>,
    /// Function to get the DAG for a workflow.
    /// This should look up the workflow definition and return its DAG.
    dag_resolver: DagResolver,
}

impl<B> SchedulerTask<B>
where
    B: CoreBackend + SchedulerBackend + Clone + Send + Sync + 'static,
{
    /// Create a new scheduler task.
    pub fn new(
        backend: B,
        config: SchedulerConfig,
        shutdown_rx: watch::Receiver<bool>,
        dag_resolver: DagResolver,
    ) -> Self {
        Self {
            backend,
            config,
            shutdown_rx,
            dag_resolver,
        }
    }

    /// Run the scheduler loop.
    pub async fn run(mut self) {
        info!(
            poll_interval_ms = self.config.poll_interval.as_millis(),
            batch_size = self.config.batch_size,
            "scheduler task started"
        );

        loop {
            tokio::select! {
                _ = self.shutdown_rx.changed() => {
                    if *self.shutdown_rx.borrow() {
                        info!("scheduler task shutting down");
                        break;
                    }
                }
                _ = tokio::time::sleep(self.config.poll_interval) => {
                    if let Err(e) = self.poll_and_fire().await {
                        error!(error = ?e, "scheduler poll failed");
                    }
                }
            }
        }
    }

    /// Poll for due schedules and fire them.
    async fn poll_and_fire(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let schedules = self
            .backend
            .find_due_schedules(self.config.batch_size)
            .await?;

        if schedules.is_empty() {
            return Ok(());
        }

        debug!(count = schedules.len(), "found due schedules");

        for schedule in schedules {
            if let Err(e) = self.fire_schedule(&schedule).await {
                error!(
                    schedule_id = %schedule.id,
                    workflow_name = %schedule.workflow_name,
                    error = ?e,
                    "failed to fire schedule"
                );
                // Skip to next run time to avoid retrying immediately
                if let Err(skip_err) = self
                    .backend
                    .skip_schedule_run(ScheduleId(schedule.id))
                    .await
                {
                    error!(error = ?skip_err, "failed to skip schedule run");
                }
            }
        }

        Ok(())
    }

    /// Fire a single schedule by creating a workflow instance.
    async fn fire_schedule(
        &self,
        schedule: &WorkflowSchedule,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Check for duplicates if not allowed
        if !schedule.allow_duplicate
            && self
                .backend
                .has_running_instance(ScheduleId(schedule.id))
                .await?
        {
            debug!(
                schedule_id = %schedule.id,
                "skipping schedule due to running instance"
            );
            return self
                .backend
                .skip_schedule_run(ScheduleId(schedule.id))
                .await
                .map_err(|e| e.into());
        }

        // Get the DAG for this workflow
        let dag = (self.dag_resolver)(&schedule.workflow_name)
            .ok_or_else(|| format!("no DAG found for workflow: {}", schedule.workflow_name))?;

        // Find the entry node
        let entry_node_str = dag
            .entry_node
            .as_ref()
            .ok_or_else(|| "DAG has no entry node".to_string())?;

        let mut state =
            crate::rappel_core::runner::RunnerState::new(Some(dag.clone()), None, None, false);
        let entry_exec = state
            .queue_template_node(entry_node_str, None)
            .map_err(|err| err.0)?;

        // Create a queued instance
        let instance_id = Uuid::new_v4();
        let queued = QueuedInstance {
            dag,
            entry_node: entry_exec.node_id,
            state: Some(state),
            action_results: HashMap::new(),
            instance_id,
            scheduled_at: None,
        };

        // Queue the instance
        self.backend.queue_instances(&[queued]).await?;

        // Mark the schedule as executed
        self.backend
            .mark_schedule_executed(ScheduleId(schedule.id), instance_id)
            .await?;

        info!(
            schedule_id = %schedule.id,
            instance_id = %instance_id,
            workflow_name = %schedule.workflow_name,
            "fired scheduled workflow"
        );

        Ok(())
    }
}

/// Convenience function to spawn a scheduler task.
pub fn spawn_scheduler<B>(
    backend: B,
    config: SchedulerConfig,
    dag_resolver: DagResolver,
) -> (tokio::task::JoinHandle<()>, watch::Sender<bool>)
where
    B: CoreBackend + SchedulerBackend + Clone + Send + Sync + 'static,
{
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let task = SchedulerTask::new(backend, config, shutdown_rx, dag_resolver);
    let handle = tokio::spawn(task.run());
    (handle, shutdown_tx)
}
