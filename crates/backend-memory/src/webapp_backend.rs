use std::collections::HashMap;

use chrono::Utc;
use uuid::Uuid;
use waymark_backends_core::{BackendError, BackendResult};
use waymark_webapp_backend::WebappBackend;
use waymark_webapp_core::{
    ExecutionGraphView, InstanceDetail, InstanceStatus, InstanceSummary, ScheduleDetail,
    ScheduleInvocationSummary, ScheduleSummary, TimelineEntry, WorkerActionRow,
    WorkerAggregateStats, WorkerStatus,
};
use waymark_worker_status_backend::WorkerStatusUpdate;

#[async_trait::async_trait]
impl WebappBackend for crate::MemoryBackend {
    async fn count_instances(&self, _search: Option<&str>) -> BackendResult<i64> {
        Ok(0)
    }

    async fn list_instances(
        &self,
        _search: Option<&str>,
        _limit: i64,
        _offset: i64,
    ) -> BackendResult<Vec<InstanceSummary>> {
        Ok(Vec::new())
    }

    async fn get_instance(&self, instance_id: Uuid) -> BackendResult<InstanceDetail> {
        Err(BackendError::Message(format!(
            "instance not found: {instance_id}"
        )))
    }

    async fn get_execution_graph(
        &self,
        _instance_id: Uuid,
    ) -> BackendResult<Option<ExecutionGraphView>> {
        Ok(None)
    }

    async fn get_workflow_graph(
        &self,
        _instance_id: Uuid,
    ) -> BackendResult<Option<ExecutionGraphView>> {
        Ok(None)
    }

    async fn get_action_results(&self, _instance_id: Uuid) -> BackendResult<Vec<TimelineEntry>> {
        Ok(Vec::new())
    }

    async fn get_distinct_workflows(&self) -> BackendResult<Vec<String>> {
        Ok(Vec::new())
    }

    async fn get_distinct_statuses(&self) -> BackendResult<Vec<String>> {
        Ok(vec![
            InstanceStatus::Queued.to_string(),
            InstanceStatus::Running.to_string(),
            InstanceStatus::Completed.to_string(),
            InstanceStatus::Failed.to_string(),
        ])
    }

    async fn count_schedules(&self) -> BackendResult<i64> {
        let guard = self.schedules.lock().expect("schedules poisoned");
        Ok(guard
            .values()
            .filter(|schedule| schedule.status != "deleted")
            .count() as i64)
    }

    async fn list_schedules(&self, limit: i64, offset: i64) -> BackendResult<Vec<ScheduleSummary>> {
        let guard = self.schedules.lock().expect("schedules poisoned");
        let mut schedules: Vec<_> = guard
            .values()
            .filter(|schedule| schedule.status != "deleted")
            .cloned()
            .collect();
        schedules.sort_by(|a, b| {
            (&a.workflow_name, &a.schedule_name).cmp(&(&b.workflow_name, &b.schedule_name))
        });

        let start = offset.max(0) as usize;
        let page_limit = limit.max(0) as usize;
        Ok(schedules
            .into_iter()
            .skip(start)
            .take(page_limit)
            .map(|schedule| ScheduleSummary {
                id: schedule.id.to_string(),
                workflow_name: schedule.workflow_name,
                schedule_name: schedule.schedule_name,
                schedule_type: schedule.schedule_type,
                cron_expression: schedule.cron_expression,
                interval_seconds: schedule.interval_seconds,
                status: schedule.status,
                next_run_at: schedule.next_run_at.map(|dt| dt.to_rfc3339()),
                last_run_at: schedule.last_run_at.map(|dt| dt.to_rfc3339()),
                created_at: schedule.created_at.to_rfc3339(),
            })
            .collect())
    }

    async fn get_schedule(&self, schedule_id: Uuid) -> BackendResult<ScheduleDetail> {
        let guard = self.schedules.lock().expect("schedules poisoned");
        let schedule = guard
            .values()
            .find(|schedule| schedule.id == schedule_id)
            .cloned()
            .ok_or_else(|| BackendError::Message(format!("schedule not found: {schedule_id}")))?;

        let input_payload = schedule.input_payload.as_ref().and_then(|bytes| {
            rmp_serde::from_slice::<serde_json::Value>(bytes)
                .ok()
                .and_then(|value| serde_json::to_string_pretty(&value).ok())
        });

        Ok(ScheduleDetail {
            id: schedule.id.to_string(),
            workflow_name: schedule.workflow_name,
            schedule_name: schedule.schedule_name,
            schedule_type: schedule.schedule_type,
            cron_expression: schedule.cron_expression,
            interval_seconds: schedule.interval_seconds,
            jitter_seconds: schedule.jitter_seconds,
            status: schedule.status,
            next_run_at: schedule.next_run_at.map(|dt| dt.to_rfc3339()),
            last_run_at: schedule.last_run_at.map(|dt| dt.to_rfc3339()),
            last_instance_id: schedule.last_instance_id.map(|id| id.to_string()),
            created_at: schedule.created_at.to_rfc3339(),
            updated_at: schedule.updated_at.to_rfc3339(),
            priority: schedule.priority,
            allow_duplicate: schedule.allow_duplicate,
            input_payload,
        })
    }

    async fn count_schedule_invocations(&self, _schedule_id: Uuid) -> BackendResult<i64> {
        Ok(0)
    }

    async fn list_schedule_invocations(
        &self,
        _schedule_id: Uuid,
        _limit: i64,
        _offset: i64,
    ) -> BackendResult<Vec<ScheduleInvocationSummary>> {
        Ok(Vec::new())
    }

    async fn update_schedule_status(&self, schedule_id: Uuid, status: &str) -> BackendResult<bool> {
        let mut guard = self.schedules.lock().expect("schedules poisoned");
        let Some(schedule) = guard
            .values_mut()
            .find(|schedule| schedule.id == schedule_id)
        else {
            return Ok(false);
        };
        schedule.status = status.to_string();
        schedule.updated_at = Utc::now();
        Ok(true)
    }

    async fn get_distinct_schedule_statuses(&self) -> BackendResult<Vec<String>> {
        Ok(vec!["active".to_string(), "paused".to_string()])
    }

    async fn get_distinct_schedule_types(&self) -> BackendResult<Vec<String>> {
        Ok(vec!["cron".to_string(), "interval".to_string()])
    }

    async fn get_worker_action_stats(
        &self,
        _window_minutes: i64,
    ) -> BackendResult<Vec<WorkerActionRow>> {
        let statuses = latest_worker_statuses(
            &self
                .worker_status_updates
                .lock()
                .expect("worker status updates poisoned"),
        );

        Ok(statuses
            .into_iter()
            .map(|status| WorkerActionRow {
                pool_id: status.pool_id.to_string(),
                active_workers: status.active_workers as i64,
                actions_per_sec: format!("{:.1}", status.actions_per_sec),
                throughput_per_min: status.throughput_per_min as i64,
                total_completed: status.total_completed,
                median_dequeue_ms: status.median_dequeue_ms,
                median_handling_ms: status.median_handling_ms,
                last_action_at: status.last_action_at.map(|dt| dt.to_rfc3339()),
                updated_at: status.updated_at.to_rfc3339(),
            })
            .collect())
    }

    async fn get_worker_aggregate_stats(
        &self,
        _window_minutes: i64,
    ) -> BackendResult<WorkerAggregateStats> {
        let statuses = latest_worker_statuses(
            &self
                .worker_status_updates
                .lock()
                .expect("worker status updates poisoned"),
        );

        let active_worker_count = statuses
            .iter()
            .map(|status| status.active_workers as i64)
            .sum();
        let total_in_flight = statuses
            .iter()
            .filter_map(|status| status.total_in_flight)
            .sum();
        let total_queue_depth = statuses
            .iter()
            .filter_map(|status| status.dispatch_queue_size)
            .sum();
        let actions_per_sec = statuses
            .iter()
            .map(|status| status.actions_per_sec)
            .sum::<f64>();

        Ok(WorkerAggregateStats {
            active_worker_count,
            actions_per_sec: format!("{:.1}", actions_per_sec),
            total_in_flight,
            total_queue_depth,
        })
    }

    async fn worker_status_table_exists(&self) -> bool {
        !self
            .worker_status_updates
            .lock()
            .expect("worker status updates poisoned")
            .is_empty()
    }

    async fn schedules_table_exists(&self) -> bool {
        !self
            .schedules
            .lock()
            .expect("schedules poisoned")
            .is_empty()
    }

    async fn get_worker_statuses(&self, _window_minutes: i64) -> BackendResult<Vec<WorkerStatus>> {
        Ok(latest_worker_statuses(
            &self
                .worker_status_updates
                .lock()
                .expect("worker status updates poisoned"),
        ))
    }
}

fn latest_worker_statuses(updates: &[WorkerStatusUpdate]) -> Vec<WorkerStatus> {
    let mut by_pool: HashMap<Uuid, WorkerStatusUpdate> = HashMap::new();
    for update in updates {
        by_pool.insert(update.pool_id, update.clone());
    }

    let now = Utc::now();
    let mut statuses: Vec<_> = by_pool
        .into_values()
        .map(|status| WorkerStatus {
            pool_id: status.pool_id,
            active_workers: status.active_workers,
            throughput_per_min: status.throughput_per_min,
            actions_per_sec: status.actions_per_sec,
            total_completed: status.total_completed,
            last_action_at: status.last_action_at,
            updated_at: now,
            median_dequeue_ms: status.median_dequeue_ms,
            median_handling_ms: status.median_handling_ms,
            dispatch_queue_size: Some(status.dispatch_queue_size),
            total_in_flight: Some(status.total_in_flight),
            median_instance_duration_secs: status.median_instance_duration_secs,
            active_instance_count: status.active_instance_count,
            total_instances_completed: status.total_instances_completed,
            instances_per_sec: status.instances_per_sec,
            instances_per_min: status.instances_per_min,
            time_series: status.time_series,
        })
        .collect();

    statuses.sort_by(|left, right| right.actions_per_sec.total_cmp(&left.actions_per_sec));
    statuses
}
