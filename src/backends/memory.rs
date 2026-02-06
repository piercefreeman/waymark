//! In-memory backend that prints persistence operations.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::base::{
    ActionDone, BackendError, BackendResult, CoreBackend, GraphUpdate, InstanceDone,
    InstanceLockStatus, LockClaim, QueuedInstance, QueuedInstanceBatch, SchedulerBackend,
    WebappBackend, WorkerStatusBackend, WorkerStatusUpdate, WorkflowRegistration,
    WorkflowRegistryBackend, WorkflowVersion,
};
use crate::scheduler::compute_next_run;
use crate::scheduler::{CreateScheduleParams, ScheduleId, ScheduleType, WorkflowSchedule};
use crate::webapp::{
    ExecutionGraphView, InstanceDetail, InstanceStatus, InstanceSummary, ScheduleDetail,
    ScheduleSummary, TimelineEntry, WorkerActionRow, WorkerAggregateStats, WorkerStatus,
};
use tonic::async_trait;

type WorkflowVersionKey = (String, String);
type WorkflowVersionValue = (Uuid, WorkflowRegistration);
type WorkflowVersionStore = HashMap<WorkflowVersionKey, WorkflowVersionValue>;
type InstanceLockStore = HashMap<Uuid, (Option<Uuid>, Option<DateTime<Utc>>)>;

/// Backend that stores updates in memory for tests or local runs.
#[derive(Clone)]
pub struct MemoryBackend {
    instance_queue: Option<Arc<Mutex<VecDeque<QueuedInstance>>>>,
    graph_updates: Arc<Mutex<Vec<GraphUpdate>>>,
    actions_done: Arc<Mutex<Vec<ActionDone>>>,
    instances_done: Arc<Mutex<Vec<InstanceDone>>>,
    worker_status_updates: Arc<Mutex<Vec<WorkerStatusUpdate>>>,
    workflow_versions: Arc<Mutex<WorkflowVersionStore>>,
    schedules: Arc<Mutex<HashMap<ScheduleId, WorkflowSchedule>>>,
    instance_locks: Arc<Mutex<InstanceLockStore>>,
}

impl Default for MemoryBackend {
    fn default() -> Self {
        Self {
            instance_queue: None,
            graph_updates: Arc::new(Mutex::new(Vec::new())),
            actions_done: Arc::new(Mutex::new(Vec::new())),
            instances_done: Arc::new(Mutex::new(Vec::new())),
            worker_status_updates: Arc::new(Mutex::new(Vec::new())),
            workflow_versions: Arc::new(Mutex::new(HashMap::new())),
            schedules: Arc::new(Mutex::new(HashMap::new())),
            instance_locks: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl MemoryBackend {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_queue(queue: Arc<Mutex<VecDeque<QueuedInstance>>>) -> Self {
        Self {
            instance_queue: Some(queue),
            ..Self::default()
        }
    }

    pub fn instance_queue(&self) -> Option<Arc<Mutex<VecDeque<QueuedInstance>>>> {
        self.instance_queue.clone()
    }

    pub fn graph_updates(&self) -> Vec<GraphUpdate> {
        self.graph_updates
            .lock()
            .expect("graph updates poisoned")
            .clone()
    }

    pub fn actions_done(&self) -> Vec<ActionDone> {
        self.actions_done
            .lock()
            .expect("actions done poisoned")
            .clone()
    }

    pub fn instances_done(&self) -> Vec<InstanceDone> {
        self.instances_done
            .lock()
            .expect("instances done poisoned")
            .clone()
    }

    pub fn worker_status_updates(&self) -> Vec<WorkerStatusUpdate> {
        self.worker_status_updates
            .lock()
            .expect("worker status updates poisoned")
            .clone()
    }
}

#[async_trait]
impl CoreBackend for MemoryBackend {
    fn clone_box(&self) -> Box<dyn CoreBackend> {
        Box::new(self.clone())
    }

    async fn save_graphs(
        &self,
        _lock_uuid: Uuid,
        graphs: &[GraphUpdate],
    ) -> BackendResult<Vec<InstanceLockStatus>> {
        let mut stored = self.graph_updates.lock().expect("graph updates poisoned");
        stored.extend(graphs.iter().cloned());
        let guard = self.instance_locks.lock().expect("instance locks poisoned");
        let mut locks = Vec::with_capacity(graphs.len());
        for graph in graphs {
            let (lock_uuid, lock_expires_at) = guard
                .get(&graph.instance_id)
                .cloned()
                .unwrap_or((None, None));
            locks.push(InstanceLockStatus {
                instance_id: graph.instance_id,
                lock_uuid,
                lock_expires_at,
            });
        }
        Ok(locks)
    }

    async fn save_actions_done(&self, actions: &[ActionDone]) -> BackendResult<()> {
        let mut stored = self.actions_done.lock().expect("actions done poisoned");
        stored.extend(actions.iter().cloned());
        Ok(())
    }

    async fn save_instances_done(&self, instances: &[InstanceDone]) -> BackendResult<()> {
        let mut stored = self.instances_done.lock().expect("instances done poisoned");
        stored.extend(instances.iter().cloned());
        if !instances.is_empty() {
            let mut locks = self.instance_locks.lock().expect("instance locks poisoned");
            for instance in instances {
                locks.remove(&instance.executor_id);
            }
        }
        Ok(())
    }

    async fn get_queued_instances(
        &self,
        size: usize,
        claim: LockClaim,
    ) -> BackendResult<QueuedInstanceBatch> {
        if size == 0 {
            return Ok(QueuedInstanceBatch {
                instances: Vec::new(),
            });
        }
        let queue = match &self.instance_queue {
            Some(queue) => queue,
            None => {
                return Ok(QueuedInstanceBatch {
                    instances: Vec::new(),
                });
            }
        };
        let mut guard = queue.lock().expect("instance queue poisoned");
        let now = Utc::now();
        let mut instances = Vec::new();
        while instances.len() < size {
            let Some(instance) = guard.front() else {
                break;
            };
            if let Some(scheduled_at) = instance.scheduled_at
                && scheduled_at > now
            {
                break;
            }
            let instance = guard.pop_front().expect("instance queue empty");
            instances.push(instance);
        }
        if !instances.is_empty() {
            let mut locks = self.instance_locks.lock().expect("instance locks poisoned");
            for instance in &instances {
                locks.insert(
                    instance.instance_id,
                    (Some(claim.lock_uuid), Some(claim.lock_expires_at)),
                );
            }
        }
        Ok(QueuedInstanceBatch { instances })
    }

    async fn queue_instances(&self, instances: &[QueuedInstance]) -> BackendResult<()> {
        if instances.is_empty() {
            return Ok(());
        }
        let queue = self.instance_queue.as_ref().ok_or_else(|| {
            BackendError::Message("memory backend missing instance queue".to_string())
        })?;
        let mut guard = queue.lock().expect("instance queue poisoned");
        for instance in instances {
            guard.push_back(instance.clone());
        }
        Ok(())
    }

    async fn refresh_instance_locks(
        &self,
        claim: LockClaim,
        instance_ids: &[Uuid],
    ) -> BackendResult<Vec<InstanceLockStatus>> {
        let mut guard = self.instance_locks.lock().expect("instance locks poisoned");
        let mut locks = Vec::new();
        for instance_id in instance_ids {
            let entry = guard
                .entry(*instance_id)
                .or_insert((Some(claim.lock_uuid), Some(claim.lock_expires_at)));
            if entry.0 == Some(claim.lock_uuid) {
                entry.1 = Some(claim.lock_expires_at);
            }
            locks.push(InstanceLockStatus {
                instance_id: *instance_id,
                lock_uuid: entry.0,
                lock_expires_at: entry.1,
            });
        }
        Ok(locks)
    }

    async fn release_instance_locks(
        &self,
        lock_uuid: Uuid,
        instance_ids: &[Uuid],
    ) -> BackendResult<()> {
        let mut guard = self.instance_locks.lock().expect("instance locks poisoned");
        for instance_id in instance_ids {
            if let Some((current_lock, _)) = guard.get(instance_id)
                && *current_lock == Some(lock_uuid)
            {
                guard.remove(instance_id);
            }
        }
        Ok(())
    }
}

#[async_trait]
impl WorkerStatusBackend for MemoryBackend {
    async fn upsert_worker_status(&self, status: &WorkerStatusUpdate) -> BackendResult<()> {
        let mut stored = self
            .worker_status_updates
            .lock()
            .expect("worker status updates poisoned");
        stored.push(status.clone());
        Ok(())
    }
}

#[async_trait]
impl WorkflowRegistryBackend for MemoryBackend {
    async fn upsert_workflow_version(
        &self,
        registration: &WorkflowRegistration,
    ) -> BackendResult<Uuid> {
        let mut guard = self
            .workflow_versions
            .lock()
            .expect("workflow versions poisoned");
        let key = (
            registration.workflow_name.clone(),
            registration.workflow_version.clone(),
        );
        if let Some((id, existing)) = guard.get(&key) {
            if existing.ir_hash != registration.ir_hash {
                return Err(BackendError::Message(format!(
                    "workflow version already exists with different IR hash: {}@{}",
                    registration.workflow_name, registration.workflow_version
                )));
            }
            return Ok(*id);
        }

        let id = Uuid::new_v4();
        guard.insert(key, (id, registration.clone()));
        Ok(id)
    }

    async fn get_workflow_versions(&self, ids: &[Uuid]) -> BackendResult<Vec<WorkflowVersion>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let guard = self
            .workflow_versions
            .lock()
            .expect("workflow versions poisoned");
        let mut versions = Vec::new();
        for (id, registration) in guard.values() {
            if ids.contains(id) {
                versions.push(WorkflowVersion {
                    id: *id,
                    workflow_name: registration.workflow_name.clone(),
                    workflow_version: registration.workflow_version.clone(),
                    ir_hash: registration.ir_hash.clone(),
                    program_proto: registration.program_proto.clone(),
                    concurrent: registration.concurrent,
                });
            }
        }
        Ok(versions)
    }
}

#[async_trait]
impl SchedulerBackend for MemoryBackend {
    async fn upsert_schedule(&self, params: &CreateScheduleParams) -> BackendResult<ScheduleId> {
        let mut guard = self.schedules.lock().expect("schedules poisoned");
        let existing_id = guard.iter().find_map(|(id, schedule)| {
            if schedule.workflow_name == params.workflow_name
                && schedule.schedule_name == params.schedule_name
            {
                Some(*id)
            } else {
                None
            }
        });
        let schedule_id = existing_id.unwrap_or_else(ScheduleId::new);
        let now = Utc::now();
        let next_run_at = compute_next_run(
            params.schedule_type,
            params.cron_expression.as_deref(),
            params.interval_seconds,
            params.jitter_seconds,
            None,
        )
        .map_err(BackendError::Message)?;
        let schedule = WorkflowSchedule {
            id: schedule_id.0,
            workflow_name: params.workflow_name.clone(),
            schedule_name: params.schedule_name.clone(),
            schedule_type: params.schedule_type.as_str().to_string(),
            cron_expression: params.cron_expression.clone(),
            interval_seconds: params.interval_seconds,
            jitter_seconds: params.jitter_seconds,
            input_payload: params.input_payload.clone(),
            status: "active".to_string(),
            next_run_at: Some(next_run_at),
            last_run_at: None,
            last_instance_id: None,
            created_at: guard
                .get(&schedule_id)
                .map(|existing| existing.created_at)
                .unwrap_or(now),
            updated_at: now,
            priority: params.priority,
            allow_duplicate: params.allow_duplicate,
        };
        guard.insert(schedule_id, schedule);
        Ok(schedule_id)
    }

    async fn get_schedule(&self, id: ScheduleId) -> BackendResult<WorkflowSchedule> {
        let guard = self.schedules.lock().expect("schedules poisoned");
        guard
            .get(&id)
            .cloned()
            .ok_or_else(|| BackendError::Message(format!("schedule not found: {id}")))
    }

    async fn get_schedule_by_name(
        &self,
        workflow_name: &str,
        schedule_name: &str,
    ) -> BackendResult<Option<WorkflowSchedule>> {
        let guard = self.schedules.lock().expect("schedules poisoned");
        Ok(guard
            .values()
            .find(|schedule| {
                schedule.workflow_name == workflow_name
                    && schedule.schedule_name == schedule_name
                    && schedule.status != "deleted"
            })
            .cloned())
    }

    async fn list_schedules(
        &self,
        limit: i64,
        offset: i64,
    ) -> BackendResult<Vec<WorkflowSchedule>> {
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
        let end = start.saturating_add(limit.max(0) as usize);
        Ok(schedules
            .into_iter()
            .skip(start)
            .take(end - start)
            .collect())
    }

    async fn count_schedules(&self) -> BackendResult<i64> {
        let guard = self.schedules.lock().expect("schedules poisoned");
        Ok(guard
            .values()
            .filter(|schedule| schedule.status != "deleted")
            .count() as i64)
    }

    async fn update_schedule_status(&self, id: ScheduleId, status: &str) -> BackendResult<bool> {
        let mut guard = self.schedules.lock().expect("schedules poisoned");
        if let Some(schedule) = guard.get_mut(&id) {
            schedule.status = status.to_string();
            schedule.updated_at = Utc::now();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn delete_schedule(&self, id: ScheduleId) -> BackendResult<bool> {
        SchedulerBackend::update_schedule_status(self, id, "deleted").await
    }

    async fn find_due_schedules(&self, limit: i32) -> BackendResult<Vec<WorkflowSchedule>> {
        let guard = self.schedules.lock().expect("schedules poisoned");
        let now = Utc::now();
        let mut schedules: Vec<_> = guard
            .values()
            .filter(|schedule| {
                schedule.status == "active"
                    && schedule
                        .next_run_at
                        .map(|next| next <= now)
                        .unwrap_or(false)
            })
            .cloned()
            .collect();
        schedules.sort_by_key(|schedule| schedule.next_run_at);
        Ok(schedules.into_iter().take(limit as usize).collect())
    }

    async fn has_running_instance(&self, _schedule_id: ScheduleId) -> BackendResult<bool> {
        Ok(false)
    }

    async fn mark_schedule_executed(
        &self,
        schedule_id: ScheduleId,
        instance_id: Uuid,
    ) -> BackendResult<()> {
        let mut guard = self.schedules.lock().expect("schedules poisoned");
        let schedule = guard
            .get_mut(&schedule_id)
            .ok_or_else(|| BackendError::Message(format!("schedule not found: {schedule_id}")))?;
        let schedule_type = ScheduleType::parse(&schedule.schedule_type)
            .ok_or_else(|| BackendError::Message("invalid schedule type".to_string()))?;
        let next_run_at = compute_next_run(
            schedule_type,
            schedule.cron_expression.as_deref(),
            schedule.interval_seconds,
            schedule.jitter_seconds,
            Some(Utc::now()),
        )
        .map_err(BackendError::Message)?;
        schedule.last_run_at = Some(Utc::now());
        schedule.last_instance_id = Some(instance_id);
        schedule.next_run_at = Some(next_run_at);
        schedule.updated_at = Utc::now();
        Ok(())
    }

    async fn skip_schedule_run(&self, schedule_id: ScheduleId) -> BackendResult<()> {
        let mut guard = self.schedules.lock().expect("schedules poisoned");
        let schedule = guard
            .get_mut(&schedule_id)
            .ok_or_else(|| BackendError::Message(format!("schedule not found: {schedule_id}")))?;
        let schedule_type = ScheduleType::parse(&schedule.schedule_type)
            .ok_or_else(|| BackendError::Message("invalid schedule type".to_string()))?;
        let next_run_at = compute_next_run(
            schedule_type,
            schedule.cron_expression.as_deref(),
            schedule.interval_seconds,
            schedule.jitter_seconds,
            Some(Utc::now()),
        )
        .map_err(BackendError::Message)?;
        schedule.next_run_at = Some(next_run_at);
        schedule.updated_at = Utc::now();
        Ok(())
    }
}

#[async_trait]
impl WebappBackend for MemoryBackend {
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
