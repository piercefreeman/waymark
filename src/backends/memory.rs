//! In-memory backend that prints persistence operations.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use chrono::Utc;
use uuid::Uuid;

use super::base::{
    ActionDone, BackendError, BackendResult, CoreBackend, GraphUpdate, InstanceDone,
    QueuedInstance, SchedulerBackend, WorkerStatusBackend, WorkerStatusUpdate,
    WorkflowRegistration, WorkflowRegistryBackend,
};
use crate::scheduler::compute_next_run;
use crate::scheduler::{CreateScheduleParams, ScheduleId, ScheduleType, WorkflowSchedule};
use tonic::async_trait;

type WorkflowVersionKey = (String, String);
type WorkflowVersionValue = (Uuid, WorkflowRegistration);
type WorkflowVersionStore = HashMap<WorkflowVersionKey, WorkflowVersionValue>;

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

    async fn save_graphs(&self, graphs: &[GraphUpdate]) -> BackendResult<()> {
        let mut stored = self.graph_updates.lock().expect("graph updates poisoned");
        stored.extend(graphs.iter().cloned());
        Ok(())
    }

    async fn save_actions_done(&self, actions: &[ActionDone]) -> BackendResult<()> {
        let mut stored = self.actions_done.lock().expect("actions done poisoned");
        stored.extend(actions.iter().cloned());
        Ok(())
    }

    async fn save_instances_done(&self, instances: &[InstanceDone]) -> BackendResult<()> {
        let mut stored = self.instances_done.lock().expect("instances done poisoned");
        stored.extend(instances.iter().cloned());
        Ok(())
    }

    async fn get_queued_instances(&self, size: usize) -> BackendResult<Vec<QueuedInstance>> {
        if size == 0 {
            return Ok(Vec::new());
        }
        let queue = match &self.instance_queue {
            Some(queue) => queue,
            None => return Ok(Vec::new()),
        };
        let mut guard = queue.lock().expect("instance queue poisoned");
        let mut instances = Vec::new();
        for _ in 0..size {
            if let Some(instance) = guard.pop_front() {
                instances.push(instance);
            } else {
                break;
            }
        }
        Ok(instances)
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
            registration.dag_hash.clone(),
        );
        let entry = guard
            .entry(key)
            .or_insert_with(|| (Uuid::new_v4(), registration.clone()));
        entry.1 = registration.clone();
        Ok(entry.0)
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
        self.update_schedule_status(id, "deleted").await
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
