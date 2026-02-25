use chrono::Utc;
use uuid::Uuid;
use waymark_backends_core::{BackendError, BackendResult};
use waymark_scheduler_backend::SchedulerBackend;
use waymark_scheduler_core::{
    CreateScheduleParams, ScheduleId, ScheduleType, WorkflowSchedule, compute_next_run,
};

#[async_trait::async_trait]
impl SchedulerBackend for crate::MemoryBackend {
    async fn upsert_schedule(&self, params: &CreateScheduleParams) -> BackendResult<ScheduleId> {
        let mut guard = self.schedules.lock().expect("schedules poisoned");
        let existing_schedule = guard.iter().find_map(|(id, schedule)| {
            if schedule.workflow_name == params.workflow_name
                && schedule.schedule_name == params.schedule_name
            {
                Some((*id, schedule.clone()))
            } else {
                None
            }
        });
        let schedule_id = existing_schedule
            .as_ref()
            .map(|(id, _)| *id)
            .unwrap_or_else(ScheduleId::new);
        let now = Utc::now();
        let next_run_at = match existing_schedule
            .as_ref()
            .and_then(|(_, schedule)| schedule.next_run_at)
        {
            Some(next_run_at) => Some(next_run_at),
            None => Some(
                compute_next_run(
                    params.schedule_type,
                    params.cron_expression.as_deref(),
                    params.interval_seconds,
                    params.jitter_seconds,
                    None,
                )
                .map_err(BackendError::Message)?,
            ),
        };
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
            next_run_at,
            last_run_at: existing_schedule
                .as_ref()
                .and_then(|(_, schedule)| schedule.last_run_at),
            last_instance_id: existing_schedule
                .as_ref()
                .and_then(|(_, schedule)| schedule.last_instance_id),
            created_at: existing_schedule
                .as_ref()
                .map(|(_, schedule)| schedule.created_at)
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
