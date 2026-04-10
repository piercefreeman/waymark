use std::collections::HashMap;

use waymark_ids::InstanceId;

#[derive(Debug, Default)]
pub struct Bufferrer {
    open_instances: HashMap<InstanceId, OpenInstance>,
}

#[derive(Debug, Default)]
struct OpenInstance {
    pub actions_correlator: crate::action::Correlator,
    pub action_log_items: Vec<waymark_vcr_file::action::LogItem>,
}

#[derive(Debug, thiserror::Error)]
#[error("instance not found")]
pub struct InstanceNotFound;

#[derive(Debug, thiserror::Error)]
pub enum RecordActionCompletionError {
    #[error("instance not found")]
    InstanceNotFound,

    #[error("action not found")]
    ActionNotFound,
}

impl Bufferrer {
    pub fn open_instance_log(&mut self, queued_instance: waymark_core_backend::QueuedInstance) {
        let open_instance = OpenInstance::default();

        self.open_instances
            .insert(queued_instance.instance_id, open_instance);
    }

    pub fn record_action_request(
        &mut self,
        action_request: waymark_worker_core::ActionRequest,
    ) -> Result<(), InstanceNotFound> {
        let open_instance = self
            .open_instances
            .get_mut(&action_request.executor_id)
            .ok_or(InstanceNotFound)?;

        open_instance
            .actions_correlator
            .insert_request(action_request);

        Ok(())
    }

    pub fn record_action_completion(
        &mut self,
        action_completion: waymark_worker_core::ActionCompletion,
    ) -> Result<(), RecordActionCompletionError> {
        let open_instance = self
            .open_instances
            .get_mut(&action_completion.executor_id)
            .ok_or(RecordActionCompletionError::InstanceNotFound)?;

        let log_item = open_instance
            .actions_correlator
            .correlate_completion(action_completion)
            .ok_or(RecordActionCompletionError::ActionNotFound)?;

        open_instance.action_log_items.push(log_item);

        Ok(())
    }

    pub fn complete_instance_log(
        &mut self,
        id: waymark_ids::InstanceId,
    ) -> Result<waymark_vcr_file::instance::LogItem, InstanceNotFound> {
        let open_instance = self.open_instances.remove(&id).ok_or(InstanceNotFound)?;

        let OpenInstance {
            actions_correlator: _,
            action_log_items: actions,
        } = open_instance;

        Ok(waymark_vcr_file::instance::LogItem { actions })
    }
}
