use std::collections::HashMap;

use waymark_ids::{InstanceId, WorkflowVersionId};

#[derive(Debug, Default)]
pub struct Bufferrer {
    open_instances: HashMap<InstanceId, OpenInstance>,
}

#[derive(Debug)]
struct OpenInstance {
    pub actions_correlator: crate::action::Correlator,
    pub action_log_items: Vec<waymark_vcr_file::action::LogItem>,
    pub workflow_version_id: WorkflowVersionId,
}

#[derive(Debug, thiserror::Error)]
pub enum OpenInstanceLogError {
    /// Returned when the instance log is already open currently;
    /// this is usually not an issue, and can happen by design since
    /// we don't expect the ingest instances to be filtered.
    /// Users of this API can simply ignore this error as essentially
    /// registers the queued instance for logging in an idempotent fashion.
    #[error("instance log already opened")]
    AlreadyOpened,
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
        let open_instance = OpenInstance {
            actions_correlator: Default::default(),
            action_log_items: Default::default(),
            workflow_version_id: queued_instance.workflow_version_id,
        };

        let entry = self.open_instances.entry(queued_instance.instance_id);

        let std::collections::hash_map::Entry::Vacant(entry) = entry else {
            // Do not replace already opened instances.
            return;
        };

        entry.insert(open_instance);
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
            workflow_version_id,
        } = open_instance;

        Ok(waymark_vcr_file::instance::LogItem {
            workflow_version_id,
            actions,
        })
    }
}
