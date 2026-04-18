use std::collections::HashMap;

use uuid::Uuid;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
struct Key {
    pub correlation_id: waymark_vcr_core::CorrelationId,
    pub dispatch_token: Uuid,
}

#[derive(Debug)]
struct IncompleteActionContext {
    pub queued_at: std::time::Instant,
    pub params: waymark_vcr_file::action::Params,
}

#[derive(Debug, Default)]
pub struct Correlator {
    incomplete_action_contexts: HashMap<Key, IncompleteActionContext>,
}

impl Correlator {
    pub fn insert_request(&mut self, request: waymark_worker_core::ActionRequest) {
        let (correlation_id, params, dispatch_token) =
            waymark_vcr_file::action::deconstruct_request(request);

        let key = Key {
            correlation_id,
            dispatch_token,
        };

        let context = IncompleteActionContext {
            queued_at: std::time::Instant::now(),
            params,
        };

        self.incomplete_action_contexts.insert(key, context);
    }

    pub fn correlate_completion(
        &mut self,
        completion: waymark_worker_core::ActionCompletion,
    ) -> Option<waymark_vcr_file::action::LogItem> {
        let waymark_worker_core::ActionCompletion {
            executor_id,
            execution_id,
            attempt_number,
            dispatch_token,
            result,
        } = completion;

        let correlation_id = waymark_vcr_core::CorrelationId {
            executor_id,
            execution_id,
            attempt_number,
        };

        let key = Key {
            correlation_id,
            dispatch_token,
        };

        let context = self.incomplete_action_contexts.remove(&key)?;

        let IncompleteActionContext { queued_at, params } = context;

        Some(waymark_vcr_file::action::LogItem {
            execution_time: queued_at.elapsed(),
            params,
            result,
        })
    }
}
