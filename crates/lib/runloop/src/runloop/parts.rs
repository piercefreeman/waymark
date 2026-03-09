use std::collections::HashMap;

use chrono::Utc;
use uuid::Uuid;
use waymark_worker_core::ActionCompletion;

use crate::runloop::{InflightActionDispatch, value_utils::action_timeout_value};

pub fn prepend_timeout_completions_from_inflight_dispatches(
    all_completions: &mut Vec<ActionCompletion>,
    inflight_dispatches: &HashMap<Uuid, InflightActionDispatch>,
) {
    if inflight_dispatches.is_empty() {
        return;
    }

    let timed_out_ids: Vec<Uuid> = {
        let now = Utc::now();
        inflight_dispatches
            .iter()
            .filter_map(|(execution_id, dispatch)| {
                dispatch
                    .deadline_at
                    .filter(|deadline| *deadline <= now)
                    .map(|_| *execution_id)
            })
            .collect()
    };

    if timed_out_ids.is_empty() {
        return;
    }

    let mut timeout_completions = Vec::with_capacity(timed_out_ids.len());
    for execution_id in timed_out_ids {
        let Some(dispatch) = inflight_dispatches.get(&execution_id) else {
            continue;
        };
        timeout_completions.push(ActionCompletion {
            executor_id: dispatch.executor_id,
            execution_id,
            attempt_number: dispatch.attempt_number,
            dispatch_token: dispatch.dispatch_token,
            result: action_timeout_value(
                execution_id,
                dispatch.attempt_number,
                dispatch.timeout_seconds,
            ),
        });
    }

    if timeout_completions.is_empty() {
        return;
    }

    timeout_completions.append(all_completions);
    *all_completions = timeout_completions;
}
