#[cfg(test)]
mod tests;

use std::collections::HashMap;

use chrono::Utc;
use waymark_ids::ExecutionId;
use waymark_worker_core::ActionCompletion;

use crate::runloop::InflightActionDispatch;

pub struct Params<'a> {
    /// Completion batch being assembled for the current coordinator tick.
    pub all_completions: &'a mut Vec<ActionCompletion>,
    /// Tracks the currently valid dispatch token, attempt, and deadline for inflight actions.
    pub inflight_dispatches: &'a HashMap<ExecutionId, InflightActionDispatch>,
}

/// Detects timed-out actions and prepends synthetic timeout completions.
///
/// **Why this part exists:** Actions have optional deadlines. If they don't complete within
/// the timeout window, the runloop must inject a timeout exception as if the worker had
/// returned a failure result. This allows workflows to handle timeouts gracefully.
///
/// **What it does:** Scans all inflight actions for past deadlines, creates synthetic
/// timeout completion results with metadata (timeout duration, attempt number), and
/// prepends them to the completion batch. Prepending ensures timeouts are processed before
/// other completions, maintaining FIFO semantics where relevant.
pub fn handle(params: Params<'_>) {
    let Params {
        all_completions,
        inflight_dispatches,
    } = params;

    if inflight_dispatches.is_empty() {
        return;
    }

    let timed_out_ids: Vec<ExecutionId> = {
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
            result: waymark_synthetic_exception::build_value(
                &waymark_synthetic_exception::ActionTimeout {
                    execution_id,
                    attempt_number: dispatch.attempt_number,
                    timeout_seconds: dispatch.timeout_seconds,
                },
            ),
        });
    }

    if timeout_completions.is_empty() {
        return;
    }

    timeout_completions.append(all_completions);
    *all_completions = timeout_completions;
}
