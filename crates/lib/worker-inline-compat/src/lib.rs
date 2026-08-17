//! Compatibility glue for serving JSON-map action bodies over the
//! inline worker pool.
//!
//! The inline callable surface speaks the framing-level kwargs and the
//! completion's result vocabulary; existing action bodies speak JSON
//! maps and pool errors.  [`inline_action`] bridges the two.

#![warn(missing_docs)]

use std::collections::HashMap;
use std::sync::Arc;

use waymark_worker_core::{UncheckedExecutionResult, WorkerPoolError, error_to_value};
use waymark_worker_inline::InlineActionCallable;

/// Adapt a JSON-map action body to the inline callable surface: decode
/// the framing-level kwargs, run the body, and render either outcome
/// into the completion's result vocabulary.
pub fn inline_action<F, Fut>(body: F) -> InlineActionCallable
where
    F: Fn(HashMap<String, serde_json::Value>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<serde_json::Value, WorkerPoolError>> + Send + 'static,
{
    let body = Arc::new(body);
    Arc::new(move |kwargs: waymark_proto::messages::WorkflowArguments| {
        let body = Arc::clone(&body);
        Box::pin(async move {
            let outcome = async {
                let json = waymark_proto_message_conversions::workflow_arguments_to_json(&kwargs)
                    .map_err(|err| WorkerPoolError::new("ActionError", err.to_string()))?;
                let serde_json::Value::Object(entries) = json else {
                    return Err(WorkerPoolError::new(
                        "ActionError",
                        "kwargs must be an object",
                    ));
                };
                body(entries.into_iter().collect()).await
            }
            .await;
            UncheckedExecutionResult(match outcome {
                Ok(value) => value,
                Err(err) => error_to_value(&err),
            })
        })
    })
}
