//! Compatibility glue for serving JSON-map action bodies over the
//! inline worker pool.
//!
//! The inline callable surface speaks the framing-level kwargs and the
//! completion's result vocabulary; existing action bodies speak JSON
//! maps and pool errors.  [`inline_action`] bridges the two.

#![warn(missing_docs)]

use std::collections::HashMap;
use std::sync::Arc;

use waymark_worker_core::WorkerPoolError;
use waymark_worker_inline::InlineActionCallable;

/// Encode how the call completed into the result the wire carries.
fn encode_result(outcome: Result<serde_json::Value, WorkerPoolError>) -> Vec<u8> {
    let result_value = match outcome {
        Ok(value) => waymark_proto_python_value_conversions::returned_value(
            waymark_proto_python_value_conversions::json_to_workflow_argument_value(&value),
        ),
        Err(err) => waymark_proto_python_value_conversions::raised_exception(
            waymark_proto_python_value_conversions::exception_value(err.kind, err.message),
        ),
    };

    waymark_proto_python_value_conversions::encode_action_result_value(&result_value)
}

/// Adapt a JSON-map action body to the inline callable surface: decode
/// the framing-level kwargs, run the body, and render either outcome
/// into the wire-shaped action result.
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
            waymark_proto::messages::ActionResult {
                payload: encode_result(outcome),
                ..Default::default()
            }
        })
    })
}
