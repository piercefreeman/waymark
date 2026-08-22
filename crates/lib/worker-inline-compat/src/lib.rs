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

/// Render a successful JSON result into the wire-shaped action result:
/// the value rides the payload under the `result` key, exactly as a
/// remote worker would have reported it.
fn success_action_result(value: &serde_json::Value) -> waymark_proto::messages::ActionResult {
    let document = waymark_proto_python_value_conversions::json_to_workflow_argument_value(value);
    let payload = waymark_proto::messages::WorkflowArguments {
        arguments: vec![waymark_proto::messages::WorkflowArgument {
            key: "result".to_owned(),
            value: waymark_proto_python_value_conversions::encode_workflow_argument_value(
                &document,
            ),
        }],
    };
    waymark_proto::messages::ActionResult {
        success: true,
        payload: Some(payload),
        ..Default::default()
    }
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
            match outcome {
                Ok(value) => success_action_result(&value),
                Err(err) => waymark_proto::messages::ActionResult {
                    success: false,
                    error_type: Some(err.kind),
                    error_message: Some(err.message),
                    ..Default::default()
                },
            }
        })
    })
}
