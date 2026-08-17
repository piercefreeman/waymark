//! Glue for serving in-process action bodies over the inline worker
//! pool.
//!
//! The inline callable surface speaks the framing-level kwargs and the
//! wire-shaped action result; action bodies speak VM values and pool
//! errors.  [`inline_action`] bridges the two.

#![warn(missing_docs)]

use std::collections::HashMap;
use std::sync::Arc;

use waymark_convert_core::{Convert as _, TryConvert as _};
use waymark_vm_value_python::ReadyValue;
use waymark_worker_core::WorkerPoolError;
use waymark_worker_inline::InlineActionCallable;

/// Decode the framing-level kwargs into the values the body is called
/// with.
///
/// The names are framing; each value is opaque bytes that decode into
/// one value of this flavor.
fn decode_kwargs(
    kwargs: waymark_proto::messages::WorkflowArguments,
) -> Result<HashMap<String, ReadyValue>, WorkerPoolError> {
    let mut decoded = HashMap::with_capacity(kwargs.arguments.len());
    for argument in kwargs.arguments {
        let value =
            waymark_proto_python_value_conversions::decode_workflow_argument_value(&argument.value)
                .map_err(|err| {
                    WorkerPoolError::new(
                        "ActionError",
                        format!("decoding the value of argument {:?}: {err}", argument.key),
                    )
                })?;
        decoded.insert(
            argument.key,
            waymark_vm_value_convert_proto::Converter::convert(&value),
        );
    }
    Ok(decoded)
}

/// Encode how the call completed into the result the wire carries.
fn encode_result(outcome: Result<ReadyValue, WorkerPoolError>) -> Vec<u8> {
    let encoded = outcome.and_then(|value| {
        // A body that hands back a value holding a pending promise names
        // a promise no one here can settle, so the call failed.
        waymark_vm_value_convert_proto::Converter::try_convert(&value)
            .map_err(|err| WorkerPoolError::new("ActionError", err.to_string()))
    });

    let result_value = match encoded {
        Ok(value) => waymark_proto_python_value_conversions::returned_value(value),
        Err(err) => waymark_proto_python_value_conversions::raised_exception(
            waymark_proto_python_value_conversions::exception_value(err.kind, err.message),
        ),
    };

    waymark_proto_python_value_conversions::encode_action_result_value(&result_value)
}

/// Adapt an action body to the inline callable surface: decode the
/// framing-level kwargs, run the body, and render either outcome into
/// the wire-shaped action result.
pub fn inline_action<F, Fut>(body: F) -> InlineActionCallable
where
    F: Fn(HashMap<String, ReadyValue>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<ReadyValue, WorkerPoolError>> + Send + 'static,
{
    let body = Arc::new(body);
    Arc::new(move |kwargs: waymark_proto::messages::WorkflowArguments| {
        let body = Arc::clone(&body);
        Box::pin(async move {
            let outcome = match decode_kwargs(kwargs) {
                Ok(kwargs) => body(kwargs).await,
                Err(err) => Err(err),
            };
            waymark_proto::messages::ActionResult {
                payload: encode_result(outcome),
                ..Default::default()
            }
        })
    })
}
