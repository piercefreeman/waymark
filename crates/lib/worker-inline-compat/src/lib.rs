//! Glue for serving in-process action bodies over the inline worker
//! pool.
//!
//! The inline callable surface speaks the framing-level kwargs and the
//! encoded result payload; action bodies speak VM values and the
//! flavor's exceptions.  [`inline_action`] bridges the two by calling
//! the conversions — it owns none of its own.

#![warn(missing_docs)]

use std::collections::HashMap;
use std::sync::Arc;

use waymark_convert_core::TryConvert as _;
use waymark_vm_value_python::ReadyValue;
use waymark_worker_inline::InlineActionCallable;

/// Decode the framing-level kwargs into the values the body is called
/// with.
///
/// The names are framing; each value is opaque bytes that decode into
/// one value of this flavor.
///
/// # Panics
///
/// Panics when an argument's bytes do not decode: the dispatch was
/// encoded by this very process, so undecodable bytes are corruption or
/// version skew — a bug, not an outcome the action produced.
fn decode_kwargs(
    kwargs: waymark_proto::messages::WorkflowArguments,
) -> HashMap<String, ReadyValue> {
    let mut decoded = HashMap::with_capacity(kwargs.arguments.len());
    for argument in kwargs.arguments {
        let value = waymark_vm_value_python_convert_proto::Converter::try_convert(
            argument.value.as_slice(),
        )
        .unwrap_or_else(|err| {
            panic!(
                "the value bytes of argument {:?} do not decode: {err}",
                argument.key
            )
        });
        decoded.insert(argument.key, value);
    }
    decoded
}

/// Encode how the call completed into the result payload the wire
/// carries.
///
/// # Panics
///
/// Panics when the outcome holds a pending promise: an in-process body
/// that hands back a pending value names a promise no one can settle —
/// a bug in the action body, not an outcome it produced.
fn encode_result(outcome: waymark_action_runtime_core::ActionCallOutcome<ReadyValue>) -> Vec<u8> {
    waymark_action_runtime_convert::Converter::try_convert(outcome)
        .expect("an action body's outcome holds no pending promise")
}

/// Adapt an action body to the inline callable surface: decode the
/// framing-level kwargs, run the body, and render either outcome into
/// the encoded result payload.
///
/// The body's error is the flavor's own exception — raising is the
/// body's decision, stated in the vocabulary the VM settles promises
/// with.
pub fn inline_action<F, Fut>(body: F) -> InlineActionCallable
where
    F: Fn(HashMap<String, ReadyValue>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<ReadyValue, waymark_vm_runtime_exception::Exception<ReadyValue>>>
        + Send
        + 'static,
{
    let body = Arc::new(body);
    Arc::new(move |kwargs: waymark_proto::messages::WorkflowArguments| {
        let body = Arc::clone(&body);
        Box::pin(async move {
            let outcome = match body(decode_kwargs(kwargs)).await {
                Ok(value) => waymark_action_runtime_core::ActionCallOutcome::Value(value),
                Err(exception) => {
                    waymark_action_runtime_core::ActionCallOutcome::Exception(exception)
                }
            };
            encode_result(outcome)
        })
    })
}
