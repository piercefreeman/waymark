//! Glue for serving in-process action bodies over the inline worker
//! pool.
//!
//! The inline callable surface speaks the dispatch's opaque encoded
//! arguments and the encoded result payload; action bodies speak VM
//! values and the flavor's exceptions.  [`inline_action`] bridges the
//! two by calling the conversions — it owns none of its own.

#![warn(missing_docs)]

use std::collections::HashMap;
use std::sync::Arc;

use waymark_convert_core::TryConvert as _;
use waymark_vm_value_python::ReadyValue;
use waymark_worker_inline::InlineActionCallable;

/// Decode the dispatch's opaque encoded arguments into the values the
/// body is called with.
///
/// The bytes carry this flavor's arguments message; empty bytes mean
/// no arguments.
///
/// # Panics
///
/// Panics when the bytes do not decode as the flavor's arguments
/// message: the dispatch was encoded by this very process, so
/// undecodable bytes are corruption or version skew — a bug, not an
/// outcome the action produced.
fn decode_arguments(arguments: Vec<u8>) -> HashMap<String, ReadyValue> {
    if arguments.is_empty() {
        return HashMap::new();
    }
    let message: waymark_proto::python_value::ActionArguments =
        waymark_vm_value_python_convert_proto::Converter::try_convert(arguments.as_slice())
            .unwrap_or_else(|err| panic!("the dispatch's argument bytes do not decode: {err}"));
    let entries: Vec<(String, ReadyValue)> =
        waymark_vm_value_python_convert_proto::Converter::try_convert(&message)
            .unwrap_or_else(|err| panic!("the dispatch's arguments are malformed: {err}"));
    entries.into_iter().collect()
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
    waymark_vm_value_python_convert_proto::Converter::try_convert(outcome)
        .expect("an action body's outcome holds no pending promise")
}

/// Adapt an action body to the inline callable surface: decode the
/// dispatch's encoded arguments, run the body, and render either
/// outcome into the encoded result payload.
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
    Arc::new(move |arguments: Vec<u8>| {
        let body = Arc::clone(&body);
        Box::pin(async move {
            let outcome = match body(decode_arguments(arguments)).await {
                Ok(value) => waymark_action_runtime_core::ActionCallOutcome::Value(value),
                Err(exception) => {
                    waymark_action_runtime_core::ActionCallOutcome::Exception(exception)
                }
            };
            encode_result(outcome)
        })
    })
}
