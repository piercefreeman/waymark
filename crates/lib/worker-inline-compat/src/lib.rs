//! Glue for serving in-process action bodies over the inline worker
//! pool.
//!
//! The inline callable surface speaks the dispatch's opaque encoded
//! arguments and the encoded result payload; action bodies speak VM
//! values and the flavor's exceptions.  [`inline_action`] bridges the
//! two by calling the conversions — it owns none of its own: the
//! arguments converter decodes the argument payload into named values
//! and the outcome converter encodes how the call completed.

#![warn(missing_docs)]

use std::collections::HashMap;
use std::sync::Arc;

use waymark_convert_core::{ConvertErrorFor, TryConvert};
use waymark_worker_inline::InlineActionCallable;

/// Decode the dispatch's opaque encoded arguments into the values the
/// body is called with.
///
/// The bytes carry the flavor's arguments message; empty bytes mean no
/// arguments.
///
/// # Panics
///
/// Panics when the bytes do not decode as the flavor's arguments
/// message: the dispatch was encoded by this very process, so
/// undecodable bytes are corruption or version skew — a bug, not an
/// outcome the action produced.
fn decode_arguments<ArgumentsConverter, Value>(arguments: Vec<u8>) -> HashMap<String, Value>
where
    ArgumentsConverter: TryConvert<Vec<u8>, HashMap<String, Value>>,
    ConvertErrorFor<ArgumentsConverter, Vec<u8>, HashMap<String, Value>>: core::fmt::Display,
{
    ArgumentsConverter::try_convert(arguments)
        .unwrap_or_else(|err| panic!("the dispatch's argument bytes do not decode: {err}"))
}

/// Encode how the call completed into the result payload the wire
/// carries.
///
/// # Panics
///
/// Panics when the outcome does not encode (e.g. it holds a pending
/// promise): an in-process body that hands back such a value names a
/// state no one can settle — a bug in the action body, not an outcome
/// it produced.
fn encode_result<OutcomeConverter, Value>(
    outcome: waymark_action_runtime_core::ActionCallOutcome<Value>,
) -> Vec<u8>
where
    OutcomeConverter: TryConvert<waymark_action_runtime_core::ActionCallOutcome<Value>, Vec<u8>>,
    ConvertErrorFor<
        OutcomeConverter,
        waymark_action_runtime_core::ActionCallOutcome<Value>,
        Vec<u8>,
    >: core::fmt::Display,
{
    OutcomeConverter::try_convert(outcome)
        .unwrap_or_else(|err| panic!("an action body's outcome does not encode: {err}"))
}

/// Adapt an action body to the inline callable surface: decode the
/// dispatch's encoded arguments, run the body, and render either
/// outcome into the encoded result payload.
///
/// The body's error is the flavor's own exception — raising is the
/// body's decision, stated in the vocabulary the VM settles promises
/// with.
pub fn inline_action<ArgumentsConverter, OutcomeConverter, Value, F, Fut>(
    body: F,
) -> InlineActionCallable
where
    Value: Send + 'static,
    ArgumentsConverter: TryConvert<Vec<u8>, HashMap<String, Value>> + Send + Sync + 'static,
    OutcomeConverter: TryConvert<waymark_action_runtime_core::ActionCallOutcome<Value>, Vec<u8>>
        + Send
        + Sync
        + 'static,
    ConvertErrorFor<ArgumentsConverter, Vec<u8>, HashMap<String, Value>>: core::fmt::Display,
    ConvertErrorFor<
        OutcomeConverter,
        waymark_action_runtime_core::ActionCallOutcome<Value>,
        Vec<u8>,
    >: core::fmt::Display,
    F: Fn(HashMap<String, Value>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Value, waymark_vm_runtime_exception::Exception<Value>>>
        + Send
        + 'static,
{
    let body = Arc::new(body);
    Arc::new(move |arguments: Vec<u8>| {
        let body = Arc::clone(&body);
        Box::pin(async move {
            let outcome = match body(decode_arguments::<ArgumentsConverter, Value>(arguments)).await
            {
                Ok(value) => waymark_action_runtime_core::ActionCallOutcome::Value(value),
                Err(exception) => {
                    waymark_action_runtime_core::ActionCallOutcome::Exception(exception)
                }
            };
            encode_result::<OutcomeConverter, Value>(outcome)
        })
    })
}
