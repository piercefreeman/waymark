//! [`Convert`](waymark_convert_core::Convert) implementation for turning a
//! [`proto::WorkflowRegistration::initial_context`] into positional
//! entry-function arguments.
//!
//! The Python side sends workflow arguments as a
//! [`proto::WorkflowArguments`] map.  This converter pairs that map with
//! the entry function's ordered input names (from the AST) and produces a
//! `Vec<`[`waymark_vm_value::Value`]`>` ready for
//! [`waymark_vm_runtime::Runtime::with_custom_entrypoint`].

#![warn(missing_docs)]

use waymark_convert_core::{Convert, TryConvert};

/// Stateless converter that builds positional entry-function arguments from
/// a keyword-argument map and the function's declared input names.
///
/// Values are converted from proto to VM via
/// [`waymark_extcall_convert_proto::Converter`].
///
/// The conversion is infallible — callers should use
/// [`Convert::convert`](waymark_convert_core::Convert::convert).
/// Missing keys default to
/// [`waymark_vm_value::Value::Ready(ReadyValue::None)`].
pub struct InitialContextConverter;

impl
    TryConvert<
        (waymark_proto::messages::WorkflowArguments, &[String]),
        Vec<waymark_vm_value::Value>,
    > for InitialContextConverter
{
    type Error = core::convert::Infallible;

    fn try_convert(
        (initial_context, input_names): (waymark_proto::messages::WorkflowArguments, &[String]),
    ) -> Result<Vec<waymark_vm_value::Value>, Self::Error> {
        let args_dict = waymark_extcall_convert_proto::Converter::convert(initial_context);
        let waymark_vm_value::ReadyValue::Dict(args_map) = args_dict else {
            // Should never happen — Converter always produces a Dict for
            // WorkflowArguments.
            return Ok(vec![
                waymark_vm_value::Value::Ready(
                    waymark_vm_value::ReadyValue::None
                );
                input_names.len()
            ]);
        };

        let positional: Vec<_> = input_names
            .iter()
            .map(|name| {
                args_map
                    .get(name)
                    .cloned()
                    .unwrap_or(waymark_vm_value::Value::Ready(
                        waymark_vm_value::ReadyValue::None,
                    ))
            })
            .collect();

        Ok(positional)
    }
}
