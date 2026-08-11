//! [`Convert`](waymark_convert_core::Convert) implementation for turning a
//! [`proto::WorkflowRegistration::initial_context`] into positional
//! entry-function arguments.
//!
//! The Python side sends workflow arguments as a
//! [`proto::WorkflowArguments`] map.  This converter pairs that map with
//! the entry function's ordered input names (from the AST) and produces a
//! `Vec<`[`waymark_vm_value_python::Value`]`>` ready for
//! [`waymark_vm_runtime::Runtime::with_custom_entrypoint`].

#![warn(missing_docs)]

use waymark_convert_core::{Convert, TryConvert};

/// Error returned when an `initial_context` is required but missing.
#[derive(Debug, thiserror::Error)]
#[error("entry function expects {entry_input_names:?} but no initial_context was provided")]
pub struct MissingInitialContextError {
    /// The expected input names.
    pub entry_input_names: Vec<String>,
}

/// Stateless converter that builds positional entry-function arguments from
/// a keyword-argument map and the function's declared input names.
///
/// Values are converted from proto to VM via
/// [`waymark_extcall_convert_proto::Converter`].
///
/// The conversion is infallible — callers should use
/// [`Convert::convert`](waymark_convert_core::Convert::convert).
/// Missing keys default to
/// [`waymark_vm_value_python::Value::Ready(ReadyValue::None)`].
pub struct InitialContextConverter;

impl
    TryConvert<
        (
            Option<&waymark_proto::messages::WorkflowArguments>,
            &[String],
        ),
        Vec<waymark_vm_value_python::Value>,
    > for InitialContextConverter
{
    type Error = MissingInitialContextError;

    fn try_convert(
        (initial_context, input_names): (
            Option<&waymark_proto::messages::WorkflowArguments>,
            &[String],
        ),
    ) -> Result<Vec<waymark_vm_value_python::Value>, Self::Error> {
        let Some(ctx) = initial_context else {
            if input_names.is_empty() {
                return Ok(Vec::new());
            }
            return Err(MissingInitialContextError {
                entry_input_names: input_names.to_vec(),
            });
        };
        Ok(InitialContextConverter::convert((ctx, input_names)))
    }
}

impl
    TryConvert<
        (&waymark_proto::messages::WorkflowArguments, &[String]),
        Vec<waymark_vm_value_python::Value>,
    > for InitialContextConverter
{
    type Error = core::convert::Infallible;

    fn try_convert(
        (initial_context, input_names): (&waymark_proto::messages::WorkflowArguments, &[String]),
    ) -> Result<Vec<waymark_vm_value_python::Value>, Self::Error> {
        let args_dict = waymark_action_runtime_convert::Converter::convert(initial_context);
        let waymark_vm_value_python::ReadyValue::Dict(args_map) = args_dict else {
            // Should never happen — Converter always produces a Dict for
            // WorkflowArguments.
            return Ok(vec![
                waymark_vm_value_python::Value::Ready(
                    waymark_vm_value_python::ReadyValue::None
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
                    .unwrap_or(waymark_vm_value_python::Value::Ready(
                        waymark_vm_value_python::ReadyValue::None,
                    ))
            })
            .collect();

        Ok(positional)
    }
}

impl<FunctionId>
    TryConvert<
        (
            Option<&waymark_proto::messages::WorkflowArguments>,
            &[String],
            FunctionId,
        ),
        waymark_vm_runtime::CallSpec<FunctionId, waymark_vm_value_python::Value>,
    > for InitialContextConverter
{
    type Error = MissingInitialContextError;

    fn try_convert(
        (initial_context, input_names, func): (
            Option<&waymark_proto::messages::WorkflowArguments>,
            &[String],
            FunctionId,
        ),
    ) -> Result<waymark_vm_runtime::CallSpec<FunctionId, waymark_vm_value_python::Value>, Self::Error>
    {
        let args = Self::try_convert((initial_context, input_names))?;
        Ok(waymark_vm_runtime::CallSpec { func, args })
    }
}

impl<FunctionId>
    TryConvert<
        (
            Option<&waymark_proto::messages::WorkflowArguments>,
            &[String],
        ),
        waymark_vm_runtime::CallSpec<FunctionId, waymark_vm_value_python::Value>,
    > for InitialContextConverter
where
    FunctionId: Default,
{
    type Error = MissingInitialContextError;

    fn try_convert(
        (initial_context, input_names): (
            Option<&waymark_proto::messages::WorkflowArguments>,
            &[String],
        ),
    ) -> Result<waymark_vm_runtime::CallSpec<FunctionId, waymark_vm_value_python::Value>, Self::Error>
    {
        Self::try_convert((initial_context, input_names, FunctionId::default()))
    }
}

impl<FunctionId>
    TryConvert<
        (
            Option<&waymark_proto::messages::WorkflowArguments>,
            &waymark_vm_compiler_metadata::Metadata<FunctionId>,
            FunctionId,
        ),
        waymark_vm_runtime::CallSpec<FunctionId, waymark_vm_value_python::Value>,
    > for InitialContextConverter
where
    FunctionId: index_type::IndexType,
{
    type Error = MissingInitialContextError;

    fn try_convert(
        (initial_context, metadata, func): (
            Option<&waymark_proto::messages::WorkflowArguments>,
            &waymark_vm_compiler_metadata::Metadata<FunctionId>,
            FunctionId,
        ),
    ) -> Result<waymark_vm_runtime::CallSpec<FunctionId, waymark_vm_value_python::Value>, Self::Error>
    {
        let entry_input_names = metadata.input_names(func).unwrap_or_default();
        Self::try_convert((initial_context, entry_input_names, func))
    }
}

impl<FunctionId>
    TryConvert<
        (
            Option<&waymark_proto::messages::WorkflowArguments>,
            &waymark_vm_compiler_metadata::Metadata<FunctionId>,
        ),
        waymark_vm_runtime::CallSpec<FunctionId, waymark_vm_value_python::Value>,
    > for InitialContextConverter
where
    FunctionId: Default + index_type::IndexType,
{
    type Error = MissingInitialContextError;

    fn try_convert(
        (initial_context, metadata): (
            Option<&waymark_proto::messages::WorkflowArguments>,
            &waymark_vm_compiler_metadata::Metadata<FunctionId>,
        ),
    ) -> Result<waymark_vm_runtime::CallSpec<FunctionId, waymark_vm_value_python::Value>, Self::Error>
    {
        Self::try_convert((initial_context, metadata, FunctionId::default()))
    }
}
