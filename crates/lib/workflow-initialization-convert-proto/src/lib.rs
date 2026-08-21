//! [`Convert`](waymark_convert_core::Convert) implementation for turning a
//! [`WorkflowRegistration`]'s opaque `arguments` payload into
//! positional entry-function arguments.
//!
//! The Python side sends workflow arguments as an encoded
//! [`WorkflowArguments`](waymark_proto::python_value::WorkflowArguments)
//! message — the initiation seam's per-language type.  This converter
//! decodes the payload through the flavor's own conversion crate, pairs
//! the named arguments with the entry function's ordered input names
//! (from the AST), and produces a
//! `Vec<`[`waymark_vm_value_python::Value`]`>` ready for
//! [`waymark_vm_runtime::Runtime::with_custom_entrypoint`].
//!
//! [`WorkflowRegistration`]: waymark_proto::messages::WorkflowRegistration

#![warn(missing_docs)]

use waymark_convert_core::TryConvert;

/// Error converting a workflow-arguments payload into entry-function
/// arguments.
#[derive(Debug, thiserror::Error)]
pub enum WorkflowArgumentsError {
    /// The payload's bytes do not decode as the flavor's workflow
    /// arguments message.
    #[error("decoding the workflow arguments")]
    Decode(#[source] prost::DecodeError),

    /// The decoded arguments are malformed.
    #[error("reading the workflow arguments")]
    Arguments(#[source] waymark_vm_value_python_convert_proto::MissingArgumentValueError),
}

/// Stateless converter that builds positional entry-function arguments from
/// an encoded keyword-argument payload and the function's declared input
/// names.
///
/// Values are decoded through
/// [`waymark_vm_value_python_convert_proto::Converter`].
///
/// Missing keys default to
/// [`waymark_vm_value_python::Value::Ready(ReadyValue::None)`]; an
/// empty payload (the no-arguments encoding) defaults every input.
pub struct Converter;

impl TryConvert<(&[u8], &[String]), Vec<waymark_vm_value_python::Value>> for Converter {
    type Error = WorkflowArgumentsError;

    fn try_convert(
        (arguments, input_names): (&[u8], &[String]),
    ) -> Result<Vec<waymark_vm_value_python::Value>, Self::Error> {
        let message: waymark_proto::python_value::WorkflowArguments =
            waymark_vm_value_python_convert_proto::Converter::try_convert(arguments)
                .map_err(WorkflowArgumentsError::Decode)?;
        let entries: Vec<(String, waymark_vm_value_python::ReadyValue)> =
            waymark_vm_value_python_convert_proto::Converter::try_convert(&message)
                .map_err(WorkflowArgumentsError::Arguments)?;
        let args_map: std::collections::HashMap<_, _> = entries.into_iter().collect();

        let positional: Vec<_> = input_names
            .iter()
            .map(|name| {
                args_map
                    .get(name)
                    .cloned()
                    .map(waymark_vm_value_python::Value::Ready)
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
        (&[u8], &[String], FunctionId),
        waymark_vm_runtime::CallSpec<FunctionId, waymark_vm_value_python::Value>,
    > for Converter
{
    type Error = WorkflowArgumentsError;

    fn try_convert(
        (arguments, input_names, func): (&[u8], &[String], FunctionId),
    ) -> Result<waymark_vm_runtime::CallSpec<FunctionId, waymark_vm_value_python::Value>, Self::Error>
    {
        let args = Self::try_convert((arguments, input_names))?;
        Ok(waymark_vm_runtime::CallSpec { func, args })
    }
}

impl<FunctionId>
    TryConvert<
        (&[u8], &[String]),
        waymark_vm_runtime::CallSpec<FunctionId, waymark_vm_value_python::Value>,
    > for Converter
where
    FunctionId: Default,
{
    type Error = WorkflowArgumentsError;

    fn try_convert(
        (arguments, input_names): (&[u8], &[String]),
    ) -> Result<waymark_vm_runtime::CallSpec<FunctionId, waymark_vm_value_python::Value>, Self::Error>
    {
        Self::try_convert((arguments, input_names, FunctionId::default()))
    }
}

impl<FunctionId>
    TryConvert<
        (
            &[u8],
            &waymark_vm_compiler_metadata::Metadata<FunctionId>,
            FunctionId,
        ),
        waymark_vm_runtime::CallSpec<FunctionId, waymark_vm_value_python::Value>,
    > for Converter
where
    FunctionId: index_type::IndexType,
{
    type Error = WorkflowArgumentsError;

    fn try_convert(
        (arguments, metadata, func): (
            &[u8],
            &waymark_vm_compiler_metadata::Metadata<FunctionId>,
            FunctionId,
        ),
    ) -> Result<waymark_vm_runtime::CallSpec<FunctionId, waymark_vm_value_python::Value>, Self::Error>
    {
        let entry_input_names = metadata.input_names(func).unwrap_or_default();
        Self::try_convert((arguments, entry_input_names, func))
    }
}

impl<FunctionId>
    TryConvert<
        (&[u8], &waymark_vm_compiler_metadata::Metadata<FunctionId>),
        waymark_vm_runtime::CallSpec<FunctionId, waymark_vm_value_python::Value>,
    > for Converter
where
    FunctionId: Default + index_type::IndexType,
{
    type Error = WorkflowArgumentsError;

    fn try_convert(
        (arguments, metadata): (&[u8], &waymark_vm_compiler_metadata::Metadata<FunctionId>),
    ) -> Result<waymark_vm_runtime::CallSpec<FunctionId, waymark_vm_value_python::Value>, Self::Error>
    {
        Self::try_convert((arguments, metadata, FunctionId::default()))
    }
}
