//! Builder for [`waymark_vm_runtime_callspec::CallSpec`]s from compiler
//! [`Metadata`](waymark_vm_compiler_metadata::Metadata).
//!
//! Select a function ([`Builder::fn_by_name`] / [`Builder::first_fn`]),
//! then match keyword arguments against its input names
//! ([`FnBuilder::args`]) to produce the positional call spec.

#![warn(missing_docs)]

/// Start building a [`waymark_vm_runtime_callspec::CallSpec`] from compiler
/// metadata.
pub fn builder<FunctionId>(
    metadata: &waymark_vm_compiler_metadata::Metadata<FunctionId>,
) -> Builder<'_, FunctionId>
where
    FunctionId: index_type::IndexType,
{
    Builder { metadata }
}

/// Call-spec builder: selects the function to call.
pub struct Builder<'a, FunctionId>
where
    FunctionId: index_type::IndexType,
{
    /// The compiler metadata to build from.
    pub metadata: &'a waymark_vm_compiler_metadata::Metadata<FunctionId>,
}

impl<'a, FunctionId> Builder<'a, FunctionId>
where
    FunctionId: index_type::IndexType,
{
    /// Select a function by name.
    ///
    /// Fails when the name is not present in the metadata, or when the
    /// metadata carries no input names for the resolved function id
    /// (inconsistent metadata).
    pub fn fn_by_name(
        self,
        function_name: &str,
    ) -> Result<FnBuilder<'a, FunctionId>, UnknownFunctionError>
    where
        FunctionId: Copy + core::hash::Hash,
    {
        let Some(function_id) = self.metadata.function_id(function_name) else {
            return Err(UnknownFunctionError {
                function_name: function_name.to_owned(),
            });
        };
        let Some(input_names) = self.metadata.input_names(function_id) else {
            return Err(UnknownFunctionError {
                function_name: function_name.to_owned(),
            });
        };
        Ok(FnBuilder {
            function_id,
            input_names,
        })
    }

    /// Select the entry function — function 0, the first function in
    /// source order ([`FunctionId::default()`](Default::default)).
    ///
    /// Fails when the program has no functions.
    pub fn first_fn(self) -> Result<FnBuilder<'a, FunctionId>, NoFunctionsError>
    where
        FunctionId: Default,
    {
        let function_id = FunctionId::default();
        let Some(input_names) = self.metadata.input_names(function_id) else {
            return Err(NoFunctionsError);
        };
        Ok(FnBuilder {
            function_id,
            input_names,
        })
    }
}

/// Call-spec builder with the function selected: matches arguments.
///
/// Only constructible through [`Builder`], which guarantees the selected
/// function id is valid for the metadata it was resolved from.
pub struct FnBuilder<'a, FunctionId> {
    function_id: FunctionId,
    input_names: &'a [String],
}

impl<'a, FunctionId> FnBuilder<'a, FunctionId> {
    /// Match keyword arguments against the function's input names and
    /// produce the [`waymark_vm_runtime_callspec::CallSpec`].
    ///
    /// Every input name must be present in `arguments`; keys that don't
    /// match an input name are ignored.
    pub fn args<Value>(
        self,
        mut arguments: std::collections::HashMap<String, Value>,
    ) -> Result<waymark_vm_runtime_callspec::CallSpec<FunctionId, Value>, MissingArgumentsError>
    {
        let mut args = Vec::with_capacity(self.input_names.len());
        let mut missing_input_names = Vec::new();
        for name in self.input_names {
            match arguments.remove(name) {
                Some(value) => args.push(value),
                None => missing_input_names.push(name.clone()),
            }
        }

        if !missing_input_names.is_empty() {
            return Err(MissingArgumentsError {
                missing_input_names,
            });
        }

        Ok(waymark_vm_runtime_callspec::CallSpec {
            func: self.function_id,
            args,
        })
    }

    /// Match keyword arguments with a default for absent inputs.
    ///
    /// Unlike [`Self::args`], absent input names are not an error — they
    /// are filled with `default` at [`FnBuilderWithDefaultArg::build`].
    pub fn args_with_default<Value>(
        self,
        arguments: std::collections::HashMap<String, Value>,
        default: Value,
    ) -> FnBuilderWithDefaultArg<'a, FunctionId, Value> {
        FnBuilderWithDefaultArg {
            function_id: self.function_id,
            input_names: self.input_names,
            arguments,
            default,
        }
    }
}

/// Call-spec builder with the function selected and a default attached
/// for absent inputs: matching cannot fail.
///
/// Only constructible through [`FnBuilder::args_with_default`], inheriting
/// its guarantee that the function id is valid for the metadata it was
/// resolved from.
pub struct FnBuilderWithDefaultArg<'a, FunctionId, Value> {
    function_id: FunctionId,
    input_names: &'a [String],
    arguments: std::collections::HashMap<String, Value>,
    default: Value,
}

impl<FunctionId, Value> FnBuilderWithDefaultArg<'_, FunctionId, Value> {
    /// Produce the [`waymark_vm_runtime_callspec::CallSpec`].
    ///
    /// Each input name is looked up in the arguments; absent keys are
    /// filled with the default. Keys that don't match an input name are
    /// ignored.
    pub fn build(mut self) -> waymark_vm_runtime_callspec::CallSpec<FunctionId, Value>
    where
        Value: Clone,
    {
        let args = self
            .input_names
            .iter()
            .map(|name| {
                self.arguments
                    .remove(name)
                    .unwrap_or_else(|| self.default.clone())
            })
            .collect();

        waymark_vm_runtime_callspec::CallSpec {
            func: self.function_id,
            args,
        }
    }
}

/// The requested function name is not present in the metadata.
#[derive(Debug, thiserror::Error)]
#[error("unknown function: {function_name}")]
pub struct UnknownFunctionError {
    /// The name that failed to resolve.
    pub function_name: String,
}

/// The program has no functions, so there is no entry function to select.
#[derive(Debug, thiserror::Error)]
#[error("the program has no functions")]
pub struct NoFunctionsError;

/// The function expects arguments that were not provided.
#[derive(Debug, thiserror::Error)]
#[error("missing arguments: {missing_input_names:?}")]
pub struct MissingArgumentsError {
    /// The input names with no provided argument.
    pub missing_input_names: Vec<String>,
}
