//! Compiler metadata: [`Metadata`] about compiled programs.
//!
//! This crate defines the [`Metadata`] type produced alongside compiled
//! bytecode.  It carries information about the original AST functions that
//! is not represented in the bytecode itself but is needed at runtime.

#![warn(missing_docs)]

use index_type::typed_vec::TypedVec;

/// Program metadata produced alongside the compiled bytecode.
///
/// Carries information about the original AST functions that is not
/// represented in the bytecode itself but is needed at runtime (e.g.,
/// entry-function argument names for building `CallSpec`s).
///
/// The `FunctionId` type parameter is the compiler's function-identifier
/// type (e.g. [`waymark_vm_bytecode_core::FunctionId`]).
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(bound(
        serialize = "FunctionId: serde::Serialize",
        deserialize = "FunctionId: serde::Deserialize<'de>",
    ))
)]
pub struct Metadata<FunctionId>
where
    FunctionId: index_type::IndexType,
{
    /// Input parameter names for each function, indexed by `FunctionId`
    /// (matching source order in the AST program).
    #[cfg_attr(feature = "serde", serde(with = "waymark_typed_vec_serde"))]
    pub function_inputs: TypedVec<FunctionId, Vec<String>>,

    /// Maps each function name to its `FunctionId`.
    pub function_ids: indexmap::IndexMap<String, FunctionId>,
}

impl<FunctionId> Metadata<FunctionId>
where
    FunctionId: index_type::IndexType,
{
    /// Return the input names for the function with the given ID, or
    /// `None` if the function ID is out of range.
    pub fn input_names(&self, function_id: FunctionId) -> Option<&[String]> {
        self.function_inputs.get(function_id).map(Vec::as_slice)
    }
}

impl<FunctionId> Metadata<FunctionId>
where
    FunctionId: index_type::IndexType + core::hash::Hash,
{
    /// Look up a function's `FunctionId` by name.
    pub fn function_id(&self, name: &str) -> Option<FunctionId>
    where
        FunctionId: Copy,
    {
        self.function_ids.get(name).copied()
    }
}
