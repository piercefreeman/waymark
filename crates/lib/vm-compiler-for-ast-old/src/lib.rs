//! Compiles the supported subset of [`waymark_vm_ast_old`] into
//! [`waymark_vm_bytecode`] using [`waymark_vm_instructions_fullset`].
//!
//! [`compile`] is the crate entry point. It resolves user-defined functions in
//! source order, lowers each function body independently, and returns a tuple
//! of the [`waymark_vm_bytecode::Executable`] and [`Metadata`] about the
//! program's functions.
//!
//! The current compiler intentionally implements only the subset that can be
//! represented by the existing VM instruction set: literals, variables,
//! scalar binary and unary operations, simple assignments, conditionals, `while`
//! and `for` loops, `break`/`continue`, returns, user function calls, and
//! action calls.
//!
//! Unsupported statements and expressions are rejected with [`CompileError`]
//! instead of being lowered to incorrect or lossy bytecode.

#![warn(missing_docs, clippy::missing_docs_in_private_items)]

pub mod function;

pub mod utils {
    //! Utilities shared across the compiler's planning and lowering passes.

    mod eevec;

    mod marked;

    pub use self::eevec::EEVec;
    pub use self::marked::Marked;
}

use self::utils::*;

#[cfg(test)]
mod tests;

use index_type::typed_vec::TypedVec;
use waymark_vm_compiler_for_ast_old_core::{ExecutableFor, Metadata, lowering};

/// Build [`Metadata`] from an AST program.
fn metadata_from_ast_program(program: &waymark_vm_ast_old::Program) -> Metadata {
    let function_inputs = program
        .functions
        .iter()
        .map(|f| f.value.io.value.inputs.clone())
        .collect();

    let function_ids = program
        .functions
        .iter()
        .enumerate()
        .map(|(i, f)| {
            (
                f.value.name.clone(),
                waymark_vm_bytecode_core::FunctionId(i),
            )
        })
        .collect();

    Metadata {
        function_inputs,
        function_ids,
    }
}

/// Errors that can occur while compiling AST into bytecode.
#[derive(Debug, thiserror::Error)]
pub enum CompileError<LiteralLoweringError, ActionLoweringError> {
    /// Errors produced while building the function table.
    #[error(transparent)]
    FunctionTable(#[from] function::table::Error),

    /// Errors produced while compiling a specific function.
    #[error(transparent)]
    FunctionCompiler(#[from] function::compiler::Error<LiteralLoweringError, ActionLoweringError>),
}

/// The [`CompileError`] alias for binding it to lowering.
pub type CompileErrorFor<Spec, Lowering> = CompileError<
    <Lowering as lowering::PureSet<Spec>>::LiteralError,
    <Lowering as lowering::ExtCallSet<Spec>>::ActionError,
>;

/// Compile an old AST program into VM bytecode.
///
/// Functions keep their source order. That means function `0` in the resulting
/// executable is the first entry from `program.functions`.
///
/// For the [`Metadata`] (function input names, etc.) use
/// [`compile_with_metadata`].
///
/// Unsupported AST constructs return [`CompileError`] instead of partial or
/// lossy bytecode.
pub fn compile<Spec, Lowering>(
    program: &waymark_vm_ast_old::Program,
) -> Result<ExecutableFor<Spec>, CompileErrorFor<Spec, Lowering>>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
    Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
{
    compile_with_metadata::<Spec, Lowering>(program).map(|(executable, _)| executable)
}

/// Compile an old AST program into VM bytecode with [`Metadata`].
///
/// Functions keep their source order. That means function `0` in the resulting
/// executable is the first entry from `program.functions`.
///
/// Returns a tuple of the [`ExecutableFor`] bytecode and the program
/// [`Metadata`] (function input names, etc.).
///
/// Unsupported AST constructs return [`CompileError`] instead of partial or
/// lossy bytecode.
pub fn compile_with_metadata<Spec, Lowering>(
    program: &waymark_vm_ast_old::Program,
) -> Result<(ExecutableFor<Spec>, Metadata), CompileErrorFor<Spec, Lowering>>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
    Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
{
    let function_table = function::table::FunctionTable::build(program)?;
    let mut functions = TypedVec::with_capacity(program.functions.len());

    for function in &program.functions {
        let compiler =
            function::compiler::FunctionCompiler::<Spec, Lowering>::new(&function_table, function)?;
        functions.push(compiler.compile(function)?);
    }

    let executable = waymark_vm_bytecode::Executable { functions };
    let metadata = metadata_from_ast_program(program);

    Ok((executable, metadata))
}
