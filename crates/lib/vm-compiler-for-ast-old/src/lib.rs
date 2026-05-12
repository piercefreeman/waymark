//! Compiles the supported subset of [`waymark_vm_ast_old`] into
//! [`waymark_vm_bytecode`] using [`waymark_vm_instructions_fullset`].
//!
//! The current compiler intentionally implements only the subset that can be
//! represented by the existing VM instruction set:
//! literals, variables, addition, simple assignments, conditionals, while
//! loops, `break`/`continue`, returns, user function calls, and action calls.
//!
//! Unsupported statements and expressions are rejected with [`CompileError`]
//! instead of being lowered to incorrect bytecode.

// #![warn(missing_docs)]

pub mod function;
pub mod lowering;

#[cfg(test)]
mod tests;

use index_type::typed_vec::TypedVec;

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
    <Lowering as lowering::CoreSet<Spec>>::ActionError,
>;

/// The [`waymark_vm_instructions_fullset::FullSet`] alias for convenience.
pub type InstructionFor<Spec> = waymark_vm_instructions_fullset::FullSet<Spec>;

/// The [`waymark_vm_bytecode::Executable`] alias for convenience.
pub type ExecutableFor<Spec> = waymark_vm_bytecode::Executable<InstructionFor<Spec>>;

/// Compile an old AST program into VM bytecode.
///
/// Functions keep their source order. That means function `0` in the resulting
/// executable is the first function in [`Program::functions`].
///
/// Unsupported AST constructs return [`CompileError`] instead of partial or
/// lossy bytecode.
pub fn compile<Spec, Lowering>(
    program: &waymark_vm_ast_old::Program,
) -> Result<ExecutableFor<Spec>, CompileErrorFor<Spec, Lowering>>
where
    Spec: waymark_vm_compiler_core::SpecRequirements,
    Lowering: lowering::FullSet<Spec>,
{
    let function_table = function::table::FunctionTable::build(program)?;
    let mut functions = TypedVec::with_capacity(program.functions.len());

    for function in &program.functions {
        let compiler =
            function::compiler::FunctionCompiler::<Spec, Lowering>::new(&function_table, function)?;
        functions.push(compiler.compile(function)?);
    }

    Ok(waymark_vm_bytecode::Executable { functions })
}
