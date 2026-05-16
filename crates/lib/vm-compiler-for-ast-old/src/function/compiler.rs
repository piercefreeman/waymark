//! Function-body lowering for [`FunctionDef`] values.
//!
//! This module owns the per-function lowering pipeline: it builds the local
//! environment, tracks flow-state across branches and loops, and emits the
//! final bytecode states for a single function body.

mod conditional;
mod context;
mod env;
mod error;
mod r#loop;
mod suspend;

mod plan {
    //! AST planning helpers that normalize statements and expressions before
    //! bytecode emission.

    use super::*;

    pub mod assignment;
    pub mod call;
    pub mod expr;
    pub mod r#loop;
    pub mod parallel;
    pub mod statement;
    pub mod unsupported;

    pub use self::unsupported::Unsupported;
}

mod lowering {
    //! Lowering helpers that translate plans into bytecode emission.

    use super::*;

    pub mod assignment;
    pub mod for_loop;
    pub mod parallel;
    pub mod statement;
    pub mod value;

    pub use self::assignment::AssignmentCompiler;
    pub use self::for_loop::ForLoopCompiler;
    pub use self::parallel::ParallelCompiler;
    pub use self::statement::StatementCompiler;
    pub use self::value::ValueCompiler;
}

mod bytecode {
    //! Bytecode assembly helpers for function lowering.

    use super::*;

    pub mod emitter;
    pub mod states;
}

#[cfg(test)]
mod test_helpers;

use self::bytecode::emitter::FunctionEmitter;
use self::context::*;
use self::env::{FlowState, LocalFrame};
use self::r#loop::LoopControlStack;
use self::lowering::StatementCompiler;
use self::lowering::ValueCompiler;

pub use self::error::*;

use waymark_vm_ast_old::{FunctionDef, Spanned};
use waymark_vm_compiler_for_ast_old_core::InstructionFor;

use super::table::FunctionTable;

/// Concrete function-compiler error type for a spec and lowering pair.
type ErrorFor<Spec, Lowering> = Error<
    <Lowering as waymark_vm_compiler_for_ast_old_core::lowering::PureSet<Spec>>::LiteralError,
    <Lowering as waymark_vm_compiler_for_ast_old_core::lowering::ExtCallSet<Spec>>::ActionError,
>;

/// Lowers one AST function body into VM bytecode.
pub(crate) struct FunctionCompiler<'a, Spec, Lowering>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
{
    /// Keeps the lowering type in the compiler state without storing a value.
    phantom_data: core::marker::PhantomData<Lowering>,

    /// Program-wide function metadata for resolving user-defined calls.
    function_table: &'a FunctionTable,

    /// Bytecode emitter for the function currently being lowered.
    emitter: FunctionEmitter<Spec>,

    /// Local-variable storage and register allocation for the function.
    local_frame: LocalFrame,

    /// Definite-initialization state at the current control-flow point.
    flow_state: FlowState,
}

impl<'a, Spec, Lowering> FunctionCompiler<'a, Spec, Lowering>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
    Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
{
    /// Creates a compiler for one function definition.
    pub fn new(
        function_table: &'a FunctionTable,
        function: &'a Spanned<FunctionDef>,
    ) -> Result<Self, ErrorFor<Spec, Lowering>> {
        let mut local_frame = LocalFrame::new();
        let mut flow_state = FlowState::new();

        for input in &function.value.io.value.inputs {
            let Some(_) = local_frame.declare_input(&mut flow_state, input.clone()) else {
                return Err(Error::DuplicateInput {
                    function: function.value.name.clone(),
                    name: input.clone(),
                });
            };
        }
        Ok(Self {
            phantom_data: core::marker::PhantomData,
            function_table,
            emitter: FunctionEmitter::new(),
            local_frame,
            flow_state,
        })
    }

    /// Compiles the stored function definition into bytecode.
    pub fn compile(
        mut self,
        function: &Spanned<FunctionDef>,
    ) -> Result<waymark_vm_bytecode::Function<InstructionFor<Spec>>, ErrorFor<Spec, Lowering>> {
        self.statement_compiler()
            .compile_block(&function.value.body)?;

        if self.emitter.is_active() {
            self.value_compiler().emit_return_none()?;
        }

        Ok(waymark_vm_bytecode::Function {
            states: self.emitter.finish(),
            num_regs: self.local_frame.num_registers(),
        })
    }

    /// Creates a value compiler over the current compiler context.
    fn value_compiler(&mut self) -> ValueCompiler<'_, 'a, Spec, Lowering> {
        ValueCompiler::new(self.context().into_ref())
    }

    /// Creates a statement compiler over the current compiler context.
    fn statement_compiler(&mut self) -> StatementCompiler<'_, 'a, Spec, Lowering> {
        StatementCompiler::new(self.context(), LoopControlStack::new())
    }

    /// Bundles the mutable compiler state needed by lowering helpers.
    fn context(&mut self) -> CompilerContextMut<'_, 'a, Spec, Lowering> {
        CompilerContextMut::new(
            self.function_table,
            &mut self.emitter,
            &mut self.local_frame,
            &mut self.flow_state,
        )
    }
}
