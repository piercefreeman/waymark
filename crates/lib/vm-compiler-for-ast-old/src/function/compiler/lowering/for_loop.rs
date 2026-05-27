//! For-loop lowering.
//!
//! # Shared skeleton
//!
//! All `for` loops share one four-state skeleton (condition, body, continue,
//! exit) so that `break`/`continue` resolution and flow-state plumbing stay
//! in one place.
//!
//! # Header variants
//!
//! Header variants exist only where they let cheaper cases skip machinery
//! the expensive ones need. In particular, a runtime-signed `range` step is
//! the only thing that forces extra condition states, so statically-known
//! steps get their own path.
//!
//! # Enumerate
//!
//! `enumerate(...)` is unwrapped during header classification to keep
//! iteration mechanics independent of variable-binding shape.

mod bindings;
mod header;
mod indexed;
mod range;
mod scaffold;
mod spread;

use waymark_vm_ast_old::{Block, Expr, Literal, Spanned};
use waymark_vm_instructions_pureset::BinaryOpKind;
use waymark_vm_runtime_core::RegisterId;

use self::header::{RangeLoop, ResolvedForLoop};

use super::CompilerContextMut;
use super::Error;
use super::ErrorFor;
use super::env::RegisterHandle;
use super::r#loop::LoopControlStack;

/// Lowers `for` loops into bytecode states and register updates.
pub struct ForLoopCompiler<'borrow, 'table, Spec, Lowering>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
    Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
{
    /// Mutable compiler context for for-loop lowering.
    context: CompilerContextMut<'borrow, 'table, Spec, Lowering>,

    /// Active loop scopes visible to nested statements.
    loop_control: LoopControlStack,
}

impl<'borrow, 'table, Spec, Lowering> CompilerContextMut<'borrow, 'table, Spec, Lowering>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
    Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
{
    /// Reborrows the context for for-loop lowering.
    pub fn for_loop_compiler(
        &mut self,
        loop_control: LoopControlStack,
    ) -> ForLoopCompiler<'_, 'table, Spec, Lowering> {
        self.reborrow_mut().into_for_loop_compiler(loop_control)
    }

    /// Converts this context into a for-loop compiler.
    pub fn into_for_loop_compiler(
        self,
        loop_control: LoopControlStack,
    ) -> ForLoopCompiler<'borrow, 'table, Spec, Lowering> {
        ForLoopCompiler::new(self, loop_control)
    }
}

/// Persistent registers from indexed spread fan-out that the join can reuse.
#[derive(Clone, Copy)]
struct IndexedSpreadJoinRegisters {
    /// Loop counter register from fan-out, reset to `0` before join.
    index_register: RegisterId,

    /// Cached `len(iterable)` register from fan-out, reused as join bound.
    length_register: RegisterId,
}

impl<'borrow, 'table, Spec, Lowering> ForLoopCompiler<'borrow, 'table, Spec, Lowering>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
    Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
{
    /// Creates a for-loop compiler over the provided context and loop scope.
    pub fn new(
        context: CompilerContextMut<'borrow, 'table, Spec, Lowering>,
        loop_control: LoopControlStack,
    ) -> Self {
        Self {
            context,
            loop_control,
        }
    }

    /// Compiles a `for` loop using the appropriate validated header shape.
    pub fn compile(
        &mut self,
        loop_vars: &[String],
        iterable: &Spanned<Expr>,
        body: &Spanned<Block>,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        match ResolvedForLoop::build::<Spec, Lowering>(iterable)? {
            ResolvedForLoop::Indexed { iterable, binding } => {
                self.compile_indexed_loop(loop_vars, iterable, body, binding)
            }
            ResolvedForLoop::Range {
                range: RangeLoop::Positive { start, end },
                binding,
            } => self.compile_positive_range_loop(loop_vars, start, end, body, binding),
            ResolvedForLoop::Range {
                range: RangeLoop::Stepped { start, end, step },
                binding,
            } => self.compile_stepped_range_loop(loop_vars, start, end, step, body, binding),
        }
    }

    /// Compiles an expression into the exact target register.
    ///
    /// Hints the value compiler with [`ResultTarget::Existing`] so it can emit
    /// directly into `target_register` when possible, and falls back to a
    /// trailing `emit_copy` when the compiler had to materialize the value in
    /// a different register (for example, because the expression evaluated to
    /// an existing local that the loop must not overwrite).
    fn compile_expr_into_register(
        &mut self,
        expr: &Spanned<Expr>,
        target_register: RegisterId,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let value_register = self
            .context
            .value_compiler()
            .compile_expr(expr, super::value::ResultTarget::Existing(target_register))?;
        if value_register.register() != target_register {
            self.context
                .emitter
                .emit_copy(target_register, value_register.register());
        }
        Ok(())
    }

    /// Emits an integer literal into the provided persistent register.
    fn emit_int_literal_into_register(
        &mut self,
        target_register: RegisterId,
        value: i64,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let literal = self.lower_int_literal(value)?;
        self.context
            .emitter
            .emit_load_const(target_register, literal);
        Ok(())
    }

    /// Compiles an integer literal into a temporary register.
    fn compile_temporary_int_literal(
        &mut self,
        value: i64,
    ) -> Result<RegisterHandle, ErrorFor<Spec, Lowering>> {
        let register =
            RegisterHandle::Temporary(self.context.local_frame.allocate_temporary_register());
        self.emit_int_literal_into_register(register.register(), value)?;
        Ok(register)
    }

    /// Lowers an integer literal into the target VM constant representation.
    fn lower_int_literal(
        &self,
        value: i64,
    ) -> Result<<Spec as waymark_vm_instructions_pureset::Spec>::ConstValue, ErrorFor<Spec, Lowering>>
    {
        Lowering::lower_literal(&Literal::Int(value)).map_err(Error::LiteralLowering)
    }

    /// Emits `target_register = target_register + immediate`.
    ///
    /// Used for the constant `+1` step of indexed and positive-range loops,
    /// as well as the enumerate-index update. The immediate is materialized
    /// through [`compile_temporary_int_literal`](Self::compile_temporary_int_literal)
    /// so it goes through the same literal-folding path as any other integer
    /// literal in the program.
    fn emit_add_assign_immediate(
        &mut self,
        target_register: RegisterId,
        immediate: i64,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let immediate_register = self.compile_temporary_int_literal(immediate)?;
        self.context.emitter.emit_binary(
            BinaryOpKind::Add,
            target_register,
            target_register,
            immediate_register.register(),
        );
        Ok(())
    }
}
