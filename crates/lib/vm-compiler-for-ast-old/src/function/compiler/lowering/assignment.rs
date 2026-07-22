//! Assignment lowering.

use waymark_vm_ast_old::{ActionCall, Expr, Spanned};

use crate::Marked;
use crate::function::compiler::env::AssignmentTargetMarker;
use crate::function::compiler::env::LocalSlot;

use super::CompilerContextMut;
use super::ErrorFor;
use super::ForLoopCompiler;
use super::ParallelCompiler;
use super::ValueCompiler;
use super::r#loop::LoopControlStack;
use super::plan::assignment::AssignmentStatementPlan;

/// Lowers assignment statements into bytecode.
pub struct AssignmentCompiler<'borrow, 'table, Spec, Lowering>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
    Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
{
    /// Mutable compiler context for assignment lowering.
    context: CompilerContextMut<'borrow, 'table, Spec, Lowering>,

    /// Active exception-handler nesting depth for this assignment context.
    exception_handler_depth: usize,
}

impl<'borrow, 'table, Spec, Lowering> AssignmentCompiler<'borrow, 'table, Spec, Lowering>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
    Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
{
    /// Creates an assignment compiler over the provided context.
    pub fn new(
        context: CompilerContextMut<'borrow, 'table, Spec, Lowering>,
        exception_handler_depth: usize,
    ) -> Self {
        Self {
            context,
            exception_handler_depth,
        }
    }

    /// Compiles one assignment statement.
    pub fn compile_statement(
        &mut self,
        targets: &[String],
        value: &Spanned<Expr>,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let function_table = self.context.function_table;
        let local_frame = &mut *self.context.local_frame;
        let flow_state = &mut *self.context.flow_state;
        let plan = AssignmentStatementPlan::<Spec>::build::<Lowering, _>(
            targets,
            value,
            function_table,
            |target| {
                Marked::<LocalSlot, AssignmentTargetMarker>::get_or_declare(
                    local_frame,
                    flow_state,
                    target,
                )
            },
        )?;

        match plan {
            AssignmentStatementPlan::Direct { target, value } => {
                self.compile_direct_assignment(target, value)?;
            }
            AssignmentStatementPlan::Spread {
                target,
                collection,
                loop_var,
                action,
            } => {
                self.compile_spread_assignment(target, collection, loop_var, action)?;
            }
            AssignmentStatementPlan::SpreadDiscard {
                collection,
                loop_var,
                action,
            } => {
                self.compile_spread_discard(collection, loop_var, action)?;
            }
            AssignmentStatementPlan::Parallel { assignment } => {
                self.parallel_compiler().compile_assignment(assignment)?;
            }
        }

        Ok(())
    }

    /// Compiles a direct assignment into its target slot.
    fn compile_direct_assignment(
        &mut self,
        target: Marked<LocalSlot, AssignmentTargetMarker>,
        value: &Spanned<Expr>,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let value_register = self.value_compiler().compile_expr(
            value,
            super::value::ResultTarget::Existing(target.register()),
        )?;

        if value_register.register() != target.register() {
            self.context
                .emitter
                .emit_copy(target.register(), value_register.register());
        }

        target.mark_initialized(self.context.flow_state);
        Ok(())
    }

    /// Compiles one spread-expression assignment into a collected result list.
    fn compile_spread_assignment(
        &mut self,
        target: Marked<LocalSlot, AssignmentTargetMarker>,
        collection: &Spanned<Expr>,
        loop_var: &str,
        action: &ActionCall,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let accumulator_register = self.context.local_frame.allocate_register();
        self.for_loop_compiler().compile_spread_expr(
            collection,
            loop_var,
            action,
            accumulator_register,
        )?;

        if accumulator_register != target.register() {
            self.context
                .emitter
                .emit_copy(target.register(), accumulator_register);
        }

        target.mark_initialized(self.context.flow_state);
        Ok(())
    }

    /// Compiles a side-effect-only spread assignment emitted by the frontend.
    fn compile_spread_discard(
        &mut self,
        collection: &Spanned<Expr>,
        loop_var: &str,
        action: &ActionCall,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        self.for_loop_compiler()
            .compile_spread_statement(collection, loop_var, action)
    }

    /// Creates a value compiler borrowing the current context.
    fn value_compiler(&mut self) -> ValueCompiler<'_, 'table, Spec, Lowering> {
        ValueCompiler::new(self.context.reborrow_ref())
    }

    /// Creates a parallel compiler borrowing the current context mutably.
    fn parallel_compiler(&mut self) -> ParallelCompiler<'_, 'table, Spec, Lowering> {
        ParallelCompiler::new(self.context.reborrow_mut())
    }

    /// Creates a for-loop compiler for internal spread lowering.
    fn for_loop_compiler(&mut self) -> ForLoopCompiler<'_, 'table, Spec, Lowering> {
        ForLoopCompiler::new(
            self.context.reborrow_mut(),
            LoopControlStack::new(),
            self.exception_handler_depth,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use index_type::IndexType;
    use waymark_vm_ast_old_helpers::variable;
    use waymark_vm_bytecode_core::StateId;
    use waymark_vm_compiler_for_ast_old_test_support::{TestLowering, TestSpec};
    use waymark_vm_instructions_fullset::FullSet as InstructionSet;
    use waymark_vm_instructions_pureset::PureSet;
    use waymark_vm_runtime_core::RegisterId;

    use crate::function::extras::ExtraFunctions;

    use crate::function::compiler::{
        CompilerContextMut,
        bytecode::emitter::FunctionEmitter,
        env::{AssignmentTargetMarker, FlowState, LocalFrame, LocalSlot},
        test_helpers::build_function_table,
    };

    #[test]
    fn direct_assignment_target_marks_flow_initialized() {
        let mut local_frame = LocalFrame::new();
        let mut flow_state = FlowState::new();
        let target = Marked::<LocalSlot, AssignmentTargetMarker>::get_or_declare(
            &mut local_frame,
            &mut flow_state,
            "value",
        );
        let target_register = target.register();

        target.mark_initialized(&mut flow_state);

        let local = local_frame
            .resolve_initialized_local("value", &flow_state)
            .expect("value should resolve after target initialization");
        assert_eq!(local.register(), target_register);
    }

    #[test]
    fn assignment_target_get_or_declare_uses_local_frame_register() {
        let mut local_frame = LocalFrame::new();
        let mut flow_state = FlowState::new();

        let target = Marked::<LocalSlot, AssignmentTargetMarker>::get_or_declare(
            &mut local_frame,
            &mut flow_state,
            "value",
        );
        let same_target = Marked::<LocalSlot, AssignmentTargetMarker>::get_or_declare(
            &mut local_frame,
            &mut flow_state,
            "value",
        );

        assert_eq!(target.register(), RegisterId(0));
        assert_eq!(same_target.register(), RegisterId(0));
        assert_eq!(local_frame.num_registers(), 1);
        assert!(
            local_frame
                .resolve_initialized_local("value", &flow_state)
                .is_none()
        );

        target.mark_initialized(&mut flow_state);

        let local = local_frame
            .resolve_initialized_local("value", &flow_state)
            .expect("value should resolve after target initialization");
        assert_eq!(local.register(), RegisterId(0));
    }

    #[test]
    fn direct_variable_assignments_emit_copy_and_initialize_target() {
        let function_table = build_function_table();
        let mut emitter = FunctionEmitter::<TestSpec>::new();
        let mut local_frame = LocalFrame::new();
        let mut flow_state = FlowState::new();
        let mut extra_fns = ExtraFunctions::<TestSpec>::new(1);
        let source_local = local_frame
            .declare_input(&mut flow_state, "source".to_owned())
            .expect("source input should declare");

        {
            let mut assignments = AssignmentCompiler::<TestSpec, TestLowering>::new(
                CompilerContextMut::new(
                    &function_table,
                    &mut emitter,
                    &mut local_frame,
                    &mut extra_fns,
                    &mut flow_state,
                ),
                0,
            );

            assignments
                .compile_statement(&["target".to_owned()], &variable("source"))
                .expect("variable assignment should compile");
        }

        let target_local = local_frame
            .resolve_initialized_local("target", &flow_state)
            .expect("target should be initialized after assignment");
        assert_eq!(source_local.register(), RegisterId(0));
        assert_eq!(target_local.register(), RegisterId(1));

        let states = emitter.finish();
        assert_eq!(states.len().to_scalar(), 1);

        let mut instructions = states[StateId(0)].instructions.iter();
        assert!(matches!(
            instructions.next(),
            Some(InstructionSet::PureSet(PureSet::Copy { dst, src }))
                if *dst == RegisterId(1) && *src == RegisterId(0)
        ));
        assert!(instructions.next().is_none());
    }
}
