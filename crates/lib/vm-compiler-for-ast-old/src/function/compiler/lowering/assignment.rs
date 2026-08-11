//! Assignment lowering.

use waymark_vm_ast_old::{ActionCall, Expr, Literal, Spanned};

use crate::Marked;
use crate::function::compiler::env::AssignmentTargetMarker;
use crate::function::compiler::env::LocalSlot;

use super::CompilerContextMut;
use super::Error;
use super::ErrorFor;
use super::ForLoopCompiler;
use super::ParallelCompiler;
use super::ValueCompiler;
use super::r#loop::LoopControlStack;
use super::plan::assignment::AssignmentStatementPlan;

/// The exception type raised when an unpacked value's length does not match
/// the assignment target count.
const UNPACK_MISMATCH_TYPE_ID: &str = "ValueError";

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
            AssignmentStatementPlan::Unpack { targets, value } => {
                self.compile_unpack_assignment(targets, value)?;
            }
            AssignmentStatementPlan::RangeValues { target, call } => {
                self.compile_range_values_assignment(target, call)?;
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

    /// Compiles a multi-target assignment: the value is evaluated once, its
    /// length is checked against the target count — a mismatch raises a
    /// catchable `ValueError` — and each target receives its item by index.
    fn compile_unpack_assignment(
        &mut self,
        targets: Vec<Marked<LocalSlot, AssignmentTargetMarker>>,
        value: &Spanned<Expr>,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        // Extraction writes every target while still reading the value, so a
        // value that lives in a target's register (`a, b = a`) needs a copy.
        let value_register = self
            .value_compiler()
            .compile_expr(value, super::value::ResultTarget::Allocate)?;
        let value_register = self.value_compiler().unalias_source(
            value_register,
            targets.iter().map(|target| target.register()),
        );

        let length_register = self.context.local_frame.allocate_register();
        self.context
            .emitter
            .emit_length(length_register, value_register.register());

        let expected_length = i64::try_from(targets.len()).expect("target count fits in i64");
        let expected_length = Lowering::lower_literal(&Literal::Int(expected_length))
            .map_err(Error::LiteralLowering)?;
        let expected_register = self.context.local_frame.allocate_register();
        self.context
            .emitter
            .emit_load_const(expected_register, expected_length);

        let length_matches_register = self.context.local_frame.allocate_register();
        self.context.emitter.emit_binary(
            waymark_vm_instructions_pureset::BinaryOpKind::Eq,
            length_matches_register,
            length_register,
            expected_register,
        );

        let unpack_state = self.context.emitter.reserve_state();
        self.context
            .emitter
            .emit_jump_if(unpack_state, length_matches_register);

        let type_id_value =
            Lowering::lower_literal(&Literal::String(UNPACK_MISMATCH_TYPE_ID.to_owned()))
                .map_err(Error::LiteralLowering)?;
        let type_id_register = self.context.local_frame.allocate_register();
        self.context
            .emitter
            .emit_load_const(type_id_register, type_id_value);

        let details_value =
            Lowering::lower_literal(&Literal::None).map_err(Error::LiteralLowering)?;
        let details_register = self.context.local_frame.allocate_register();
        self.context
            .emitter
            .emit_load_const(details_register, details_value);

        let exception_register = self.context.local_frame.allocate_register();
        self.context.emitter.emit_make_exception(
            exception_register,
            type_id_register,
            details_register,
        );
        self.context.emitter.emit_raise(exception_register);

        self.context.emitter.switch_to(unpack_state);
        for (item_index, target) in targets.into_iter().enumerate() {
            let item_index = i64::try_from(item_index).expect("target count fits in i64");
            let index_value = Lowering::lower_literal(&Literal::Int(item_index))
                .map_err(Error::LiteralLowering)?;
            let index_register = self.context.local_frame.allocate_register();
            self.context
                .emitter
                .emit_load_const(index_register, index_value);
            self.context.emitter.emit_index(
                target.register(),
                value_register.register(),
                index_register,
            );
            target.mark_initialized(self.context.flow_state);
        }

        Ok(())
    }

    /// Compiles a `range(...)` assignment into a materialized value list.
    ///
    /// The list accumulates in a fresh register and is copied into the
    /// target only after the loop, so the range bounds may read the
    /// target's previous value.
    fn compile_range_values_assignment(
        &mut self,
        target: Marked<LocalSlot, AssignmentTargetMarker>,
        call: &waymark_vm_ast_old::FunctionCall,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let accumulator_register = self.context.local_frame.allocate_register();
        self.for_loop_compiler()
            .compile_range_values(call, accumulator_register)?;

        if accumulator_register != target.register() {
            self.context
                .emitter
                .emit_copy(target.register(), accumulator_register);
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

    #[test]
    fn range_assignments_materialize_a_value_list() {
        let function_table = build_function_table();
        let mut emitter = FunctionEmitter::<TestSpec>::new();
        let mut local_frame = LocalFrame::new();
        let mut flow_state = FlowState::new();
        let mut extra_fns = ExtraFunctions::<TestSpec>::new(1);
        local_frame
            .declare_input(&mut flow_state, "stop".to_owned())
            .expect("stop input should declare");

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

            let mut call =
                waymark_vm_ast_old_helpers::function_call("range", vec![variable("stop")]);
            call.global_function = Some(waymark_vm_ast_old::GlobalFunction::Range);
            let value =
                waymark_vm_ast_old_helpers::spanned(waymark_vm_ast_old::Expr::FunctionCall {
                    call,
                });

            assignments
                .compile_statement(&["values".to_owned()], &value)
                .expect("range assignment should compile");
        }

        assert!(
            local_frame
                .resolve_initialized_local("values", &flow_state)
                .is_some()
        );

        let function = waymark_vm_bytecode::Function {
            states: emitter.finish(),
            num_regs: local_frame.num_registers(),
        };
        insta::assert_snapshot!(waymark_vm_bytecode_fmt::display(&function).to_string(), @r"
        s0:
          PureSet(MakeList { dst: r2, items: [] })
          PureSet(LoadConst { dst: r3, value: Int(0) })
          PureSet(Copy { dst: r4, src: r0 })
          CoreSet(Jump { target_state: s1 })
        s1:
          PureSet(Binary { kind: Lt, op: BinaryOp { dst: r5, a: r3, b: r4 } })
          CoreSet(JumpIf { target_state: s2, cond: r5 })
          CoreSet(Jump { target_state: s4 })
        s2:
          PureSet(ListAppend { dst: r2, list: r2, item: r3 })
          CoreSet(Jump { target_state: s3 })
        s3:
          PureSet(LoadConst { dst: r5, value: Int(1) })
          PureSet(Binary { kind: Add, op: BinaryOp { dst: r3, a: r3, b: r5 } })
          CoreSet(Jump { target_state: s1 })
        s4:
          PureSet(Copy { dst: r1, src: r2 })
        ");
    }

    #[test]
    fn unpack_assignments_guard_length_and_extract_by_index() {
        let function_table = build_function_table();
        let mut emitter = FunctionEmitter::<TestSpec>::new();
        let mut local_frame = LocalFrame::new();
        let mut flow_state = FlowState::new();
        let mut extra_fns = ExtraFunctions::<TestSpec>::new(1);
        local_frame
            .declare_input(&mut flow_state, "pair".to_owned())
            .expect("pair input should declare");

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
                .compile_statement(&["left".to_owned(), "right".to_owned()], &variable("pair"))
                .expect("unpack assignment should compile");
        }

        assert!(
            local_frame
                .resolve_initialized_local("left", &flow_state)
                .is_some()
        );
        assert!(
            local_frame
                .resolve_initialized_local("right", &flow_state)
                .is_some()
        );

        let function = waymark_vm_bytecode::Function {
            states: emitter.finish(),
            num_regs: local_frame.num_registers(),
        };
        insta::assert_snapshot!(waymark_vm_bytecode_fmt::display(&function).to_string(), @r#"
        s0:
          PureSet(Length { dst: r3, src: r0 })
          PureSet(LoadConst { dst: r4, value: Int(2) })
          PureSet(Binary { kind: Eq, op: BinaryOp { dst: r5, a: r3, b: r4 } })
          CoreSet(JumpIf { target_state: s1, cond: r5 })
          PureSet(LoadConst { dst: r6, value: String("ValueError") })
          PureSet(LoadConst { dst: r7, value: None })
          PureSet(MakeException { dst: r8, type_id: r6, details: r7 })
          CoreSet(Raise { src: r8 })
        s1:
          PureSet(LoadConst { dst: r9, value: Int(0) })
          PureSet(Index { dst: r1, object: r0, index: r9 })
          PureSet(LoadConst { dst: r10, value: Int(1) })
          PureSet(Index { dst: r2, object: r0, index: r10 })
        "#);
    }

    #[test]
    fn unpack_assignments_copy_a_value_that_aliases_a_target() {
        let function_table = build_function_table();
        let mut emitter = FunctionEmitter::<TestSpec>::new();
        let mut local_frame = LocalFrame::new();
        let mut flow_state = FlowState::new();
        let mut extra_fns = ExtraFunctions::<TestSpec>::new(1);
        local_frame
            .declare_input(&mut flow_state, "pair".to_owned())
            .expect("pair input should declare");

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
                .compile_statement(&["pair".to_owned(), "second".to_owned()], &variable("pair"))
                .expect("self-referential unpack assignment should compile");
        }

        // The value lives in r0, which is also the first target: extraction
        // reads the copy in r2 instead, so `second` still sees the original.
        let function = waymark_vm_bytecode::Function {
            states: emitter.finish(),
            num_regs: local_frame.num_registers(),
        };
        insta::assert_snapshot!(waymark_vm_bytecode_fmt::display(&function).to_string(), @r#"
        s0:
          PureSet(Copy { dst: r2, src: r0 })
          PureSet(Length { dst: r3, src: r2 })
          PureSet(LoadConst { dst: r4, value: Int(2) })
          PureSet(Binary { kind: Eq, op: BinaryOp { dst: r5, a: r3, b: r4 } })
          CoreSet(JumpIf { target_state: s1, cond: r5 })
          PureSet(LoadConst { dst: r6, value: String("ValueError") })
          PureSet(LoadConst { dst: r7, value: None })
          PureSet(MakeException { dst: r8, type_id: r6, details: r7 })
          CoreSet(Raise { src: r8 })
        s1:
          PureSet(LoadConst { dst: r9, value: Int(0) })
          PureSet(Index { dst: r0, object: r2, index: r9 })
          PureSet(LoadConst { dst: r10, value: Int(1) })
          PureSet(Index { dst: r1, object: r2, index: r10 })
        "#);
    }
}
