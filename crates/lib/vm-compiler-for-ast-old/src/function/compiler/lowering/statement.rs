//! Statement lowering.

use waymark_vm_ast_old::{Block, ElifBranch, ElseBranch, Expr, IfBranch, Spanned, Statement};

use super::AssignmentCompiler;
use super::CompilerContextMut;
use super::ForLoopCompiler;
use super::ParallelCompiler;
use super::SpreadCompiler;
use super::ValueCompiler;
use super::conditional::{ConditionalJoin, ConditionalJoinFinish};
use super::env::FlowState;
use super::r#loop::LoopControlStack;
use super::plan::r#loop::WhileLoopPlan;
use super::plan::statement::StatementPlan;
use super::{Error, ErrorFor, LoopControlKind};

use waymark_vm_bytecode_core::StateId;

/// Lowers statements and control flow into bytecode states.
pub struct StatementCompiler<'borrow, 'table, Spec, Lowering>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
    Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
{
    /// Mutable compiler context for statement lowering.
    context: CompilerContextMut<'borrow, 'table, Spec, Lowering>,

    /// Active loop scopes available to nested statements.
    loop_control: LoopControlStack,
}

/// Whether a compiled branch terminates or continues with a flow state.
enum BranchBodyOutcome {
    /// The branch does not reach the shared continuation point.
    Terminated,

    /// The branch continues and contributes flow information.
    Continues {
        /// Flow state observed when the branch reaches the continuation point.
        flow_state: FlowState,
    },
}

impl<'borrow, 'table, Spec, Lowering> StatementCompiler<'borrow, 'table, Spec, Lowering>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
    Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
{
    /// Creates a statement compiler over the provided context and loop scope.
    pub fn new(
        context: CompilerContextMut<'borrow, 'table, Spec, Lowering>,
        loop_control: LoopControlStack,
    ) -> Self {
        Self {
            context,
            loop_control,
        }
    }

    /// Compiles a block until control flow terminates or statements are exhausted.
    pub fn compile_block(
        &mut self,
        block: &Spanned<Block>,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        for statement in &block.value.statements {
            if !self.context.emitter.is_active() {
                break;
            }

            self.compile_statement(statement)?;
        }

        Ok(())
    }

    /// Compiles a single statement.
    fn compile_statement(
        &mut self,
        statement: &Spanned<Statement>,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        match StatementPlan::<Spec>::build::<Lowering>(
            &statement.value,
            self.context.function_table,
        )? {
            StatementPlan::Assignment { targets, value } => {
                self.assignment_compiler()
                    .compile_statement(targets, value)?;
            }
            StatementPlan::ActionCall { call } => {
                self.value_compiler().compile_action_statement(call)?;
            }
            StatementPlan::Return { value } => {
                self.value_compiler().compile_return_statement(value)?;
            }
            StatementPlan::Expr { expr } => {
                self.value_compiler().compile_expression_statement(expr)?;
            }
            StatementPlan::Sleep { duration } => {
                self.value_compiler().compile_sleep_statement(duration)?;
            }
            StatementPlan::ParallelBlock { calls } => {
                self.parallel_compiler().compile_block(calls)?;
            }
            StatementPlan::Spread { spread } => {
                self.spread_compiler().compile_statement(spread)?;
            }
            StatementPlan::WhileLoop { condition, body } => {
                self.compile_while_loop(condition, body)?;
            }
            StatementPlan::ForLoop {
                loop_vars,
                iterable,
                body,
            } => {
                self.compile_for_loop(loop_vars, iterable, body)?;
            }
            StatementPlan::Conditional {
                if_branch,
                elif_branches,
                else_branch,
            } => {
                self.compile_conditional(if_branch, elif_branches, else_branch)?;
            }
            StatementPlan::Break => {
                self.compile_break()?;
            }
            StatementPlan::Continue => {
                self.compile_continue()?;
            }
        }

        Ok(())
    }

    /// Compiles an `if`/`elif`/`else` chain.
    fn compile_conditional(
        &mut self,
        if_branch: &Spanned<IfBranch>,
        elif_branches: &[Spanned<ElifBranch>],
        else_branch: Option<&Spanned<ElseBranch>>,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let join_state = self.new_state();
        let if_body_state = self.new_state();
        let elif_body_states = elif_branches
            .iter()
            .map(|_| self.new_state())
            .collect::<Vec<_>>();
        let mut conditional_join = ConditionalJoin::new(&*self.context.flow_state, join_state);

        self.emit_jump_if_condition(&if_branch.value.condition, if_body_state)?;

        for (branch, body_state) in elif_branches.iter().zip(elif_body_states.iter().copied()) {
            self.emit_jump_if_condition(&branch.value.condition, body_state)?;
        }

        *self.context.flow_state = conditional_join.branch_flow();

        match else_branch {
            Some(else_branch) => {
                self.compile_block(&else_branch.value.body)?;
                if self.context.emitter.is_active() {
                    conditional_join.record_continuation(self.context.flow_state.clone());
                    self.context
                        .emitter
                        .emit_jump(conditional_join.join_state());
                }
            }
            None => {
                conditional_join.record_fallthrough();
                self.context
                    .emitter
                    .emit_jump(conditional_join.join_state());
            }
        }

        if let BranchBodyOutcome::Continues { flow_state } = self.compile_branch_body(
            if_body_state,
            &if_branch.value.body,
            conditional_join.incoming_flow(),
            conditional_join.join_state(),
        )? {
            conditional_join.record_continuation(flow_state);
        }

        for (branch, body_state) in elif_branches.iter().zip(elif_body_states) {
            if let BranchBodyOutcome::Continues { flow_state } = self.compile_branch_body(
                body_state,
                &branch.value.body,
                conditional_join.incoming_flow(),
                conditional_join.join_state(),
            )? {
                conditional_join.record_continuation(flow_state);
            }
        }

        if let ConditionalJoinFinish::Join {
            join_state,
            merged_flow,
        } = conditional_join.finish()
        {
            self.switch_to_with_flow(join_state, merged_flow);
        }

        Ok(())
    }

    /// Compiles one conditional branch body from its entry state.
    fn compile_branch_body(
        &mut self,
        state_id: StateId,
        body: &Spanned<Block>,
        incoming_flow: &FlowState,
        join_state: StateId,
    ) -> Result<BranchBodyOutcome, ErrorFor<Spec, Lowering>> {
        self.switch_to_with_flow(state_id, incoming_flow.clone());
        self.compile_block(body)?;

        if !self.context.emitter.is_active() {
            return Ok(BranchBodyOutcome::Terminated);
        }

        let flow_state = self.context.flow_state.clone();
        self.context.emitter.emit_jump(join_state);
        Ok(BranchBodyOutcome::Continues { flow_state })
    }

    /// Compiles a `while` loop with dedicated condition, body, and exit states.
    fn compile_while_loop(
        &mut self,
        condition: &Spanned<Expr>,
        body: &Spanned<Block>,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let incoming_flow = self.context.flow_state.clone();
        let condition_state = self.new_state();
        let body_state = self.new_state();
        let exit_state = self.new_state();
        let while_loop =
            WhileLoopPlan::new(&incoming_flow, condition_state, body_state, exit_state);
        let body_loop_scope = while_loop.loop_scope();

        self.context.emitter.emit_jump(while_loop.condition_state());

        self.switch_to_with_flow(while_loop.condition_state(), while_loop.condition_flow());
        self.emit_jump_if_condition(condition, while_loop.body_state())?;
        self.context
            .emitter
            .emit_jump(body_loop_scope.target(LoopControlKind::Break));

        let body_loop_control = self.loop_control.with_loop(body_loop_scope);

        self.switch_to_with_flow(while_loop.body_state(), while_loop.body_flow());
        {
            let mut body_compiler = self.nested_statement_compiler(body_loop_control);
            body_compiler.compile_block(body)?;
        }

        if self.context.emitter.is_active() {
            self.context
                .emitter
                .emit_jump(body_loop_scope.target(LoopControlKind::Continue));
        }

        let (exit_state, exit_flow) = while_loop.finish();
        self.switch_to_with_flow(exit_state, exit_flow);

        Ok(())
    }

    /// Compiles a `for` loop over a generic iterable, `range(...)`, or
    /// `enumerate(...)`.
    fn compile_for_loop(
        &mut self,
        loop_vars: &[String],
        iterable: &Spanned<Expr>,
        body: &Spanned<Block>,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        self.for_loop_compiler().compile(loop_vars, iterable, body)
    }

    /// Compiles a `break` statement.
    fn compile_break(&mut self) -> Result<(), ErrorFor<Spec, Lowering>> {
        self.compile_loop_control(LoopControlKind::Break)
    }

    /// Compiles a `continue` statement.
    fn compile_continue(&mut self) -> Result<(), ErrorFor<Spec, Lowering>> {
        self.compile_loop_control(LoopControlKind::Continue)
    }

    /// Switches the emitter and flow state to a reserved state id.
    fn switch_to_with_flow(&mut self, state_id: StateId, flow_state: FlowState) {
        self.context.emitter.switch_to(state_id);
        *self.context.flow_state = flow_state;
    }

    /// Reserves a new bytecode state id.
    fn new_state(&mut self) -> StateId {
        self.context.emitter.reserve_state()
    }

    /// Emits a conditional jump when `condition` evaluates to true.
    fn emit_jump_if_condition(
        &mut self,
        condition: &Spanned<Expr>,
        target_state: StateId,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let condition_register = self
            .value_compiler()
            .compile_expr(condition, super::value::ResultTarget::Allocate)?;
        self.context
            .emitter
            .emit_jump_if(target_state, condition_register.register());
        Ok(())
    }

    /// Compiles a loop-control statement against the current loop scope.
    fn compile_loop_control(
        &mut self,
        kind: LoopControlKind,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let Some(loop_scope) = self.loop_control.current() else {
            return Err(Error::LoopControlOutsideLoop { kind });
        };

        self.context.emitter.emit_jump(loop_scope.target(kind));
        Ok(())
    }

    /// Creates a value compiler borrowing the current context.
    fn value_compiler(&mut self) -> ValueCompiler<'_, 'table, Spec, Lowering> {
        ValueCompiler::new(self.context.reborrow_ref())
    }

    /// Creates a for-loop compiler borrowing the current context mutably.
    fn for_loop_compiler(&mut self) -> ForLoopCompiler<'_, 'table, Spec, Lowering> {
        ForLoopCompiler::new(self.context.reborrow_mut(), self.loop_control.clone())
    }

    /// Creates an assignment compiler borrowing the current context mutably.
    fn assignment_compiler(&mut self) -> AssignmentCompiler<'_, 'table, Spec, Lowering> {
        AssignmentCompiler::new(self.context.reborrow_mut())
    }

    /// Creates a parallel compiler borrowing the current context mutably.
    fn parallel_compiler(&mut self) -> ParallelCompiler<'_, 'table, Spec, Lowering> {
        ParallelCompiler::new(self.context.reborrow_mut())
    }

    /// Creates a spread compiler borrowing the current context mutably.
    fn spread_compiler(&mut self) -> SpreadCompiler<'_, 'table, Spec, Lowering> {
        SpreadCompiler::new(self.context.reborrow_mut())
    }

    /// Creates a nested statement compiler with derived loop-control scope.
    fn nested_statement_compiler(
        &mut self,
        loop_control: LoopControlStack,
    ) -> StatementCompiler<'_, 'table, Spec, Lowering> {
        StatementCompiler::new(self.context.reborrow_mut(), loop_control)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use index_type::IndexType;
    use waymark_vm_ast_old_helpers::{assignment, block, conditional_stmt, variable};
    use waymark_vm_bytecode_core::StateId;
    use waymark_vm_compiler_for_ast_old_test_support::{TestLowering, TestSpec};
    use waymark_vm_instructions_coreset::CoreSet;
    use waymark_vm_instructions_fullset::FullSet as InstructionSet;
    use waymark_vm_instructions_pureset::PureSet;
    use waymark_vm_runtime_core::RegisterId;

    use crate::function::compiler::{
        CompilerContextMut,
        bytecode::emitter::FunctionEmitter,
        env::{FlowState, LocalFrame},
        r#loop::LoopControlStack,
        test_helpers::build_function_table,
    };

    #[test]
    fn statement_compiler_compiles_conditional_bodies_and_merges_flow() {
        let function_table = build_function_table();
        let mut emitter = FunctionEmitter::<TestSpec>::new();
        let mut local_frame = LocalFrame::new();
        let mut flow_state = FlowState::new();
        let flag_local = local_frame
            .declare_input(&mut flow_state, "flag".to_owned())
            .expect("flag input should declare");
        let loop_control = LoopControlStack::new();

        {
            let mut control = StatementCompiler::<TestSpec, TestLowering>::new(
                CompilerContextMut::new(
                    &function_table,
                    &mut emitter,
                    &mut local_frame,
                    &mut flow_state,
                ),
                loop_control,
            );

            control
                .compile_block(&block(vec![conditional_stmt(
                    variable("flag"),
                    vec![assignment("resolved", variable("flag"))],
                    Vec::new(),
                    Some(vec![assignment("resolved", variable("flag"))]),
                )]))
                .expect("conditional block should compile");
        }

        let resolved_local = local_frame
            .resolve_initialized_local("resolved", &flow_state)
            .expect("resolved should be initialized after both branches assign it");
        assert_eq!(flag_local.register(), RegisterId(0));
        assert_eq!(resolved_local.register(), RegisterId(1));

        let states = emitter.finish();
        assert_eq!(states.len().to_scalar(), 3);

        let mut entry_instructions = states[StateId(0)].instructions.iter();
        assert!(matches!(
            entry_instructions.next(),
            Some(InstructionSet::CoreSet(CoreSet::JumpIf { target_state, cond }))
                if *target_state == StateId(2) && *cond == RegisterId(0)
        ));
        assert!(matches!(
            entry_instructions.next(),
            Some(InstructionSet::PureSet(PureSet::Copy { dst, src }))
                if *dst == RegisterId(1) && *src == RegisterId(0)
        ));
        assert!(matches!(
            entry_instructions.next(),
            Some(InstructionSet::CoreSet(CoreSet::Jump { target_state }))
                if *target_state == StateId(1)
        ));
        assert!(entry_instructions.next().is_none());

        let mut if_body_instructions = states[StateId(2)].instructions.iter();
        assert!(matches!(
            if_body_instructions.next(),
            Some(InstructionSet::PureSet(PureSet::Copy { dst, src }))
                if *dst == RegisterId(1) && *src == RegisterId(0)
        ));
        assert!(matches!(
            if_body_instructions.next(),
            Some(InstructionSet::CoreSet(CoreSet::Jump { target_state }))
                if *target_state == StateId(1)
        ));
        assert!(if_body_instructions.next().is_none());
        assert!(states[StateId(1)].instructions.is_empty());
    }
}
