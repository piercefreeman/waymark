//! Statement lowering.

use waymark_vm_ast_old::{
    Block, ElifBranch, ElseBranch, ExceptHandler, Expr, IfBranch, Spanned, Statement,
};

use super::AssignmentCompiler;
use super::CompilerContextMut;
use super::ForLoopCompiler;
use super::ParallelCompiler;
use super::ValueCompiler;
use super::conditional::{ConditionalJoin, ConditionalJoinFinish};
use super::env::FlowState;
use super::r#loop::LoopControlStack;
use super::plan::call::CallPlanFor;
use super::plan::r#loop::WhileLoopPlan;
use super::plan::statement::StatementPlan;
use super::{Error, ErrorFor, LoopControlKind};

use nonempty_collections::NEVec;
use waymark_vm_bytecode_core::StateId;
use waymark_vm_runtime_core::RegisterId;

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

    /// Active frame unwind depth for this block.
    unwind_depth: usize,

    /// Finalizers crossed by control flow leaving the current block.
    finally_scopes: Vec<FinallyScope>,
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

#[derive(Clone, Copy)]
/// A finalizer and the unwind depth outside its protected statement.
struct FinallyScope {
    /// Shared state containing the finalizer body.
    state: StateId,

    /// Stack depth restored before the finalizer executes.
    unwind_depth: usize,
}

#[derive(Clone, Copy)]
/// Control flow performed after shared finalizer states return.
enum ControlTransfer {
    /// Jump to a state.
    Jump(StateId),

    /// Raise an exception register.
    Raise(RegisterId),

    /// Return a value register.
    Return(RegisterId),
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
            unwind_depth: 0,
            finally_scopes: Vec::new(),
        }
    }

    /// Returns a compiler configured with the provided active unwind depth.
    pub fn with_unwind_depth(mut self, unwind_depth: usize) -> Self {
        self.unwind_depth = unwind_depth;
        self
    }

    /// Adds one finalizer around blocks compiled by this compiler.
    fn with_finally_scope(mut self, finally_scope: FinallyScope) -> Self {
        self.finally_scopes.push(finally_scope);
        self
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
            StatementPlan::SpreadAction {
                collection,
                loop_var,
                call,
            } => {
                self.compile_spread_action(collection, loop_var, call)?;
            }
            StatementPlan::Return { value } => {
                self.compile_return(value)?;
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
            StatementPlan::TryExcept {
                handlers,
                try_block,
                finally_block,
            } => {
                match finally_block {
                    Some(finally_block) => {
                        self.compile_try_finally(handlers, try_block, finally_block)
                    }
                    None => self.compile_try_except(handlers, try_block),
                }?;
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

    /// Compiles a `try`/`finally` region around a block or nested `try`/`except`.
    fn compile_try_finally(
        &mut self,
        handlers: &[Spanned<ExceptHandler>],
        try_block: &Spanned<Block>,
        finally_block: &Spanned<Block>,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let incoming_flow = self.context.flow_state.clone();
        let exception_finally_state = self.new_state();
        let finally_state = self.new_state();
        let exception_register = self.context.local_frame.allocate_register();
        self.context.emitter.emit_push_exception_handlers(vec![
            waymark_vm_exception_handler::ExceptionHandler {
                handler_state: exception_finally_state,
                exception_types: Vec::new(),
                exception_dst: Some(exception_register),
            },
        ]);

        let protected_unwind_depth = self.unwind_depth + 1;
        let finally_scope = FinallyScope {
            state: finally_state,
            unwind_depth: self.unwind_depth,
        };
        {
            let mut protected_compiler = self
                .nested_statement_compiler(self.loop_control.clone())
                .with_unwind_depth(protected_unwind_depth)
                .with_finally_scope(finally_scope);
            if handlers.is_empty() {
                protected_compiler.compile_block(try_block)?;
            } else {
                protected_compiler.compile_try_except(handlers, try_block)?;
            }
        }

        let normal_continuation = self.context.emitter.is_active().then(|| {
            let continuation_state = self.new_state();
            let flow_state = self.context.flow_state.clone();
            self.emit_finalizer_calls(
                vec![finally_scope],
                ControlTransfer::Jump(continuation_state),
                self.unwind_depth,
            );
            (continuation_state, flow_state)
        });

        self.switch_to_with_flow(exception_finally_state, incoming_flow.clone());
        self.emit_finalizer_calls(
            vec![finally_scope],
            ControlTransfer::Raise(exception_register),
            self.unwind_depth,
        );

        self.switch_to_with_flow(finally_state, incoming_flow);
        let finalizer_unwind_depth = self.unwind_depth + 1;
        self.nested_statement_compiler(self.loop_control.clone())
            .with_unwind_depth(finalizer_unwind_depth)
            .compile_block(finally_block)?;
        let finalizer_flow = self.context.emitter.is_active().then(|| {
            let flow_state = self.context.flow_state.clone();
            self.context.emitter.emit_return_state();
            flow_state
        });

        if let (Some((continuation_state, normal_flow)), Some(finalizer_flow)) =
            (normal_continuation, finalizer_flow)
        {
            let mut continuation_flows = NEVec::new(normal_flow);
            continuation_flows.push(finalizer_flow);
            self.switch_to_with_flow(
                continuation_state,
                FlowState::union_branches(continuation_flows),
            );
        }

        Ok(())
    }

    /// Compiles a `try`/`except` block by pushing one protected handler block.
    fn compile_try_except(
        &mut self,
        handlers: &[Spanned<ExceptHandler>],
        try_block: &Spanned<Block>,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let incoming_flow = self.context.flow_state.clone();
        let join_state = self.new_state();
        let handler_states = handlers
            .iter()
            .map(|_| self.new_state())
            .collect::<Vec<_>>();
        let mut try_handlers = Vec::with_capacity(handlers.len());

        for (handler, handler_state) in handlers.iter().zip(handler_states.iter().copied()) {
            let exception_types = if handler.value.exception_types == ["Exception"] {
                Vec::new()
            } else {
                handler.value.exception_types.clone()
            };
            let exception_dst = handler.value.exception_var.as_ref().map(|exception_var| {
                self.context
                    .local_frame
                    .get_or_declare_local(exception_var, self.context.flow_state)
                    .register()
            });

            try_handlers.push(waymark_vm_exception_handler::ExceptionHandler {
                handler_state,
                exception_types,
                exception_dst,
            });
        }
        let try_unwind_depth = self.unwind_depth + 1;
        let mut continuation_flows: Option<NEVec<FlowState>> = None;

        self.context
            .emitter
            .emit_push_exception_handlers(try_handlers);

        {
            let mut try_compiler = self
                .nested_statement_compiler(self.loop_control.clone())
                .with_unwind_depth(try_unwind_depth);
            try_compiler.compile_block(try_block)?;
        }

        if self.context.emitter.is_active() {
            let flow_state = self.context.flow_state.clone();
            self.context.emitter.emit_unwind(self.unwind_depth);
            self.context.emitter.emit_jump(join_state);
            continuation_flows = Some(NEVec::new(flow_state));
        }

        for (handler, handler_state) in handlers.iter().zip(handler_states) {
            self.switch_to_with_flow(handler_state, incoming_flow.clone());

            if let Some(exception_var) = &handler.value.exception_var {
                let local = self
                    .context
                    .local_frame
                    .get_or_declare_local(exception_var, self.context.flow_state);
                self.context.flow_state.mark_initialized(local);
            }

            {
                let mut handler_compiler =
                    self.nested_statement_compiler(self.loop_control.clone());
                handler_compiler.compile_block(&handler.value.body)?;
            }

            if self.context.emitter.is_active() {
                let flow_state = self.context.flow_state.clone();
                self.context.emitter.emit_jump(join_state);

                match &mut continuation_flows {
                    Some(flows) => flows.push(flow_state),
                    None => continuation_flows = Some(NEVec::new(flow_state)),
                }
            }
        }

        if let Some(continuation_flows) = continuation_flows {
            self.switch_to_with_flow(join_state, FlowState::union_branches(continuation_flows));
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
        let body_loop_scope = while_loop.loop_scope(self.unwind_depth);

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

    /// Compiles a spread statement as an internal loop over action calls.
    fn compile_spread_action(
        &mut self,
        collection: &Spanned<Expr>,
        loop_var: &str,
        call: CallPlanFor<'_, Spec>,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        self.for_loop_compiler()
            .compile_spread_statement(collection, loop_var, call)
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

        self.compile_finally_transfer_to(
            loop_scope.unwind_depth(),
            ControlTransfer::Jump(loop_scope.target(kind)),
        );
        Ok(())
    }

    /// Compiles a return, running active finalizers before emitting it.
    fn compile_return(
        &mut self,
        value: Option<&Spanned<Expr>>,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        if self.finally_scopes.is_empty() {
            return self.value_compiler().compile_return_statement(value);
        }

        let return_register = match value {
            Some(value) => {
                let source = self
                    .value_compiler()
                    .compile_expr(value, super::value::ResultTarget::Allocate)?;
                let source_register = source.register();
                self.value_compiler()
                    .unalias_source(source, [source_register])
            }
            None => self.value_compiler().compile_none_literal()?,
        };

        self.compile_finally_transfer_to(0, ControlTransfer::Return(return_register.register()));
        Ok(())
    }

    /// Runs shared finalizer states before completing a non-local transfer.
    fn compile_finally_transfer_to(
        &mut self,
        target_unwind_depth: usize,
        transfer: ControlTransfer,
    ) {
        let finalizers = self
            .finally_scopes
            .iter()
            .rev()
            .copied()
            .take_while(|finally_scope| finally_scope.unwind_depth >= target_unwind_depth)
            .collect::<Vec<_>>();

        if finalizers.is_empty() {
            self.emit_unwind_to(target_unwind_depth);
            self.emit_control_transfer(transfer);
            return;
        }

        self.emit_finalizer_calls(finalizers, transfer, target_unwind_depth);
    }

    /// Calls finalizer states in order before completing `transfer`.
    fn emit_finalizer_calls(
        &mut self,
        finalizers: Vec<FinallyScope>,
        transfer: ControlTransfer,
        transfer_unwind_depth: usize,
    ) {
        let transfer_state = match transfer {
            ControlTransfer::Jump(state) => state,
            ControlTransfer::Raise(_) | ControlTransfer::Return(_) => self.new_state(),
        };
        let flow_state = self.context.flow_state.clone();
        self.context.emitter.emit_call_states(
            finalizers
                .into_iter()
                .map(
                    |finally_scope| waymark_vm_instructions_coreset::StateTarget {
                        state: finally_scope.state,
                        unwind_depth: finally_scope.unwind_depth,
                    },
                )
                .collect(),
            waymark_vm_instructions_coreset::StateTarget {
                state: transfer_state,
                unwind_depth: transfer_unwind_depth,
            },
        );

        if !matches!(transfer, ControlTransfer::Jump(_)) {
            self.switch_to_with_flow(transfer_state, flow_state);
            self.emit_control_transfer(transfer);
        }
    }

    /// Emits a terminal control-flow instruction.
    fn emit_control_transfer(&mut self, transfer: ControlTransfer) {
        match transfer {
            ControlTransfer::Jump(state) => self.context.emitter.emit_jump(state),
            ControlTransfer::Raise(register) => self.context.emitter.emit_raise(register),
            ControlTransfer::Return(register) => self.context.emitter.emit_return(register),
        }
    }

    /// Discards unwind entries deeper than `target_depth`.
    fn emit_unwind_to(&mut self, target_depth: usize) {
        if self.unwind_depth > target_depth {
            self.context.emitter.emit_unwind(target_depth);
            self.unwind_depth = target_depth;
        }
    }

    /// Creates a value compiler borrowing the current context.
    fn value_compiler(&mut self) -> ValueCompiler<'_, 'table, Spec, Lowering> {
        ValueCompiler::new(self.context.reborrow_ref())
    }

    /// Creates a for-loop compiler borrowing the current context mutably.
    fn for_loop_compiler(&mut self) -> ForLoopCompiler<'_, 'table, Spec, Lowering> {
        ForLoopCompiler::new(
            self.context.reborrow_mut(),
            self.loop_control.clone(),
            self.unwind_depth,
        )
    }

    /// Creates an assignment compiler borrowing the current context mutably.
    fn assignment_compiler(&mut self) -> AssignmentCompiler<'_, 'table, Spec, Lowering> {
        AssignmentCompiler::new(self.context.reborrow_mut(), self.unwind_depth)
    }

    /// Creates a parallel compiler borrowing the current context mutably.
    fn parallel_compiler(&mut self) -> ParallelCompiler<'_, 'table, Spec, Lowering> {
        ParallelCompiler::new(self.context.reborrow_mut())
    }

    /// Creates a nested statement compiler with derived loop-control scope.
    fn nested_statement_compiler(
        &mut self,
        loop_control: LoopControlStack,
    ) -> StatementCompiler<'_, 'table, Spec, Lowering> {
        let mut compiler = StatementCompiler::new(self.context.reborrow_mut(), loop_control)
            .with_unwind_depth(self.unwind_depth);
        compiler.finally_scopes = self.finally_scopes.clone();
        compiler
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

    use crate::function::extras::ExtraFunctions;

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
        let mut extra_fns = ExtraFunctions::<TestSpec>::new(1);
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
                    &mut extra_fns,
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
