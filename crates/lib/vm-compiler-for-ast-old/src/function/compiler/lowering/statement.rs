//! Statement lowering.

use waymark_vm_ast_old::{
    ActionCall, Block, ElifBranch, ElseBranch, ExceptHandler, Expr, IfBranch, Literal, Spanned,
    Statement,
};

use nonempty_collections::NEVec;

use super::AssignmentCompiler;
use super::CompilerContextMut;
use super::ForLoopCompiler;
use super::ParallelCompiler;
use super::ValueCompiler;
use super::conditional::{ConditionalJoin, ConditionalJoinFinish};
use super::env::FlowState;
use super::exception::{ExceptionHandlerDispatch, ExceptionScope, ExceptionScopeStack};
use super::r#loop::LoopControlStack;
use super::plan::r#loop::WhileLoopPlan;
use super::plan::statement::StatementPlan;
use super::{Error, ErrorFor, LoopControlKind};

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
            StatementPlan::SpreadAction {
                collection,
                loop_var,
                action,
            } => {
                self.compile_spread_action(collection, loop_var, action)?;
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
                try_block,
                handlers,
            } => {
                self.compile_try_except(try_block, handlers)?;
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

    /// Compiles a `try`/`except` block by routing awaited exception values into
    /// handler entry states.
    fn compile_try_except(
        &mut self,
        try_block: &Spanned<Block>,
        handlers: &[Spanned<ExceptHandler>],
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        if handlers.is_empty() {
            return self.compile_block(try_block);
        }

        let incoming_flow = self.context.flow_state.clone();
        let join_state = self.new_state();
        let prepared = self.prepare_exception_handlers(handlers)?;

        let scoped_exceptions = self.context.exception_scope.push(ExceptionScope::new(
            self.context.exception_scope.clone(),
            prepared.exception_register,
            prepared.dispatch,
        ));
        let current_scope = scoped_exceptions
            .current_scope()
            .expect("newly-pushed exception scope should exist");
        let mut join = ConditionalJoin::new(&incoming_flow, join_state);
        let mut handler_continuations: Vec<FlowState> = Vec::new();

        {
            let mut try_compiler = self.nested_statement_compiler_with_exception_scope(
                self.loop_control.clone(),
                scoped_exceptions.clone(),
            );
            try_compiler.compile_block(try_block)?;
        }
        if self.context.emitter.is_active() {
            join.record_continuation(self.context.flow_state.clone());
            self.context.emitter.emit_jump(join_state);
        }

        for ((handler, entry_state), entry_flow) in handlers
            .iter()
            .zip(prepared.entry_states)
            .zip(current_scope.handler_entry_flows())
        {
            let Some(entry_flow) = entry_flow else {
                continue;
            };

            self.compile_handler_body(
                handler,
                entry_state,
                entry_flow,
                current_scope.exception_register(),
            )?;

            if self.context.emitter.is_active() {
                let continuation_flow = self.context.flow_state.clone();
                handler_continuations.push(continuation_flow.clone());
                join.record_continuation(continuation_flow);
                self.context.emitter.emit_jump(join_state);
            }
        }

        if let ConditionalJoinFinish::Join {
            join_state,
            merged_flow,
        } = join.finish()
        {
            let merged_flow = merge_flow_with_handlers(merged_flow, handler_continuations);
            self.switch_to_with_flow(join_state, merged_flow);
        }

        Ok(())
    }

    /// Allocates the per-scope exception register, reserves an entry state for
    /// every handler, and lowers each handler's candidate type-id registers.
    fn prepare_exception_handlers(
        &mut self,
        handlers: &[Spanned<ExceptHandler>],
    ) -> Result<PreparedExceptionHandlers, ErrorFor<Spec, Lowering>> {
        let exception_register = self.context.local_frame.allocate_register();
        let mut entry_states = Vec::with_capacity(handlers.len());
        let mut dispatch = Vec::with_capacity(handlers.len());

        for handler in handlers {
            let entry_state = self.new_state();
            let catch_all = handler_is_catch_all(&handler.value.exception_types);
            let mut exception_type_registers = Vec::new();

            if !catch_all {
                for exception_type in &handler.value.exception_types {
                    exception_type_registers
                        .push(self.compile_exception_type_register(exception_type)?);
                }
            }

            entry_states.push(entry_state);
            dispatch.push(ExceptionHandlerDispatch::new(
                entry_state,
                exception_type_registers,
                catch_all,
            ));
        }

        Ok(PreparedExceptionHandlers {
            exception_register,
            entry_states,
            dispatch,
        })
    }

    /// Switches to `entry_state` with `entry_flow`, binds the optional
    /// exception variable, and lowers the handler body.
    fn compile_handler_body(
        &mut self,
        handler: &Spanned<ExceptHandler>,
        entry_state: StateId,
        entry_flow: FlowState,
        exception_register: RegisterId,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        self.switch_to_with_flow(entry_state, entry_flow);

        if let Some(exception_var) = handler.value.exception_var.as_deref() {
            let exception_local = self
                .context
                .local_frame
                .get_or_declare_local(exception_var, self.context.flow_state);
            self.context
                .emitter
                .emit_exception_details(exception_local.register(), exception_register);
            self.context.flow_state.mark_initialized(exception_local);
        }

        let mut handler_compiler = self.nested_statement_compiler(self.loop_control.clone());
        handler_compiler.compile_block(&handler.value.body)
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

    /// Compiles a spread statement as an internal loop over action calls.
    fn compile_spread_action(
        &mut self,
        collection: &Spanned<Expr>,
        loop_var: &str,
        action: &ActionCall,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        self.for_loop_compiler()
            .compile_spread_statement(collection, loop_var, action)
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

    /// Creates a nested statement compiler with derived loop-control scope.
    fn nested_statement_compiler(
        &mut self,
        loop_control: LoopControlStack,
    ) -> StatementCompiler<'_, 'table, Spec, Lowering> {
        StatementCompiler::new(self.context.reborrow_mut(), loop_control)
    }

    /// Creates a nested statement compiler with a scoped exception-stack override.
    fn nested_statement_compiler_with_exception_scope(
        &mut self,
        loop_control: LoopControlStack,
        exception_scope: ExceptionScopeStack,
    ) -> StatementCompiler<'_, 'table, Spec, Lowering> {
        StatementCompiler::new(
            self.context
                .reborrow_mut()
                .with_exception_scope(exception_scope),
            loop_control,
        )
    }

    /// Materializes one exception type-id literal into a stable register.
    fn compile_exception_type_register(
        &mut self,
        exception_type: &str,
    ) -> Result<RegisterId, ErrorFor<Spec, Lowering>> {
        let register = self.context.local_frame.allocate_register();
        let value = Lowering::lower_literal(&Literal::String(exception_type.to_owned()))
            .map_err(Error::LiteralLowering)?;
        self.context.emitter.emit_load_const(register, value);
        Ok(register)
    }
}

/// Returns whether this handler should match any exception type.
///
/// The canonical catch-all representation in the old AST is an empty
/// `exception_types` list. We also treat a literal `"Exception"` entry as a
/// catch-all here as a compiler-local compatibility shim for the current
/// Python frontend output and existing snapshots. This is not meant to define
/// a broader runtime exception-matching rule.
fn handler_is_catch_all(exception_types: &[String]) -> bool {
    exception_types.is_empty() || exception_types.iter().any(|value| value == "Exception")
}

/// Lowered dispatch state for a `try`/`except` statement, produced before
/// compiling the try and handler bodies.
struct PreparedExceptionHandlers {
    /// Register that stores the active exception while dispatching handlers.
    exception_register: RegisterId,

    /// Entry state reserved for each handler in source order.
    entry_states: Vec<StateId>,

    /// Per-handler dispatch metadata in source order.
    dispatch: Vec<ExceptionHandlerDispatch>,
}

/// Combines the intersected join flow with handler continuations so locals
/// that were only assigned along a handler path remain visible after the join.
fn merge_flow_with_handlers(
    merged_flow: FlowState,
    handler_continuations: Vec<FlowState>,
) -> FlowState {
    if handler_continuations.is_empty() {
        return merged_flow;
    }
    let mut branches = NEVec::new(merged_flow);
    for flow in handler_continuations {
        branches.push(flow);
    }
    FlowState::union_branches(branches)
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
