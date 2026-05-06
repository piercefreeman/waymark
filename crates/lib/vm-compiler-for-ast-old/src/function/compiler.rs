//! Function-body lowering used by the compiler.

mod error;

use crate::InstructionFor;

pub use self::error::*;

use std::collections::{HashMap, HashSet};

use waymark_vm_ast_old::{
    ActionCall, BinaryOperator, Block, Call, ElifBranch, ElseBranch, Expr, FunctionCall,
    FunctionDef, IfBranch, Literal, Spanned, Statement,
};
use waymark_vm_bytecode_core::{FunctionId, StateId};
use waymark_vm_runtime_core::RegisterId;

use super::{states::FunctionStates, table::FunctionTable};

type ErrorFor<Spec, Lowering> = Error<
    <Lowering as crate::lowering::PureSet<Spec>>::LiteralError,
    <Lowering as crate::lowering::CoreSet<Spec>>::ActionError,
>;

#[derive(Clone, Copy)]
struct LoopContext {
    break_state: StateId,
    continue_state: StateId,
}

pub(crate) struct FunctionCompiler<'a, Spec, Lowering>
where
    Spec: waymark_vm_compiler_core::SpecRequirements,
{
    phantom_data: core::marker::PhantomData<Lowering>,
    function_table: &'a FunctionTable,
    function_states: FunctionStates<InstructionFor<Spec>>,
    variables: HashMap<String, RegisterId>,
    initialized_variables: HashSet<String>,
    loop_stack: Vec<LoopContext>,
    next_register_index: usize,
}

impl<'a, Spec, Lowering> FunctionCompiler<'a, Spec, Lowering>
where
    Spec: waymark_vm_compiler_core::SpecRequirements,
    Lowering: crate::lowering::FullSet<Spec>,
{
    pub fn new(
        function_table: &'a FunctionTable,
        function: &'a Spanned<FunctionDef>,
    ) -> Result<Self, ErrorFor<Spec, Lowering>> {
        let mut variables = HashMap::new();
        let mut initialized_variables = HashSet::new();
        let mut next_register_index = 0;

        for input in &function.value.io.value.inputs {
            let register = RegisterId(next_register_index);
            next_register_index += 1;

            if variables.insert(input.clone(), register).is_some() {
                return Err(Error::DuplicateInput {
                    function: function.value.name.clone(),
                    name: input.clone(),
                });
            }

            initialized_variables.insert(input.clone());
        }

        Ok(Self {
            phantom_data: core::marker::PhantomData,
            function_table,
            function_states: FunctionStates::new(),
            variables,
            initialized_variables,
            loop_stack: Vec::new(),
            next_register_index,
        })
    }

    pub fn compile(
        mut self,
        function: &Spanned<FunctionDef>,
    ) -> Result<waymark_vm_bytecode::Function<InstructionFor<Spec>>, ErrorFor<Spec, Lowering>> {
        self.compile_block(&function.value.body)?;

        if self.function_states.is_active() {
            self.emit_return_none()?;
        }

        Ok(waymark_vm_bytecode::Function {
            states: self.function_states.finish(),
            num_regs: self.next_register_index,
        })
    }

    fn compile_block(&mut self, block: &Spanned<Block>) -> Result<(), ErrorFor<Spec, Lowering>> {
        for statement in &block.value.statements {
            if !self.function_states.is_active() {
                break;
            }

            self.compile_statement(statement)?;
        }

        Ok(())
    }

    fn compile_statement(
        &mut self,
        statement: &Spanned<Statement>,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        match &statement.value {
            Statement::Assignment { targets, value } => {
                if let Expr::ParallelExpr { calls } = &value.value {
                    self.compile_parallel_assignment(targets, calls)?;
                    return Ok(());
                }

                if targets.len() != 1 {
                    return Err(Unsupported::AssignmentTargetCount {
                        count: targets.len(),
                    }
                    .into());
                }

                let target = &targets[0];
                let target_register = self.ensure_variable_register(target);
                let value_register = self.compile_expr(value, Some(target_register))?;

                if value_register != target_register {
                    return Err(Unsupported::AssignmentNeedsCopy {
                        target: target.clone(),
                    }
                    .into());
                }

                self.initialized_variables.insert(target.clone());
            }
            Statement::ActionCall { call } => {
                let _ = self.compile_action_call(call, None)?;
            }
            Statement::Return { value } => {
                let register = match value {
                    Some(value) => self.compile_expr(value, None)?,
                    None => self.compile_none_literal(None)?,
                };
                self.emit_return(register);
            }
            Statement::ExprStmt { expr } => {
                let _ = self.compile_expr(expr, None)?;
            }
            Statement::SpreadAction { .. } => {
                return Err(Unsupported::Statement {
                    kind: "SpreadAction",
                }
                .into());
            }
            Statement::ParallelBlock { calls } => {
                self.compile_parallel_block(calls)?;
            }
            Statement::ForLoop { .. } => {
                return Err(Unsupported::Statement { kind: "ForLoop" }.into());
            }
            Statement::WhileLoop { condition, body } => {
                self.compile_while_loop(condition, body)?;
            }
            Statement::Conditional {
                if_branch,
                elif_branches,
                else_branch,
            } => {
                self.compile_conditional(if_branch, elif_branches, else_branch.as_ref())?;
            }
            Statement::TryExcept { .. } => {
                return Err(Unsupported::Statement { kind: "TryExcept" }.into());
            }
            Statement::Break => {
                self.compile_break()?;
            }
            Statement::Continue => {
                self.compile_continue()?;
            }
            Statement::Sleep { .. } => {
                return Err(Unsupported::Statement { kind: "Sleep" }.into());
            }
        }

        Ok(())
    }

    fn compile_conditional(
        &mut self,
        if_branch: &Spanned<IfBranch>,
        elif_branches: &[Spanned<ElifBranch>],
        else_branch: Option<&Spanned<ElseBranch>>,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let incoming_initialized = self.initialized_variables.clone();
        let join_state = self.new_state();
        let if_body_state = self.new_state();
        let elif_body_states = elif_branches
            .iter()
            .map(|_| self.new_state())
            .collect::<Vec<_>>();

        let condition_register = self.compile_expr(&if_branch.value.condition, None)?;
        self.emit_jump_if(if_body_state, condition_register);

        for (branch, body_state) in elif_branches.iter().zip(elif_body_states.iter().copied()) {
            let condition_register = self.compile_expr(&branch.value.condition, None)?;
            self.emit_jump_if(body_state, condition_register);
        }

        self.initialized_variables = incoming_initialized.clone();
        let mut continuation_envs = Vec::new();

        match else_branch {
            Some(else_branch) => {
                self.compile_block(&else_branch.value.body)?;
                if self.function_states.is_active() {
                    continuation_envs.push(self.initialized_variables.clone());
                    self.emit_jump(join_state);
                }
            }
            None => {
                continuation_envs.push(incoming_initialized.clone());
                self.emit_jump(join_state);
            }
        }

        if let Some(continuation) = self.compile_branch_body(
            if_body_state,
            &if_branch.value.body,
            &incoming_initialized,
            join_state,
        )? {
            continuation_envs.push(continuation);
        }

        for (branch, body_state) in elif_branches.iter().zip(elif_body_states) {
            if let Some(continuation) = self.compile_branch_body(
                body_state,
                &branch.value.body,
                &incoming_initialized,
                join_state,
            )? {
                continuation_envs.push(continuation);
            }
        }

        if let Some(merged_initialized) = Self::merge_initialized_variables(continuation_envs) {
            self.switch_to_state(join_state);
            self.initialized_variables = merged_initialized;
        }

        Ok(())
    }

    fn compile_branch_body(
        &mut self,
        state_id: StateId,
        body: &Spanned<Block>,
        incoming_initialized: &HashSet<String>,
        join_state: StateId,
    ) -> Result<Option<HashSet<String>>, ErrorFor<Spec, Lowering>> {
        self.switch_to_state(state_id);
        self.initialized_variables = incoming_initialized.clone();
        self.compile_block(body)?;

        if !self.function_states.is_active() {
            return Ok(None);
        }

        let continuation = self.initialized_variables.clone();
        self.emit_jump(join_state);
        Ok(Some(continuation))
    }

    fn compile_while_loop(
        &mut self,
        condition: &Spanned<Expr>,
        body: &Spanned<Block>,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let incoming_initialized = self.initialized_variables.clone();
        let condition_state = self.new_state();
        let body_state = self.new_state();
        let exit_state = self.new_state();

        self.emit_jump(condition_state);

        self.switch_to_state(condition_state);
        self.initialized_variables = incoming_initialized.clone();
        let condition_register = self.compile_expr(condition, None)?;
        self.emit_jump_if(body_state, condition_register);
        self.emit_jump(exit_state);

        self.loop_stack.push(LoopContext {
            break_state: exit_state,
            continue_state: condition_state,
        });

        self.switch_to_state(body_state);
        self.initialized_variables = incoming_initialized.clone();
        self.compile_block(body)?;

        let loop_context = self
            .loop_stack
            .pop()
            .expect("loop context should exist while compiling a loop body");
        debug_assert_eq!(loop_context.break_state, exit_state);
        debug_assert_eq!(loop_context.continue_state, condition_state);

        if self.function_states.is_active() {
            self.emit_jump(condition_state);
        }

        self.switch_to_state(exit_state);
        self.initialized_variables = incoming_initialized;

        Ok(())
    }

    fn compile_parallel_block(&mut self, calls: &[Call]) -> Result<(), ErrorFor<Spec, Lowering>> {
        let promise_registers = self.compile_parallel_calls_start(calls)?;

        for promise_register in promise_registers {
            self.compile_await_register(promise_register);
        }

        Ok(())
    }

    fn compile_parallel_assignment(
        &mut self,
        targets: &[String],
        calls: &[Call],
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        if targets.len() == 1 {
            return Err(Unsupported::ParallelExprAssignment {
                target_count: targets.len(),
                call_count: calls.len(),
                reason: "single-target parallel expressions require list aggregation, which is not implemented",
            }
            .into());
        }

        if targets.len() != calls.len() {
            return Err(Unsupported::ParallelExprAssignment {
                target_count: targets.len(),
                call_count: calls.len(),
                reason: "parallel expressions currently require one assignment target per call",
            }
            .into());
        }

        let target_registers = targets
            .iter()
            .map(|target| self.ensure_variable_register(target))
            .collect::<Vec<_>>();
        let promise_registers = self.compile_parallel_calls_start(calls)?;

        for (target_register, promise_register) in
            target_registers.into_iter().zip(promise_registers)
        {
            self.compile_await_into_register(target_register, promise_register);
        }

        self.initialized_variables.extend(targets.iter().cloned());

        Ok(())
    }

    fn compile_parallel_calls_start(
        &mut self,
        calls: &[Call],
    ) -> Result<Vec<RegisterId>, ErrorFor<Spec, Lowering>> {
        let mut promise_registers = Vec::with_capacity(calls.len());

        for call in calls {
            let promise_register = self.allocate_register();
            promise_registers.push(promise_register);

            match call {
                Call::Action(call) => {
                    let resume_state = self.new_state();
                    self.compile_action_call_start(call, promise_register, resume_state)?;
                    self.switch_to_state(resume_state);
                }
                Call::Function(call) => {
                    self.compile_function_call_start(call, promise_register)?;
                }
            }
        }

        Ok(promise_registers)
    }

    fn compile_break(&mut self) -> Result<(), ErrorFor<Spec, Lowering>> {
        let Some(loop_context) = self.loop_stack.last().copied() else {
            return Err(Error::LoopControlOutsideLoop { kind: "break" });
        };

        self.emit_jump(loop_context.break_state);
        Ok(())
    }

    fn compile_continue(&mut self) -> Result<(), ErrorFor<Spec, Lowering>> {
        let Some(loop_context) = self.loop_stack.last().copied() else {
            return Err(Error::LoopControlOutsideLoop { kind: "continue" });
        };

        self.emit_jump(loop_context.continue_state);
        Ok(())
    }

    fn merge_initialized_variables(
        mut continuation_envs: Vec<HashSet<String>>,
    ) -> Option<HashSet<String>> {
        let mut merged = continuation_envs.pop()?;

        for env in continuation_envs {
            merged.retain(|name| env.contains(name));
        }

        Some(merged)
    }

    fn compile_expr(
        &mut self,
        expr: &Spanned<Expr>,
        preferred_dst: Option<RegisterId>,
    ) -> Result<RegisterId, ErrorFor<Spec, Lowering>> {
        match &expr.value {
            Expr::Literal { value } => self.compile_literal(value, preferred_dst),
            Expr::Variable { name } => {
                if !self.initialized_variables.contains(name) {
                    return Err(Error::UnknownVariable { name: name.clone() });
                }

                self.variables
                    .get(name)
                    .copied()
                    .ok_or_else(|| Error::UnknownVariable { name: name.clone() })
            }
            Expr::BinaryOp { left, op, right } => {
                if !matches!(op, BinaryOperator::Add) {
                    return Err(Unsupported::BinaryOperator { op: op.clone() }.into());
                }

                let left_register = self.compile_expr(left, None)?;
                let right_register = self.compile_expr(right, None)?;
                let dst = preferred_dst.unwrap_or_else(|| self.allocate_register());
                self.emit_add(dst, left_register, right_register);
                Ok(dst)
            }
            Expr::FunctionCall { call } => self.compile_function_call(call, preferred_dst),
            Expr::ActionCall { call } => self.compile_action_call(call, preferred_dst),
            Expr::UnaryOp { .. } => Err(Unsupported::Expression { kind: "UnaryOp" }.into()),
            Expr::List { .. } => Err(Unsupported::Expression { kind: "List" }.into()),
            Expr::Dict { .. } => Err(Unsupported::Expression { kind: "Dict" }.into()),
            Expr::Index { .. } => Err(Unsupported::Expression { kind: "Index" }.into()),
            Expr::Dot { .. } => Err(Unsupported::Expression { kind: "Dot" }.into()),
            Expr::ParallelExpr { .. } => Err(Unsupported::Expression {
                kind: "ParallelExpr",
            }
            .into()),
            Expr::SpreadExpr { .. } => Err(Unsupported::Expression { kind: "SpreadExpr" }.into()),
        }
    }

    fn compile_literal(
        &mut self,
        literal: &Literal,
        preferred_dst: Option<RegisterId>,
    ) -> Result<RegisterId, ErrorFor<Spec, Lowering>> {
        let dst = preferred_dst.unwrap_or_else(|| self.allocate_register());
        let value =
            Lowering::lower_literal(literal).map_err(|error| Error::LiteralLowering { error })?;
        self.emit_load_const(dst, value);
        Ok(dst)
    }

    fn compile_none_literal(
        &mut self,
        preferred_dst: Option<RegisterId>,
    ) -> Result<RegisterId, ErrorFor<Spec, Lowering>> {
        self.compile_literal(&Literal::None, preferred_dst)
    }

    fn compile_function_call(
        &mut self,
        call: &FunctionCall,
        preferred_dst: Option<RegisterId>,
    ) -> Result<RegisterId, ErrorFor<Spec, Lowering>> {
        let dst = preferred_dst.unwrap_or_else(|| self.allocate_register());
        self.compile_function_call_start(call, dst)?;
        self.compile_await_register(dst);
        Ok(dst)
    }

    fn compile_function_call_start(
        &mut self,
        call: &FunctionCall,
        dst: RegisterId,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        if !call.kwargs.is_empty() {
            return Err(Unsupported::FunctionCall {
                name: call.name.clone(),
                reason: "keyword arguments are not supported",
            }
            .into());
        }

        if call.global_function.is_some() {
            return Err(Unsupported::FunctionCall {
                name: call.name.clone(),
                reason: "global functions are not supported",
            }
            .into());
        }

        let known = self
            .function_table
            .get(&call.name)
            .ok_or_else(|| Error::UnknownFunction {
                name: call.name.clone(),
            })?;

        if call.args.len() != known.arity {
            return Err(Error::FunctionArityMismatch {
                function: call.name.clone(),
                expected: known.arity,
                actual: call.args.len(),
            });
        }

        let mut args = Vec::with_capacity(call.args.len());
        for arg in &call.args {
            args.push(self.compile_expr(arg, None)?);
        }

        self.emit_call(dst, known.id, args);
        Ok(())
    }

    fn compile_action_call(
        &mut self,
        call: &ActionCall,
        preferred_dst: Option<RegisterId>,
    ) -> Result<RegisterId, ErrorFor<Spec, Lowering>> {
        let dst = preferred_dst.unwrap_or_else(|| self.allocate_register());
        let await_state = self.new_state();
        self.compile_action_call_start(call, dst, await_state)?;
        self.switch_to_state(await_state);
        self.compile_await_register(dst);
        Ok(dst)
    }

    fn compile_action_call_start(
        &mut self,
        call: &ActionCall,
        dst: RegisterId,
        resume_state: StateId,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let mut args = Vec::with_capacity(call.kwargs.len());
        for kwarg in &call.kwargs {
            args.push(self.compile_expr(&kwarg.value, None)?);
        }

        let extcall_id = Lowering::lower_action(call).map_err(|error| Error::ActionLowering {
            action_name: call.action_name.clone(),
            error,
        })?;

        self.emit_extcall(dst, extcall_id, args, resume_state);
        Ok(())
    }

    fn compile_await_register(&mut self, promise_register: RegisterId) {
        self.compile_await_into_register(promise_register, promise_register);
    }

    fn compile_await_into_register(
        &mut self,
        target_register: RegisterId,
        promise_register: RegisterId,
    ) {
        let resume_state = self.new_state();
        self.emit_await(target_register, promise_register, resume_state);
        self.switch_to_state(resume_state);
    }

    fn emit_load_const(
        &mut self,
        dst: RegisterId,
        value: <Spec as waymark_vm_instructions_pureset::Spec>::ConstValue,
    ) {
        self.emit(waymark_vm_instructions_pureset::PureSet::LoadConst { dst, value }.into());
    }

    fn emit_add(&mut self, dst: RegisterId, a: RegisterId, b: RegisterId) {
        self.emit(waymark_vm_instructions_pureset::PureSet::Add { dst, a, b }.into());
    }

    fn emit_call(&mut self, dst: RegisterId, function_id: FunctionId, args: Vec<RegisterId>) {
        self.emit(
            waymark_vm_instructions_coreset::CoreSet::Call {
                dst,
                function_id,
                args,
            }
            .into(),
        );
    }

    fn emit_extcall(
        &mut self,
        dst: RegisterId,
        extcall_id: <Spec as waymark_vm_instructions_coreset::Spec>::ExtCallId,
        args: Vec<RegisterId>,
        resume: StateId,
    ) {
        self.emit(
            waymark_vm_instructions_coreset::CoreSet::ExtCall {
                dst,
                extcall_id,
                args,
                resume,
            }
            .into(),
        );
    }

    fn emit_await(&mut self, dst: RegisterId, src: RegisterId, resume: StateId) {
        self.emit(waymark_vm_instructions_coreset::CoreSet::Await { dst, src, resume }.into());
    }

    fn emit_jump_if(&mut self, target_state: StateId, cond: RegisterId) {
        self.emit(waymark_vm_instructions_coreset::CoreSet::JumpIf { target_state, cond }.into());
    }

    fn emit_jump(&mut self, target_state: StateId) {
        self.emit(waymark_vm_instructions_coreset::CoreSet::Jump { target_state }.into());
        self.function_states.terminate();
    }

    fn emit_return(&mut self, src: RegisterId) {
        self.emit(waymark_vm_instructions_coreset::CoreSet::Return { src }.into());
        self.function_states.terminate();
    }

    fn emit_return_none(&mut self) -> Result<(), ErrorFor<Spec, Lowering>> {
        let register = self.compile_none_literal(None)?;
        self.emit_return(register);
        Ok(())
    }

    fn emit(&mut self, instruction: InstructionFor<Spec>) {
        self.function_states.emit(instruction);
    }

    fn new_state(&mut self) -> StateId {
        self.function_states.reserve_state()
    }

    fn switch_to_state(&mut self, state_id: StateId) {
        self.function_states.switch_to(state_id);
    }

    fn allocate_register(&mut self) -> RegisterId {
        let register = RegisterId(self.next_register_index);
        self.next_register_index += 1;
        register
    }

    fn ensure_variable_register(&mut self, name: &str) -> RegisterId {
        if let Some(register) = self.variables.get(name) {
            return *register;
        }

        let register = self.allocate_register();
        self.variables.insert(name.to_owned(), register);
        register
    }
}
