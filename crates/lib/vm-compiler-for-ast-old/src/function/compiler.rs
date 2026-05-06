//! Function-body lowering used by the compiler.

mod error;

use crate::InstructionFor;

pub use self::error::*;

use std::collections::HashMap;

use waymark_vm_ast_old::{
    ActionCall, BinaryOperator, Block, Expr, FunctionCall, FunctionDef, Literal, Spanned, Statement,
};
use waymark_vm_bytecode_core::{FunctionId, StateId};
use waymark_vm_runtime_core::RegisterId;

use super::{states::FunctionStates, table::FunctionTable};

type ErrorFor<Spec, Lowering> = Error<
    <Lowering as crate::lowering::PureSet<Spec>>::LiteralError,
    <Lowering as crate::lowering::CoreSet<Spec>>::ActionError,
>;

pub(crate) struct FunctionCompiler<'a, Spec, Lowering>
where
    Spec: waymark_vm_compiler_core::SpecRequirements,
{
    phantom_data: core::marker::PhantomData<Lowering>,
    function_table: &'a FunctionTable,
    function_states: FunctionStates<InstructionFor<Spec>>,
    variables: HashMap<String, RegisterId>,
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
        }

        Ok(Self {
            phantom_data: core::marker::PhantomData,
            function_table,
            function_states: FunctionStates::new(),
            variables,
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
                if targets.len() != 1 {
                    return Err(Unsupported::AssignmentTargetCount {
                        count: targets.len(),
                    }
                    .into());
                }

                let target = &targets[0];
                let target_register = self.ensure_variable(target);
                let value_register = self.compile_expr(value, Some(target_register))?;

                if value_register != target_register {
                    return Err(Unsupported::AssignmentNeedsCopy {
                        target: target.clone(),
                    }
                    .into());
                }
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
            Statement::ParallelBlock { .. } => {
                return Err(Unsupported::Statement {
                    kind: "ParallelBlock",
                }
                .into());
            }
            Statement::ForLoop { .. } => {
                return Err(Unsupported::Statement { kind: "ForLoop" }.into());
            }
            Statement::WhileLoop { .. } => {
                return Err(Unsupported::Statement { kind: "WhileLoop" }.into());
            }
            Statement::Conditional { .. } => {
                return Err(Unsupported::Statement {
                    kind: "Conditional",
                }
                .into());
            }
            Statement::TryExcept { .. } => {
                return Err(Unsupported::Statement { kind: "TryExcept" }.into());
            }
            Statement::Break => {
                return Err(Unsupported::Statement { kind: "Break" }.into());
            }
            Statement::Continue => {
                return Err(Unsupported::Statement { kind: "Continue" }.into());
            }
            Statement::Sleep { .. } => {
                return Err(Unsupported::Statement { kind: "Sleep" }.into());
            }
        }

        Ok(())
    }

    fn compile_expr(
        &mut self,
        expr: &Spanned<Expr>,
        preferred_dst: Option<RegisterId>,
    ) -> Result<RegisterId, ErrorFor<Spec, Lowering>> {
        match &expr.value {
            Expr::Literal { value } => self.compile_literal(value, preferred_dst),
            Expr::Variable { name } => self
                .variables
                .get(name)
                .copied()
                .ok_or_else(|| Error::UnknownVariable { name: name.clone() }),
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

        let dst = preferred_dst.unwrap_or_else(|| self.allocate_register());
        let resume = self.new_state();
        self.emit_call(dst, known.id, args);
        self.emit_await(dst, dst, resume);
        self.switch_to_state(resume);
        Ok(dst)
    }

    fn compile_action_call(
        &mut self,
        call: &ActionCall,
        preferred_dst: Option<RegisterId>,
    ) -> Result<RegisterId, ErrorFor<Spec, Lowering>> {
        let mut args = Vec::with_capacity(call.kwargs.len());
        for kwarg in &call.kwargs {
            args.push(self.compile_expr(&kwarg.value, None)?);
        }

        let extcall_id = Lowering::lower_action(call).map_err(|error| Error::ActionLowering {
            action_name: call.action_name.clone(),
            error,
        })?;

        let dst = preferred_dst.unwrap_or_else(|| self.allocate_register());
        let await_state = self.new_state();
        let resume_state = self.new_state();
        self.emit_extcall(dst, extcall_id, args, await_state);
        self.switch_to_state(await_state);
        self.emit_await(dst, dst, resume_state);
        self.switch_to_state(resume_state);
        Ok(dst)
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

    fn ensure_variable(&mut self, name: &str) -> RegisterId {
        if let Some(register) = self.variables.get(name) {
            return *register;
        }

        let register = self.allocate_register();
        self.variables.insert(name.to_owned(), register);
        register
    }
}
