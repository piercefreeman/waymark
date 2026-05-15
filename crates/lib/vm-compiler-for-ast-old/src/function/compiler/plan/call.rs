//! Call planning.

use nonempty_collections::{IntoNonEmptyIterator as _, NESlice, NEVec, NonEmptyIterator as _};
use waymark_vm_ast_old::{ActionCall, Call, Expr, FunctionCall, Kwarg, Spanned};
use waymark_vm_bytecode_core::FunctionId;
use waymark_vm_compiler_for_ast_old_core::lowering;

use crate::function::compiler::Error;
use crate::function::table::FunctionTable;

use super::Unsupported;

/// A validated user-function call with a resolved callee id.
#[derive(Debug, Clone, Copy)]
pub struct FunctionCallPlan<'a> {
    /// Callee id resolved from the function table.
    function_id: FunctionId,

    /// Positional argument expressions in source order.
    args: &'a [Spanned<Expr>],
}

/// A validated action call with a lowered action reference.
#[derive(Debug)]
pub struct ActionCallPlan<'a, ActionRef> {
    /// Lowered action reference for the action invocation.
    action_ref: ActionRef,

    /// Keyword arguments forwarded to the action.
    kwargs: &'a [Kwarg],
}

/// A normalized call plan for either a function call or an action call.
#[derive(Debug)]
pub enum CallPlan<'a, ActionRef> {
    /// A call into another user-defined function.
    Function(FunctionCallPlan<'a>),

    /// A call into an external action.
    Action(ActionCallPlan<'a, ActionRef>),
}

/// Action-call plan specialized to a VM spec.
pub type ActionCallPlanFor<'a, Spec> =
    ActionCallPlan<'a, <Spec as waymark_vm_instructions_extcallset::Spec>::ActionRef>;

/// Generic call plan specialized to a VM spec.
pub type CallPlanFor<'a, Spec> =
    CallPlan<'a, <Spec as waymark_vm_instructions_extcallset::Spec>::ActionRef>;

/// Reasons a function-call shape cannot be represented by this compiler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnsupportedFunctionCall {
    /// The call supplied keyword arguments, but compiled function calls are
    /// positional only.
    KeywordArguments,

    /// The call targeted a global built-in rather than a user-defined function.
    GlobalFunction,
}

impl<'a> FunctionCallPlan<'a> {
    /// Validates and resolves a user-function call.
    pub fn build<LiteralLoweringError, ActionLoweringError>(
        call: &'a FunctionCall,
        function_table: &FunctionTable,
    ) -> Result<Self, Error<LiteralLoweringError, ActionLoweringError>> {
        if !call.kwargs.is_empty() {
            return Err(Unsupported::FunctionCall {
                name: call.name.clone(),
                reason: UnsupportedFunctionCall::KeywordArguments,
            }
            .into());
        }

        if call.global_function.is_some() {
            return Err(Unsupported::FunctionCall {
                name: call.name.clone(),
                reason: UnsupportedFunctionCall::GlobalFunction,
            }
            .into());
        }

        let known = function_table
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

        Ok(Self {
            function_id: known.id,
            args: &call.args,
        })
    }

    /// Returns the resolved callee id.
    pub fn function_id(self) -> FunctionId {
        self.function_id
    }

    /// Returns the positional arguments for the call.
    pub fn args(self) -> &'a [Spanned<Expr>] {
        self.args
    }
}

impl<'a, ActionRef> ActionCallPlan<'a, ActionRef> {
    /// Lowers an action call into the target spec's action.
    pub fn lower<Spec, Lowering, LiteralLoweringError>(
        call: &'a ActionCall,
    ) -> Result<Self, Error<LiteralLoweringError, Lowering::ActionError>>
    where
        Spec: waymark_vm_instructions_extcallset::Spec<ActionRef = ActionRef>,
        Lowering: lowering::ExtCallSet<Spec>,
    {
        let action_ref = Lowering::lower_action(call).map_err(|error| Error::ActionLowering {
            action_name: call.action_name.clone(),
            error,
        })?;

        Ok(Self {
            action_ref,
            kwargs: &call.kwargs,
        })
    }

    /// Returns the lowered action reference and original keyword arguments.
    pub fn into_parts(self) -> (ActionRef, &'a [Kwarg]) {
        (self.action_ref, self.kwargs)
    }
}

impl<'a, ActionRef> CallPlan<'a, ActionRef> {
    /// Builds a plan for a user-function call.
    pub fn build_function<LiteralLoweringError, ActionLoweringError>(
        call: &'a FunctionCall,
        function_table: &FunctionTable,
    ) -> Result<Self, Error<LiteralLoweringError, ActionLoweringError>> {
        Ok(Self::Function(FunctionCallPlan::build(
            call,
            function_table,
        )?))
    }

    /// Builds a plan for an action call.
    pub fn build_action<Spec, Lowering, LiteralLoweringError>(
        call: &'a ActionCall,
    ) -> Result<Self, Error<LiteralLoweringError, Lowering::ActionError>>
    where
        Spec: waymark_vm_instructions_extcallset::Spec<ActionRef = ActionRef>,
        Lowering: lowering::ExtCallSet<Spec>,
    {
        Ok(Self::Action(ActionCallPlan::lower::<Spec, Lowering, _>(
            call,
        )?))
    }

    /// Builds a call plan for either AST call variant.
    pub fn build<Spec, Lowering, LiteralLoweringError>(
        call: &'a Call,
        function_table: &FunctionTable,
    ) -> Result<Self, Error<LiteralLoweringError, Lowering::ActionError>>
    where
        Spec: waymark_vm_instructions_extcallset::Spec<ActionRef = ActionRef>,
        Lowering: lowering::ExtCallSet<Spec>,
    {
        match call {
            Call::Function(call) => Self::build_function(call, function_table),
            Call::Action(call) => Self::build_action::<Spec, Lowering, _>(call),
        }
    }

    /// Builds call plans for a non-empty slice of calls.
    pub fn build_all<Spec, Lowering, LiteralLoweringError>(
        calls: NESlice<'a, Call>,
        function_table: &FunctionTable,
    ) -> Result<NEVec<Self>, Error<LiteralLoweringError, Lowering::ActionError>>
    where
        Spec: waymark_vm_instructions_extcallset::Spec<ActionRef = ActionRef>,
        Lowering: lowering::ExtCallSet<Spec>,
    {
        calls
            .into_nonempty_iter()
            .map(|call| Self::build::<Spec, Lowering, _>(call, function_table))
            .collect()
    }
}

/// Compiles expression operands from `items` into registers.
pub fn compile_expr_registers<T, R, E, ExtractFn, CompileFn>(
    items: &[T],
    mut extract_expr: ExtractFn,
    mut compile_expr: CompileFn,
) -> Result<Vec<R>, E>
where
    ExtractFn: FnMut(&T) -> &Spanned<Expr>,
    CompileFn: FnMut(&Spanned<Expr>) -> Result<R, E>,
{
    let mut registers = Vec::with_capacity(items.len());
    for item in items {
        registers.push(compile_expr(extract_expr(item))?);
    }
    Ok(registers)
}

impl core::fmt::Display for UnsupportedFunctionCall {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str(match self {
            Self::KeywordArguments => "keyword arguments are not supported",
            Self::GlobalFunction => "global functions are not supported",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use waymark_vm_ast_old::{ActionCall, GlobalFunction, Kwarg, Literal};
    use waymark_vm_ast_old_helpers::{action_call, function_call, int};
    use waymark_vm_bytecode_core::FunctionId;
    use waymark_vm_compiler_for_ast_old_core::lowering;
    use waymark_vm_compiler_for_ast_old_test_support::{TestActionRef, TestLowering, TestSpec};
    use waymark_vm_runtime_core::RegisterId;

    use crate::function::compiler::{Error, test_helpers::build_function_table};

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum TestActionLoweringError {
        Unsupported,
    }

    struct FailingLowering;

    impl lowering::ExtCallSet<TestSpec> for FailingLowering {
        type ActionError = TestActionLoweringError;

        fn lower_action(_call: &ActionCall) -> Result<TestActionRef, Self::ActionError> {
            Err(TestActionLoweringError::Unsupported)
        }
    }

    #[test]
    fn builds_known_function_call_plan() {
        let function_table = build_function_table();
        let call = function_call("child", vec![int(1)]);

        let target = FunctionCallPlan::build::<(), ()>(&call, &function_table)
            .expect("known call should resolve");

        assert_eq!(target.function_id(), FunctionId(0));
        assert_eq!(target.args().len(), 1);
    }

    #[test]
    fn rejects_unknown_function_calls() {
        let function_table = build_function_table();
        let call = function_call("missing", vec![int(1)]);

        let error = FunctionCallPlan::build::<(), ()>(&call, &function_table)
            .expect_err("unknown calls should be rejected");

        assert!(matches!(error, Error::UnknownFunction { name } if name == "missing"));
    }

    #[test]
    fn rejects_function_calls_with_kwargs() {
        let function_table = build_function_table();
        let mut call = function_call("child", vec![int(1)]);
        call.kwargs.push(Kwarg {
            name: "value".to_owned(),
            value: int(2),
        });

        let error = FunctionCallPlan::build::<(), ()>(&call, &function_table)
            .expect_err("kwargs should be rejected");

        assert!(matches!(
            error,
            Error::Unsupported(Unsupported::FunctionCall { name, reason })
                if name == "child" && reason == UnsupportedFunctionCall::KeywordArguments
        ));
    }

    #[test]
    fn rejects_global_function_calls() {
        let function_table = build_function_table();
        let mut call = function_call("child", vec![int(1)]);
        call.global_function = Some(GlobalFunction::Len);

        let error = FunctionCallPlan::build::<(), ()>(&call, &function_table)
            .expect_err("global functions should be rejected");

        assert!(matches!(
            error,
            Error::Unsupported(Unsupported::FunctionCall { name, reason })
                if name == "child" && reason == UnsupportedFunctionCall::GlobalFunction
        ));
    }

    #[test]
    fn rejects_function_calls_with_wrong_arity() {
        let function_table = build_function_table();
        let call = function_call("child", Vec::new());

        let error = FunctionCallPlan::build::<(), ()>(&call, &function_table)
            .expect_err("arity mismatch should be rejected");

        assert!(matches!(
            error,
            Error::FunctionArityMismatch {
                function,
                expected,
                actual,
            } if function == "child" && expected == 1 && actual == 0
        ));
    }

    #[test]
    fn positional_arg_compilation_preserves_source_order() {
        let args = vec![int(7), int(3), int(9)];

        let registers = compile_expr_registers(
            &args,
            |arg| arg,
            |arg| match &arg.value {
                waymark_vm_ast_old::Expr::Literal {
                    value: Literal::Int(value),
                } => Ok::<RegisterId, ()>(RegisterId(*value as usize)),
                other => panic!("unexpected arg {other:?}"),
            },
        )
        .expect("positional args should compile");

        assert_eq!(registers, vec![RegisterId(7), RegisterId(3), RegisterId(9)]);
    }

    #[test]
    fn kwarg_value_compilation_preserves_source_order() {
        let kwargs = vec![
            Kwarg {
                name: "first".to_owned(),
                value: int(4),
            },
            Kwarg {
                name: "second".to_owned(),
                value: int(2),
            },
        ];

        let registers = compile_expr_registers(
            &kwargs,
            |kwarg| &kwarg.value,
            |arg| match &arg.value {
                waymark_vm_ast_old::Expr::Literal {
                    value: Literal::Int(value),
                } => Ok::<RegisterId, ()>(RegisterId(*value as usize)),
                other => panic!("unexpected kwarg value {other:?}"),
            },
        )
        .expect("kwarg values should compile");

        assert_eq!(registers, vec![RegisterId(4), RegisterId(2)]);
    }

    #[test]
    fn lowers_action_call_plan_ids() {
        let call = action_call("notify", Vec::new());
        let planned_call = ActionCallPlan::lower::<TestSpec, TestLowering, ()>(&call)
            .expect("supported action should lower");
        let (action_ref, kwargs) = planned_call.into_parts();

        assert!(matches!(action_ref, TestActionRef(name) if name == "notify"));
        assert!(kwargs.is_empty());
    }

    #[test]
    fn preserves_action_lowering_errors() {
        let error = ActionCallPlan::<TestActionRef>::lower::<TestSpec, FailingLowering, ()>(
            &action_call("notify", Vec::new()),
        )
        .expect_err("unsupported action should fail");

        assert!(matches!(
            error,
            Error::ActionLowering {
                action_name,
                error: TestActionLoweringError::Unsupported,
            } if action_name == "notify"
        ));
    }

    #[test]
    fn builds_raw_call_plan_variants() {
        let function_table = build_function_table();
        let function_call =
            waymark_vm_ast_old::Call::Function(function_call("child", vec![int(1)]));
        let action_call = waymark_vm_ast_old::Call::Action(action_call("notify", Vec::new()));

        let function_plan =
            CallPlan::build::<TestSpec, TestLowering, ()>(&function_call, &function_table)
                .expect("function call should plan");
        let action_plan =
            CallPlan::build::<TestSpec, TestLowering, ()>(&action_call, &function_table)
                .expect("action call should plan");

        assert!(matches!(
            function_plan,
            CallPlan::Function(plan) if plan.function_id() == FunctionId(0)
        ));
        assert!(matches!(action_plan, CallPlan::Action(_)));
    }

    #[test]
    fn builds_nonempty_call_plan_collection() {
        let function_table = build_function_table();
        let calls = nonempty_collections::nev![
            waymark_vm_ast_old::Call::Function(function_call("child", vec![int(1)])),
            waymark_vm_ast_old::Call::Action(action_call("notify", Vec::new())),
        ];

        let planned_calls = CallPlan::build_all::<TestSpec, TestLowering, ()>(
            calls.as_nonempty_slice(),
            &function_table,
        )
        .expect("non-empty calls should plan");

        let (first_call, remaining_calls) = planned_calls.into_nonempty_iter().next();
        let remaining_calls = remaining_calls.collect::<Vec<_>>();

        assert!(matches!(
            first_call,
            CallPlan::Function(plan) if plan.function_id() == FunctionId(0)
        ));
        assert_eq!(remaining_calls.len(), 1);
        assert!(matches!(remaining_calls[0], CallPlan::Action(_)));
    }
}
