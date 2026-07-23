//! Call planning.

use nonempty_collections::{IntoNonEmptyIterator as _, NESlice, NEVec, NonEmptyIterator as _};
use waymark_vm_ast_old::{ActionCall, Call, Expr, FunctionCall, Kwarg, PolicyBracket, Spanned};
use waymark_vm_bytecode_core::FunctionId;
use waymark_vm_compiler_for_ast_old_core::lowering;

use crate::function::compiler::Error;
use crate::function::table::FunctionTable;

use super::Unsupported;

/// A validated user-function call with a resolved callee id.
#[derive(Debug, Clone)]
pub struct FunctionCallPlan<'a> {
    /// Callee id resolved from the function table.
    function_id: FunctionId,

    /// Argument expressions in source order: positional first, then the
    /// keyword argument values as written.
    args: Vec<&'a Spanned<Expr>>,

    /// For each callee input slot, the index into `args` that binds it.
    input_bindings: Vec<usize>,
}

/// A validated action call with a lowered action reference.
#[derive(Debug)]
pub struct ActionCallPlan<'a, ActionRef> {
    /// Lowered action reference for the action invocation.
    action_ref: ActionRef,

    /// Keyword arguments forwarded to the action.
    kwargs: &'a [Kwarg],

    /// The source action name for wrapper generation and diagnostics.
    action_name: &'a str,

    /// The policy brackets attached to this call site.
    policies: &'a [PolicyBracket],
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
    /// A built-in call form received keyword arguments it does not accept.
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

        let expected = known.inputs.len();
        let actual = call.args.len() + call.kwargs.len();
        if actual != expected {
            return Err(Error::FunctionArityMismatch {
                function: call.name.clone(),
                expected,
                actual,
            });
        }

        // Positional arguments bind the leading input slots; keyword
        // arguments bind the remaining slots by declared input name. The
        // argument count parity above plus the per-slot collision check
        // below make the binding exhaustive.
        let mut slot_bindings: Vec<Option<usize>> = vec![None; expected];
        let mut args = Vec::with_capacity(actual);
        for (slot_binding, arg) in slot_bindings.iter_mut().zip(&call.args) {
            *slot_binding = Some(args.len());
            args.push(arg);
        }
        for kwarg in &call.kwargs {
            let input_index = known
                .inputs
                .iter()
                .position(|input| input == &kwarg.name)
                .ok_or_else(|| Error::UnknownKeywordArgument {
                    function: call.name.clone(),
                    keyword: kwarg.name.clone(),
                })?;
            if slot_bindings[input_index].is_some() {
                return Err(Error::DuplicateFunctionArgument {
                    function: call.name.clone(),
                    input: kwarg.name.clone(),
                });
            }
            slot_bindings[input_index] = Some(args.len());
            args.push(&kwarg.value);
        }
        let input_bindings = slot_bindings
            .into_iter()
            .map(|slot_binding| slot_binding.expect("argument count parity fills every input slot"))
            .collect();

        Ok(Self {
            function_id: known.id,
            args,
            input_bindings,
        })
    }

    /// Returns the resolved callee id.
    pub fn function_id(&self) -> FunctionId {
        self.function_id
    }

    /// Returns the argument expressions in source order.
    pub fn args(&self) -> &[&'a Spanned<Expr>] {
        &self.args
    }

    /// Returns, for each callee input slot, the index into
    /// [`Self::args`] that binds it.
    pub fn input_bindings(&self) -> &[usize] {
        &self.input_bindings
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
            action_name: &call.action_name,
            policies: &call.policies,
        })
    }

    /// Returns the lowered action reference, the original keyword arguments,
    /// the source action name, and the policy brackets of this call site.
    pub fn into_parts(self) -> (ActionRef, &'a [Kwarg], &'a str, &'a [PolicyBracket]) {
        (
            self.action_ref,
            self.kwargs,
            self.action_name,
            self.policies,
        )
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

    /// Function table with a two-input callee for keyword-binding tests.
    fn build_pair_function_table() -> crate::function::table::FunctionTable {
        let program =
            waymark_vm_ast_old_helpers::program(vec![waymark_vm_ast_old_helpers::function(
                "pair",
                &["left", "right"],
                vec![],
            )]);
        crate::function::table::FunctionTable::build(&program).expect("function table should build")
    }

    fn kwarg(name: &str, value: Spanned<Expr>) -> Kwarg {
        Kwarg {
            name: name.to_owned(),
            value,
        }
    }

    fn literal_int(expr: &Spanned<Expr>) -> i64 {
        match &expr.value {
            waymark_vm_ast_old::Expr::Literal {
                value: Literal::Int(value),
            } => *value,
            other => panic!("unexpected argument expression {other:?}"),
        }
    }

    #[test]
    fn binds_keyword_arguments_by_declared_input_order() {
        let function_table = build_pair_function_table();
        let mut call = function_call("pair", Vec::new());
        call.kwargs.push(kwarg("right", int(2)));
        call.kwargs.push(kwarg("left", int(1)));

        let plan = FunctionCallPlan::build::<(), ()>(&call, &function_table)
            .expect("keyword arguments should bind");

        // Source order is preserved for the argument expressions...
        let args: Vec<i64> = plan.args().iter().map(|arg| literal_int(arg)).collect();
        assert_eq!(args, vec![2, 1]);
        // ...while the input bindings map them to the declared input order.
        assert_eq!(plan.input_bindings(), &[1, 0]);
    }

    #[test]
    fn binds_mixed_positional_and_keyword_arguments() {
        let function_table = build_pair_function_table();
        let mut call = function_call("pair", vec![int(1)]);
        call.kwargs.push(kwarg("right", int(2)));

        let plan = FunctionCallPlan::build::<(), ()>(&call, &function_table)
            .expect("mixed arguments should bind");

        let args: Vec<i64> = plan.args().iter().map(|arg| literal_int(arg)).collect();
        assert_eq!(args, vec![1, 2]);
        assert_eq!(plan.input_bindings(), &[0, 1]);
    }

    #[test]
    fn rejects_unknown_keyword_arguments() {
        let function_table = build_pair_function_table();
        let mut call = function_call("pair", vec![int(1)]);
        call.kwargs.push(kwarg("missing", int(2)));

        let error = FunctionCallPlan::build::<(), ()>(&call, &function_table)
            .expect_err("unknown keyword arguments should be rejected");

        assert!(matches!(
            error,
            Error::UnknownKeywordArgument { function, keyword }
                if function == "pair" && keyword == "missing"
        ));
    }

    #[test]
    fn rejects_keyword_argument_rebinding_a_positional_slot() {
        let function_table = build_pair_function_table();
        let mut call = function_call("pair", vec![int(1)]);
        call.kwargs.push(kwarg("left", int(2)));

        let error = FunctionCallPlan::build::<(), ()>(&call, &function_table)
            .expect_err("rebinding a positionally-filled input should be rejected");

        assert!(matches!(
            error,
            Error::DuplicateFunctionArgument { function, input }
                if function == "pair" && input == "left"
        ));
    }

    #[test]
    fn rejects_duplicate_keyword_arguments() {
        let function_table = build_pair_function_table();
        let mut call = function_call("pair", Vec::new());
        call.kwargs.push(kwarg("left", int(1)));
        call.kwargs.push(kwarg("left", int(2)));

        let error = FunctionCallPlan::build::<(), ()>(&call, &function_table)
            .expect_err("duplicate keyword arguments should be rejected");

        assert!(matches!(
            error,
            Error::DuplicateFunctionArgument { function, input }
                if function == "pair" && input == "left"
        ));
    }

    #[test]
    fn counts_keyword_arguments_toward_arity() {
        let function_table = build_pair_function_table();
        let mut call = function_call("pair", Vec::new());
        call.kwargs.push(kwarg("left", int(1)));

        let error = FunctionCallPlan::build::<(), ()>(&call, &function_table)
            .expect_err("missing arguments should be rejected");

        assert!(matches!(
            error,
            Error::FunctionArityMismatch {
                function,
                expected,
                actual,
            } if function == "pair" && expected == 2 && actual == 1
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
        let (action_ref, kwargs, action_name, policies) = planned_call.into_parts();

        assert!(matches!(action_ref, TestActionRef(name) if name == "notify"));
        assert!(kwargs.is_empty());
        assert_eq!(action_name, "notify");
        assert!(policies.is_empty());
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
