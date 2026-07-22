//! Tests for policy brackets on action calls.
//!
//! Policy-annotated action calls route through wrapper-function generation;
//! brackets without a lowering yet are rejected.

use waymark_vm_ast_old::{
    DurationLiteral, PolicyBracket, RetryPolicy, Spanned, Statement, TimeoutPolicy,
};
use waymark_vm_ast_old_helpers::{action_call, function, program, spanned};
use waymark_vm_compiler_for_ast_old_test_support::{TestLowering, TestSpec};

use crate::{CompileError, compile, function::compiler};

/// Builds a bare action-call statement carrying the provided policies.
fn policy_action_stmt(policies: Vec<PolicyBracket>) -> Spanned<Statement> {
    let mut call = action_call("notify", Vec::new());
    call.policies = policies;
    spanned(Statement::ActionCall { call })
}

#[test]
fn rejects_retry_policies_pending_their_lowering() {
    let program = program(vec![function(
        "main",
        &[],
        vec![policy_action_stmt(vec![PolicyBracket::Retry(
            RetryPolicy {
                exception_types: Vec::new(),
                max_retries: 2,
                backoff: None,
            },
        )])],
    )]);

    let error = match compile::<TestSpec, TestLowering>(&program) {
        Ok(_) => panic!("retry policies should fail until their lowering lands"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        CompileError::FunctionCompiler(compiler::Error::Unsupported(
            compiler::Unsupported::RetryPolicy { action_name }
        )) if action_name == "notify"
    ));
}

#[test]
fn lowers_timeout_policies_through_a_wrapper_function() {
    let program = program(vec![function(
        "main",
        &[],
        vec![policy_action_stmt(vec![PolicyBracket::Timeout(
            TimeoutPolicy {
                timeout: DurationLiteral { seconds: 30 },
            },
        )])],
    )]);

    let executable =
        compile::<TestSpec, TestLowering>(&program).expect("timeout policies should compile");

    // The call site invokes the timed wrapper - the program's one extra
    // function - instead of the raw action.
    insta::assert_snapshot!(waymark_vm_bytecode_fmt::display(&executable), @r#"
    f0: [1 registers]
      s0:
        CoreSet(Call { dst: r0, function_id: f1, args: [] })
        CoreSet(Await { dst: r0, src: r0, resume: s1 })
      s1:
        PureSet(LoadConst { dst: r0, value: None })
        CoreSet(Return { src: r0 })
    f1: [8 registers]
      s0:
        ExtCallSet(ActionCall { dst: r0, action_ref: TestActionRef("notify"), args: [], resume: s1 })
      s1:
        PureSet(LoadConst { dst: r1, value: Int(30) })
        ExtCallSet(Sleep { dst: r2, duration: r1, resume: s2, unskippable: true })
      s2:
        CoreSet(Select { arms: [SelectArm { src: r0, dst: r3, resume: s3 }, SelectArm { src: r2, dst: r4, resume: s4 }] })
      s3:
        CoreSet(Return { src: r3 })
      s4:
        PureSet(LoadConst { dst: r5, value: String("ActionTimeout") })
        PureSet(LoadConst { dst: r6, value: None })
        PureSet(MakeException { dst: r7, type_id: r5, details: r6 })
        CoreSet(Raise { src: r7 })
    "#);
}
