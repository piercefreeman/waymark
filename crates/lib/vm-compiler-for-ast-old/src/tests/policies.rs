//! Tests for policy brackets on action calls.
//!
//! Policy-annotated action calls route through wrapper-function generation.

use waymark_vm_ast_old::{
    DurationLiteral, PolicyBracket, RetryPolicy, Spanned, Statement, TimeoutPolicy,
};
use waymark_vm_ast_old_helpers::{action_call, function, program, spanned};
use waymark_vm_compiler_for_ast_old_test_support::{TestLowering, TestSpec};

use crate::compile;

/// Builds a bare action-call statement carrying the provided policies.
fn policy_action_stmt(policies: Vec<PolicyBracket>) -> Spanned<Statement> {
    let mut call = action_call("notify", Vec::new());
    call.policies = policies;
    spanned(Statement::ActionCall { call })
}

#[test]
fn lowers_retry_policies_through_a_wrapper_function() {
    let program = program(vec![function(
        "main",
        &[],
        vec![policy_action_stmt(vec![
            PolicyBracket::Retry(RetryPolicy {
                exception_types: Vec::new(),
                max_retries: 2,
                backoff: None,
            }),
            PolicyBracket::Timeout(TimeoutPolicy {
                timeout: DurationLiteral { seconds: 30 },
            }),
        ])],
    )]);

    let executable =
        compile::<TestSpec, TestLowering>(&program).expect("retry policies should compile");

    // The call site invokes the retrying wrapper - the program's one extra
    // function - instead of the raw action.
    insta::assert_snapshot!(waymark_vm_bytecode_fmt::display(&executable), @r#"
    f0: [1 registers]
      s0:
        CoreSet(Call { dst: r0, function_id: f1, args: [] })
        CoreSet(Await { dst: r0, src: r0, resume: s1 })
      s1:
        PureSet(LoadConst { dst: r0, value: None })
        CoreSet(Return { src: r0 })
    f1: [13 registers]
      s0:
        PureSet(LoadConst { dst: r0, value: Int(0) })
        PureSet(LoadConst { dst: r1, value: Int(1) })
        CoreSet(Jump { target_state: s1 })
      s1:
        CoreSet(PushExceptionHandlers { handlers: [ExceptionHandler { handler_state: s2, exception_types: [], exception_dst: Some(r2) }] })
        ExtCallSet(ActionCall { dst: r3, action_ref: TestActionRef("notify"), args: [], resume: s4 })
      s2:
        PureSet(LoadConst { dst: r8, value: Int(2) })
        PureSet(Binary { kind: Lt, op: BinaryOp { dst: r9, a: r0, b: r8 } })
        CoreSet(JumpIf { target_state: s3, cond: r9 })
        CoreSet(Raise { src: r2 })
      s3:
        PureSet(Binary { kind: Add, op: BinaryOp { dst: r0, a: r0, b: r1 } })
        CoreSet(Jump { target_state: s1 })
      s4:
        PureSet(LoadConst { dst: r4, value: Int(30) })
        ExtCallSet(Sleep { dst: r5, duration: r4, resume: s6, unskippable: true })
      s5:
        CoreSet(PopExceptionHandlers { count: 1 })
        CoreSet(Return { src: r6 })
      s6:
        CoreSet(Select { arms: [SelectArm { src: r3, dst: r6, resume: s5 }, SelectArm { src: r5, dst: r7, resume: s7 }] })
      s7:
        CoreSet(PopExceptionHandlers { count: 1 })
        PureSet(LoadConst { dst: r10, value: String("ActionTimeout") })
        PureSet(LoadConst { dst: r11, value: None })
        PureSet(MakeException { dst: r12, type_id: r10, details: r11 })
        CoreSet(Raise { src: r12 })
    "#);
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
