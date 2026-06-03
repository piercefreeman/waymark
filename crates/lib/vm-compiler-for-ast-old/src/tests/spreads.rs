//! Tests for spread-statement and spread-expression lowering.

use waymark_vm_ast_old_helpers::{
    action_call, assignment, assignment_targets, function, program, return_stmt, spread_expr,
    spread_stmt, variable,
};
use waymark_vm_compiler_for_ast_old_test_support::{TestLowering, TestSpec};

use crate::compile;

#[test]
fn compiles_spread_action_statements() {
    let program = program(vec![function(
        "main",
        &["items"],
        vec![
            spread_stmt(
                variable("items"),
                "item",
                action_call("notify", vec![("value", variable("item"))]),
            ),
            return_stmt(None),
        ],
    )]);

    compile::<TestSpec, TestLowering>(&program).expect("spread statements should compile");
}

#[test]
fn spread_expression_assignments_lower_to_looped_action_collection() {
    let program = program(vec![function(
        "main",
        &["items"],
        vec![
            assignment(
                "results",
                spread_expr(
                    variable("items"),
                    "item",
                    action_call("double", vec![("value", variable("item"))]),
                ),
            ),
            return_stmt(Some(variable("results"))),
        ],
    )]);

    let executable =
        compile::<TestSpec, TestLowering>(&program).expect("spread expressions should compile");

    insta::assert_snapshot!(waymark_vm_bytecode_fmt::display(&executable), @r#"
    f0: [8 registers]
      s0:
        PureSet(MakeList { dst: r2, items: [] })
        PureSet(MakeList { dst: r3, items: [] })
        PureSet(LoadConst { dst: r4, value: Int(0) })
        PureSet(Length { dst: r5, src: r0 })
        CoreSet(Jump { target_state: s1 })
      s1:
        PureSet(Binary { kind: Lt, op: BinaryOp { dst: r6, a: r4, b: r5 } })
        CoreSet(JumpIf { target_state: s2, cond: r6 })
        CoreSet(Jump { target_state: s4 })
      s2:
        PureSet(Index { dst: r6, object: r0, index: r4 })
        ExtCallSet(ActionCall { dst: r7, action_ref: TestActionRef("double"), args: [r6], resume: s5 })
      s3:
        PureSet(LoadConst { dst: r6, value: Int(1) })
        PureSet(Binary { kind: Add, op: BinaryOp { dst: r4, a: r4, b: r6 } })
        CoreSet(Jump { target_state: s1 })
      s4:
        PureSet(LoadConst { dst: r4, value: Int(0) })
        CoreSet(Jump { target_state: s6 })
      s5:
        PureSet(ListAppend { dst: r3, list: r3, item: r7 })
        CoreSet(Jump { target_state: s3 })
      s6:
        PureSet(Binary { kind: Lt, op: BinaryOp { dst: r6, a: r4, b: r5 } })
        CoreSet(JumpIf { target_state: s7, cond: r6 })
        CoreSet(Jump { target_state: s9 })
      s7:
        PureSet(Index { dst: r6, object: r3, index: r4 })
        CoreSet(Await { dst: r6, src: r6, resume: s10 })
      s8:
        PureSet(LoadConst { dst: r6, value: Int(1) })
        PureSet(Binary { kind: Add, op: BinaryOp { dst: r4, a: r4, b: r6 } })
        CoreSet(Jump { target_state: s6 })
      s9:
        PureSet(Copy { dst: r1, src: r2 })
        CoreSet(Return { src: r1 })
      s10:
        ExcSet(ShouldBubble { dst: r7, src: r6 })
        CoreSet(JumpIf { target_state: s12, cond: r7 })
        CoreSet(Jump { target_state: s11 })
      s11:
        PureSet(ListAppend { dst: r2, list: r2, item: r6 })
        CoreSet(Jump { target_state: s8 })
      s12:
        ExcSet(Raise { src: r6 })
    "#);
}

#[test]
fn zero_target_spread_assignments_compile_as_side_effect_spreads() {
    let program = program(vec![function(
        "main",
        &["items"],
        vec![
            assignment_targets(
                &[],
                spread_expr(
                    variable("items"),
                    "item",
                    action_call("notify", vec![("value", variable("item"))]),
                ),
            ),
            return_stmt(None),
        ],
    )]);

    let executable = compile::<TestSpec, TestLowering>(&program)
        .expect("zero-target spread assignments should compile as side-effect spreads");

    insta::assert_snapshot!(waymark_vm_bytecode_fmt::display(&executable), @r#"
    f0: [6 registers]
      s0:
        PureSet(MakeList { dst: r1, items: [] })
        PureSet(LoadConst { dst: r2, value: Int(0) })
        PureSet(Length { dst: r3, src: r0 })
        CoreSet(Jump { target_state: s1 })
      s1:
        PureSet(Binary { kind: Lt, op: BinaryOp { dst: r4, a: r2, b: r3 } })
        CoreSet(JumpIf { target_state: s2, cond: r4 })
        CoreSet(Jump { target_state: s4 })
      s2:
        PureSet(Index { dst: r4, object: r0, index: r2 })
        ExtCallSet(ActionCall { dst: r5, action_ref: TestActionRef("notify"), args: [r4], resume: s5 })
      s3:
        PureSet(LoadConst { dst: r4, value: Int(1) })
        PureSet(Binary { kind: Add, op: BinaryOp { dst: r2, a: r2, b: r4 } })
        CoreSet(Jump { target_state: s1 })
      s4:
        PureSet(LoadConst { dst: r2, value: Int(0) })
        CoreSet(Jump { target_state: s6 })
      s5:
        PureSet(ListAppend { dst: r1, list: r1, item: r5 })
        CoreSet(Jump { target_state: s3 })
      s6:
        PureSet(Binary { kind: Lt, op: BinaryOp { dst: r4, a: r2, b: r3 } })
        CoreSet(JumpIf { target_state: s7, cond: r4 })
        CoreSet(Jump { target_state: s9 })
      s7:
        PureSet(Index { dst: r4, object: r1, index: r2 })
        CoreSet(Await { dst: r4, src: r4, resume: s10 })
      s8:
        PureSet(LoadConst { dst: r4, value: Int(1) })
        PureSet(Binary { kind: Add, op: BinaryOp { dst: r2, a: r2, b: r4 } })
        CoreSet(Jump { target_state: s6 })
      s9:
        PureSet(LoadConst { dst: r4, value: None })
        CoreSet(Return { src: r4 })
      s10:
        ExcSet(ShouldBubble { dst: r5, src: r4 })
        CoreSet(JumpIf { target_state: s12, cond: r5 })
        CoreSet(Jump { target_state: s11 })
      s11:
        CoreSet(Jump { target_state: s8 })
      s12:
        ExcSet(Raise { src: r4 })
    "#);
}
