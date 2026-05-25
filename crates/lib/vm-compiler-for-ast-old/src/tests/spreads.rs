//! Tests for spread-statement and spread-expression lowering.

use waymark_vm_ast_old_helpers::{
    action_call, assignment, function, program, return_stmt, spread_expr, spread_stmt, variable,
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
    f0: [9 registers]
      s0:
        PureSet(MakeList { dst: r2, items: [] })
        PureSet(MakeList { dst: r3, items: [] })
        PureSet(Copy { dst: r4, src: r0 })
        PureSet(LoadConst { dst: r5, value: Int(0) })
        PureSet(Length { dst: r6, src: r4 })
        CoreSet(Jump { target_state: s1 })
      s1:
        PureSet(Binary { kind: Lt, op: BinaryOp { dst: r7, a: r5, b: r6 } })
        CoreSet(JumpIf { target_state: s2, cond: r7 })
        CoreSet(Jump { target_state: s4 })
      s2:
        PureSet(Index { dst: r7, object: r4, index: r5 })
        ExtCallSet(ActionCall { dst: r8, action_ref: TestActionRef("double"), args: [r7], resume: s5 })
      s3:
        PureSet(LoadConst { dst: r7, value: Int(1) })
        PureSet(Binary { kind: Add, op: BinaryOp { dst: r5, a: r5, b: r7 } })
        CoreSet(Jump { target_state: s1 })
      s4:
        PureSet(LoadConst { dst: r5, value: Int(0) })
        CoreSet(Jump { target_state: s6 })
      s5:
        PureSet(ListAppend { dst: r3, list: r3, item: r8 })
        CoreSet(Jump { target_state: s3 })
      s6:
        PureSet(Binary { kind: Lt, op: BinaryOp { dst: r7, a: r5, b: r6 } })
        CoreSet(JumpIf { target_state: s7, cond: r7 })
        CoreSet(Jump { target_state: s9 })
      s7:
        PureSet(Index { dst: r7, object: r3, index: r5 })
        CoreSet(Await { dst: r7, src: r7, resume: s10 })
      s8:
        PureSet(LoadConst { dst: r7, value: Int(1) })
        PureSet(Binary { kind: Add, op: BinaryOp { dst: r5, a: r5, b: r7 } })
        CoreSet(Jump { target_state: s6 })
      s9:
        PureSet(Copy { dst: r1, src: r2 })
        CoreSet(Return { src: r1 })
      s10:
        PureSet(ListAppend { dst: r2, list: r2, item: r7 })
        CoreSet(Jump { target_state: s8 })
    "#);
}
