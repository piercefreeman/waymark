//! Tests for `try`/`except` flow-state handling.

use waymark_vm_ast_old_helpers::{
    action_expr, action_stmt, assignment, except_handler, function, int, program, return_stmt,
    try_except_stmt, variable,
};
use waymark_vm_compiler_for_ast_old_test_support::{TestLowering, TestSpec};

use crate::compile;

#[test]
fn allows_variables_initialized_on_try_and_except_paths() {
    let program = program(vec![function(
        "main",
        &[],
        vec![
            try_except_stmt(
                vec![assignment("x", action_expr("ok", Vec::new()))],
                vec![except_handler(
                    &["ValueError"],
                    None,
                    vec![assignment("x", int(2))],
                )],
            ),
            return_stmt(Some(variable("x"))),
        ],
    )]);

    let executable = compile::<TestSpec, TestLowering>(&program)
        .expect("variables initialized on both try and except paths should compile");

    insta::assert_snapshot!(waymark_vm_bytecode_fmt::display(&executable), @r#"
    f0: [5 registers]
      s0:
        PureSet(LoadConst { dst: r1, value: String("ValueError") })
        ExtCallSet(ActionCall { dst: r2, action_ref: TestActionRef("ok"), args: [], resume: s3 })
      s1:
        CoreSet(Return { src: r2 })
      s2:
        ExcSet(CatchException { src: r0 })
        PureSet(LoadConst { dst: r2, value: Int(2) })
        CoreSet(Jump { target_state: s1 })
      s3:
        CoreSet(Await { dst: r2, src: r2, resume: s4 })
      s4:
        ExcSet(ShouldBubble { dst: r3, src: r2 })
        CoreSet(JumpIf { target_state: s6, cond: r3 })
        CoreSet(Jump { target_state: s5 })
      s5:
        CoreSet(Jump { target_state: s1 })
      s6:
        PureSet(Copy { dst: r0, src: r2 })
        ExcSet(IsException { dst: r4, src: r0, exception_type_id: Some(r1) })
        CoreSet(JumpIf { target_state: s2, cond: r4 })
        ExcSet(Raise { src: r0 })
    "#);
}

#[test]
fn reuses_function_scope_locals_after_except_only_declarations() {
    let program = program(vec![function(
        "main",
        &[],
        vec![
            try_except_stmt(
                vec![action_stmt("boom")],
                vec![except_handler(
                    &["ValueError"],
                    None,
                    vec![assignment("x", int(1))],
                )],
            ),
            assignment("x", int(2)),
            return_stmt(Some(variable("x"))),
        ],
    )]);

    let executable = compile::<TestSpec, TestLowering>(&program)
        .expect("locals declared in except blocks should remain bound at function scope");

    insta::assert_snapshot!(waymark_vm_bytecode_fmt::display(&executable), @r#"
    f0: [6 registers]
      s0:
        PureSet(LoadConst { dst: r1, value: String("ValueError") })
        ExtCallSet(ActionCall { dst: r2, action_ref: TestActionRef("boom"), args: [], resume: s3 })
      s1:
        PureSet(LoadConst { dst: r5, value: Int(2) })
        CoreSet(Return { src: r5 })
      s2:
        ExcSet(CatchException { src: r0 })
        PureSet(LoadConst { dst: r5, value: Int(1) })
        CoreSet(Jump { target_state: s1 })
      s3:
        CoreSet(Await { dst: r2, src: r2, resume: s4 })
      s4:
        ExcSet(ShouldBubble { dst: r3, src: r2 })
        CoreSet(JumpIf { target_state: s6, cond: r3 })
        CoreSet(Jump { target_state: s5 })
      s5:
        CoreSet(Jump { target_state: s1 })
      s6:
        PureSet(Copy { dst: r0, src: r2 })
        ExcSet(IsException { dst: r4, src: r0, exception_type_id: Some(r1) })
        CoreSet(JumpIf { target_state: s2, cond: r4 })
        ExcSet(Raise { src: r0 })
    "#);
}

#[test]
fn allows_post_join_handler_assignments_when_try_success_path_terminates() {
    let program = program(vec![function(
        "main",
        &[],
        vec![
            try_except_stmt(
                vec![return_stmt(Some(action_expr("ok", Vec::new())))],
                vec![
                    except_handler(&["ValueError"], None, vec![assignment("x", int(1))]),
                    except_handler(&["TypeError"], None, vec![assignment("x", int(2))]),
                ],
            ),
            return_stmt(Some(variable("x"))),
        ],
    )]);

    let executable = compile::<TestSpec, TestLowering>(&program)
        .expect("handler assignments should remain available when the try success path terminates");

    insta::assert_snapshot!(waymark_vm_bytecode_fmt::display(&executable), @r#"
    f0: [7 registers]
      s0:
        PureSet(LoadConst { dst: r1, value: String("ValueError") })
        PureSet(LoadConst { dst: r2, value: String("TypeError") })
        ExtCallSet(ActionCall { dst: r3, action_ref: TestActionRef("ok"), args: [], resume: s4 })
      s1:
        CoreSet(Return { src: r6 })
      s2:
        ExcSet(CatchException { src: r0 })
        PureSet(LoadConst { dst: r6, value: Int(1) })
        CoreSet(Jump { target_state: s1 })
      s3:
        ExcSet(CatchException { src: r0 })
        PureSet(LoadConst { dst: r6, value: Int(2) })
        CoreSet(Jump { target_state: s1 })
      s4:
        CoreSet(Await { dst: r3, src: r3, resume: s5 })
      s5:
        ExcSet(ShouldBubble { dst: r4, src: r3 })
        CoreSet(JumpIf { target_state: s7, cond: r4 })
        CoreSet(Jump { target_state: s6 })
      s6:
        CoreSet(Return { src: r3 })
      s7:
        PureSet(Copy { dst: r0, src: r3 })
        ExcSet(IsException { dst: r5, src: r0, exception_type_id: Some(r1) })
        CoreSet(JumpIf { target_state: s2, cond: r5 })
        ExcSet(IsException { dst: r5, src: r0, exception_type_id: Some(r2) })
        CoreSet(JumpIf { target_state: s3, cond: r5 })
        ExcSet(Raise { src: r0 })
    "#);
}
