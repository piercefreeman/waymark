//! Tests for `for` loop lowering: indexed, positive-range, stepped-range,
//! and `enumerate` header variants, plus the corresponding rejection paths.

use waymark_vm_ast_old::{BinaryOperator, Expr, GlobalFunction, Kwarg};
use waymark_vm_ast_old_helpers::{
    action_expr, assignment, binary_expr, builtin_function_call, continue_stmt, enumerate_expr,
    for_stmt, function, int, program, range_expr, return_stmt, spanned, variable,
};
use waymark_vm_compiler_for_ast_old_test_support::{TestLowering, TestSpec};

use crate::{CompileError, compile, function::compiler};

#[test]
fn compiles_for_loops_over_lists() {
    let program = program(vec![function(
        "main",
        &["items"],
        vec![
            assignment("total", int(0)),
            for_stmt(
                &["item"],
                variable("items"),
                vec![assignment(
                    "total",
                    binary_expr(variable("total"), BinaryOperator::Add, variable("item")),
                )],
            ),
            return_stmt(Some(variable("total"))),
        ],
    )]);

    compile::<TestSpec, TestLowering>(&program).expect("list for loops should compile");
}

#[test]
fn whole_program_async_indexed_for_loops_match_the_documented_lowering_shape() {
    // Raw code:
    //
    //   results = []
    //   for item in items:
    //       processed = await process_value(item)
    //       results.append(processed)
    //   return results
    //
    // VM registers:
    //
    //   r0 = input items
    //   r1 = results
    //   r2 = iterable snapshot
    //   r3 = loop index
    //   r4 = length
    //   r5 = temp reused for cond/item/+1
    //   r6 = item local
    //   r7 = processed promise/value
    //
    // VM compilation:
    //
    //   S0: results=[], r2=r0, r3=0, r4=len(r2), jump S1
    //   S1: r5=(r3<r4), if r5 jump S2 else S4
    //   S2: r5=r2[r3], r6=r5, ActionCall -> resume S5
    //   S5: Await r7 -> resume S6
    //   S6: results += [r7], jump S3
    //   S3: r3=r3+1, jump S1
    //   S4: return r1
    //
    // The old AST does not expose method-call syntax, so the body models
    // `results.append(processed)` as `results = results + [processed]`.
    let program = program(vec![function(
        "main",
        &["items"],
        vec![
            assignment(
                "results",
                spanned(Expr::List {
                    elements: Vec::new(),
                }),
            ),
            for_stmt(
                &["item"],
                variable("items"),
                vec![
                    assignment(
                        "processed",
                        action_expr(
                            waymark_action_core::ActionRuntime::Python,
                            "process_value",
                            vec![("item", variable("item"))],
                        ),
                    ),
                    assignment(
                        "results",
                        binary_expr(
                            variable("results"),
                            BinaryOperator::Add,
                            spanned(Expr::List {
                                elements: vec![variable("processed")],
                            }),
                        ),
                    ),
                ],
            ),
            return_stmt(Some(variable("results"))),
        ],
    )]);

    let executable = compile::<TestSpec, TestLowering>(&program)
        .expect("documented async for loop should compile");

    insta::assert_snapshot!(waymark_vm_bytecode_fmt::display(&executable), @r#"
    f0: [8 registers]
      s0:
        PureSet(MakeList { dst: r1, items: [] })
        PureSet(Copy { dst: r2, src: r0 })
        PureSet(LoadConst { dst: r3, value: Int(0) })
        PureSet(Length { dst: r4, src: r2 })
        CoreSet(Jump { target_state: s1 })
      s1:
        PureSet(Binary { kind: Lt, op: BinaryOp { dst: r5, a: r3, b: r4 } })
        CoreSet(JumpIf { target_state: s2, cond: r5 })
        CoreSet(Jump { target_state: s4 })
      s2:
        PureSet(Index { dst: r5, object: r2, index: r3 })
        PureSet(Copy { dst: r6, src: r5 })
        ExtCallSet(ActionCall { dst: r7, action_ref: TestActionRef("process_value"), args: [r6], resume: s5 })
      s3:
        PureSet(LoadConst { dst: r5, value: Int(1) })
        PureSet(Binary { kind: Add, op: BinaryOp { dst: r3, a: r3, b: r5 } })
        CoreSet(Jump { target_state: s1 })
      s4:
        CoreSet(Return { src: r1 })
      s5:
        CoreSet(Await { dst: r7, src: r7, resume: s6 })
      s6:
        PureSet(MakeList { dst: r5, items: [r7] })
        PureSet(Binary { kind: Add, op: BinaryOp { dst: r1, a: r1, b: r5 } })
        CoreSet(Jump { target_state: s3 })
    "#);
}

#[test]
fn compiles_for_loops_over_range() {
    let program = program(vec![function(
        "main",
        &[],
        vec![
            assignment("total", int(0)),
            for_stmt(
                &["item"],
                range_expr(vec![int(1), int(4)]),
                vec![assignment(
                    "total",
                    binary_expr(variable("total"), BinaryOperator::Add, variable("item")),
                )],
            ),
            return_stmt(Some(variable("total"))),
        ],
    )]);

    compile::<TestSpec, TestLowering>(&program).expect("range for loops should compile");
}

#[test]
fn compiles_for_loops_over_single_arg_range() {
    let program = program(vec![function(
        "main",
        &[],
        vec![
            assignment("total", int(0)),
            for_stmt(
                &["item"],
                range_expr(vec![int(4)]),
                vec![assignment(
                    "total",
                    binary_expr(variable("total"), BinaryOperator::Add, variable("item")),
                )],
            ),
            return_stmt(Some(variable("total"))),
        ],
    )]);

    let executable =
        compile::<TestSpec, TestLowering>(&program).expect("range(stop) for loops should compile");

    insta::assert_snapshot!(waymark_vm_bytecode_fmt::display(&executable), @r#"
    f0: [5 registers]
      s0:
        PureSet(LoadConst { dst: r0, value: Int(0) })
        PureSet(LoadConst { dst: r1, value: Int(0) })
        PureSet(LoadConst { dst: r2, value: Int(4) })
        CoreSet(Jump { target_state: s1 })
      s1:
        PureSet(Binary { kind: Lt, op: BinaryOp { dst: r3, a: r1, b: r2 } })
        CoreSet(JumpIf { target_state: s2, cond: r3 })
        CoreSet(Jump { target_state: s4 })
      s2:
        PureSet(Copy { dst: r4, src: r1 })
        PureSet(Binary { kind: Add, op: BinaryOp { dst: r0, a: r0, b: r4 } })
        CoreSet(Jump { target_state: s3 })
      s3:
        PureSet(LoadConst { dst: r3, value: Int(1) })
        PureSet(Binary { kind: Add, op: BinaryOp { dst: r1, a: r1, b: r3 } })
        CoreSet(Jump { target_state: s1 })
      s4:
        CoreSet(Return { src: r0 })
    "#);
}

#[test]
fn compiles_for_loops_over_stepped_range() {
    let program = program(vec![function(
        "main",
        &[],
        vec![
            assignment("total", int(0)),
            for_stmt(
                &["item"],
                range_expr(vec![int(0), int(10), int(2)]),
                vec![assignment(
                    "total",
                    binary_expr(variable("total"), BinaryOperator::Add, variable("item")),
                )],
            ),
            return_stmt(Some(variable("total"))),
        ],
    )]);

    compile::<TestSpec, TestLowering>(&program)
        .expect("range(start, stop, step) for loops should compile");
}

#[test]
fn compiles_for_loops_over_enumerate() {
    let program = program(vec![function(
        "main",
        &["items"],
        vec![
            assignment("total", int(0)),
            for_stmt(
                &["idx", "item"],
                enumerate_expr(variable("items")),
                vec![assignment(
                    "total",
                    binary_expr(
                        variable("total"),
                        BinaryOperator::Add,
                        binary_expr(variable("idx"), BinaryOperator::Add, variable("item")),
                    ),
                )],
            ),
            return_stmt(Some(variable("total"))),
        ],
    )]);

    compile::<TestSpec, TestLowering>(&program).expect("enumerate for loops should compile");
}

#[test]
fn compiles_enumerate_over_range_for_loops() {
    let program = program(vec![function(
        "main",
        &[],
        vec![
            assignment("total", int(0)),
            for_stmt(
                &["idx", "item"],
                enumerate_expr(range_expr(vec![int(2), int(5)])),
                vec![assignment(
                    "total",
                    binary_expr(
                        variable("total"),
                        BinaryOperator::Add,
                        binary_expr(variable("idx"), BinaryOperator::Add, variable("item")),
                    ),
                )],
            ),
            return_stmt(Some(variable("total"))),
        ],
    )]);

    compile::<TestSpec, TestLowering>(&program).expect("enumerated range for loops should compile");
}

#[test]
fn compiles_enumerate_over_stepped_range_for_loops() {
    let program = program(vec![function(
        "main",
        &[],
        vec![
            assignment("total", int(0)),
            for_stmt(
                &["idx", "item"],
                enumerate_expr(range_expr(vec![int(10), int(0), int(-2)])),
                vec![assignment(
                    "total",
                    binary_expr(
                        variable("total"),
                        BinaryOperator::Add,
                        binary_expr(variable("idx"), BinaryOperator::Add, variable("item")),
                    ),
                )],
            ),
            return_stmt(Some(variable("total"))),
        ],
    )]);

    compile::<TestSpec, TestLowering>(&program)
        .expect("enumerated stepped range for loops should compile");
}

#[test]
fn compiles_for_loops_over_enumerate_items_kwarg() {
    let mut call = builtin_function_call(GlobalFunction::Enumerate, Vec::new());
    call.kwargs.push(Kwarg {
        name: "items".to_owned(),
        value: variable("items"),
    });

    let program = program(vec![function(
        "main",
        &["items"],
        vec![
            assignment("total", int(0)),
            for_stmt(
                &["idx", "item"],
                spanned(Expr::FunctionCall { call }),
                vec![assignment(
                    "total",
                    binary_expr(
                        variable("total"),
                        BinaryOperator::Add,
                        binary_expr(variable("idx"), BinaryOperator::Add, variable("item")),
                    ),
                )],
            ),
            return_stmt(Some(variable("total"))),
        ],
    )]);

    compile::<TestSpec, TestLowering>(&program)
        .expect("enumerate(items=...) for loops should compile");
}

#[test]
fn compiles_for_loops_binding_enumerate_as_single_pair() {
    let program = program(vec![function(
        "main",
        &["items"],
        vec![
            assignment("count", int(0)),
            for_stmt(
                &["pair"],
                enumerate_expr(variable("items")),
                vec![assignment(
                    "count",
                    binary_expr(variable("count"), BinaryOperator::Add, variable("pair")),
                )],
            ),
            return_stmt(Some(variable("count"))),
        ],
    )]);

    compile::<TestSpec, TestLowering>(&program)
        .expect("single-variable enumerate for loops should compile");
}

#[test]
fn rejects_for_loops_over_range_with_no_args() {
    let program = program(vec![function(
        "main",
        &[],
        vec![for_stmt(&["item"], range_expr(Vec::new()), Vec::new())],
    )]);

    let error = match compile::<TestSpec, TestLowering>(&program) {
        Ok(_) => panic!("range with no args should fail"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        CompileError::FunctionCompiler(compiler::Error::FunctionArityMismatch {
            function,
            expected,
            actual,
        }) if function == "range" && expected == 1 && actual == 0
    ));
}

#[test]
fn rejects_for_loops_over_range_with_too_many_args() {
    let program = program(vec![function(
        "main",
        &[],
        vec![for_stmt(
            &["item"],
            range_expr(vec![int(0), int(4), int(1), int(2)]),
            Vec::new(),
        )],
    )]);

    let error = match compile::<TestSpec, TestLowering>(&program) {
        Ok(_) => panic!("range with too many args should fail"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        CompileError::FunctionCompiler(compiler::Error::FunctionArityMismatch {
            function,
            expected,
            actual,
        }) if function == "range" && expected == 3 && actual == 4
    ));
}

#[test]
fn rejects_for_loops_over_range_with_keyword_args() {
    let mut call = builtin_function_call(GlobalFunction::Range, vec![int(4)]);
    call.kwargs.push(Kwarg {
        name: "stop".to_owned(),
        value: int(10),
    });

    let program = program(vec![function(
        "main",
        &[],
        vec![for_stmt(
            &["item"],
            spanned(Expr::FunctionCall { call }),
            Vec::new(),
        )],
    )]);

    let error = match compile::<TestSpec, TestLowering>(&program) {
        Ok(_) => panic!("range with keyword args should fail"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        CompileError::FunctionCompiler(compiler::Error::Unsupported(
            compiler::Unsupported::FunctionCall { name, reason }
        )) if name == "range"
            && reason == compiler::UnsupportedFunctionCall::KeywordArguments
    ));
}

#[test]
fn rejects_for_loops_over_enumerate_with_too_many_args() {
    let program = program(vec![function(
        "main",
        &["items", "other"],
        vec![for_stmt(
            &["idx", "item"],
            spanned(Expr::FunctionCall {
                call: builtin_function_call(
                    GlobalFunction::Enumerate,
                    vec![variable("items"), variable("other")],
                ),
            }),
            Vec::new(),
        )],
    )]);

    let error = match compile::<TestSpec, TestLowering>(&program) {
        Ok(_) => panic!("enumerate with too many args should fail"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        CompileError::FunctionCompiler(compiler::Error::FunctionArityMismatch {
            function,
            expected,
            actual,
        }) if function == "enumerate" && expected == 1 && actual == 2
    ));
}

#[test]
fn rejects_for_loops_over_enumerate_with_mixed_positional_and_keyword_args() {
    let mut call = builtin_function_call(GlobalFunction::Enumerate, vec![variable("items")]);
    call.kwargs.push(Kwarg {
        name: "items".to_owned(),
        value: variable("other"),
    });

    let program = program(vec![function(
        "main",
        &["items", "other"],
        vec![for_stmt(
            &["idx", "item"],
            spanned(Expr::FunctionCall { call }),
            Vec::new(),
        )],
    )]);

    let error = match compile::<TestSpec, TestLowering>(&program) {
        Ok(_) => panic!("enumerate with mixed args should fail"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        CompileError::FunctionCompiler(compiler::Error::Unsupported(
            compiler::Unsupported::FunctionCall { name, reason }
        )) if name == "enumerate"
            && reason == compiler::UnsupportedFunctionCall::KeywordArguments
    ));
}

#[test]
fn rejects_loop_variables_initialized_only_inside_for_loops() {
    let program = program(vec![function(
        "main",
        &["items"],
        vec![
            for_stmt(&["item"], variable("items"), vec![continue_stmt()]),
            return_stmt(Some(variable("item"))),
        ],
    )]);

    let error = match compile::<TestSpec, TestLowering>(&program) {
        Ok(_) => panic!("loop-only bindings should fail outside the for loop"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        CompileError::FunctionCompiler(compiler::Error::UnknownVariable { name })
            if name == "item"
    ));
}
