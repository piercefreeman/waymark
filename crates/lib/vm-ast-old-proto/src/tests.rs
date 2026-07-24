use super::*;

fn span(line: u32) -> Option<ast::Span> {
    Some(ast::Span {
        start_line: line,
        start_col: 1,
        end_line: line,
        end_col: 10,
    })
}

fn int_expr(value: i64, line: u32) -> ast::Expr {
    ast::Expr {
        kind: Some(ast::expr::Kind::Literal(ast::Literal {
            value: Some(ast::literal::Value::IntValue(value)),
        })),
        span: span(line),
    }
}

#[test]
fn converts_program_happy_path() {
    let program = ast::Program {
        functions: vec![ast::FunctionDef {
            name: "main".to_string(),
            io: Some(ast::IoDecl {
                inputs: vec!["input".to_string()],
                outputs: vec!["output".to_string()],
                span: span(2),
            }),
            body: Some(ast::Block {
                statements: vec![ast::Statement {
                    kind: Some(ast::statement::Kind::ReturnStmt(ast::ReturnStmt {
                        value: Some(int_expr(7, 4)),
                    })),
                    span: span(3),
                }],
                span: span(2),
            }),
            span: span(1),
        }],
    };

    let converted = convert(program).expect("program conversion should succeed");
    let function = &converted.functions[0];

    assert_eq!(function.span.start_line, 1);
    assert_eq!(function.value.name, "main");
    assert_eq!(function.value.io.span.start_line, 2);
    assert_eq!(function.value.body.span.start_line, 2);

    let statement = &function.value.body.value.statements[0];
    assert_eq!(statement.span.start_line, 3);

    match &statement.value {
        vm_ast::Statement::Return { value: Some(expr) } => {
            assert_eq!(expr.span.start_line, 4);
            match &expr.value {
                vm_ast::Expr::Literal {
                    value: vm_ast::Literal::Int(value),
                } => assert_eq!(*value, 7),
                other => panic!("unexpected expression: {other:?}"),
            }
        }
        other => panic!("unexpected statement: {other:?}"),
    }
}

#[test]
fn defaults_missing_span_and_unspecified_global_function() {
    let expr = ast::Expr {
        kind: Some(ast::expr::Kind::FunctionCall(ast::FunctionCall {
            name: "range".to_string(),
            args: Vec::new(),
            kwargs: Vec::new(),
            global_function: ast::GlobalFunction::Unspecified as i32,
        })),
        span: None,
    };

    let converted = convert(expr).expect("function call conversion should succeed");

    assert_eq!(converted.span, default_span());

    match converted.value {
        vm_ast::Expr::FunctionCall { call } => {
            assert_eq!(call.name, "range");
            assert!(call.global_function.is_none());
        }
        other => panic!("unexpected expression: {other:?}"),
    }
}

#[test]
fn returns_missing_field_error() {
    let function = ast::FunctionDef {
        name: "main".to_string(),
        io: None,
        body: Some(ast::Block {
            statements: Vec::new(),
            span: None,
        }),
        span: None,
    };

    let error = convert(function).expect_err("missing io should fail conversion");

    assert_eq!(
        error,
        ConvertError::MissingField {
            field: "FunctionDef.io"
        }
    );
}

#[test]
fn returns_missing_sleep_duration_error() {
    let statement = ast::Statement {
        kind: Some(ast::statement::Kind::SleepStmt(ast::SleepStmt {
            duration: None,
        })),
        span: span(1),
    };

    let error = convert(statement).expect_err("missing sleep duration should fail conversion");

    assert_eq!(
        error,
        ConvertError::MissingField {
            field: "SleepStmt.duration"
        }
    );
}

#[test]
fn returns_invalid_enum_error() {
    let expr = ast::Expr {
        kind: Some(ast::expr::Kind::FunctionCall(ast::FunctionCall {
            name: "bad".to_string(),
            args: Vec::new(),
            kwargs: Vec::new(),
            global_function: 99,
        })),
        span: None,
    };

    let error = convert(expr).expect_err("invalid global function should fail conversion");

    assert_eq!(
        error,
        ConvertError::InvalidEnumValue {
            enum_name: "GlobalFunction",
            value: 99,
        }
    );
}

#[test]
fn returns_unspecified_required_enum_error() {
    let expr = ast::Expr {
        kind: Some(ast::expr::Kind::BinaryOp(Box::new(ast::BinaryOp {
            left: Some(Box::new(int_expr(1, 1))),
            op: ast::BinaryOperator::BinaryOpUnspecified as i32,
            right: Some(Box::new(int_expr(2, 1))),
        }))),
        span: span(1),
    };

    let error = convert(expr).expect_err("unspecified binary operator should fail conversion");

    assert_eq!(
        error,
        ConvertError::UnspecifiedEnumValue {
            enum_name: "BinaryOperator",
        }
    );
}

#[test]
fn preserves_javascript_action_runtime() {
    let call = ast::ActionCall {
        action_name: "send_email".to_owned(),
        kwargs: Vec::new(),
        policies: Vec::new(),
        module_name: Some("src/actions/email.ts".to_owned()),
        runtime: waymark_proto::action::ActionRuntime::Javascript as i32,
    };

    let converted = convert(call).expect("JavaScript action call should convert");

    assert_eq!(
        converted.runtime,
        waymark_action_core::ActionRuntime::JavaScript
    );
}

#[test]
fn rejects_unspecified_action_runtime() {
    let call = ast::ActionCall {
        action_name: "send_email".to_owned(),
        kwargs: Vec::new(),
        policies: Vec::new(),
        module_name: Some("src/actions/email.ts".to_owned()),
        runtime: waymark_proto::action::ActionRuntime::Unspecified as i32,
    };

    let error = convert(call).expect_err("unspecified action runtime should fail");

    assert_eq!(
        error,
        ConvertError::UnspecifiedEnumValue {
            enum_name: "ActionRuntime",
        }
    );
}
