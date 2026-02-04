//! Helper constructors for building IR example programs.

use crate::messages::ast as ir;

pub fn literal(value: &serde_json::Value) -> ir::Expr {
    match value {
        serde_json::Value::Bool(value) => ir::Expr {
            kind: Some(ir::expr::Kind::Literal(ir::Literal {
                value: Some(ir::literal::Value::BoolValue(*value)),
            })),
            span: None,
        },
        serde_json::Value::Number(num) => {
            if let Some(i) = num.as_i64() {
                ir::Expr {
                    kind: Some(ir::expr::Kind::Literal(ir::Literal {
                        value: Some(ir::literal::Value::IntValue(i)),
                    })),
                    span: None,
                }
            } else {
                ir::Expr {
                    kind: Some(ir::expr::Kind::Literal(ir::Literal {
                        value: Some(ir::literal::Value::FloatValue(num.as_f64().unwrap_or(0.0))),
                    })),
                    span: None,
                }
            }
        }
        serde_json::Value::String(value) => ir::Expr {
            kind: Some(ir::expr::Kind::Literal(ir::Literal {
                value: Some(ir::literal::Value::StringValue(value.clone())),
            })),
            span: None,
        },
        serde_json::Value::Null => ir::Expr {
            kind: Some(ir::expr::Kind::Literal(ir::Literal {
                value: Some(ir::literal::Value::IsNone(true)),
            })),
            span: None,
        },
        _ => panic!("unsupported literal"),
    }
}

pub fn variable(name: &str) -> ir::Expr {
    ir::Expr {
        kind: Some(ir::expr::Kind::Variable(ir::Variable {
            name: name.to_string(),
        })),
        span: None,
    }
}

pub fn binary(left: ir::Expr, op: ir::BinaryOperator, right: ir::Expr) -> ir::Expr {
    ir::Expr {
        kind: Some(ir::expr::Kind::BinaryOp(Box::new(ir::BinaryOp {
            left: Some(Box::new(left)),
            op: op as i32,
            right: Some(Box::new(right)),
        }))),
        span: None,
    }
}

pub fn unary(op: ir::UnaryOperator, operand: ir::Expr) -> ir::Expr {
    ir::Expr {
        kind: Some(ir::expr::Kind::UnaryOp(Box::new(ir::UnaryOp {
            op: op as i32,
            operand: Some(Box::new(operand)),
        }))),
        span: None,
    }
}

pub fn list_expr(elements: Vec<ir::Expr>) -> ir::Expr {
    ir::Expr {
        kind: Some(ir::expr::Kind::List(ir::ListExpr { elements })),
        span: None,
    }
}

pub fn dict_entry(key: ir::Expr, value: ir::Expr) -> ir::DictEntry {
    ir::DictEntry {
        key: Some(key),
        value: Some(value),
    }
}

pub fn dict_expr(entries: Vec<(ir::Expr, ir::Expr)>) -> ir::Expr {
    let entries = entries
        .into_iter()
        .map(|(key, value)| dict_entry(key, value))
        .collect();
    ir::Expr {
        kind: Some(ir::expr::Kind::Dict(ir::DictExpr { entries })),
        span: None,
    }
}

pub fn index_expr(object_expr: ir::Expr, index_value: ir::Expr) -> ir::Expr {
    ir::Expr {
        kind: Some(ir::expr::Kind::Index(Box::new(ir::IndexAccess {
            object: Some(Box::new(object_expr)),
            index: Some(Box::new(index_value)),
        }))),
        span: None,
    }
}

pub fn dot_expr(object_expr: ir::Expr, attribute: &str) -> ir::Expr {
    ir::Expr {
        kind: Some(ir::expr::Kind::Dot(Box::new(ir::DotAccess {
            object: Some(Box::new(object_expr)),
            attribute: attribute.to_string(),
        }))),
        span: None,
    }
}

pub fn kwarg(name: &str, value: ir::Expr) -> ir::Kwarg {
    ir::Kwarg {
        name: name.to_string(),
        value: Some(value),
    }
}

pub fn action_call(
    name: &str,
    kwargs: Vec<(&str, ir::Expr)>,
    module_name: Option<&str>,
) -> ir::ActionCall {
    let call_kwargs = kwargs
        .into_iter()
        .map(|(key, value)| kwarg(key, value))
        .collect();
    ir::ActionCall {
        action_name: name.to_string(),
        kwargs: call_kwargs,
        policies: Vec::new(),
        module_name: module_name.map(|value| value.to_string()),
    }
}

pub fn action_expr(
    name: &str,
    kwargs: Vec<(&str, ir::Expr)>,
    module_name: Option<&str>,
) -> ir::Expr {
    ir::Expr {
        kind: Some(ir::expr::Kind::ActionCall(action_call(
            name,
            kwargs,
            module_name,
        ))),
        span: None,
    }
}

pub fn function_call(
    name: &str,
    args: Vec<ir::Expr>,
    kwargs: Vec<(&str, ir::Expr)>,
) -> ir::FunctionCall {
    let call_kwargs = kwargs
        .into_iter()
        .map(|(key, value)| kwarg(key, value))
        .collect();
    ir::FunctionCall {
        name: name.to_string(),
        args,
        kwargs: call_kwargs,
        global_function: ir::GlobalFunction::Unspecified as i32,
    }
}

pub fn function_expr(name: &str, args: Vec<ir::Expr>, kwargs: Vec<(&str, ir::Expr)>) -> ir::Expr {
    ir::Expr {
        kind: Some(ir::expr::Kind::FunctionCall(function_call(
            name, args, kwargs,
        ))),
        span: None,
    }
}

pub fn global_call(
    global_function: ir::GlobalFunction,
    args: Vec<ir::Expr>,
    kwargs: Vec<(&str, ir::Expr)>,
) -> ir::FunctionCall {
    let call_kwargs = kwargs
        .into_iter()
        .map(|(key, value)| kwarg(key, value))
        .collect();
    ir::FunctionCall {
        name: String::new(),
        args,
        kwargs: call_kwargs,
        global_function: global_function as i32,
    }
}

pub fn global_expr(
    global_function: ir::GlobalFunction,
    args: Vec<ir::Expr>,
    kwargs: Vec<(&str, ir::Expr)>,
) -> ir::Expr {
    ir::Expr {
        kind: Some(ir::expr::Kind::FunctionCall(global_call(
            global_function,
            args,
            kwargs,
        ))),
        span: None,
    }
}

pub fn call_action(action: ir::ActionCall) -> ir::Call {
    ir::Call {
        kind: Some(ir::call::Kind::Action(action)),
    }
}

pub fn call_function(call: ir::FunctionCall) -> ir::Call {
    ir::Call {
        kind: Some(ir::call::Kind::Function(call)),
    }
}

pub fn parallel_expr(calls: Vec<ir::Call>) -> ir::Expr {
    ir::Expr {
        kind: Some(ir::expr::Kind::ParallelExpr(ir::ParallelExpr { calls })),
        span: None,
    }
}

pub fn spread_expr(collection: ir::Expr, loop_var: &str, action: ir::ActionCall) -> ir::Expr {
    ir::Expr {
        kind: Some(ir::expr::Kind::SpreadExpr(Box::new(ir::SpreadExpr {
            collection: Some(Box::new(collection)),
            loop_var: loop_var.to_string(),
            action: Some(action),
        }))),
        span: None,
    }
}

pub fn assignment(targets: Vec<&str>, value: ir::Expr) -> ir::Statement {
    ir::Statement {
        kind: Some(ir::statement::Kind::Assignment(ir::Assignment {
            targets: targets.into_iter().map(|item| item.to_string()).collect(),
            value: Some(value),
        })),
        span: None,
    }
}

pub fn return_stmt(value: ir::Expr) -> ir::Statement {
    ir::Statement {
        kind: Some(ir::statement::Kind::ReturnStmt(ir::ReturnStmt {
            value: Some(value),
        })),
        span: None,
    }
}

pub fn break_stmt() -> ir::Statement {
    ir::Statement {
        kind: Some(ir::statement::Kind::BreakStmt(ir::BreakStmt {})),
        span: None,
    }
}

pub fn continue_stmt() -> ir::Statement {
    ir::Statement {
        kind: Some(ir::statement::Kind::ContinueStmt(ir::ContinueStmt {})),
        span: None,
    }
}

pub fn expr_stmt(value: ir::Expr) -> ir::Statement {
    ir::Statement {
        kind: Some(ir::statement::Kind::ExprStmt(ir::ExprStmt {
            expr: Some(value),
        })),
        span: None,
    }
}

pub fn block(statements: Vec<ir::Statement>) -> ir::Block {
    ir::Block {
        statements,
        span: None,
    }
}

pub fn for_loop_stmt(
    loop_vars: Vec<&str>,
    iterable: ir::Expr,
    body_statements: Vec<ir::Statement>,
) -> ir::Statement {
    ir::Statement {
        kind: Some(ir::statement::Kind::ForLoop(ir::ForLoop {
            loop_vars: loop_vars.into_iter().map(|item| item.to_string()).collect(),
            iterable: Some(iterable),
            block_body: Some(block(body_statements)),
        })),
        span: None,
    }
}

pub fn while_loop_stmt(condition: ir::Expr, body_statements: Vec<ir::Statement>) -> ir::Statement {
    ir::Statement {
        kind: Some(ir::statement::Kind::WhileLoop(ir::WhileLoop {
            condition: Some(condition),
            block_body: Some(block(body_statements)),
        })),
        span: None,
    }
}

pub fn if_branch(condition: ir::Expr, body_statements: Vec<ir::Statement>) -> ir::IfBranch {
    ir::IfBranch {
        condition: Some(condition),
        span: None,
        block_body: Some(block(body_statements)),
    }
}

pub fn elif_branch(condition: ir::Expr, body_statements: Vec<ir::Statement>) -> ir::ElifBranch {
    ir::ElifBranch {
        condition: Some(condition),
        span: None,
        block_body: Some(block(body_statements)),
    }
}

pub fn else_branch(body_statements: Vec<ir::Statement>) -> ir::ElseBranch {
    ir::ElseBranch {
        span: None,
        block_body: Some(block(body_statements)),
    }
}

pub fn conditional_stmt(
    if_condition: ir::Expr,
    if_body: Vec<ir::Statement>,
    elifs: Vec<(ir::Expr, Vec<ir::Statement>)>,
    else_body: Option<Vec<ir::Statement>>,
) -> ir::Statement {
    let elif_branches = elifs
        .into_iter()
        .map(|(condition, body)| elif_branch(condition, body))
        .collect();
    let else_branch = else_body.map(else_branch);
    ir::Statement {
        kind: Some(ir::statement::Kind::Conditional(ir::Conditional {
            if_branch: Some(if_branch(if_condition, if_body)),
            elif_branches,
            else_branch,
        })),
        span: None,
    }
}

pub fn except_handler(
    exception_types: Vec<&str>,
    body_statements: Vec<ir::Statement>,
    exception_var: Option<&str>,
) -> ir::ExceptHandler {
    let mut handler = ir::ExceptHandler {
        exception_types: exception_types
            .into_iter()
            .map(|item| item.to_string())
            .collect(),
        span: None,
        block_body: Some(block(body_statements)),
        exception_var: None,
    };
    if let Some(var) = exception_var {
        handler.exception_var = Some(var.to_string());
    }
    handler
}

pub fn try_except_stmt(
    try_body: Vec<ir::Statement>,
    handlers: Vec<ir::ExceptHandler>,
) -> ir::Statement {
    ir::Statement {
        kind: Some(ir::statement::Kind::TryExcept(ir::TryExcept {
            try_block: Some(block(try_body)),
            handlers,
        })),
        span: None,
    }
}

pub fn function_def(
    name: &str,
    inputs: Vec<&str>,
    outputs: Vec<&str>,
    body_statements: Vec<ir::Statement>,
) -> ir::FunctionDef {
    ir::FunctionDef {
        name: name.to_string(),
        io: Some(ir::IoDecl {
            inputs: inputs.into_iter().map(|item| item.to_string()).collect(),
            outputs: outputs.into_iter().map(|item| item.to_string()).collect(),
            span: None,
        }),
        body: Some(block(body_statements)),
        span: None,
    }
}

pub fn program(functions: Vec<ir::FunctionDef>) -> ir::Program {
    ir::Program { functions }
}
