//! Render a [`ValueExpr`] to a python-like string for debugging/visualization.

use waymark_proto::ast as ir;

use waymark_runner_expr::ValueExpr;

/// Render a [`ValueExpr`] to a python-like string for debugging/visualization.
///
/// Example:
/// - BinaryOpValue(VariableValue("a"), +, LiteralValue(1)) -> "a + 1"
pub fn format_value<NodeId>(expr: &ValueExpr<NodeId>) -> String {
    format_value_inner(expr, 0)
}

/// Recursive [`ValueExpr`] formatter with operator precedence handling.
///
/// Example:
/// - (a + b) * c renders with parentheses when needed.
fn format_value_inner<NodeId>(expr: &ValueExpr<NodeId>, parent_prec: i32) -> String {
    match expr {
        ValueExpr::Literal(lit) => format_literal(&lit.value),
        ValueExpr::Variable(var) => var.name.clone(),
        ValueExpr::ActionResult(value) => value.label(),
        ValueExpr::BinaryOp(value) => {
            let (op_str, prec) = binary_operator(value.op);
            let left = format_value_inner(&value.left, prec);
            let right = format_value_inner(&value.right, prec + 1);
            let rendered = format!("{left} {op_str} {right}");
            if prec < parent_prec {
                format!("({rendered})")
            } else {
                rendered
            }
        }
        ValueExpr::UnaryOp(value) => {
            let (op_str, prec) = unary_operator(value.op);
            let operand = format_value_inner(&value.operand, prec);
            let rendered = format!("{op_str}{operand}");
            if prec < parent_prec {
                format!("({rendered})")
            } else {
                rendered
            }
        }
        ValueExpr::List(value) => {
            let items: Vec<String> = value
                .elements
                .iter()
                .map(|item| format_value_inner(item, 0))
                .collect();
            format!("[{}]", items.join(", "))
        }
        ValueExpr::Dict(value) => {
            let entries: Vec<String> = value
                .entries
                .iter()
                .map(|entry| {
                    format!(
                        "{}: {}",
                        format_value_inner(&entry.key, 0),
                        format_value_inner(&entry.value, 0)
                    )
                })
                .collect();
            format!("{{{}}}", entries.join(", "))
        }
        ValueExpr::Index(value) => {
            let prec = precedence("index");
            let obj = format_value_inner(&value.object, prec);
            let idx = format_value_inner(&value.index, 0);
            let rendered = format!("{obj}[{idx}]");
            if prec < parent_prec {
                format!("({rendered})")
            } else {
                rendered
            }
        }
        ValueExpr::Dot(value) => {
            let prec = precedence("dot");
            let obj = format_value_inner(&value.object, prec);
            let rendered = format!("{obj}.{}", value.attribute);
            if prec < parent_prec {
                format!("({rendered})")
            } else {
                rendered
            }
        }
        ValueExpr::FunctionCall(value) => {
            let mut args: Vec<String> = value
                .args
                .iter()
                .map(|arg| format_value_inner(arg, 0))
                .collect();
            for (name, val) in &value.kwargs {
                args.push(format!("{name}={}", format_value_inner(val, 0)));
            }
            format!("{}({})", value.name, args.join(", "))
        }
        ValueExpr::Spread(value) => {
            let collection = format_value_inner(&value.collection, 0);
            let mut args: Vec<String> = Vec::new();
            for (name, val) in &value.action.kwargs {
                args.push(format!("{name}={}", format_value_inner(val, 0)));
            }
            let call = format!("@{}({})", value.action.action_name, args.join(", "));
            format!("spread {collection}:{} -> {call}", value.loop_var)
        }
    }
}

/// Map binary operator enums to (symbol, precedence) for formatting.
fn binary_operator(op: i32) -> (&'static str, i32) {
    match ir::BinaryOperator::try_from(op).ok() {
        Some(ir::BinaryOperator::BinaryOpOr) => ("or", 10),
        Some(ir::BinaryOperator::BinaryOpAnd) => ("and", 20),
        Some(ir::BinaryOperator::BinaryOpEq) => ("==", 30),
        Some(ir::BinaryOperator::BinaryOpNe) => ("!=", 30),
        Some(ir::BinaryOperator::BinaryOpLt) => ("<", 30),
        Some(ir::BinaryOperator::BinaryOpLe) => ("<=", 30),
        Some(ir::BinaryOperator::BinaryOpGt) => (">", 30),
        Some(ir::BinaryOperator::BinaryOpGe) => (">=", 30),
        Some(ir::BinaryOperator::BinaryOpIn) => ("in", 30),
        Some(ir::BinaryOperator::BinaryOpNotIn) => ("not in", 30),
        Some(ir::BinaryOperator::BinaryOpAdd) => ("+", 40),
        Some(ir::BinaryOperator::BinaryOpSub) => ("-", 40),
        Some(ir::BinaryOperator::BinaryOpMul) => ("*", 50),
        Some(ir::BinaryOperator::BinaryOpDiv) => ("/", 50),
        Some(ir::BinaryOperator::BinaryOpFloorDiv) => ("//", 50),
        Some(ir::BinaryOperator::BinaryOpMod) => ("%", 50),
        _ => ("?", 0),
    }
}

/// Map unary operator enums to (symbol, precedence) for formatting.
fn unary_operator(op: i32) -> (&'static str, i32) {
    match ir::UnaryOperator::try_from(op).ok() {
        Some(ir::UnaryOperator::UnaryOpNeg) => ("-", 60),
        Some(ir::UnaryOperator::UnaryOpNot) => ("not ", 60),
        _ => ("?", 0),
    }
}

/// Return precedence for non-operator constructs like index/dot.
fn precedence(kind: &str) -> i32 {
    match kind {
        "index" | "dot" => 80,
        _ => 0,
    }
}

/// Format Python literals as source-like text.
fn format_literal(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "None".to_string(),
        serde_json::Value::Bool(value) => {
            if *value {
                "True".to_string()
            } else {
                "False".to_string()
            }
        }
        serde_json::Value::String(value) => {
            serde_json::to_string(value).unwrap_or_else(|_| format!("\"{value}\""))
        }
        _ => value.to_string(),
    }
}
