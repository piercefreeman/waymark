use waymark_proto::ast as ir;

use crate::util::is_truthy;

/// Try to fold a literal binary operation to a concrete value.
///
/// Example:
/// - (1, 2, BINARY_OP_ADD) -> 3
pub fn literal_binary(
    op: i32,
    left: &serde_json::Value,
    right: &serde_json::Value,
) -> Option<serde_json::Value> {
    match ir::BinaryOperator::try_from(op).ok() {
        Some(ir::BinaryOperator::BinaryOpAdd) => {
            if let (Some(left), Some(right)) = (left.as_i64(), right.as_i64()) {
                return Some(serde_json::Value::Number((left + right).into()));
            }
            if let (Some(left), Some(right)) = (left.as_f64(), right.as_f64()) {
                return serde_json::Number::from_f64(left + right).map(serde_json::Value::Number);
            }
            if let (Some(left), Some(right)) = (left.as_str(), right.as_str()) {
                return Some(serde_json::Value::String(format!("{left}{right}")));
            }
            None
        }
        Some(ir::BinaryOperator::BinaryOpSub) => {
            if let (Some(left), Some(right)) = (left.as_f64(), right.as_f64()) {
                return serde_json::Number::from_f64(left - right).map(serde_json::Value::Number);
            }
            None
        }
        Some(ir::BinaryOperator::BinaryOpMul) => {
            if let (Some(left), Some(right)) = (left.as_f64(), right.as_f64()) {
                return serde_json::Number::from_f64(left * right).map(serde_json::Value::Number);
            }
            None
        }
        Some(ir::BinaryOperator::BinaryOpDiv) => {
            if let (Some(left), Some(right)) = (left.as_f64(), right.as_f64()) {
                return serde_json::Number::from_f64(left / right).map(serde_json::Value::Number);
            }
            None
        }
        Some(ir::BinaryOperator::BinaryOpFloorDiv) => {
            if let (Some(left), Some(right)) = (left.as_f64(), right.as_f64()) {
                if right == 0.0 {
                    return None;
                }
                let value = (left / right).floor();
                return serde_json::Number::from_f64(value).map(serde_json::Value::Number);
            }
            None
        }
        Some(ir::BinaryOperator::BinaryOpMod) => {
            if let (Some(left), Some(right)) = (left.as_f64(), right.as_f64()) {
                return serde_json::Number::from_f64(left % right).map(serde_json::Value::Number);
            }
            None
        }
        _ => None,
    }
}

/// Try to fold a literal unary operation to a concrete value.
///
/// Example:
/// - (UNARY_OP_NEG, 4) -> -4
pub fn literal_unary(op: i32, operand: &serde_json::Value) -> Option<serde_json::Value> {
    match ir::UnaryOperator::try_from(op).ok() {
        Some(ir::UnaryOperator::UnaryOpNeg) => operand
            .as_f64()
            .and_then(|value| serde_json::Number::from_f64(-value).map(serde_json::Value::Number)),
        Some(ir::UnaryOperator::UnaryOpNot) => Some(serde_json::Value::Bool(!is_truthy(operand))),
        _ => None,
    }
}
