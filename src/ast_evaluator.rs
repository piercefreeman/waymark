//! AST Expression Evaluator
//!
//! Evaluates AST expressions (boolean logic, arithmetic, comparisons, etc.)
//! at runtime against a variable scope.
//!
//! This module provides the core expression evaluation logic used by the DAG runner
//! to evaluate guard expressions, assignments, and inline expressions.

use std::collections::HashMap;

use crate::value::WorkflowValue;
use thiserror::Error;

use crate::parser::ast;

// ============================================================================
// Error Types
// ============================================================================

/// Errors that can occur during expression evaluation.
#[derive(Debug, Error)]
pub enum EvaluationError {
    #[error("Expression evaluation error: {0}")]
    Evaluation(String),

    #[error("Variable not found: {0}")]
    VariableNotFound(String),

    #[error("Function not found: {0}")]
    FunctionNotFound(String),
}

pub type EvaluationResult<T> = Result<T, EvaluationError>;

// ============================================================================
// Scope
// ============================================================================

/// Scope for inline expression evaluation.
/// This is a simple in-memory variable map used during inline node execution.
pub type Scope = HashMap<String, WorkflowValue>;

// ============================================================================
// Expression Evaluator
// ============================================================================

/// Evaluates AST expressions in a given context.
pub struct ExpressionEvaluator;

impl ExpressionEvaluator {
    /// Evaluate an expression to a runtime value.
    pub fn evaluate(expr: &ast::Expr, scope: &Scope) -> EvaluationResult<WorkflowValue> {
        let kind = expr
            .kind
            .as_ref()
            .ok_or_else(|| EvaluationError::Evaluation("Empty expression".to_string()))?;

        match kind {
            ast::expr::Kind::Literal(lit) => Self::eval_literal(lit),
            ast::expr::Kind::Variable(var) => Self::eval_variable(var, scope),
            ast::expr::Kind::BinaryOp(op) => Self::eval_binary_op(op, scope),
            ast::expr::Kind::UnaryOp(op) => Self::eval_unary_op(op, scope),
            ast::expr::Kind::List(list) => Self::eval_list(list, scope),
            ast::expr::Kind::Dict(dict) => Self::eval_dict(dict, scope),
            ast::expr::Kind::Index(idx) => Self::eval_index(idx, scope),
            ast::expr::Kind::Dot(dot) => Self::eval_dot(dot, scope),
            ast::expr::Kind::FunctionCall(call) => Self::eval_function_call(call, scope),
            ast::expr::Kind::ActionCall(_) => Err(EvaluationError::Evaluation(
                "Action calls cannot be evaluated inline".to_string(),
            )),
            ast::expr::Kind::ParallelExpr(_) => Err(EvaluationError::Evaluation(
                "Parallel expressions cannot be evaluated inline".to_string(),
            )),
            ast::expr::Kind::SpreadExpr(_) => Err(EvaluationError::Evaluation(
                "Spread expressions cannot be evaluated inline".to_string(),
            )),
        }
    }

    fn eval_literal(lit: &ast::Literal) -> EvaluationResult<WorkflowValue> {
        let value = lit
            .value
            .as_ref()
            .ok_or_else(|| EvaluationError::Evaluation("Empty literal".to_string()))?;

        Ok(match value {
            ast::literal::Value::IntValue(i) => WorkflowValue::Int(*i),
            ast::literal::Value::FloatValue(f) => WorkflowValue::Float(*f),
            ast::literal::Value::StringValue(s) => WorkflowValue::String(s.clone()),
            ast::literal::Value::BoolValue(b) => WorkflowValue::Bool(*b),
            ast::literal::Value::IsNone(true) => WorkflowValue::Null,
            ast::literal::Value::IsNone(false) => WorkflowValue::Null,
        })
    }

    fn eval_variable(var: &ast::Variable, scope: &Scope) -> EvaluationResult<WorkflowValue> {
        let value = scope
            .get(&var.name)
            .cloned()
            .ok_or_else(|| EvaluationError::VariableNotFound(var.name.clone()))?;

        if var.name == "__loop_loop_8_i" {
            tracing::debug!(
                var_name = %var.name,
                value = ?value,
                "eval_variable __loop_loop_8_i"
            );
        }

        Ok(value)
    }

    fn eval_binary_op(op: &ast::BinaryOp, scope: &Scope) -> EvaluationResult<WorkflowValue> {
        let left_expr = op
            .left
            .as_ref()
            .ok_or_else(|| EvaluationError::Evaluation("Missing left operand".to_string()))?;
        let right_expr = op
            .right
            .as_ref()
            .ok_or_else(|| EvaluationError::Evaluation("Missing right operand".to_string()))?;

        let left = Self::evaluate(left_expr, scope)?;
        let right = Self::evaluate(right_expr, scope)?;

        match ast::BinaryOperator::try_from(op.op)
            .unwrap_or(ast::BinaryOperator::BinaryOpUnspecified)
        {
            ast::BinaryOperator::BinaryOpAdd => Self::apply_add(&left, &right),
            ast::BinaryOperator::BinaryOpSub => Self::apply_sub(&left, &right),
            ast::BinaryOperator::BinaryOpMul => Self::apply_mul(&left, &right),
            ast::BinaryOperator::BinaryOpDiv => Self::apply_div(&left, &right),
            ast::BinaryOperator::BinaryOpEq => Ok(WorkflowValue::Bool(left == right)),
            ast::BinaryOperator::BinaryOpNe => Ok(WorkflowValue::Bool(left != right)),
            ast::BinaryOperator::BinaryOpLt => Self::apply_lt(&left, &right),
            ast::BinaryOperator::BinaryOpLe => Self::apply_le(&left, &right),
            ast::BinaryOperator::BinaryOpGt => Self::apply_gt(&left, &right),
            ast::BinaryOperator::BinaryOpGe => Self::apply_ge(&left, &right),
            ast::BinaryOperator::BinaryOpAnd => Self::apply_and(&left, &right),
            ast::BinaryOperator::BinaryOpOr => Self::apply_or(&left, &right),
            ast::BinaryOperator::BinaryOpIn => Self::apply_in(&left, &right),
            ast::BinaryOperator::BinaryOpNotIn => {
                let result = Self::apply_in(&left, &right)?;
                let is_in = matches!(result, WorkflowValue::Bool(true));
                Ok(WorkflowValue::Bool(!is_in))
            }
            _ => Err(EvaluationError::Evaluation(
                "Unknown binary operator".to_string(),
            )),
        }
    }

    fn eval_unary_op(op: &ast::UnaryOp, scope: &Scope) -> EvaluationResult<WorkflowValue> {
        let operand_expr = op
            .operand
            .as_ref()
            .ok_or_else(|| EvaluationError::Evaluation("Missing operand".to_string()))?;

        let operand = Self::evaluate(operand_expr, scope)?;

        match ast::UnaryOperator::try_from(op.op).unwrap_or(ast::UnaryOperator::UnaryOpUnspecified)
        {
            ast::UnaryOperator::UnaryOpNeg => match &operand {
                WorkflowValue::Int(i) => Ok(WorkflowValue::Int(-i)),
                WorkflowValue::Float(f) => Ok(WorkflowValue::Float(-f)),
                _ => Err(EvaluationError::Evaluation(
                    "Cannot negate non-numeric value".to_string(),
                )),
            },
            ast::UnaryOperator::UnaryOpNot => Ok(WorkflowValue::Bool(!Self::is_truthy(&operand))),
            _ => Err(EvaluationError::Evaluation(
                "Unknown unary operator".to_string(),
            )),
        }
    }

    fn eval_list(list: &ast::ListExpr, scope: &Scope) -> EvaluationResult<WorkflowValue> {
        let elements: Result<Vec<WorkflowValue>, _> = list
            .elements
            .iter()
            .map(|e| Self::evaluate(e, scope))
            .collect();
        Ok(WorkflowValue::List(elements?))
    }

    fn eval_dict(dict: &ast::DictExpr, scope: &Scope) -> EvaluationResult<WorkflowValue> {
        let mut map = HashMap::new();
        for entry in &dict.entries {
            let key_expr = entry
                .key
                .as_ref()
                .ok_or_else(|| EvaluationError::Evaluation("Missing dict key".to_string()))?;
            let val_expr = entry
                .value
                .as_ref()
                .ok_or_else(|| EvaluationError::Evaluation("Missing dict value".to_string()))?;

            let key = Self::evaluate(key_expr, scope)?;
            let val = Self::evaluate(val_expr, scope)?;

            let key_str = match key {
                WorkflowValue::String(s) => s,
                other => other.to_key_string(),
            };
            map.insert(key_str, val);
        }
        Ok(WorkflowValue::Dict(map))
    }

    fn eval_index(idx: &ast::IndexAccess, scope: &Scope) -> EvaluationResult<WorkflowValue> {
        let obj_expr = idx
            .object
            .as_ref()
            .ok_or_else(|| EvaluationError::Evaluation("Missing index object".to_string()))?;
        let idx_expr = idx
            .index
            .as_ref()
            .ok_or_else(|| EvaluationError::Evaluation("Missing index".to_string()))?;

        let obj = Self::evaluate(obj_expr, scope)?;
        let index = Self::evaluate(idx_expr, scope)?;

        tracing::debug!(
            obj = ?obj,
            index = ?index,
            idx_expr = ?idx_expr,
            "eval_index"
        );

        match (&obj, &index) {
            (WorkflowValue::List(arr), WorkflowValue::Int(i)) => {
                let idx = *i as usize;
                arr.get(idx).cloned().ok_or_else(|| {
                    EvaluationError::Evaluation(format!("Index {} out of bounds", idx))
                })
            }
            (WorkflowValue::Tuple(arr), WorkflowValue::Int(i)) => {
                let idx = *i as usize;
                arr.get(idx).cloned().ok_or_else(|| {
                    EvaluationError::Evaluation(format!("Index {} out of bounds", idx))
                })
            }
            (WorkflowValue::Dict(map), WorkflowValue::String(key)) => map
                .get(key)
                .cloned()
                .ok_or_else(|| EvaluationError::Evaluation(format!("Key '{}' not found", key))),
            (WorkflowValue::Dict(map), other) => {
                let key = other.to_key_string();
                map.get(&key)
                    .cloned()
                    .ok_or_else(|| EvaluationError::Evaluation(format!("Key '{}' not found", key)))
            }
            (WorkflowValue::Exception { .. }, _) => {
                let key = match &index {
                    WorkflowValue::String(key) => key.clone(),
                    other => other.to_key_string(),
                };
                Self::exception_field(&obj, &key)
                    .ok_or_else(|| EvaluationError::Evaluation(format!("Key '{}' not found", key)))
            }
            (WorkflowValue::String(s), WorkflowValue::Int(i)) => {
                let idx = *i as usize;
                s.chars()
                    .nth(idx)
                    .map(|c| WorkflowValue::String(c.to_string()))
                    .ok_or_else(|| {
                        EvaluationError::Evaluation(format!("Index {} out of bounds", idx))
                    })
            }
            _ => Err(EvaluationError::Evaluation(
                "Invalid index operation".to_string(),
            )),
        }
    }

    fn eval_dot(dot: &ast::DotAccess, scope: &Scope) -> EvaluationResult<WorkflowValue> {
        let obj_expr = dot
            .object
            .as_ref()
            .ok_or_else(|| EvaluationError::Evaluation("Missing dot object".to_string()))?;

        let obj = Self::evaluate(obj_expr, scope)?;

        match &obj {
            WorkflowValue::Dict(map) => map.get(&dot.attribute).cloned().ok_or_else(|| {
                EvaluationError::Evaluation(format!("Attribute '{}' not found", dot.attribute))
            }),
            WorkflowValue::Exception { .. } => Self::exception_field(&obj, &dot.attribute)
                .ok_or_else(|| {
                    EvaluationError::Evaluation(format!("Attribute '{}' not found", dot.attribute))
                }),
            _ => Err(EvaluationError::Evaluation(
                "Dot access on non-object".to_string(),
            )),
        }
    }

    fn exception_field(value: &WorkflowValue, key: &str) -> Option<WorkflowValue> {
        let WorkflowValue::Exception {
            exc_type,
            module,
            message,
            traceback,
            values,
            ..
        } = value
        else {
            return None;
        };
        match key {
            "type" => Some(WorkflowValue::String(exc_type.clone())),
            "module" => Some(WorkflowValue::String(module.clone())),
            "message" => Some(WorkflowValue::String(message.clone())),
            "traceback" => Some(WorkflowValue::String(traceback.clone())),
            "values" => Some(WorkflowValue::Dict(values.clone())),
            _ => values.get(key).cloned(),
        }
    }

    fn eval_function_call(
        call: &ast::FunctionCall,
        scope: &Scope,
    ) -> EvaluationResult<WorkflowValue> {
        // Evaluate positional args
        let mut args = Vec::new();
        for arg_expr in &call.args {
            let val = Self::evaluate(arg_expr, scope)?;
            args.push(val);
        }

        // Evaluate kwargs
        let mut kwargs = HashMap::new();
        for kwarg in &call.kwargs {
            let val_expr = kwarg
                .value
                .as_ref()
                .ok_or_else(|| EvaluationError::Evaluation("Missing kwarg value".to_string()))?;
            let val = Self::evaluate(val_expr, scope)?;
            kwargs.insert(kwarg.name.clone(), val);
        }

        // Built-in functions - support both positional and keyword args
        let global_function = ast::GlobalFunction::try_from(call.global_function)
            .unwrap_or(ast::GlobalFunction::Unspecified);
        match global_function {
            ast::GlobalFunction::Range => Self::builtin_range(&args, &kwargs),
            ast::GlobalFunction::Len => Self::builtin_len(&args, &kwargs),
            ast::GlobalFunction::Enumerate => Self::builtin_enumerate(&args, &kwargs),
            ast::GlobalFunction::Unspecified => match call.name.as_str() {
                "range" => Self::builtin_range(&args, &kwargs),
                "len" => Self::builtin_len(&args, &kwargs),
                "enumerate" => Self::builtin_enumerate(&args, &kwargs),
                _ => Err(EvaluationError::FunctionNotFound(call.name.clone())),
            },
        }
    }

    // Helper methods for operators
    fn apply_add(left: &WorkflowValue, right: &WorkflowValue) -> EvaluationResult<WorkflowValue> {
        match (left, right) {
            (WorkflowValue::Int(a), WorkflowValue::Int(b)) => Ok(WorkflowValue::Int(a + b)),
            (WorkflowValue::Int(a), WorkflowValue::Float(b)) => {
                Ok(WorkflowValue::Float(*a as f64 + b))
            }
            (WorkflowValue::Float(a), WorkflowValue::Int(b)) => {
                Ok(WorkflowValue::Float(a + *b as f64))
            }
            (WorkflowValue::Float(a), WorkflowValue::Float(b)) => Ok(WorkflowValue::Float(a + b)),
            (WorkflowValue::String(a), WorkflowValue::String(b)) => {
                Ok(WorkflowValue::String(format!("{a}{b}")))
            }
            (WorkflowValue::List(a), WorkflowValue::List(b)) => {
                let mut result = a.clone();
                result.extend(b.clone());
                Ok(WorkflowValue::List(result))
            }
            _ => Err(EvaluationError::Evaluation(
                "Cannot add incompatible types".to_string(),
            )),
        }
    }

    fn apply_sub(left: &WorkflowValue, right: &WorkflowValue) -> EvaluationResult<WorkflowValue> {
        match (left, right) {
            (WorkflowValue::Int(a), WorkflowValue::Int(b)) => Ok(WorkflowValue::Int(a - b)),
            (WorkflowValue::Int(a), WorkflowValue::Float(b)) => {
                Ok(WorkflowValue::Float(*a as f64 - b))
            }
            (WorkflowValue::Float(a), WorkflowValue::Int(b)) => {
                Ok(WorkflowValue::Float(a - *b as f64))
            }
            (WorkflowValue::Float(a), WorkflowValue::Float(b)) => Ok(WorkflowValue::Float(a - b)),
            _ => Err(EvaluationError::Evaluation(
                "Cannot subtract non-numbers".to_string(),
            )),
        }
    }

    fn apply_mul(left: &WorkflowValue, right: &WorkflowValue) -> EvaluationResult<WorkflowValue> {
        match (left, right) {
            (WorkflowValue::Int(a), WorkflowValue::Int(b)) => Ok(WorkflowValue::Int(a * b)),
            (WorkflowValue::Int(a), WorkflowValue::Float(b)) => {
                Ok(WorkflowValue::Float(*a as f64 * b))
            }
            (WorkflowValue::Float(a), WorkflowValue::Int(b)) => {
                Ok(WorkflowValue::Float(a * *b as f64))
            }
            (WorkflowValue::Float(a), WorkflowValue::Float(b)) => Ok(WorkflowValue::Float(a * b)),
            _ => Err(EvaluationError::Evaluation(
                "Cannot multiply non-numbers".to_string(),
            )),
        }
    }

    fn apply_div(left: &WorkflowValue, right: &WorkflowValue) -> EvaluationResult<WorkflowValue> {
        let Some(af) = left.as_f64() else {
            return Err(EvaluationError::Evaluation(
                "Cannot divide non-numbers".to_string(),
            ));
        };
        let Some(bf) = right.as_f64() else {
            return Err(EvaluationError::Evaluation(
                "Cannot divide non-numbers".to_string(),
            ));
        };
        if bf == 0.0 {
            return Err(EvaluationError::Evaluation("Division by zero".to_string()));
        }
        Ok(WorkflowValue::Float(af / bf))
    }

    fn apply_lt(left: &WorkflowValue, right: &WorkflowValue) -> EvaluationResult<WorkflowValue> {
        match (left, right) {
            (WorkflowValue::String(a), WorkflowValue::String(b)) => Ok(WorkflowValue::Bool(a < b)),
            _ => {
                let (Some(af), Some(bf)) = (left.as_f64(), right.as_f64()) else {
                    return Err(EvaluationError::Evaluation(
                        "Cannot compare incompatible types".to_string(),
                    ));
                };
                Ok(WorkflowValue::Bool(af < bf))
            }
        }
    }

    fn apply_le(left: &WorkflowValue, right: &WorkflowValue) -> EvaluationResult<WorkflowValue> {
        match (left, right) {
            (WorkflowValue::String(a), WorkflowValue::String(b)) => Ok(WorkflowValue::Bool(a <= b)),
            _ => {
                let (Some(af), Some(bf)) = (left.as_f64(), right.as_f64()) else {
                    return Err(EvaluationError::Evaluation(
                        "Cannot compare incompatible types".to_string(),
                    ));
                };
                Ok(WorkflowValue::Bool(af <= bf))
            }
        }
    }

    fn apply_gt(left: &WorkflowValue, right: &WorkflowValue) -> EvaluationResult<WorkflowValue> {
        match (left, right) {
            (WorkflowValue::String(a), WorkflowValue::String(b)) => Ok(WorkflowValue::Bool(a > b)),
            _ => {
                let (Some(af), Some(bf)) = (left.as_f64(), right.as_f64()) else {
                    return Err(EvaluationError::Evaluation(
                        "Cannot compare incompatible types".to_string(),
                    ));
                };
                Ok(WorkflowValue::Bool(af > bf))
            }
        }
    }

    fn apply_ge(left: &WorkflowValue, right: &WorkflowValue) -> EvaluationResult<WorkflowValue> {
        match (left, right) {
            (WorkflowValue::String(a), WorkflowValue::String(b)) => Ok(WorkflowValue::Bool(a >= b)),
            _ => {
                let (Some(af), Some(bf)) = (left.as_f64(), right.as_f64()) else {
                    return Err(EvaluationError::Evaluation(
                        "Cannot compare incompatible types".to_string(),
                    ));
                };
                Ok(WorkflowValue::Bool(af >= bf))
            }
        }
    }

    fn apply_and(left: &WorkflowValue, right: &WorkflowValue) -> EvaluationResult<WorkflowValue> {
        Ok(WorkflowValue::Bool(
            Self::is_truthy(left) && Self::is_truthy(right),
        ))
    }

    fn apply_or(left: &WorkflowValue, right: &WorkflowValue) -> EvaluationResult<WorkflowValue> {
        Ok(WorkflowValue::Bool(
            Self::is_truthy(left) || Self::is_truthy(right),
        ))
    }

    fn apply_in(left: &WorkflowValue, right: &WorkflowValue) -> EvaluationResult<WorkflowValue> {
        match right {
            WorkflowValue::List(arr) | WorkflowValue::Tuple(arr) => {
                Ok(WorkflowValue::Bool(arr.contains(left)))
            }
            WorkflowValue::Dict(map) => {
                let key = match left {
                    WorkflowValue::String(s) => s.clone(),
                    other => other.to_key_string(),
                };
                Ok(WorkflowValue::Bool(map.contains_key(&key)))
            }
            WorkflowValue::String(s) => {
                let needle = match left {
                    WorkflowValue::String(n) => n.clone(),
                    other => other.to_key_string(),
                };
                Ok(WorkflowValue::Bool(s.contains(&needle)))
            }
            _ => Err(EvaluationError::Evaluation(
                "'in' requires list, tuple, dict, or string".to_string(),
            )),
        }
    }

    /// Check if a value is truthy (Python-style semantics).
    pub fn is_truthy(value: &WorkflowValue) -> bool {
        value.is_truthy()
    }

    // Built-in functions - support both positional args and kwargs
    // Positional args take precedence over kwargs

    fn builtin_range(
        args: &[WorkflowValue],
        kwargs: &HashMap<String, WorkflowValue>,
    ) -> EvaluationResult<WorkflowValue> {
        // range(stop) or range(start, stop) or range(start, stop, step)
        let (start, stop, step) = match args.len() {
            0 => {
                // Fall back to kwargs
                let start = kwargs
                    .get("start")
                    .and_then(WorkflowValue::as_i64)
                    .unwrap_or(0);
                let stop = kwargs
                    .get("stop")
                    .and_then(WorkflowValue::as_i64)
                    .ok_or_else(|| {
                        EvaluationError::Evaluation("range() requires 'stop' argument".to_string())
                    })?;
                let step = kwargs
                    .get("step")
                    .and_then(WorkflowValue::as_i64)
                    .unwrap_or(1);
                (start, stop, step)
            }
            1 => {
                // range(stop)
                let stop = args[0].as_i64().ok_or_else(|| {
                    EvaluationError::Evaluation("range() argument must be integer".to_string())
                })?;
                (0, stop, 1)
            }
            2 => {
                // range(start, stop)
                let start = args[0].as_i64().ok_or_else(|| {
                    EvaluationError::Evaluation("range() start must be integer".to_string())
                })?;
                let stop = args[1].as_i64().ok_or_else(|| {
                    EvaluationError::Evaluation("range() stop must be integer".to_string())
                })?;
                (start, stop, 1)
            }
            _ => {
                // range(start, stop, step)
                let start = args[0].as_i64().ok_or_else(|| {
                    EvaluationError::Evaluation("range() start must be integer".to_string())
                })?;
                let stop = args[1].as_i64().ok_or_else(|| {
                    EvaluationError::Evaluation("range() stop must be integer".to_string())
                })?;
                let step = args[2].as_i64().ok_or_else(|| {
                    EvaluationError::Evaluation("range() step must be integer".to_string())
                })?;
                (start, stop, step)
            }
        };

        if step == 0 {
            return Err(EvaluationError::Evaluation(
                "range() step cannot be zero".to_string(),
            ));
        }

        let mut result = Vec::new();
        let mut i = start;
        while (step > 0 && i < stop) || (step < 0 && i > stop) {
            result.push(WorkflowValue::Int(i));
            i += step;
        }

        Ok(WorkflowValue::List(result))
    }

    fn builtin_len(
        args: &[WorkflowValue],
        kwargs: &HashMap<String, WorkflowValue>,
    ) -> EvaluationResult<WorkflowValue> {
        // len(items)
        let items = if !args.is_empty() {
            &args[0]
        } else {
            kwargs.get("items").ok_or_else(|| {
                EvaluationError::Evaluation("len() requires an argument".to_string())
            })?
        };

        let len = match items {
            WorkflowValue::List(a) => a.len(),
            WorkflowValue::Tuple(a) => a.len(),
            WorkflowValue::Dict(o) => o.len(),
            WorkflowValue::String(s) => s.len(),
            _ => {
                return Err(EvaluationError::Evaluation(
                    "len() requires array, object, or string".to_string(),
                ));
            }
        };

        Ok(WorkflowValue::Int(len as i64))
    }

    fn builtin_enumerate(
        args: &[WorkflowValue],
        kwargs: &HashMap<String, WorkflowValue>,
    ) -> EvaluationResult<WorkflowValue> {
        // enumerate(items)
        let items = if !args.is_empty() {
            &args[0]
        } else {
            kwargs.get("items").ok_or_else(|| {
                EvaluationError::Evaluation("enumerate() requires an argument".to_string())
            })?
        };

        let arr = match items {
            WorkflowValue::List(a) => a,
            _ => {
                return Err(EvaluationError::Evaluation(
                    "enumerate() requires array".to_string(),
                ));
            }
        };

        let result: Vec<WorkflowValue> = arr
            .iter()
            .enumerate()
            .map(|(i, v)| WorkflowValue::Tuple(vec![WorkflowValue::Int(i as i64), v.clone()]))
            .collect();

        Ok(WorkflowValue::List(result))
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    // Helper to create a literal expression
    fn int_literal(value: i64) -> ast::Expr {
        ast::Expr {
            kind: Some(ast::expr::Kind::Literal(ast::Literal {
                value: Some(ast::literal::Value::IntValue(value)),
            })),
            span: None,
        }
    }

    fn float_literal(value: f64) -> ast::Expr {
        ast::Expr {
            kind: Some(ast::expr::Kind::Literal(ast::Literal {
                value: Some(ast::literal::Value::FloatValue(value)),
            })),
            span: None,
        }
    }

    fn string_literal(value: &str) -> ast::Expr {
        ast::Expr {
            kind: Some(ast::expr::Kind::Literal(ast::Literal {
                value: Some(ast::literal::Value::StringValue(value.to_string())),
            })),
            span: None,
        }
    }

    fn bool_literal(value: bool) -> ast::Expr {
        ast::Expr {
            kind: Some(ast::expr::Kind::Literal(ast::Literal {
                value: Some(ast::literal::Value::BoolValue(value)),
            })),
            span: None,
        }
    }

    fn none_literal() -> ast::Expr {
        ast::Expr {
            kind: Some(ast::expr::Kind::Literal(ast::Literal {
                value: Some(ast::literal::Value::IsNone(true)),
            })),
            span: None,
        }
    }

    fn variable(name: &str) -> ast::Expr {
        ast::Expr {
            kind: Some(ast::expr::Kind::Variable(ast::Variable {
                name: name.to_string(),
            })),
            span: None,
        }
    }

    fn binary_op(left: ast::Expr, op: ast::BinaryOperator, right: ast::Expr) -> ast::Expr {
        ast::Expr {
            kind: Some(ast::expr::Kind::BinaryOp(Box::new(ast::BinaryOp {
                left: Some(Box::new(left)),
                op: op as i32,
                right: Some(Box::new(right)),
            }))),
            span: None,
        }
    }

    fn unary_op(op: ast::UnaryOperator, operand: ast::Expr) -> ast::Expr {
        ast::Expr {
            kind: Some(ast::expr::Kind::UnaryOp(Box::new(ast::UnaryOp {
                op: op as i32,
                operand: Some(Box::new(operand)),
            }))),
            span: None,
        }
    }

    fn list_expr(elements: Vec<ast::Expr>) -> ast::Expr {
        ast::Expr {
            kind: Some(ast::expr::Kind::List(ast::ListExpr { elements })),
            span: None,
        }
    }

    fn dict_expr(entries: Vec<(ast::Expr, ast::Expr)>) -> ast::Expr {
        ast::Expr {
            kind: Some(ast::expr::Kind::Dict(ast::DictExpr {
                entries: entries
                    .into_iter()
                    .map(|(k, v)| ast::DictEntry {
                        key: Some(k),
                        value: Some(v),
                    })
                    .collect(),
            })),
            span: None,
        }
    }

    fn index_access(object: ast::Expr, index: ast::Expr) -> ast::Expr {
        ast::Expr {
            kind: Some(ast::expr::Kind::Index(Box::new(ast::IndexAccess {
                object: Some(Box::new(object)),
                index: Some(Box::new(index)),
            }))),
            span: None,
        }
    }

    fn dot_access(object: ast::Expr, attribute: &str) -> ast::Expr {
        ast::Expr {
            kind: Some(ast::expr::Kind::Dot(Box::new(ast::DotAccess {
                object: Some(Box::new(object)),
                attribute: attribute.to_string(),
            }))),
            span: None,
        }
    }

    fn function_call(name: &str, kwargs: Vec<(&str, ast::Expr)>) -> ast::Expr {
        ast::Expr {
            kind: Some(ast::expr::Kind::FunctionCall(ast::FunctionCall {
                name: name.to_string(),
                args: vec![],
                kwargs: kwargs
                    .into_iter()
                    .map(|(k, v)| ast::Kwarg {
                        name: k.to_string(),
                        value: Some(v),
                    })
                    .collect(),
                global_function: global_function_for_name(name) as i32,
            })),
            span: None,
        }
    }

    fn function_call_without_global(name: &str, kwargs: Vec<(&str, ast::Expr)>) -> ast::Expr {
        ast::Expr {
            kind: Some(ast::expr::Kind::FunctionCall(ast::FunctionCall {
                name: name.to_string(),
                args: vec![],
                kwargs: kwargs
                    .into_iter()
                    .map(|(k, v)| ast::Kwarg {
                        name: k.to_string(),
                        value: Some(v),
                    })
                    .collect(),
                global_function: ast::GlobalFunction::Unspecified as i32,
            })),
            span: None,
        }
    }

    fn global_function_for_name(name: &str) -> ast::GlobalFunction {
        match name {
            "range" => ast::GlobalFunction::Range,
            "len" => ast::GlobalFunction::Len,
            "enumerate" => ast::GlobalFunction::Enumerate,
            _ => ast::GlobalFunction::Unspecified,
        }
    }

    // ========================================================================
    // Literal Tests
    // ========================================================================

    #[test]
    fn test_eval_int_literal() {
        let scope = Scope::new();
        let expr = int_literal(42);
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Int(42));
    }

    #[test]
    fn test_eval_float_literal() {
        let scope = Scope::new();
        let expr = float_literal(1.5);
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Float(1.5));
    }

    #[test]
    fn test_eval_string_literal() {
        let scope = Scope::new();
        let expr = string_literal("hello");
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::String("hello".to_string()));
    }

    #[test]
    fn test_eval_bool_literal() {
        let scope = Scope::new();
        let expr = bool_literal(true);
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Bool(true));
    }

    #[test]
    fn test_eval_none_literal() {
        let scope = Scope::new();
        let expr = none_literal();
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Null);
    }

    // ========================================================================
    // Variable Tests
    // ========================================================================

    #[test]
    fn test_eval_variable_found() {
        let mut scope = Scope::new();
        scope.insert("x".to_string(), WorkflowValue::Int(10.into()));
        let expr = variable("x");
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Int(10));
    }

    #[test]
    fn test_eval_variable_not_found() {
        let scope = Scope::new();
        let expr = variable("x");
        let result = ExpressionEvaluator::evaluate(&expr, &scope);
        assert!(matches!(result, Err(EvaluationError::VariableNotFound(_))));
    }

    // ========================================================================
    // Arithmetic Operator Tests
    // ========================================================================

    #[test]
    fn test_eval_add_integers() {
        let scope = Scope::new();
        let expr = binary_op(
            int_literal(10),
            ast::BinaryOperator::BinaryOpAdd,
            int_literal(5),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Int(15));
    }

    #[test]
    fn test_eval_add_floats() {
        let scope = Scope::new();
        let expr = binary_op(
            float_literal(1.5),
            ast::BinaryOperator::BinaryOpAdd,
            float_literal(2.5),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Float(4.0));
    }

    #[test]
    fn test_eval_add_strings() {
        let scope = Scope::new();
        let expr = binary_op(
            string_literal("hello"),
            ast::BinaryOperator::BinaryOpAdd,
            string_literal(" world"),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::String("hello world".to_string()));
    }

    #[test]
    fn test_eval_add_arrays() {
        let scope = Scope::new();
        let expr = binary_op(
            list_expr(vec![int_literal(1), int_literal(2)]),
            ast::BinaryOperator::BinaryOpAdd,
            list_expr(vec![int_literal(3)]),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(
            result,
            WorkflowValue::List(vec![
                WorkflowValue::Int(1.into()),
                WorkflowValue::Int(2.into()),
                WorkflowValue::Int(3.into()),
            ])
        );
    }

    #[test]
    fn test_eval_subtract() {
        let scope = Scope::new();
        let expr = binary_op(
            int_literal(10),
            ast::BinaryOperator::BinaryOpSub,
            int_literal(3),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Int(7.into()));
    }

    #[test]
    fn test_eval_multiply() {
        let scope = Scope::new();
        let expr = binary_op(
            int_literal(6),
            ast::BinaryOperator::BinaryOpMul,
            int_literal(7),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Int(42.into()));
    }

    #[test]
    fn test_eval_divide() {
        let scope = Scope::new();
        let expr = binary_op(
            int_literal(10),
            ast::BinaryOperator::BinaryOpDiv,
            int_literal(4),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Float(2.5));
    }

    #[test]
    fn test_eval_divide_by_zero() {
        let scope = Scope::new();
        let expr = binary_op(
            int_literal(10),
            ast::BinaryOperator::BinaryOpDiv,
            int_literal(0),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope);
        assert!(matches!(result, Err(EvaluationError::Evaluation(_))));
    }

    // ========================================================================
    // Comparison Operator Tests
    // ========================================================================

    #[test]
    fn test_eval_equal() {
        let scope = Scope::new();
        let expr = binary_op(
            int_literal(5),
            ast::BinaryOperator::BinaryOpEq,
            int_literal(5),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Bool(true));
    }

    #[test]
    fn test_eval_not_equal() {
        let scope = Scope::new();
        let expr = binary_op(
            int_literal(5),
            ast::BinaryOperator::BinaryOpNe,
            int_literal(3),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Bool(true));
    }

    #[test]
    fn test_eval_less_than() {
        let scope = Scope::new();
        let expr = binary_op(
            int_literal(3),
            ast::BinaryOperator::BinaryOpLt,
            int_literal(5),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Bool(true));
    }

    #[test]
    fn test_eval_less_than_or_equal() {
        let scope = Scope::new();
        let expr = binary_op(
            int_literal(5),
            ast::BinaryOperator::BinaryOpLe,
            int_literal(5),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Bool(true));
    }

    #[test]
    fn test_eval_greater_than() {
        let scope = Scope::new();
        let expr = binary_op(
            int_literal(5),
            ast::BinaryOperator::BinaryOpGt,
            int_literal(3),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Bool(true));
    }

    #[test]
    fn test_eval_greater_than_or_equal() {
        let scope = Scope::new();
        let expr = binary_op(
            int_literal(5),
            ast::BinaryOperator::BinaryOpGe,
            int_literal(5),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Bool(true));
    }

    #[test]
    fn test_eval_string_comparison() {
        let scope = Scope::new();
        let expr = binary_op(
            string_literal("apple"),
            ast::BinaryOperator::BinaryOpLt,
            string_literal("banana"),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Bool(true));
    }

    // ========================================================================
    // Boolean Operator Tests
    // ========================================================================

    #[test]
    fn test_eval_and_true_true() {
        let scope = Scope::new();
        let expr = binary_op(
            bool_literal(true),
            ast::BinaryOperator::BinaryOpAnd,
            bool_literal(true),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Bool(true));
    }

    #[test]
    fn test_eval_and_true_false() {
        let scope = Scope::new();
        let expr = binary_op(
            bool_literal(true),
            ast::BinaryOperator::BinaryOpAnd,
            bool_literal(false),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Bool(false));
    }

    #[test]
    fn test_eval_or_false_true() {
        let scope = Scope::new();
        let expr = binary_op(
            bool_literal(false),
            ast::BinaryOperator::BinaryOpOr,
            bool_literal(true),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Bool(true));
    }

    #[test]
    fn test_eval_or_false_false() {
        let scope = Scope::new();
        let expr = binary_op(
            bool_literal(false),
            ast::BinaryOperator::BinaryOpOr,
            bool_literal(false),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Bool(false));
    }

    #[test]
    fn test_eval_not_true() {
        let scope = Scope::new();
        let expr = unary_op(ast::UnaryOperator::UnaryOpNot, bool_literal(true));
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Bool(false));
    }

    #[test]
    fn test_eval_not_false() {
        let scope = Scope::new();
        let expr = unary_op(ast::UnaryOperator::UnaryOpNot, bool_literal(false));
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Bool(true));
    }

    #[test]
    fn test_eval_unary_negate() {
        let scope = Scope::new();
        let expr = unary_op(ast::UnaryOperator::UnaryOpNeg, int_literal(42));
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Int((-42).into()));
    }

    // ========================================================================
    // Membership Tests (in / not in)
    // ========================================================================

    #[test]
    fn test_eval_in_array() {
        let scope = Scope::new();
        let expr = binary_op(
            int_literal(2),
            ast::BinaryOperator::BinaryOpIn,
            list_expr(vec![int_literal(1), int_literal(2), int_literal(3)]),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Bool(true));
    }

    #[test]
    fn test_eval_not_in_array() {
        let scope = Scope::new();
        let expr = binary_op(
            int_literal(5),
            ast::BinaryOperator::BinaryOpNotIn,
            list_expr(vec![int_literal(1), int_literal(2), int_literal(3)]),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Bool(true));
    }

    #[test]
    fn test_eval_in_string() {
        let scope = Scope::new();
        let expr = binary_op(
            string_literal("ell"),
            ast::BinaryOperator::BinaryOpIn,
            string_literal("hello"),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Bool(true));
    }

    #[test]
    fn test_eval_in_dict() {
        let scope = Scope::new();
        let expr = binary_op(
            string_literal("key"),
            ast::BinaryOperator::BinaryOpIn,
            dict_expr(vec![(string_literal("key"), int_literal(42))]),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Bool(true));
    }

    // ========================================================================
    // Truthiness Tests
    // ========================================================================

    #[test]
    fn test_is_truthy_bool() {
        assert!(ExpressionEvaluator::is_truthy(&WorkflowValue::Bool(true)));
        assert!(!ExpressionEvaluator::is_truthy(&WorkflowValue::Bool(false)));
    }

    #[test]
    fn test_is_truthy_number() {
        assert!(ExpressionEvaluator::is_truthy(&WorkflowValue::Int(
            1.into()
        )));
        assert!(!ExpressionEvaluator::is_truthy(&WorkflowValue::Int(
            0.into()
        )));
    }

    #[test]
    fn test_is_truthy_string() {
        assert!(ExpressionEvaluator::is_truthy(&WorkflowValue::String(
            "hello".to_string()
        )));
        assert!(!ExpressionEvaluator::is_truthy(&WorkflowValue::String(
            "".to_string()
        )));
    }

    #[test]
    fn test_is_truthy_array() {
        assert!(ExpressionEvaluator::is_truthy(&WorkflowValue::List(vec![
            WorkflowValue::Int(1.into())
        ])));
        assert!(!ExpressionEvaluator::is_truthy(&WorkflowValue::List(
            vec![]
        )));
    }

    #[test]
    fn test_is_truthy_null() {
        assert!(!ExpressionEvaluator::is_truthy(&WorkflowValue::Null));
    }

    // ========================================================================
    // Collection Tests
    // ========================================================================

    #[test]
    fn test_eval_list() {
        let scope = Scope::new();
        let expr = list_expr(vec![int_literal(1), int_literal(2), int_literal(3)]);
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(
            result,
            WorkflowValue::List(vec![
                WorkflowValue::Int(1.into()),
                WorkflowValue::Int(2.into()),
                WorkflowValue::Int(3.into()),
            ])
        );
    }

    #[test]
    fn test_eval_dict() {
        let scope = Scope::new();
        let expr = dict_expr(vec![
            (string_literal("a"), int_literal(1)),
            (string_literal("b"), int_literal(2)),
        ]);
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        let expected: HashMap<String, WorkflowValue> = [
            ("a".to_string(), WorkflowValue::Int(1)),
            ("b".to_string(), WorkflowValue::Int(2)),
        ]
        .into_iter()
        .collect();
        assert_eq!(result, WorkflowValue::Dict(expected));
    }

    #[test]
    fn test_eval_index_array() {
        let scope = Scope::new();
        let expr = index_access(
            list_expr(vec![int_literal(10), int_literal(20), int_literal(30)]),
            int_literal(1),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Int(20.into()));
    }

    #[test]
    fn test_eval_index_dict() {
        let scope = Scope::new();
        let expr = index_access(
            dict_expr(vec![(string_literal("key"), int_literal(42))]),
            string_literal("key"),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Int(42.into()));
    }

    #[test]
    fn test_eval_index_string() {
        let scope = Scope::new();
        let expr = index_access(string_literal("hello"), int_literal(1));
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::String("e".to_string()));
    }

    #[test]
    fn test_eval_dot_access() {
        let mut scope = Scope::new();
        let obj: HashMap<String, WorkflowValue> = [("x".to_string(), WorkflowValue::Int(10))]
            .into_iter()
            .collect();
        scope.insert("obj".to_string(), WorkflowValue::Dict(obj));

        let expr = dot_access(variable("obj"), "x");
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Int(10.into()));
    }

    #[test]
    fn test_eval_dot_access_pydantic_model() {
        // Test that dot access works on model-like dict structures
        let mut scope = Scope::new();

        // Create a flattened Pydantic model structure
        let model: HashMap<String, WorkflowValue> = [
            (
                "__type__".to_string(),
                WorkflowValue::String("mymodule.MyModel".to_string()),
            ),
            (
                "archive_s3_urls".to_string(),
                WorkflowValue::List(vec![
                    WorkflowValue::String("url1".to_string()),
                    WorkflowValue::String("url2".to_string()),
                    WorkflowValue::String("url3".to_string()),
                ]),
            ),
            ("count".to_string(), WorkflowValue::Int(42)),
        ]
        .into_iter()
        .collect();

        scope.insert("response".to_string(), WorkflowValue::Dict(model));

        // Access field directly (no nested "data" wrapper)
        let expr = dot_access(variable("response"), "archive_s3_urls");
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(
            result,
            WorkflowValue::List(vec![
                WorkflowValue::String("url1".to_string()),
                WorkflowValue::String("url2".to_string()),
                WorkflowValue::String("url3".to_string()),
            ])
        );

        // Access another field
        let expr = dot_access(variable("response"), "count");
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Int(42));
    }

    #[test]
    fn test_eval_exception_fields() {
        let mut scope = Scope::new();
        let mut values = HashMap::new();
        values.insert("code".to_string(), WorkflowValue::Int(418));
        values.insert(
            "detail".to_string(),
            WorkflowValue::String("teapot".to_string()),
        );
        scope.insert(
            "err".to_string(),
            WorkflowValue::Exception {
                exc_type: "ExceptionMetadataError".to_string(),
                module: "example_app".to_string(),
                message: "Metadata error triggered".to_string(),
                traceback: "trace".to_string(),
                values,
                type_hierarchy: vec![
                    "ExceptionMetadataError".to_string(),
                    "Exception".to_string(),
                    "BaseException".to_string(),
                ],
            },
        );

        let expr = dot_access(variable("err"), "code");
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Int(418));

        let expr = index_access(variable("err"), string_literal("detail"));
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::String("teapot".to_string()));

        let expr = dot_access(variable("err"), "type");
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(
            result,
            WorkflowValue::String("ExceptionMetadataError".to_string())
        );
    }

    // ========================================================================
    // Built-in Function Tests
    // ========================================================================

    #[test]
    fn test_builtin_range() {
        let scope = Scope::new();
        let expr = function_call("range", vec![("stop", int_literal(5))]);
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(
            result,
            WorkflowValue::List(vec![
                WorkflowValue::Int(0.into()),
                WorkflowValue::Int(1.into()),
                WorkflowValue::Int(2.into()),
                WorkflowValue::Int(3.into()),
                WorkflowValue::Int(4.into()),
            ])
        );
    }

    #[test]
    fn test_builtin_range_with_start() {
        let scope = Scope::new();
        let expr = function_call(
            "range",
            vec![("start", int_literal(2)), ("stop", int_literal(5))],
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(
            result,
            WorkflowValue::List(vec![
                WorkflowValue::Int(2.into()),
                WorkflowValue::Int(3.into()),
                WorkflowValue::Int(4.into()),
            ])
        );
    }

    #[test]
    fn test_builtin_range_with_step() {
        let scope = Scope::new();
        let expr = function_call(
            "range",
            vec![
                ("start", int_literal(0)),
                ("stop", int_literal(10)),
                ("step", int_literal(2)),
            ],
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(
            result,
            WorkflowValue::List(vec![
                WorkflowValue::Int(0.into()),
                WorkflowValue::Int(2.into()),
                WorkflowValue::Int(4.into()),
                WorkflowValue::Int(6.into()),
                WorkflowValue::Int(8.into()),
            ])
        );
    }

    #[test]
    fn test_builtin_len_array() {
        let scope = Scope::new();
        let expr = function_call(
            "len",
            vec![(
                "items",
                list_expr(vec![int_literal(1), int_literal(2), int_literal(3)]),
            )],
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Int(3.into()));
    }

    #[test]
    fn test_builtin_len_string() {
        let scope = Scope::new();
        let expr = function_call("len", vec![("items", string_literal("hello"))]);
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Int(5.into()));
    }

    #[test]
    fn test_builtin_len_without_global_function() {
        let scope = Scope::new();
        let expr = function_call_without_global(
            "len",
            vec![(
                "items",
                list_expr(vec![int_literal(1), int_literal(2), int_literal(3)]),
            )],
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Int(3.into()));
    }

    #[test]
    fn test_builtin_enumerate() {
        let scope = Scope::new();
        let expr = function_call(
            "enumerate",
            vec![(
                "items",
                list_expr(vec![
                    string_literal("a"),
                    string_literal("b"),
                    string_literal("c"),
                ]),
            )],
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(
            result,
            WorkflowValue::List(vec![
                WorkflowValue::Tuple(vec![
                    WorkflowValue::Int(0.into()),
                    WorkflowValue::String("a".to_string())
                ]),
                WorkflowValue::Tuple(vec![
                    WorkflowValue::Int(1.into()),
                    WorkflowValue::String("b".to_string())
                ]),
                WorkflowValue::Tuple(vec![
                    WorkflowValue::Int(2.into()),
                    WorkflowValue::String("c".to_string())
                ]),
            ])
        );
    }

    #[test]
    fn test_unknown_function() {
        let scope = Scope::new();
        let expr = function_call("unknown", vec![]);
        let result = ExpressionEvaluator::evaluate(&expr, &scope);
        assert!(matches!(result, Err(EvaluationError::FunctionNotFound(_))));
    }

    // ========================================================================
    // Complex Expression Tests
    // ========================================================================

    #[test]
    fn test_complex_expression() {
        // Test: (x + y) * 2 == 10 and z
        let mut scope = Scope::new();
        scope.insert("x".to_string(), WorkflowValue::Int(2.into()));
        scope.insert("y".to_string(), WorkflowValue::Int(3.into()));
        scope.insert("z".to_string(), WorkflowValue::Bool(true));

        let add = binary_op(
            variable("x"),
            ast::BinaryOperator::BinaryOpAdd,
            variable("y"),
        );
        let mul = binary_op(add, ast::BinaryOperator::BinaryOpMul, int_literal(2));
        let eq = binary_op(mul, ast::BinaryOperator::BinaryOpEq, int_literal(10));
        let expr = binary_op(eq, ast::BinaryOperator::BinaryOpAnd, variable("z"));

        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Bool(true));
    }

    #[test]
    fn test_nested_collections() {
        let scope = Scope::new();
        // [[1, 2], [3, 4]][0][1] == 2
        let nested_list = list_expr(vec![
            list_expr(vec![int_literal(1), int_literal(2)]),
            list_expr(vec![int_literal(3), int_literal(4)]),
        ]);
        let first_access = index_access(nested_list, int_literal(0));
        let second_access = index_access(first_access, int_literal(1));

        let result = ExpressionEvaluator::evaluate(&second_access, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Int(2.into()));
    }

    #[test]
    fn test_conditional_with_truthiness() {
        // Test truthy values in boolean context: 1 and "hello" -> true
        let scope = Scope::new();
        let expr = binary_op(
            int_literal(1),
            ast::BinaryOperator::BinaryOpAnd,
            string_literal("hello"),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Bool(true));
    }

    #[test]
    fn test_conditional_with_falsy() {
        // Test falsy values: 0 or "" -> false
        let scope = Scope::new();
        let expr = binary_op(
            int_literal(0),
            ast::BinaryOperator::BinaryOpOr,
            string_literal(""),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Bool(false));
    }
}
