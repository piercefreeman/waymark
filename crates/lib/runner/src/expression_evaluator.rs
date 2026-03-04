use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use serde_json::Value;
use uuid::Uuid;

use waymark_dag::{DAGEdge, EdgeType};
use waymark_observability::obs;
use waymark_proto::ast as ir;
use waymark_runner_state::{
    ActionCallSpec, ActionResultValue, BinaryOpValue, DictEntryValue, DictValue, DotValue,
    FunctionCallValue, IndexValue, ListValue, LiteralValue, UnaryOpValue, VariableValue,
    literal_value,
    value_visitor::{ValueExpr, ValueExprEvaluator},
};

use super::{RunnerExecutor, RunnerExecutorError};

impl RunnerExecutor {
    /// Convert a pure IR expression into a ValueExpr without side effects.
    pub(super) fn expr_to_value(expr: &ir::Expr) -> Result<ValueExpr, RunnerExecutorError> {
        match expr.kind.as_ref() {
            Some(ir::expr::Kind::Literal(lit)) => Ok(ValueExpr::Literal(LiteralValue {
                value: literal_value(lit),
            })),
            Some(ir::expr::Kind::Variable(var)) => Ok(ValueExpr::Variable(VariableValue {
                name: var.name.clone(),
            })),
            Some(ir::expr::Kind::BinaryOp(op)) => {
                let left = op
                    .left
                    .as_ref()
                    .ok_or_else(|| RunnerExecutorError("binary op missing left".to_string()))?;
                let right = op
                    .right
                    .as_ref()
                    .ok_or_else(|| RunnerExecutorError("binary op missing right".to_string()))?;
                Ok(ValueExpr::BinaryOp(BinaryOpValue {
                    left: Box::new(Self::expr_to_value(left)?),
                    op: op.op,
                    right: Box::new(Self::expr_to_value(right)?),
                }))
            }
            Some(ir::expr::Kind::UnaryOp(op)) => {
                let operand = op
                    .operand
                    .as_ref()
                    .ok_or_else(|| RunnerExecutorError("unary op missing operand".to_string()))?;
                Ok(ValueExpr::UnaryOp(UnaryOpValue {
                    op: op.op,
                    operand: Box::new(Self::expr_to_value(operand)?),
                }))
            }
            Some(ir::expr::Kind::List(list)) => {
                let mut elements = Vec::new();
                for item in &list.elements {
                    elements.push(Self::expr_to_value(item)?);
                }
                Ok(ValueExpr::List(ListValue { elements }))
            }
            Some(ir::expr::Kind::Dict(dict_expr)) => {
                let mut entries = Vec::new();
                for entry in &dict_expr.entries {
                    let key = entry
                        .key
                        .as_ref()
                        .ok_or_else(|| RunnerExecutorError("dict entry missing key".to_string()))?;
                    let value = entry.value.as_ref().ok_or_else(|| {
                        RunnerExecutorError("dict entry missing value".to_string())
                    })?;
                    entries.push(DictEntryValue {
                        key: Self::expr_to_value(key)?,
                        value: Self::expr_to_value(value)?,
                    });
                }
                Ok(ValueExpr::Dict(DictValue { entries }))
            }
            Some(ir::expr::Kind::Index(index)) => {
                let object = index.object.as_ref().ok_or_else(|| {
                    RunnerExecutorError("index access missing object".to_string())
                })?;
                let index_expr = index
                    .index
                    .as_ref()
                    .ok_or_else(|| RunnerExecutorError("index access missing index".to_string()))?;
                Ok(ValueExpr::Index(IndexValue {
                    object: Box::new(Self::expr_to_value(object)?),
                    index: Box::new(Self::expr_to_value(index_expr)?),
                }))
            }
            Some(ir::expr::Kind::Dot(dot)) => {
                let object = dot
                    .object
                    .as_ref()
                    .ok_or_else(|| RunnerExecutorError("dot access missing object".to_string()))?;
                Ok(ValueExpr::Dot(DotValue {
                    object: Box::new(Self::expr_to_value(object)?),
                    attribute: dot.attribute.clone(),
                }))
            }
            Some(ir::expr::Kind::FunctionCall(call)) => {
                let mut args = Vec::new();
                for arg in &call.args {
                    args.push(Self::expr_to_value(arg)?);
                }
                let mut kwargs = HashMap::new();
                for kw in &call.kwargs {
                    if let Some(value) = &kw.value {
                        kwargs.insert(kw.name.clone(), Self::expr_to_value(value)?);
                    }
                }
                let global_fn = if call.global_function != 0 {
                    Some(call.global_function)
                } else {
                    None
                };
                Ok(ValueExpr::FunctionCall(FunctionCallValue {
                    name: call.name.clone(),
                    args,
                    kwargs,
                    global_function: global_fn,
                }))
            }
            Some(
                ir::expr::Kind::ActionCall(_)
                | ir::expr::Kind::ParallelExpr(_)
                | ir::expr::Kind::SpreadExpr(_),
            ) => Err(RunnerExecutorError(
                "action/spread calls not allowed in guard expressions".to_string(),
            )),
            None => Ok(ValueExpr::Literal(LiteralValue { value: Value::Null })),
        }
    }

    /// Evaluate a guard expression using current symbolic assignments.
    pub(super) fn evaluate_guard(
        &self,
        expr: Option<&ir::Expr>,
    ) -> Result<bool, RunnerExecutorError> {
        let expr = match expr {
            Some(expr) => expr,
            None => return Ok(false),
        };
        let value_expr = self.state().materialize_value(Self::expr_to_value(expr)?);
        let result = self.evaluate_value_expr(&value_expr)?;
        Ok(is_truthy(&result))
    }

    /// Resolve an action's symbolic kwargs to concrete Python values.
    ///
    /// Example:
    /// - spec.kwargs={"value": VariableValue("x")}
    /// - with x assigned to LiteralValue(10), returns {"value": 10}.
    #[obs]
    pub fn resolve_action_kwargs(
        &self,
        node_id: Uuid,
        action: &ActionCallSpec,
    ) -> Result<HashMap<String, Value>, RunnerExecutorError> {
        let mut resolved = HashMap::new();
        for (name, expr) in &action.kwargs {
            resolved.insert(
                name.clone(),
                self.evaluate_value_expr_for_node(expr, Some(node_id))?,
            );
        }
        Ok(resolved)
    }

    /// Evaluate a ValueExpr into a concrete Python value.
    #[obs]
    pub(super) fn evaluate_value_expr(
        &self,
        expr: &ValueExpr,
    ) -> Result<Value, RunnerExecutorError> {
        self.evaluate_value_expr_for_node(expr, None)
    }

    fn evaluate_value_expr_for_node(
        &self,
        expr: &ValueExpr,
        current_node_id: Option<Uuid>,
    ) -> Result<Value, RunnerExecutorError> {
        let stack = Rc::new(RefCell::new(HashSet::new()));
        let resolve_variable = {
            let stack = stack.clone();
            let this = self;
            move |name: &str| {
                this.evaluate_variable_with_context(current_node_id, name, stack.clone())
            }
        };
        let resolve_action_result = {
            let this = self;
            move |value: &ActionResultValue| this.resolve_action_result(value)
        };
        let resolve_function_call = {
            let this = self;
            move |value: &FunctionCallValue, args, kwargs| {
                this.evaluate_function_call(value, args, kwargs)
            }
        };
        let apply_binary = |op, left, right| Self::apply_binary(op, left, right);
        let apply_unary = |op, operand| Self::apply_unary(op, operand);
        let error_factory = |message: &str| RunnerExecutorError(message.to_string());
        let evaluator = ValueExprEvaluator::new(
            &resolve_variable,
            &resolve_action_result,
            &resolve_function_call,
            &apply_binary,
            &apply_unary,
            &error_factory,
        );
        evaluator.visit(expr)
    }

    fn find_variable_source_node(&self, current_node_id: Uuid, name: &str) -> Option<Uuid> {
        let timeline_index: HashMap<Uuid, usize> = self
            .state()
            .timeline
            .iter()
            .enumerate()
            .map(|(idx, node_id)| (*node_id, idx))
            .collect();

        self.state()
            .edges
            .iter()
            .filter(|edge| edge.edge_type == EdgeType::DataFlow && edge.target == current_node_id)
            .map(|edge| edge.source)
            .filter(|source| {
                self.state()
                    .nodes
                    .get(source)
                    .map(|node| node.assignments.contains_key(name))
                    .unwrap_or(false)
            })
            .max_by_key(|source| timeline_index.get(source).copied().unwrap_or(0))
    }

    fn evaluate_variable_with_context(
        &self,
        current_node_id: Option<Uuid>,
        name: &str,
        stack: Rc<RefCell<HashSet<(Uuid, String)>>>,
    ) -> Result<Value, RunnerExecutorError> {
        let node_id = current_node_id
            .and_then(|node_id| self.find_variable_source_node(node_id, name))
            .or_else(|| self.state().latest_assignment(name))
            .ok_or_else(|| RunnerExecutorError(format!("variable not found: {name}")))?;
        self.evaluate_assignment(node_id, name, stack)
    }

    pub(super) fn evaluate_assignment(
        &self,
        node_id: Uuid,
        target: &str,
        stack: Rc<RefCell<HashSet<(Uuid, String)>>>,
    ) -> Result<Value, RunnerExecutorError> {
        let key = (node_id, target.to_string());
        if let Some(value) = self.eval_cache_get(&key) {
            return Ok(value);
        }
        if stack.borrow().contains(&key) {
            return Err(RunnerExecutorError(format!(
                "recursive assignment detected for {target}"
            )));
        }

        let node = self
            .state()
            .nodes
            .get(&node_id)
            .ok_or_else(|| RunnerExecutorError(format!("missing assignment for {target}")))?;
        let expr = node
            .assignments
            .get(target)
            .ok_or_else(|| RunnerExecutorError(format!("missing assignment for {target}")))?;

        stack.borrow_mut().insert(key.clone());
        let resolve_variable = {
            let stack = stack.clone();
            let this = self;
            move |name: &str| {
                this.evaluate_variable_with_context(Some(node_id), name, stack.clone())
            }
        };
        let resolve_action_result = {
            let this = self;
            move |value: &ActionResultValue| this.resolve_action_result(value)
        };
        let resolve_function_call = {
            let this = self;
            move |value: &FunctionCallValue, args, kwargs| {
                this.evaluate_function_call(value, args, kwargs)
            }
        };
        let apply_binary = |op, left, right| Self::apply_binary(op, left, right);
        let apply_unary = |op, operand| Self::apply_unary(op, operand);
        let error_factory = |message: &str| RunnerExecutorError(message.to_string());
        let evaluator = ValueExprEvaluator::new(
            &resolve_variable,
            &resolve_action_result,
            &resolve_function_call,
            &apply_binary,
            &apply_unary,
            &error_factory,
        );
        let value = evaluator.visit(expr)?;
        stack.borrow_mut().remove(&key);
        self.eval_cache_insert(key, value.clone());
        Ok(value)
    }

    pub(super) fn resolve_action_result(
        &self,
        expr: &ActionResultValue,
    ) -> Result<Value, RunnerExecutorError> {
        let value = self
            .action_results()
            .get(&expr.node_id)
            .cloned()
            .ok_or_else(|| {
                RunnerExecutorError(format!("missing action result for {}", expr.node_id))
            })?;
        if let Some(idx) = expr.result_index {
            if let Value::Array(items) = value {
                let idx = idx as usize;
                return items.get(idx).cloned().ok_or_else(|| {
                    RunnerExecutorError(format!(
                        "action result for {} has no index {}",
                        expr.node_id, idx
                    ))
                });
            }
            return Err(RunnerExecutorError(format!(
                "action result for {} has no index {}",
                expr.node_id, idx
            )));
        }
        Ok(value)
    }

    pub(super) fn evaluate_function_call(
        &self,
        expr: &FunctionCallValue,
        args: Vec<Value>,
        kwargs: HashMap<String, Value>,
    ) -> Result<Value, RunnerExecutorError> {
        if let Some(global_fn) = expr.global_function
            && global_fn != ir::GlobalFunction::Unspecified as i32
        {
            return self.evaluate_global_function(global_fn, args, kwargs);
        }
        Err(RunnerExecutorError(format!(
            "cannot evaluate non-global function call: {}",
            expr.name
        )))
    }

    pub(super) fn evaluate_global_function(
        &self,
        global_function: i32,
        args: Vec<Value>,
        kwargs: HashMap<String, Value>,
    ) -> Result<Value, RunnerExecutorError> {
        let error = executor_error;
        match ir::GlobalFunction::try_from(global_function).ok() {
            Some(ir::GlobalFunction::Range) => Ok(range_from_args(&args).into()),
            Some(ir::GlobalFunction::Len) => {
                if let Some(first) = args.first() {
                    return Ok(Value::Number(len_of_value(first, error)?));
                }
                if let Some(items) = kwargs.get("items") {
                    return Ok(Value::Number(len_of_value(items, error)?));
                }
                Err(RunnerExecutorError("len() missing argument".to_string()))
            }
            Some(ir::GlobalFunction::Enumerate) => {
                let items = if let Some(first) = args.first() {
                    first.clone()
                } else if let Some(items) = kwargs.get("items") {
                    items.clone()
                } else {
                    return Err(RunnerExecutorError(
                        "enumerate() missing argument".to_string(),
                    ));
                };
                let list = match items {
                    Value::Array(items) => items,
                    _ => return Err(RunnerExecutorError("enumerate() expects list".to_string())),
                };
                let pairs: Vec<Value> = list
                    .into_iter()
                    .enumerate()
                    .map(|(idx, item)| Value::Array(vec![Value::Number((idx as i64).into()), item]))
                    .collect();
                Ok(Value::Array(pairs))
            }
            Some(ir::GlobalFunction::Isexception) => {
                if let Some(first) = args.first() {
                    return Ok(Value::Bool(is_exception_value(first)));
                }
                if let Some(value) = kwargs.get("value") {
                    return Ok(Value::Bool(is_exception_value(value)));
                }
                Err(RunnerExecutorError(
                    "isexception() missing argument".to_string(),
                ))
            }
            Some(ir::GlobalFunction::Unspecified) | None => Err(RunnerExecutorError(
                "global function unspecified".to_string(),
            )),
        }
    }

    pub(super) fn apply_binary(
        op: i32,
        left: Value,
        right: Value,
    ) -> Result<Value, RunnerExecutorError> {
        let error = executor_error;
        match ir::BinaryOperator::try_from(op).ok() {
            Some(ir::BinaryOperator::BinaryOpOr) => {
                if is_truthy(&left) {
                    Ok(left)
                } else {
                    Ok(right)
                }
            }
            Some(ir::BinaryOperator::BinaryOpAnd) => {
                if is_truthy(&left) {
                    Ok(right)
                } else {
                    Ok(left)
                }
            }
            Some(ir::BinaryOperator::BinaryOpEq) => Ok(Value::Bool(left == right)),
            Some(ir::BinaryOperator::BinaryOpNe) => Ok(Value::Bool(left != right)),
            Some(ir::BinaryOperator::BinaryOpLt) => {
                compare_values(left, right, |a, b| a < b, error)
            }
            Some(ir::BinaryOperator::BinaryOpLe) => {
                compare_values(left, right, |a, b| a <= b, error)
            }
            Some(ir::BinaryOperator::BinaryOpGt) => {
                compare_values(left, right, |a, b| a > b, error)
            }
            Some(ir::BinaryOperator::BinaryOpGe) => {
                compare_values(left, right, |a, b| a >= b, error)
            }
            Some(ir::BinaryOperator::BinaryOpIn) => Ok(Value::Bool(value_in(&left, &right))),
            Some(ir::BinaryOperator::BinaryOpNotIn) => Ok(Value::Bool(!value_in(&left, &right))),
            Some(ir::BinaryOperator::BinaryOpAdd) => add_values(left, right, error),
            Some(ir::BinaryOperator::BinaryOpSub) => {
                numeric_op(left, right, |a, b| a - b, true, error)
            }
            Some(ir::BinaryOperator::BinaryOpMul) => {
                numeric_op(left, right, |a, b| a * b, true, error)
            }
            Some(ir::BinaryOperator::BinaryOpDiv) => {
                numeric_op(left, right, |a, b| a / b, false, error)
            }
            Some(ir::BinaryOperator::BinaryOpFloorDiv) => {
                numeric_op(left, right, |a, b| (a / b).floor(), true, error)
            }
            Some(ir::BinaryOperator::BinaryOpMod) => {
                numeric_op(left, right, |a, b| a % b, true, error)
            }
            Some(ir::BinaryOperator::BinaryOpUnspecified) | None => Err(RunnerExecutorError(
                "binary operator unspecified".to_string(),
            )),
        }
    }

    pub(super) fn apply_unary(op: i32, operand: Value) -> Result<Value, RunnerExecutorError> {
        match ir::UnaryOperator::try_from(op).ok() {
            Some(ir::UnaryOperator::UnaryOpNeg) => {
                if let Some(value) = int_value(&operand) {
                    return Ok(Value::Number((-value).into()));
                }
                match operand.as_f64() {
                    Some(value) => Ok(Value::Number(
                        serde_json::Number::from_f64(-value)
                            .unwrap_or_else(|| serde_json::Number::from(0)),
                    )),
                    None => Err(RunnerExecutorError("unary neg expects number".to_string())),
                }
            }
            Some(ir::UnaryOperator::UnaryOpNot) => Ok(Value::Bool(!is_truthy(&operand))),
            Some(ir::UnaryOperator::UnaryOpUnspecified) | None => Err(RunnerExecutorError(
                "unary operator unspecified".to_string(),
            )),
        }
    }

    pub(super) fn exception_matches(&self, edge: &DAGEdge, exception_value: &Value) -> bool {
        let exception_types = match &edge.exception_types {
            Some(types) => types,
            None => return false,
        };
        if exception_types.is_empty() {
            return true;
        }
        let exc_name = match exception_value {
            Value::Object(map) => map
                .get("type")
                .and_then(|value| value.as_str())
                .map(|value| value.to_string()),
            _ => None,
        };
        if let Some(name) = exc_name {
            return exception_types.iter().any(|value| value == &name);
        }
        false
    }
}

fn executor_error(message: &'static str) -> RunnerExecutorError {
    RunnerExecutorError(message.to_string())
}

pub(crate) fn int_value(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
}

pub(crate) fn numeric_op<E>(
    left: Value,
    right: Value,
    op: impl Fn(f64, f64) -> f64,
    prefer_int: bool,
    error: fn(&'static str) -> E,
) -> Result<Value, E> {
    let left_num = left
        .as_f64()
        .ok_or_else(|| error("numeric operation expects number"))?;
    let right_num = right
        .as_f64()
        .ok_or_else(|| error("numeric operation expects number"))?;
    let result = op(left_num, right_num);
    if prefer_int && int_value(&left).is_some() && int_value(&right).is_some() && result.is_finite()
    {
        let rounded = result.round();
        if (result - rounded).abs() < 1e-9
            && rounded >= (i64::MIN as f64)
            && rounded <= (i64::MAX as f64)
        {
            return Ok(Value::Number((rounded as i64).into()));
        }
    }
    Ok(Value::Number(
        serde_json::Number::from_f64(result).unwrap_or_else(|| serde_json::Number::from(0)),
    ))
}

pub(crate) fn add_values<E>(
    left: Value,
    right: Value,
    error: fn(&'static str) -> E,
) -> Result<Value, E> {
    if let (Value::Array(mut left), Value::Array(right)) = (left.clone(), right.clone()) {
        left.extend(right);
        return Ok(Value::Array(left));
    }
    if let (Some(left), Some(right)) = (left.as_str(), right.as_str()) {
        return Ok(Value::String(format!("{left}{right}")));
    }
    numeric_op(left, right, |a, b| a + b, true, error)
}

pub(crate) fn compare_values<E>(
    left: Value,
    right: Value,
    op: impl Fn(f64, f64) -> bool,
    error: fn(&'static str) -> E,
) -> Result<Value, E> {
    let left = left
        .as_f64()
        .ok_or_else(|| error("comparison expects number"))?;
    let right = right
        .as_f64()
        .ok_or_else(|| error("comparison expects number"))?;
    Ok(Value::Bool(op(left, right)))
}

pub(crate) fn value_in(value: &Value, container: &Value) -> bool {
    match container {
        Value::Array(items) => items.iter().any(|item| item == value),
        Value::Object(map) => value
            .as_str()
            .map(|key| map.contains_key(key))
            .unwrap_or(false),
        Value::String(text) => value
            .as_str()
            .map(|needle| text.contains(needle))
            .unwrap_or(false),
        _ => false,
    }
}

pub(crate) fn is_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(number) => number.as_f64().map(|value| value != 0.0).unwrap_or(false),
        Value::String(value) => !value.is_empty(),
        Value::Array(values) => !values.is_empty(),
        Value::Object(map) => !map.is_empty(),
    }
}

pub(crate) fn is_exception_value(value: &Value) -> bool {
    if let Value::Object(map) = value {
        return map.contains_key("type") && map.contains_key("message");
    }
    false
}

pub(crate) fn len_of_value<E>(
    value: &Value,
    error: fn(&'static str) -> E,
) -> Result<serde_json::Number, E> {
    let len = match value {
        Value::Array(items) => items.len() as i64,
        Value::String(text) => text.len() as i64,
        Value::Object(map) => map.len() as i64,
        _ => return Err(error("len() expects list, string, or dict")),
    };
    Ok(len.into())
}

pub(crate) fn range_from_args(args: &[Value]) -> Vec<Value> {
    let mut start = 0i64;
    let mut end = 0i64;
    let mut step = 1i64;
    if args.len() == 1 {
        end = args[0].as_i64().unwrap_or(0);
    } else if args.len() >= 2 {
        start = args[0].as_i64().unwrap_or(0);
        end = args[1].as_i64().unwrap_or(0);
        if args.len() >= 3 {
            step = args[2].as_i64().unwrap_or(1);
        }
    }
    if step == 0 {
        return Vec::new();
    }
    let mut values = Vec::new();
    if step > 0 {
        let mut current = start;
        while current < end {
            values.push(Value::Number(current.into()));
            current += step;
        }
    } else {
        let mut current = start;
        while current > end {
            values.push(Value::Number(current.into()));
            current += step;
        }
    }
    values
}

#[cfg(test)]
mod tests;
