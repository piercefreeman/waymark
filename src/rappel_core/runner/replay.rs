//! Replay variable values from a runner state snapshot.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use serde_json::Value;
use uuid::Uuid;

use crate::messages::ast as ir;
use crate::rappel_core::dag::EdgeType;
use crate::rappel_core::runner::state::{ActionResultValue, FunctionCallValue, RunnerState};
use crate::rappel_core::runner::value_visitor::ValueExprEvaluator;

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct ReplayError(pub String);

#[derive(Clone, Debug)]
pub struct ReplayResult {
    pub variables: HashMap<String, Value>,
}

pub struct ReplayEngine<'a> {
    state: &'a RunnerState,
    action_results: &'a HashMap<Uuid, Value>,
    cache: RefCell<HashMap<(Uuid, String), Value>>,
    timeline: Vec<Uuid>,
    index: HashMap<Uuid, usize>,
    incoming_data: HashMap<Uuid, Vec<Uuid>>,
}

impl<'a> ReplayEngine<'a> {
    pub fn new(state: &'a RunnerState, action_results: &'a HashMap<Uuid, Value>) -> Self {
        let timeline = if state.timeline.is_empty() {
            state.nodes.keys().cloned().collect()
        } else {
            state.timeline.clone()
        };
        let index = timeline
            .iter()
            .enumerate()
            .map(|(idx, node_id)| (*node_id, idx))
            .collect();
        let incoming_data = build_incoming_data_map(state, &index);
        Self {
            state,
            action_results,
            cache: RefCell::new(HashMap::new()),
            timeline,
            index,
            incoming_data,
        }
    }

    pub fn replay_variables(&self) -> Result<ReplayResult, ReplayError> {
        let mut variables: HashMap<String, Value> = HashMap::new();
        for node_id in self.timeline.iter().rev() {
            let node = match self.state.nodes.get(node_id) {
                Some(node) => node,
                None => continue,
            };
            if node.assignments.is_empty() {
                continue;
            }
            for target in node.assignments.keys() {
                if variables.contains_key(target) {
                    continue;
                }
                let value = self.evaluate_assignment(
                    *node_id,
                    target,
                    Rc::new(RefCell::new(HashSet::new())),
                )?;
                variables.insert(target.clone(), value);
            }
        }
        Ok(ReplayResult { variables })
    }

    fn evaluate_assignment(
        &self,
        node_id: Uuid,
        target: &str,
        stack: Rc<RefCell<HashSet<(Uuid, String)>>>,
    ) -> Result<Value, ReplayError> {
        let key = (node_id, target.to_string());
        if let Some(value) = self.cache.borrow().get(&key) {
            return Ok(value.clone());
        }
        if stack.borrow().contains(&key) {
            return Err(ReplayError(format!(
                "recursive assignment detected for {target} in {node_id}"
            )));
        }

        let node =
            self.state.nodes.get(&node_id).ok_or_else(|| {
                ReplayError(format!("missing assignment for {target} in {node_id}"))
            })?;
        let expr = node
            .assignments
            .get(target)
            .ok_or_else(|| ReplayError(format!("missing assignment for {target} in {node_id}")))?;

        stack.borrow_mut().insert(key.clone());
        let resolve_variable = {
            let stack = stack.clone();
            let this = self;
            move |name: &str| this.resolve_variable(node_id, name, stack.clone())
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
        let apply_binary = |op, left, right| apply_binary(op, left, right);
        let apply_unary = |op, operand| apply_unary(op, operand);
        let error_factory = |message: &str| ReplayError(message.to_string());
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
        self.cache.borrow_mut().insert(key, value.clone());
        Ok(value)
    }

    fn resolve_variable(
        &self,
        current_node_id: Uuid,
        name: &str,
        stack: Rc<RefCell<HashSet<(Uuid, String)>>>,
    ) -> Result<Value, ReplayError> {
        let source_node_id = self
            .find_variable_source_node(current_node_id, name)
            .ok_or_else(|| {
                ReplayError(format!("variable not found via data-flow edges: {name}"))
            })?;
        self.evaluate_assignment(source_node_id, name, stack)
    }

    fn find_variable_source_node(&self, current_node_id: Uuid, name: &str) -> Option<Uuid> {
        let sources = self.incoming_data.get(&current_node_id)?;
        let current_idx = self
            .index
            .get(&current_node_id)
            .copied()
            .unwrap_or(self.index.len());
        for source_id in sources {
            if self.index.get(source_id).copied().unwrap_or(0) > current_idx {
                continue;
            }
            if let Some(node) = self.state.nodes.get(source_id)
                && node.assignments.contains_key(name)
            {
                return Some(*source_id);
            }
        }
        None
    }

    fn resolve_action_result(&self, expr: &ActionResultValue) -> Result<Value, ReplayError> {
        let value = self
            .action_results
            .get(&expr.node_id)
            .cloned()
            .ok_or_else(|| ReplayError(format!("missing action result for {}", expr.node_id)))?;
        if let Some(idx) = expr.result_index {
            if let Value::Array(items) = value {
                let idx = idx as usize;
                return items.get(idx).cloned().ok_or_else(|| {
                    ReplayError(format!(
                        "action result for {} has no index {}",
                        expr.node_id, idx
                    ))
                });
            }
            return Err(ReplayError(format!(
                "action result for {} has no index {}",
                expr.node_id, idx
            )));
        }
        Ok(value)
    }

    fn evaluate_function_call(
        &self,
        expr: &FunctionCallValue,
        args: Vec<Value>,
        kwargs: HashMap<String, Value>,
    ) -> Result<Value, ReplayError> {
        if let Some(global_fn) = expr.global_function
            && global_fn != ir::GlobalFunction::Unspecified as i32
        {
            return evaluate_global_function(global_fn, args, kwargs);
        }
        Err(ReplayError(format!(
            "cannot replay non-global function call: {}",
            expr.name
        )))
    }
}

fn apply_binary(op: i32, left: Value, right: Value) -> Result<Value, ReplayError> {
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
        Some(ir::BinaryOperator::BinaryOpLt) => compare_values(left, right, |a, b| a < b),
        Some(ir::BinaryOperator::BinaryOpLe) => compare_values(left, right, |a, b| a <= b),
        Some(ir::BinaryOperator::BinaryOpGt) => compare_values(left, right, |a, b| a > b),
        Some(ir::BinaryOperator::BinaryOpGe) => compare_values(left, right, |a, b| a >= b),
        Some(ir::BinaryOperator::BinaryOpIn) => Ok(Value::Bool(value_in(&left, &right))),
        Some(ir::BinaryOperator::BinaryOpNotIn) => Ok(Value::Bool(!value_in(&left, &right))),
        Some(ir::BinaryOperator::BinaryOpAdd) => add_values(left, right),
        Some(ir::BinaryOperator::BinaryOpSub) => numeric_op(left, right, |a, b| a - b, true),
        Some(ir::BinaryOperator::BinaryOpMul) => numeric_op(left, right, |a, b| a * b, true),
        Some(ir::BinaryOperator::BinaryOpDiv) => numeric_op(left, right, |a, b| a / b, false),
        Some(ir::BinaryOperator::BinaryOpFloorDiv) => {
            numeric_op(left, right, |a, b| (a / b).floor(), true)
        }
        Some(ir::BinaryOperator::BinaryOpMod) => numeric_op(left, right, |a, b| a % b, true),
        Some(ir::BinaryOperator::BinaryOpUnspecified) | None => {
            Err(ReplayError("binary operator unspecified".to_string()))
        }
    }
}

fn apply_unary(op: i32, operand: Value) -> Result<Value, ReplayError> {
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
                None => Err(ReplayError("unary neg expects number".to_string())),
            }
        }
        Some(ir::UnaryOperator::UnaryOpNot) => Ok(Value::Bool(!is_truthy(&operand))),
        Some(ir::UnaryOperator::UnaryOpUnspecified) | None => {
            Err(ReplayError("unary operator unspecified".to_string()))
        }
    }
}

fn evaluate_global_function(
    global_function: i32,
    args: Vec<Value>,
    kwargs: HashMap<String, Value>,
) -> Result<Value, ReplayError> {
    match ir::GlobalFunction::try_from(global_function).ok() {
        Some(ir::GlobalFunction::Range) => Ok(range_from_args(&args).into()),
        Some(ir::GlobalFunction::Len) => {
            if let Some(first) = args.first() {
                return Ok(Value::Number(len_of_value(first)?));
            }
            if let Some(items) = kwargs.get("items") {
                return Ok(Value::Number(len_of_value(items)?));
            }
            Err(ReplayError("len() missing argument".to_string()))
        }
        Some(ir::GlobalFunction::Enumerate) => {
            let items = if let Some(first) = args.first() {
                first.clone()
            } else if let Some(items) = kwargs.get("items") {
                items.clone()
            } else {
                return Err(ReplayError("enumerate() missing argument".to_string()));
            };
            let list = match items {
                Value::Array(items) => items,
                _ => return Err(ReplayError("enumerate() expects list".to_string())),
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
            Err(ReplayError("isexception() missing argument".to_string()))
        }
        Some(ir::GlobalFunction::Unspecified) | None => {
            Err(ReplayError("global function unspecified".to_string()))
        }
    }
}

fn build_incoming_data_map(
    state: &RunnerState,
    index: &HashMap<Uuid, usize>,
) -> HashMap<Uuid, Vec<Uuid>> {
    let mut incoming: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
    for edge in &state.edges {
        if edge.edge_type != EdgeType::DataFlow {
            continue;
        }
        incoming.entry(edge.target).or_default().push(edge.source);
    }
    for (_target, sources) in incoming.iter_mut() {
        sources.sort_by_key(|node_id| {
            (
                index.get(node_id).copied().unwrap_or(0),
                node_id.to_string(),
            )
        });
        sources.reverse();
    }
    incoming
}

fn int_value(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
}

fn numeric_op(
    left: Value,
    right: Value,
    op: impl Fn(f64, f64) -> f64,
    prefer_int: bool,
) -> Result<Value, ReplayError> {
    let left_num = left
        .as_f64()
        .ok_or_else(|| ReplayError("numeric operation expects number".to_string()))?;
    let right_num = right
        .as_f64()
        .ok_or_else(|| ReplayError("numeric operation expects number".to_string()))?;
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

fn add_values(left: Value, right: Value) -> Result<Value, ReplayError> {
    if let (Value::Array(mut left), Value::Array(right)) = (left.clone(), right.clone()) {
        left.extend(right);
        return Ok(Value::Array(left));
    }
    if let (Some(left), Some(right)) = (left.as_str(), right.as_str()) {
        return Ok(Value::String(format!("{left}{right}")));
    }
    numeric_op(left, right, |a, b| a + b, true)
}

fn compare_values(
    left: Value,
    right: Value,
    op: impl Fn(f64, f64) -> bool,
) -> Result<Value, ReplayError> {
    let left = left
        .as_f64()
        .ok_or_else(|| ReplayError("comparison expects number".to_string()))?;
    let right = right
        .as_f64()
        .ok_or_else(|| ReplayError("comparison expects number".to_string()))?;
    Ok(Value::Bool(op(left, right)))
}

fn value_in(value: &Value, container: &Value) -> bool {
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

fn is_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(number) => number.as_f64().map(|value| value != 0.0).unwrap_or(false),
        Value::String(value) => !value.is_empty(),
        Value::Array(values) => !values.is_empty(),
        Value::Object(map) => !map.is_empty(),
    }
}

fn is_exception_value(value: &Value) -> bool {
    if let Value::Object(map) = value {
        return map.contains_key("type") && map.contains_key("message");
    }
    false
}

fn len_of_value(value: &Value) -> Result<serde_json::Number, ReplayError> {
    let len = match value {
        Value::Array(items) => items.len() as i64,
        Value::String(text) => text.len() as i64,
        Value::Object(map) => map.len() as i64,
        _ => {
            return Err(ReplayError(
                "len() expects list, string, or dict".to_string(),
            ));
        }
    };
    Ok(len.into())
}

fn range_from_args(args: &[Value]) -> Vec<Value> {
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

pub fn replay_variables(
    state: &RunnerState,
    action_results: &HashMap<Uuid, Value>,
) -> Result<ReplayResult, ReplayError> {
    ReplayEngine::new(state, action_results).replay_variables()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::ast as ir;
    use crate::rappel_core::runner::state::RunnerState;

    fn action_plus_two_expr() -> ir::Expr {
        ir::Expr {
            kind: Some(ir::expr::Kind::BinaryOp(Box::new(ir::BinaryOp {
                left: Some(Box::new(ir::Expr {
                    kind: Some(ir::expr::Kind::Variable(ir::Variable {
                        name: "action_result".to_string(),
                    })),
                    span: None,
                })),
                op: ir::BinaryOperator::BinaryOpAdd as i32,
                right: Some(Box::new(ir::Expr {
                    kind: Some(ir::expr::Kind::Literal(ir::Literal {
                        value: Some(ir::literal::Value::IntValue(2)),
                    })),
                    span: None,
                })),
            }))),
            span: None,
        }
    }

    #[test]
    fn test_replay_variables_resolves_action_results() {
        let mut state = RunnerState::new(None, None, None, true);

        let action0 = state
            .queue_action(
                "action",
                Some(vec!["action_result".to_string()]),
                None,
                None,
                Some(0),
            )
            .expect("queue action");
        let first_list = ir::Expr {
            kind: Some(ir::expr::Kind::List(ir::ListExpr {
                elements: vec![action_plus_two_expr()],
            })),
            span: None,
        };
        state
            .record_assignment(vec!["results".to_string()], &first_list, None, None)
            .expect("record assignment");

        let action1 = state
            .queue_action(
                "action",
                Some(vec!["action_result".to_string()]),
                None,
                None,
                Some(1),
            )
            .expect("queue action");
        let second_list = ir::Expr {
            kind: Some(ir::expr::Kind::List(ir::ListExpr {
                elements: vec![action_plus_two_expr()],
            })),
            span: None,
        };
        let concat_expr = ir::Expr {
            kind: Some(ir::expr::Kind::BinaryOp(Box::new(ir::BinaryOp {
                left: Some(Box::new(ir::Expr {
                    kind: Some(ir::expr::Kind::Variable(ir::Variable {
                        name: "results".to_string(),
                    })),
                    span: None,
                })),
                op: ir::BinaryOperator::BinaryOpAdd as i32,
                right: Some(Box::new(second_list)),
            }))),
            span: None,
        };
        state
            .record_assignment(vec!["results".to_string()], &concat_expr, None, None)
            .expect("record assignment");

        let replayed = replay_variables(
            &state,
            &HashMap::from([
                (action0.node_id, Value::Number(1.into())),
                (action1.node_id, Value::Number(2.into())),
            ]),
        )
        .expect("replay");

        assert_eq!(
            replayed.variables.get("results"),
            Some(&Value::Array(vec![3.into(), 4.into()])),
        );
    }
}
