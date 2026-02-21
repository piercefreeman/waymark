//! Replay variable values from a runner state snapshot.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use serde_json::Value;
use uuid::Uuid;

use crate::messages::ast as ir;
use crate::waymark_core::runner::expression_evaluator::{
    add_values, compare_values, int_value, is_exception_value, is_truthy, len_of_value, numeric_op,
    range_from_args, value_in,
};
use crate::waymark_core::runner::state::{ActionResultValue, FunctionCallValue, RunnerState};
use crate::waymark_core::runner::value_visitor::{ValueExpr, ValueExprEvaluator};
use waymark_dag::{EXCEPTION_SCOPE_VAR, EdgeType};

/// Raised when replay cannot reconstruct variable values.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct ReplayError(pub String);

#[derive(Clone, Debug)]
pub struct ReplayResult {
    pub variables: HashMap<String, Value>,
}

/// Replay variable values from a runner state snapshot.
pub struct ReplayEngine<'a> {
    state: &'a RunnerState,
    action_results: &'a HashMap<Uuid, Value>,
    cache: RefCell<HashMap<(Uuid, String), Value>>,
    timeline: Vec<Uuid>,
    index: HashMap<Uuid, usize>,
    incoming_data: HashMap<Uuid, Vec<Uuid>>,
}

impl<'a> ReplayEngine<'a> {
    /// Prepare replay state derived from a runner snapshot.
    ///
    /// We precompute a timeline index and incoming data-flow map so lookups are
    /// O(1) during evaluation.
    ///
    /// Example:
    /// - timeline = [node_a, node_b]
    /// - index[node_b] == 1 and incoming data edges are pre-sorted.
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

    /// Replay variable values by scanning assignments from newest to oldest.
    ///
    /// We walk the timeline in reverse to capture the latest assignment for each
    /// variable and skip older definitions once a value is known. This mirrors
    /// "last write wins" semantics while avoiding redundant evaluation work.
    ///
    /// Example:
    /// - x = 1
    /// - x = 2
    ///   Reverse traversal yields x=2 without evaluating the older assignment.
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

    /// Replay concrete kwargs for an action execution node.
    ///
    /// This resolves symbolic kwargs from the action node in the context of
    /// the node's incoming data-flow edges.
    pub fn replay_action_kwargs(
        &self,
        node_id: Uuid,
    ) -> Result<HashMap<String, Value>, ReplayError> {
        let node = self
            .state
            .nodes
            .get(&node_id)
            .ok_or_else(|| ReplayError(format!("action node not found: {node_id}")))?;
        let action = node
            .action
            .as_ref()
            .ok_or_else(|| ReplayError(format!("node is not an action call: {node_id}")))?;
        let mut resolved = HashMap::new();
        for (name, expr) in &action.kwargs {
            let value = self.evaluate_value_expr_at_node(node_id, expr)?;
            resolved.insert(name.clone(), value);
        }
        Ok(resolved)
    }

    /// Evaluate a single assignment expression with cycle detection.
    ///
    /// We memoize evaluated (node, target) pairs and guard against recursive
    /// references by tracking a stack of active evaluations.
    ///
    /// Example:
    /// - x = y + 1
    /// - y = 2
    ///   Evaluating x resolves y first, then computes x.
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

    fn evaluate_value_expr_at_node(
        &self,
        node_id: Uuid,
        expr: &ValueExpr,
    ) -> Result<Value, ReplayError> {
        let stack = Rc::new(RefCell::new(HashSet::new()));
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
        evaluator.visit(expr)
    }

    /// Resolve a variable reference via data-flow edges.
    ///
    /// This walks to the closest upstream definition and replays that
    /// assignment for the requested variable.
    ///
    /// Example:
    /// - action_1 defines x
    /// - assign_2 uses x
    ///   Resolving x from assign_2 evaluates action_1's assignment.
    fn resolve_variable(
        &self,
        current_node_id: Uuid,
        name: &str,
        stack: Rc<RefCell<HashSet<(Uuid, String)>>>,
    ) -> Result<Value, ReplayError> {
        let mut source_node_id = self.find_variable_source_node(current_node_id, name);
        if source_node_id.is_none() && name == EXCEPTION_SCOPE_VAR {
            source_node_id = self.state.latest_assignment(name);
        }
        let source_node_id = source_node_id.ok_or_else(|| {
            ReplayError(format!("variable not found via data-flow edges: {name}"))
        })?;
        self.evaluate_assignment(source_node_id, name, stack)
    }

    /// Find the nearest upstream node that defines the variable.
    ///
    /// We consult pre-sorted incoming data edges and ignore sources that are
    /// later in the timeline than the current node.
    ///
    /// Example:
    /// - if node_b comes after node_a, node_b cannot be a source for node_a.
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

    /// Fetch an action result by node id, handling indexed results.
    ///
    /// Example:
    /// - result = @fetch()
    /// - result[0]
    ///   The evaluator looks up the action result and returns index 0.
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

    /// Evaluate a function call during replay.
    ///
    /// Only global functions are supported because user-defined functions are
    /// not available in this replay context.
    ///
    /// Example:
    /// - len(items=[1, 2]) -> 2
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

fn replay_error(message: &'static str) -> ReplayError {
    ReplayError(message.to_string())
}

/// Apply a binary operator to replayed operands.
///
/// Example:
/// - left=1, right=2, op=ADD -> 3
fn apply_binary(op: i32, left: Value, right: Value) -> Result<Value, ReplayError> {
    let error = replay_error;
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
        Some(ir::BinaryOperator::BinaryOpLt) => compare_values(left, right, |a, b| a < b, error),
        Some(ir::BinaryOperator::BinaryOpLe) => compare_values(left, right, |a, b| a <= b, error),
        Some(ir::BinaryOperator::BinaryOpGt) => compare_values(left, right, |a, b| a > b, error),
        Some(ir::BinaryOperator::BinaryOpGe) => compare_values(left, right, |a, b| a >= b, error),
        Some(ir::BinaryOperator::BinaryOpIn) => Ok(Value::Bool(value_in(&left, &right))),
        Some(ir::BinaryOperator::BinaryOpNotIn) => Ok(Value::Bool(!value_in(&left, &right))),
        Some(ir::BinaryOperator::BinaryOpAdd) => add_values(left, right, error),
        Some(ir::BinaryOperator::BinaryOpSub) => numeric_op(left, right, |a, b| a - b, true, error),
        Some(ir::BinaryOperator::BinaryOpMul) => numeric_op(left, right, |a, b| a * b, true, error),
        Some(ir::BinaryOperator::BinaryOpDiv) => {
            numeric_op(left, right, |a, b| a / b, false, error)
        }
        Some(ir::BinaryOperator::BinaryOpFloorDiv) => {
            numeric_op(left, right, |a, b| (a / b).floor(), true, error)
        }
        Some(ir::BinaryOperator::BinaryOpMod) => numeric_op(left, right, |a, b| a % b, true, error),
        Some(ir::BinaryOperator::BinaryOpUnspecified) | None => {
            Err(ReplayError("binary operator unspecified".to_string()))
        }
    }
}

/// Apply a unary operator to a replayed operand.
///
/// Example:
/// - op=NOT, operand=True -> False
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

/// Evaluate supported global helper functions.
///
/// Example:
/// - range(0, 3) -> [0, 1, 2]
/// - isexception(value={"type": "...", "message": "..."}) -> True
fn evaluate_global_function(
    global_function: i32,
    args: Vec<Value>,
    kwargs: HashMap<String, Value>,
) -> Result<Value, ReplayError> {
    match ir::GlobalFunction::try_from(global_function).ok() {
        Some(ir::GlobalFunction::Range) => Ok(range_from_args(&args).into()),
        Some(ir::GlobalFunction::Len) => {
            if let Some(first) = args.first() {
                return Ok(Value::Number(len_of_value(first, replay_error)?));
            }
            if let Some(items) = kwargs.get("items") {
                return Ok(Value::Number(len_of_value(items, replay_error)?));
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

/// Build a reverse index of incoming data-flow edges.
///
/// Sources are sorted from most-recent to oldest by timeline index so
/// lookups can short-circuit on the first viable definition.
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

/// Replay variable values from a runner state snapshot.
///
/// This is a convenience wrapper around ReplayEngine that prefers the latest
/// assignment for each variable and returns a fully materialized mapping.
pub fn replay_variables(
    state: &RunnerState,
    action_results: &HashMap<Uuid, Value>,
) -> Result<ReplayResult, ReplayError> {
    ReplayEngine::new(state, action_results).replay_variables()
}

/// Replay concrete kwargs for a specific action node from a state snapshot.
pub fn replay_action_kwargs(
    state: &RunnerState,
    action_results: &HashMap<Uuid, Value>,
    node_id: Uuid,
) -> Result<HashMap<String, Value>, ReplayError> {
    ReplayEngine::new(state, action_results).replay_action_kwargs(node_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::ast as ir;
    use crate::waymark_core::runner::state::{RunnerState, VariableValue};
    use crate::waymark_core::runner::value_visitor::ValueExpr;

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

    #[test]
    fn test_replay_action_kwargs_resolves_variable_inputs() {
        let mut state = RunnerState::new(None, None, None, true);

        let number_expr = ir::Expr {
            kind: Some(ir::expr::Kind::Literal(ir::Literal {
                value: Some(ir::literal::Value::IntValue(7)),
            })),
            span: None,
        };
        state
            .record_assignment(
                vec!["number".to_string()],
                &number_expr,
                None,
                Some("number = 7".to_string()),
            )
            .expect("record assignment");

        let kwargs = HashMap::from([(
            "value".to_string(),
            ValueExpr::Variable(VariableValue {
                name: "number".to_string(),
            }),
        )]);

        let action = state
            .queue_action(
                "compute",
                Some(vec!["result".to_string()]),
                Some(kwargs),
                Some("tests".to_string()),
                None,
            )
            .expect("queue action");

        let kwargs = replay_action_kwargs(
            &state,
            &HashMap::from([(action.node_id, Value::Number(14.into()))]),
            action.node_id,
        )
        .expect("replay kwargs");

        assert_eq!(kwargs.get("value"), Some(&Value::Number(7.into())));
    }
}
