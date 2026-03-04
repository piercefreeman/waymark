//! Shared ValueExpr visitors for traversal, resolution, and evaluation.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::state::{
    ActionCallSpec, ActionResultValue, BinaryOpValue, DictEntryValue, DictValue, DotValue,
    FunctionCallValue, IndexValue, ListValue, LiteralValue, SpreadValue, UnaryOpValue,
    VariableValue,
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum ValueExpr {
    Literal(LiteralValue),
    Variable(VariableValue),
    ActionResult(ActionResultValue),
    BinaryOp(BinaryOpValue),
    UnaryOp(UnaryOpValue),
    List(ListValue),
    Dict(DictValue),
    Index(IndexValue),
    Dot(DotValue),
    FunctionCall(FunctionCallValue),
    Spread(SpreadValue),
}

/// Resolve variables inside a ValueExpr tree without executing actions.
///
/// Example IR:
/// - y = x + 1 (where x -> LiteralValue(2))
///   Produces BinaryOpValue(LiteralValue(2), +, LiteralValue(1)).
pub struct ValueExprResolver<'a> {
    resolve_variable: &'a dyn Fn(&str, &mut HashSet<String>) -> ValueExpr,
    seen: &'a mut HashSet<String>,
}

impl<'a> ValueExprResolver<'a> {
    pub fn new(
        resolve_variable: &'a dyn Fn(&str, &mut HashSet<String>) -> ValueExpr,
        seen: &'a mut HashSet<String>,
    ) -> Self {
        Self {
            resolve_variable,
            seen,
        }
    }

    pub fn visit(&mut self, expr: &ValueExpr) -> ValueExpr {
        match expr {
            ValueExpr::Literal(value) => ValueExpr::Literal(value.clone()),
            ValueExpr::Variable(value) => (self.resolve_variable)(&value.name, self.seen),
            ValueExpr::ActionResult(value) => ValueExpr::ActionResult(value.clone()),
            ValueExpr::BinaryOp(value) => ValueExpr::BinaryOp(BinaryOpValue {
                left: Box::new(self.visit(&value.left)),
                op: value.op,
                right: Box::new(self.visit(&value.right)),
            }),
            ValueExpr::UnaryOp(value) => ValueExpr::UnaryOp(UnaryOpValue {
                op: value.op,
                operand: Box::new(self.visit(&value.operand)),
            }),
            ValueExpr::List(value) => ValueExpr::List(ListValue {
                elements: value.elements.iter().map(|item| self.visit(item)).collect(),
            }),
            ValueExpr::Dict(value) => ValueExpr::Dict(DictValue {
                entries: value
                    .entries
                    .iter()
                    .map(|entry| DictEntryValue {
                        key: self.visit(&entry.key),
                        value: self.visit(&entry.value),
                    })
                    .collect(),
            }),
            ValueExpr::Index(value) => ValueExpr::Index(IndexValue {
                object: Box::new(self.visit(&value.object)),
                index: Box::new(self.visit(&value.index)),
            }),
            ValueExpr::Dot(value) => ValueExpr::Dot(DotValue {
                object: Box::new(self.visit(&value.object)),
                attribute: value.attribute.clone(),
            }),
            ValueExpr::FunctionCall(value) => ValueExpr::FunctionCall(FunctionCallValue {
                name: value.name.clone(),
                args: value.args.iter().map(|arg| self.visit(arg)).collect(),
                kwargs: value
                    .kwargs
                    .iter()
                    .map(|(name, arg)| (name.clone(), self.visit(arg)))
                    .collect(),
                global_function: value.global_function,
            }),
            ValueExpr::Spread(value) => {
                let kwargs = value
                    .action
                    .kwargs
                    .iter()
                    .map(|(name, arg)| (name.clone(), self.visit(arg)))
                    .collect::<HashMap<_, _>>();
                let action = ActionCallSpec {
                    action_name: value.action.action_name.clone(),
                    module_name: value.action.module_name.clone(),
                    kwargs,
                };
                ValueExpr::Spread(SpreadValue {
                    collection: Box::new(self.visit(&value.collection)),
                    loop_var: value.loop_var.clone(),
                    action,
                })
            }
        }
    }
}

/// Collect execution node ids that supply data to a ValueExpr tree.
///
/// Example IR:
/// - total = a + @sum(values)
///   Returns the node ids that last defined `a` and the action node for sum().
pub struct ValueExprSourceCollector<'a> {
    resolve_variable: &'a dyn Fn(&str) -> Option<Uuid>,
}

impl<'a> ValueExprSourceCollector<'a> {
    pub fn new(resolve_variable: &'a dyn Fn(&str) -> Option<Uuid>) -> Self {
        Self { resolve_variable }
    }

    pub fn visit(&self, expr: &ValueExpr) -> HashSet<Uuid> {
        match expr {
            ValueExpr::Literal(_) => HashSet::new(),
            ValueExpr::Variable(value) => {
                (self.resolve_variable)(&value.name).into_iter().collect()
            }
            ValueExpr::ActionResult(value) => [value.node_id].into_iter().collect(),
            ValueExpr::BinaryOp(value) => {
                let mut sources = self.visit(&value.left);
                sources.extend(self.visit(&value.right));
                sources
            }
            ValueExpr::UnaryOp(value) => self.visit(&value.operand),
            ValueExpr::List(value) => {
                let mut sources = HashSet::new();
                for item in &value.elements {
                    sources.extend(self.visit(item));
                }
                sources
            }
            ValueExpr::Dict(value) => {
                let mut sources = HashSet::new();
                for entry in &value.entries {
                    sources.extend(self.visit(&entry.key));
                    sources.extend(self.visit(&entry.value));
                }
                sources
            }
            ValueExpr::Index(value) => {
                let mut sources = self.visit(&value.object);
                sources.extend(self.visit(&value.index));
                sources
            }
            ValueExpr::Dot(value) => self.visit(&value.object),
            ValueExpr::FunctionCall(value) => {
                let mut sources = HashSet::new();
                for arg in &value.args {
                    sources.extend(self.visit(arg));
                }
                for arg in value.kwargs.values() {
                    sources.extend(self.visit(arg));
                }
                sources
            }
            ValueExpr::Spread(value) => {
                let mut sources = self.visit(&value.collection);
                for arg in value.action.kwargs.values() {
                    sources.extend(self.visit(arg));
                }
                sources
            }
        }
    }
}

/// Evaluate ValueExpr nodes into concrete Python values.
///
/// Example:
/// - BinaryOpValue(VariableValue("a"), +, LiteralValue(1)) becomes the
///   current value of a plus 1.
pub struct ValueExprEvaluator<'a, E> {
    resolve_variable: &'a dyn Fn(&str) -> Result<serde_json::Value, E>,
    resolve_action_result: &'a dyn Fn(&ActionResultValue) -> Result<serde_json::Value, E>,
    resolve_function_call: &'a ResolveFunctionCall<'a, E>,
    apply_binary:
        &'a dyn Fn(i32, serde_json::Value, serde_json::Value) -> Result<serde_json::Value, E>,
    apply_unary: &'a dyn Fn(i32, serde_json::Value) -> Result<serde_json::Value, E>,
    error_factory: &'a dyn Fn(&str) -> E,
}

type ResolveFunctionCall<'a, E> = dyn Fn(
        &FunctionCallValue,
        Vec<serde_json::Value>,
        HashMap<String, serde_json::Value>,
    ) -> Result<serde_json::Value, E>
    + 'a;

impl<'a, E> ValueExprEvaluator<'a, E> {
    pub fn new(
        resolve_variable: &'a dyn Fn(&str) -> Result<serde_json::Value, E>,
        resolve_action_result: &'a dyn Fn(&ActionResultValue) -> Result<serde_json::Value, E>,
        resolve_function_call: &'a ResolveFunctionCall<'a, E>,
        apply_binary: &'a dyn Fn(
            i32,
            serde_json::Value,
            serde_json::Value,
        ) -> Result<serde_json::Value, E>,
        apply_unary: &'a dyn Fn(i32, serde_json::Value) -> Result<serde_json::Value, E>,
        error_factory: &'a dyn Fn(&str) -> E,
    ) -> Self {
        Self {
            resolve_variable,
            resolve_action_result,
            resolve_function_call,
            apply_binary,
            apply_unary,
            error_factory,
        }
    }

    pub fn visit(&self, expr: &ValueExpr) -> Result<serde_json::Value, E> {
        enum EvalFrame<'b> {
            Eval(&'b ValueExpr),
            ApplyBinary(i32),
            ApplyUnary(i32),
            BuildList(usize),
            BuildDict(usize),
            ApplyIndex,
            ApplyDot(String),
            ApplyFunctionCall {
                call: &'b FunctionCallValue,
                args_len: usize,
                kwarg_names: Vec<String>,
            },
        }

        let mut frames = vec![EvalFrame::Eval(expr)];
        let mut values: Vec<serde_json::Value> = Vec::new();

        while let Some(frame) = frames.pop() {
            match frame {
                EvalFrame::Eval(current) => match current {
                    ValueExpr::Literal(value) => values.push(value.value.clone()),
                    ValueExpr::Variable(value) => {
                        values.push((self.resolve_variable)(&value.name)?);
                    }
                    ValueExpr::ActionResult(value) => {
                        values.push((self.resolve_action_result)(value)?);
                    }
                    ValueExpr::BinaryOp(value) => {
                        frames.push(EvalFrame::ApplyBinary(value.op));
                        frames.push(EvalFrame::Eval(&value.right));
                        frames.push(EvalFrame::Eval(&value.left));
                    }
                    ValueExpr::UnaryOp(value) => {
                        frames.push(EvalFrame::ApplyUnary(value.op));
                        frames.push(EvalFrame::Eval(&value.operand));
                    }
                    ValueExpr::List(value) => {
                        frames.push(EvalFrame::BuildList(value.elements.len()));
                        for item in value.elements.iter().rev() {
                            frames.push(EvalFrame::Eval(item));
                        }
                    }
                    ValueExpr::Dict(value) => {
                        frames.push(EvalFrame::BuildDict(value.entries.len()));
                        for entry in value.entries.iter().rev() {
                            frames.push(EvalFrame::Eval(&entry.value));
                            frames.push(EvalFrame::Eval(&entry.key));
                        }
                    }
                    ValueExpr::Index(value) => {
                        frames.push(EvalFrame::ApplyIndex);
                        frames.push(EvalFrame::Eval(&value.index));
                        frames.push(EvalFrame::Eval(&value.object));
                    }
                    ValueExpr::Dot(value) => {
                        frames.push(EvalFrame::ApplyDot(value.attribute.clone()));
                        frames.push(EvalFrame::Eval(&value.object));
                    }
                    ValueExpr::FunctionCall(value) => {
                        let kwarg_names: Vec<String> = value.kwargs.keys().cloned().collect();
                        frames.push(EvalFrame::ApplyFunctionCall {
                            call: value,
                            args_len: value.args.len(),
                            kwarg_names: kwarg_names.clone(),
                        });
                        for name in kwarg_names.iter().rev() {
                            let arg = value.kwargs.get(name).ok_or_else(|| {
                                (self.error_factory)("function call kwargs mismatch")
                            })?;
                            frames.push(EvalFrame::Eval(arg));
                        }
                        for arg in value.args.iter().rev() {
                            frames.push(EvalFrame::Eval(arg));
                        }
                    }
                    ValueExpr::Spread(_) => {
                        return Err((self.error_factory)(
                            "cannot replay unresolved spread expression",
                        ));
                    }
                },
                EvalFrame::ApplyBinary(op) => {
                    let right = values
                        .pop()
                        .ok_or_else(|| (self.error_factory)("binary op missing right operand"))?;
                    let left = values
                        .pop()
                        .ok_or_else(|| (self.error_factory)("binary op missing left operand"))?;
                    values.push((self.apply_binary)(op, left, right)?);
                }
                EvalFrame::ApplyUnary(op) => {
                    let operand = values
                        .pop()
                        .ok_or_else(|| (self.error_factory)("unary op missing operand"))?;
                    values.push((self.apply_unary)(op, operand)?);
                }
                EvalFrame::BuildList(len) => {
                    let mut items = Vec::with_capacity(len);
                    for _ in 0..len {
                        items.push(
                            values
                                .pop()
                                .ok_or_else(|| (self.error_factory)("list missing element"))?,
                        );
                    }
                    items.reverse();
                    values.push(serde_json::Value::Array(items));
                }
                EvalFrame::BuildDict(len) => {
                    let mut entries: Vec<(String, serde_json::Value)> = Vec::with_capacity(len);
                    for _ in 0..len {
                        let entry_value = values
                            .pop()
                            .ok_or_else(|| (self.error_factory)("dict missing value"))?;
                        let key_value = values
                            .pop()
                            .ok_or_else(|| (self.error_factory)("dict missing key"))?;
                        let key = key_value
                            .as_str()
                            .map(|value| value.to_string())
                            .unwrap_or_else(|| key_value.to_string());
                        entries.push((key, entry_value));
                    }
                    entries.reverse();
                    let mut map = serde_json::Map::with_capacity(len);
                    for (key, value) in entries {
                        map.insert(key, value);
                    }
                    values.push(serde_json::Value::Object(map));
                }
                EvalFrame::ApplyIndex => {
                    let index = values
                        .pop()
                        .ok_or_else(|| (self.error_factory)("index missing index value"))?;
                    let object = values
                        .pop()
                        .ok_or_else(|| (self.error_factory)("index missing object value"))?;
                    let resolved = match (object, index) {
                        (serde_json::Value::Array(items), serde_json::Value::Number(idx)) => {
                            let idx = idx.as_i64().unwrap_or(-1);
                            if idx < 0 || idx as usize >= items.len() {
                                return Err((self.error_factory)("index out of range"));
                            }
                            items[idx as usize].clone()
                        }
                        (serde_json::Value::Object(map), serde_json::Value::String(key)) => map
                            .get(&key)
                            .cloned()
                            .or_else(|| lookup_exception_value(&map, &key))
                            .ok_or_else(|| (self.error_factory)("dict has no key"))?,
                        _ => return Err((self.error_factory)("unsupported index operation")),
                    };
                    values.push(resolved);
                }
                EvalFrame::ApplyDot(attribute) => {
                    let object = values
                        .pop()
                        .ok_or_else(|| (self.error_factory)("dot access missing object"))?;
                    if let serde_json::Value::Object(map) = object {
                        let resolved = map
                            .get(&attribute)
                            .cloned()
                            .or_else(|| lookup_exception_value(&map, &attribute))
                            .ok_or_else(|| (self.error_factory)("dict has no key"))?;
                        values.push(resolved);
                    } else {
                        return Err((self.error_factory)("attribute not found"));
                    }
                }
                EvalFrame::ApplyFunctionCall {
                    call,
                    args_len,
                    kwarg_names,
                } => {
                    let mut kwargs = HashMap::with_capacity(kwarg_names.len());
                    for name in kwarg_names.iter().rev() {
                        let arg_value = values
                            .pop()
                            .ok_or_else(|| (self.error_factory)("function call missing kwarg"))?;
                        kwargs.insert(name.clone(), arg_value);
                    }
                    let mut args = Vec::with_capacity(args_len);
                    for _ in 0..args_len {
                        args.push(
                            values
                                .pop()
                                .ok_or_else(|| (self.error_factory)("function call missing arg"))?,
                        );
                    }
                    args.reverse();
                    values.push((self.resolve_function_call)(call, args, kwargs)?);
                }
            }
        }

        if values.len() == 1 {
            return values
                .pop()
                .ok_or_else(|| (self.error_factory)("expression stack produced no result"));
        }
        Err((self.error_factory)(
            "expression stack produced invalid result count",
        ))
    }
}

fn lookup_exception_value(
    map: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Option<serde_json::Value> {
    if !(map.contains_key("type") && map.contains_key("message")) {
        return None;
    }
    map.get("values")
        .and_then(|value| value.as_object())
        .and_then(|values| values.get(key))
        .cloned()
}

/// Recursively resolve variable references throughout a value tree.
///
/// Use this as the core materialization step before assignment storage.
///
/// Example IR:
/// - z = (x + y) * 2
///   The tree walk replaces VariableValue("x")/("y") with their latest
///   symbolic definitions before storing z.
pub fn resolve_value_tree(
    value: &ValueExpr,
    resolve_variable: &dyn Fn(&str, &mut HashSet<String>) -> ValueExpr,
) -> ValueExpr {
    let mut seen = HashSet::new();
    let mut resolver = ValueExprResolver::new(resolve_variable, &mut seen);
    resolver.visit(value)
}

/// Find execution node ids that supply data to the given value.
///
/// Example IR:
/// - total = a + @sum(values)
///   Returns the latest assignment node for a and the action node for sum().
pub fn collect_value_sources(
    value: &ValueExpr,
    resolve_variable: &dyn Fn(&str) -> Option<Uuid>,
) -> HashSet<Uuid> {
    let collector = ValueExprSourceCollector::new(resolve_variable);
    collector.visit(value)
}

#[cfg(test)]
mod tests;
