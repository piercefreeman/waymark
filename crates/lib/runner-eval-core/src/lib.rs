//! Core expression evaluator.

mod error;
mod fold;
mod util;

pub use self::error::*;

use std::collections::HashMap;

use waymark_runner_expr::*;

use waymark_ir_conversions::literal_to_json_value;
use waymark_proto::ast as ir;

#[derive(Debug)]
pub struct CoreEvaluator<SideEffectApplicator>(pub SideEffectApplicator);

#[derive(Debug)]
pub struct ActionCallParams<NodeId> {
    pub action: ActionCallSpec<NodeId>,
    pub targets: Option<Vec<String>>,
}

pub trait SideEffectApplicator {
    type ActionCallError;
    type NodeId: Clone;

    fn action_call(
        &mut self,
        iteration_index: Option<i32>,
        params: ActionCallParams<Self::NodeId>,
    ) -> Result<ActionResultValue<Self::NodeId>, Self::ActionCallError>;
}

type ValueExprFor<SideEffectApplicator> =
    ValueExpr<<SideEffectApplicator as self::SideEffectApplicator>::NodeId>;

impl<SideEffectApplicator> CoreEvaluator<SideEffectApplicator>
where
    SideEffectApplicator: self::SideEffectApplicator,
{
    /// Convert an IR expression into a symbolic ValueExpr tree.
    ///
    /// Use this when interpreting IR statements or DAG templates into the
    /// runtime state; it queues actions and spreads as needed.
    ///
    /// Example IR:
    /// - total = base + 1
    ///   Produces BinaryOpValue(VariableValue("base"), LiteralValue(1)).
    pub fn expr_to_value(
        &mut self,
        expr: &ir::Expr,
        local_scope: Option<&HashMap<String, ValueExprFor<SideEffectApplicator>>>,
    ) -> Result<
        ValueExprFor<SideEffectApplicator>,
        ExprToValueError<SideEffectApplicator::ActionCallError>,
    > {
        match expr.kind.as_ref() {
            Some(ir::expr::Kind::Literal(lit)) => Ok(ValueExpr::Literal(LiteralValue {
                value: literal_to_json_value(lit),
            })),
            Some(ir::expr::Kind::Variable(var)) => {
                if let Some(scope) = local_scope
                    && let Some(value) = scope.get(&var.name)
                {
                    return Ok(value.clone());
                }
                Ok(ValueExpr::Variable(VariableValue {
                    name: var.name.clone(),
                }))
            }
            Some(ir::expr::Kind::BinaryOp(op)) => {
                let left = op.left.as_ref().ok_or(BinaryOpError::LeftMissing)?;
                let right = op.right.as_ref().ok_or(BinaryOpError::RightMissing)?;
                let left_value = self.expr_to_value(left, local_scope)?;
                let right_value = self.expr_to_value(right, local_scope)?;
                Ok(self.binary_op_value(op.op, left_value, right_value))
            }
            Some(ir::expr::Kind::UnaryOp(op)) => {
                let operand = op.operand.as_ref().ok_or(UnaryOpError::Missing)?;
                let operand_value = self.expr_to_value(operand, local_scope)?;
                Ok(self.unary_op_value(op.op, operand_value))
            }
            Some(ir::expr::Kind::List(list)) => {
                let elements = list
                    .elements
                    .iter()
                    .map(|item| self.expr_to_value(item, local_scope))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(ValueExpr::List(ListValue { elements }))
            }
            Some(ir::expr::Kind::Dict(dict_expr)) => {
                let mut entries = Vec::new();
                for entry in &dict_expr.entries {
                    let key_expr = entry.key.as_ref().ok_or(DictEntryError::MissingKey)?;
                    let value_expr = entry.value.as_ref().ok_or(DictEntryError::MissingValue)?;
                    entries.push(DictEntryValue {
                        key: self.expr_to_value(key_expr, local_scope)?,
                        value: self.expr_to_value(value_expr, local_scope)?,
                    });
                }
                Ok(ValueExpr::Dict(DictValue { entries }))
            }
            Some(ir::expr::Kind::Index(index)) => {
                let object = index
                    .object
                    .as_ref()
                    .ok_or(IndexAccessError::MissingObject)?;
                let index_expr = index.index.as_ref().ok_or(IndexAccessError::MissingIndex)?;
                let object_value = self.expr_to_value(object, local_scope)?;
                let index_value = self.expr_to_value(index_expr, local_scope)?;
                Ok(self.index_value(object_value, index_value))
            }
            Some(ir::expr::Kind::Dot(dot)) => {
                let object = dot.object.as_ref().ok_or(DotAccessError::MissingObject)?;
                Ok(ValueExpr::Dot(DotValue {
                    object: Box::new(self.expr_to_value(object, local_scope)?),
                    attribute: dot.attribute.clone(),
                }))
            }
            Some(ir::expr::Kind::FunctionCall(call)) => {
                let args = call
                    .args
                    .iter()
                    .map(|arg| self.expr_to_value(arg, local_scope))
                    .collect::<Result<Vec<_>, _>>()?;
                let mut kwargs = HashMap::new();
                for kw in &call.kwargs {
                    if let Some(value) = &kw.value {
                        kwargs.insert(kw.name.clone(), self.expr_to_value(value, local_scope)?);
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
            Some(ir::expr::Kind::ActionCall(action)) => {
                let result = self.action_call(action, None, None, local_scope)?;
                Ok(ValueExpr::ActionResult(result))
            }
            Some(ir::expr::Kind::ParallelExpr(parallel)) => {
                let mut calls = Vec::new();
                for call in &parallel.calls {
                    calls.push(self.call_to_value(call, local_scope)?);
                }
                Ok(ValueExpr::List(ListValue { elements: calls }))
            }
            Some(ir::expr::Kind::SpreadExpr(spread)) => self.spread_expr_value(spread, local_scope),
            None => Ok(ValueExpr::Literal(LiteralValue {
                value: serde_json::Value::Null,
            })),
        }
    }

    /// Convert an IR call (action/function) into a ValueExpr.
    ///
    /// Use this for parallel expressions that contain mixed call types.
    ///
    /// Example IR:
    /// - parallel { @double(x), helper(x) }
    ///   Action calls become ActionResultValue nodes; function calls become
    ///   FunctionCallValue expressions.
    fn call_to_value(
        &mut self,
        call: &ir::Call,
        local_scope: Option<&HashMap<String, ValueExprFor<SideEffectApplicator>>>,
    ) -> Result<
        ValueExprFor<SideEffectApplicator>,
        ExprToValueError<SideEffectApplicator::ActionCallError>,
    > {
        match call.kind.as_ref() {
            Some(ir::call::Kind::Action(action)) => Ok(ValueExpr::ActionResult(self.action_call(
                action,
                None,
                None,
                local_scope,
            )?)),
            Some(ir::call::Kind::Function(function)) => self.expr_to_value(
                &ir::Expr {
                    kind: Some(ir::expr::Kind::FunctionCall(function.clone())),
                    span: None,
                },
                local_scope,
            ),
            None => Ok(ValueExpr::Literal(LiteralValue {
                value: serde_json::Value::Null,
            })),
        }
    }

    /// Materialize a spread expression into concrete calls or a symbolic spread.
    ///
    /// Use this when converting IR spreads so known list collections unroll to
    /// explicit action calls, while unknown collections stay symbolic.
    ///
    /// Example IR:
    /// - spread [1, 2]:item -> @double(value=item)
    ///   Produces a ListValue of ActionResultValue entries for each item.
    fn spread_expr_value(
        &mut self,
        spread: &ir::SpreadExpr,
        local_scope: Option<&HashMap<String, ValueExprFor<SideEffectApplicator>>>,
    ) -> Result<
        ValueExprFor<SideEffectApplicator>,
        ExprToValueError<SideEffectApplicator::ActionCallError>,
    > {
        let collection = self.expr_to_value(
            spread
                .collection
                .as_ref()
                .ok_or(SpreadError::CollectionMissing)?,
            local_scope,
        )?;
        if let ValueExpr::List(list) = &collection {
            let mut results = Vec::new();
            for (idx, item) in list.elements.iter().enumerate() {
                let mut scope = HashMap::new();
                scope.insert(spread.loop_var.clone(), item.clone());
                let result = self.action_call(
                    spread.action.as_ref().ok_or(SpreadError::ActionMissing)?,
                    None,
                    Some(idx as i32),
                    Some(&scope),
                )?;
                results.push(ValueExpr::ActionResult(result));
            }
            return Ok(ValueExpr::List(ListValue { elements: results }));
        }

        let action_spec = self.action_spec_from_ir(
            spread.action.as_ref().ok_or(SpreadError::ActionMissing)?,
            None,
        )?;
        Ok(ValueExpr::Spread(SpreadValue {
            collection: Box::new(collection),
            loop_var: spread.loop_var.clone(),
            action: action_spec,
        }))
    }

    /// Build a binary-op value with simple constant folding.
    ///
    /// Use this when converting IR so literals and list concatenations are
    /// simplified early.
    ///
    /// Example IR:
    /// - total = 1 + 2
    ///   Produces LiteralValue(3) instead of a BinaryOpValue.
    fn binary_op_value(
        &self,
        op: i32,
        left: ValueExprFor<SideEffectApplicator>,
        right: ValueExprFor<SideEffectApplicator>,
    ) -> ValueExprFor<SideEffectApplicator> {
        if ir::BinaryOperator::try_from(op).ok() == Some(ir::BinaryOperator::BinaryOpAdd)
            && let (ValueExpr::List(left_list), ValueExpr::List(right_list)) = (&left, &right)
        {
            let mut elements = left_list.elements.clone();
            elements.extend(right_list.elements.clone());
            return ValueExpr::List(ListValue { elements });
        }
        if let (ValueExpr::Literal(left_val), ValueExpr::Literal(right_val)) = (&left, &right)
            && let Some(folded) = fold::literal_binary(op, &left_val.value, &right_val.value)
        {
            return ValueExpr::Literal(LiteralValue { value: folded });
        }
        ValueExpr::BinaryOp(BinaryOpValue {
            left: Box::new(left),
            op,
            right: Box::new(right),
        })
    }

    /// Build a unary-op value with constant folding for literals.
    ///
    /// Example IR:
    /// - neg = -1
    ///   Produces LiteralValue(-1) instead of UnaryOpValue.
    fn unary_op_value(
        &self,
        op: i32,
        operand: ValueExprFor<SideEffectApplicator>,
    ) -> ValueExprFor<SideEffectApplicator> {
        if let ValueExpr::Literal(lit) = &operand
            && let Some(folded) = fold::literal_unary(op, &lit.value)
        {
            return ValueExpr::Literal(LiteralValue { value: folded });
        }
        ValueExpr::UnaryOp(UnaryOpValue {
            op,
            operand: Box::new(operand),
        })
    }

    /// Build an index value, folding list literals when possible.
    ///
    /// Example IR:
    /// - first = [10, 20][0]
    ///   Produces LiteralValue(10) when the list is fully literal.
    fn index_value(
        &self,
        object: ValueExprFor<SideEffectApplicator>,
        index: ValueExprFor<SideEffectApplicator>,
    ) -> ValueExprFor<SideEffectApplicator> {
        if let (ValueExpr::List(list), ValueExpr::Literal(idx)) = (&object, &index)
            && let Some(idx) = idx.value.as_i64()
            && idx >= 0
            && (idx as usize) < list.elements.len()
        {
            return list.elements[idx as usize].clone();
        }
        ValueExpr::Index(IndexValue {
            object: Box::new(object),
            index: Box::new(index),
        })
    }

    pub fn resolve_kwargs<'a>(
        &mut self,
        kwarg_exprs: impl IntoIterator<Item = (String, &'a ir::Expr)>,
        local_scope: Option<&HashMap<String, ValueExprFor<SideEffectApplicator>>>,
    ) -> Result<
        HashMap<String, ValueExprFor<SideEffectApplicator>>,
        ExprToValueError<SideEffectApplicator::ActionCallError>,
    > {
        let mut kwargs = HashMap::new();
        for (name, expr) in kwarg_exprs {
            kwargs.insert(name, self.expr_to_value(expr, local_scope)?);
        }
        Ok(kwargs)
    }

    /// Extract an action call spec from IR, applying local scope bindings.
    ///
    /// Example IR:
    /// - @double(value=item) with local_scope["item"]=LiteralValue(2)
    ///   Produces kwargs {"value": LiteralValue(2)}.
    pub fn action_spec_from_ir(
        &mut self,
        action: &ir::ActionCall,
        local_scope: Option<&HashMap<String, ValueExprFor<SideEffectApplicator>>>,
    ) -> Result<
        ActionCallSpec<SideEffectApplicator::NodeId>,
        ExprToValueError<SideEffectApplicator::ActionCallError>,
    > {
        let kwargs = action
            .kwargs
            .iter()
            .filter_map(|kw| kw.value.as_ref().map(|value| (kw.name.clone(), value)));
        Ok(ActionCallSpec {
            action_name: action.action_name.clone(),
            module_name: action.module_name.clone(),
            kwargs: self.resolve_kwargs(kwargs, local_scope)?,
        })
    }

    /// Queue an action call from IR, respecting a local scope for loop vars.
    ///
    /// Use this during IR -> runner-state conversion (including spreads) so
    /// action arguments are converted to symbolic expressions.
    ///
    /// Example IR:
    /// - @double(value=item)
    ///   With local_scope={"item": LiteralValue(2)}, the queued action uses a
    ///   literal argument and links data-flow to the literal's source nodes.
    pub fn action_call(
        &mut self,
        action: &ir::ActionCall,
        targets: Option<Vec<String>>,
        iteration_index: Option<i32>,
        local_scope: Option<&HashMap<String, ValueExprFor<SideEffectApplicator>>>,
    ) -> Result<
        ActionResultValue<SideEffectApplicator::NodeId>,
        ExprToValueError<SideEffectApplicator::ActionCallError>,
    > {
        let spec = self.action_spec_from_ir(action, local_scope)?;
        let result = self
            .0
            .action_call(
                iteration_index,
                ActionCallParams {
                    targets: targets.clone(),
                    action: spec.clone(),
                },
            )
            .map_err(ExprToValueError::QueueActionCall)?;
        Ok(result)
    }
}
