//! Assignment and expression conversion helpers.

use std::collections::HashMap;

use waymark_proto::ast as ir;

use super::super::nodes::{
    ActionCallNode, ActionCallParams, AssignmentNode, ExpressionNode, FnCallNode, FnCallParams,
    SleepNode,
};
use super::converter::DAGConverter;

/// Convert assignments and expression statements into DAG nodes.
impl DAGConverter {
    /// Convert an assignment into one or more DAG nodes.
    ///
    /// Example IR:
    /// - x = @action()
    ///   Produces a call node with target x and tracks x as defined at that node.
    pub fn convert_assignment(&mut self, assign: &ir::Assignment) -> Vec<String> {
        let value = assign.value.as_ref();
        if value.is_none() {
            return Vec::new();
        }
        let value = value.unwrap();
        let targets: Vec<String> = assign.targets.clone();
        match value.kind.as_ref() {
            Some(ir::expr::Kind::FunctionCall(call)) => {
                let target = targets.first().cloned().unwrap_or_else(|| "_".to_string());
                self.convert_fn_call_assignment(&target, &targets, call)
            }
            Some(ir::expr::Kind::ActionCall(action)) => {
                self.convert_action_call_with_targets(action, &targets)
            }
            Some(ir::expr::Kind::ParallelExpr(parallel)) => {
                self.convert_parallel_expr(parallel, &targets)
            }
            Some(ir::expr::Kind::SpreadExpr(spread)) => self.convert_spread_expr(spread, &targets),
            Some(
                ir::expr::Kind::Literal(_)
                | ir::expr::Kind::Variable(_)
                | ir::expr::Kind::BinaryOp(_)
                | ir::expr::Kind::UnaryOp(_)
                | ir::expr::Kind::List(_)
                | ir::expr::Kind::Dict(_)
                | ir::expr::Kind::Index(_)
                | ir::expr::Kind::Dot(_),
            ) => {
                let node_id = self.next_id("assign");
                let node = AssignmentNode::new(
                    node_id.clone(),
                    targets.clone(),
                    None,
                    Some(value.clone()),
                    None,
                    self.current_function.clone(),
                );
                self.dag.add_node(node.into());
                for target in targets {
                    self.track_var_definition(&target, &node_id);
                }
                vec![node_id]
            }
            None => Vec::new(),
        }
    }

    /// Convert a function call assignment into a FnCallNode.
    pub fn convert_fn_call_assignment(
        &mut self,
        target: &str,
        targets: &[String],
        call: &ir::FunctionCall,
    ) -> Vec<String> {
        let node_id = self.next_id("fn_call");
        let (kwargs, kwarg_exprs) = self.extract_fn_call_args(call);
        let call_expr = ir::Expr {
            kind: Some(ir::expr::Kind::FunctionCall(call.clone())),
            span: None,
        };
        let node = FnCallNode::new(
            node_id.clone(),
            call.name.clone(),
            FnCallParams {
                kwargs,
                kwarg_exprs,
                targets: if targets.is_empty() {
                    None
                } else {
                    Some(targets.to_vec())
                },
                target: if targets.is_empty() {
                    Some(target.to_string())
                } else {
                    None
                },
                assign_expr: Some(call_expr),
                parallel_index: None,
                aggregates_to: None,
                function_name: self.current_function.clone(),
            },
        );
        self.dag.add_node(node.into());

        for target in targets {
            self.track_var_definition(target, &node_id);
        }

        vec![node_id]
    }

    /// Convert an action call into an ActionCallNode and track target bindings.
    pub fn convert_action_call_with_targets(
        &mut self,
        action: &ir::ActionCall,
        targets: &[String],
    ) -> Vec<String> {
        let node_id = self.next_id("action");
        let kwargs = self.extract_kwargs(&action.kwargs);
        let kwarg_exprs = self.extract_kwarg_exprs(&action.kwargs);
        let module_name = action.module_name.clone();
        let node = ActionCallNode::new(
            node_id.clone(),
            action.action_name.clone(),
            ActionCallParams {
                module_name,
                kwargs,
                kwarg_exprs,
                policies: action.policies.clone(),
                targets: if targets.is_empty() {
                    None
                } else {
                    Some(targets.to_vec())
                },
                target: None,
                parallel_index: None,
                aggregates_to: None,
                spread_loop_var: None,
                spread_collection_expr: None,
                function_name: self.current_function.clone(),
            },
        );
        self.dag.add_node(node.into());

        for target in targets {
            self.track_var_definition(target, &node_id);
        }

        vec![node_id]
    }

    /// Convert kwarg expressions into their string forms for labels/guards.
    pub fn extract_kwargs(&self, kwargs: &[ir::Kwarg]) -> HashMap<String, String> {
        let mut result = HashMap::new();
        for kwarg in kwargs {
            if let Some(value) = &kwarg.value {
                result.insert(kwarg.name.clone(), self.expr_to_string(value));
            }
        }
        result
    }

    /// Copy kwarg expressions so nodes can inspect or rewrite them later.
    pub fn extract_kwarg_exprs(&self, kwargs: &[ir::Kwarg]) -> HashMap<String, ir::Expr> {
        let mut result = HashMap::new();
        for kwarg in kwargs {
            if let Some(value) = &kwarg.value {
                result.insert(kwarg.name.clone(), value.clone());
            }
        }
        result
    }

    /// Build kwargs/kwarg_exprs by merging positional args with known inputs.
    pub fn extract_fn_call_args(
        &self,
        call: &ir::FunctionCall,
    ) -> (HashMap<String, String>, HashMap<String, ir::Expr>) {
        let mut kwargs = self.extract_kwargs(&call.kwargs);
        let mut kwarg_exprs = self.extract_kwarg_exprs(&call.kwargs);

        let mut input_names: Option<Vec<String>> = None;
        if let Some(io) = self
            .function_defs
            .get(&call.name)
            .and_then(|fn_def| fn_def.io.as_ref())
        {
            input_names = Some(io.inputs.clone());
        }

        if let Some(inputs) = input_names {
            for (idx, arg) in call.args.iter().enumerate() {
                if idx >= inputs.len() {
                    break;
                }
                let param_name = &inputs[idx];
                if !kwargs.contains_key(param_name) {
                    kwargs.insert(param_name.clone(), self.expr_to_string(arg));
                }
                if !kwarg_exprs.contains_key(param_name) {
                    kwarg_exprs.insert(param_name.clone(), arg.clone());
                }
            }
        }

        (kwargs, kwarg_exprs)
    }

    /// Render a best-effort string for UI labels and quick debugging.
    pub fn expr_to_string(&self, expr: &ir::Expr) -> String {
        match expr.kind.as_ref() {
            Some(ir::expr::Kind::Variable(var)) => format!("${}", var.name),
            Some(ir::expr::Kind::Literal(lit)) => self.literal_to_string(lit),
            Some(ir::expr::Kind::List(list)) => {
                let items: Vec<String> = list
                    .elements
                    .iter()
                    .map(|item| self.expr_to_string(item))
                    .collect();
                format!("[{}]", items.join(", "))
            }
            Some(ir::expr::Kind::Dict(dict_expr)) => {
                let entries: Vec<String> = dict_expr
                    .entries
                    .iter()
                    .map(|entry| {
                        let key = entry
                            .key
                            .as_ref()
                            .map(|k| self.expr_to_string(k))
                            .unwrap_or_default();
                        let value = entry
                            .value
                            .as_ref()
                            .map(|v| self.expr_to_string(v))
                            .unwrap_or_default();
                        format!("{key}: {value}")
                    })
                    .collect();
                format!("{{{}}}", entries.join(", "))
            }
            Some(ir::expr::Kind::FunctionCall(call)) => {
                let mut parts: Vec<String> = call
                    .args
                    .iter()
                    .map(|arg| self.expr_to_string(arg))
                    .collect();
                for kw in &call.kwargs {
                    if let Some(value) = &kw.value {
                        parts.push(format!("{}={}", kw.name, self.expr_to_string(value)));
                    }
                }
                format!("{}({})", call.name, parts.join(", "))
            }
            Some(
                ir::expr::Kind::BinaryOp(_)
                | ir::expr::Kind::UnaryOp(_)
                | ir::expr::Kind::Index(_)
                | ir::expr::Kind::Dot(_)
                | ir::expr::Kind::ActionCall(_)
                | ir::expr::Kind::ParallelExpr(_)
                | ir::expr::Kind::SpreadExpr(_),
            ) => "null".to_string(),
            None => "null".to_string(),
        }
    }

    /// Render a literal to a stable string for labels.
    pub fn literal_to_string(&self, lit: &ir::Literal) -> String {
        match lit.value.as_ref() {
            Some(ir::literal::Value::IntValue(value)) => value.to_string(),
            Some(ir::literal::Value::FloatValue(value)) => value.to_string(),
            Some(ir::literal::Value::StringValue(value)) => format!("\"{}\"", value),
            Some(ir::literal::Value::BoolValue(value)) => value.to_string().to_lowercase(),
            Some(ir::literal::Value::IsNone(_)) => "null".to_string(),
            None => "null".to_string(),
        }
    }

    /// Convert an expression statement into a node (or a no-op).
    pub fn convert_expr_statement(&mut self, expr_stmt: &ir::ExprStmt) -> Vec<String> {
        let expr = match expr_stmt.expr.as_ref() {
            Some(expr) => expr,
            None => return Vec::new(),
        };

        match expr.kind.as_ref() {
            Some(ir::expr::Kind::ActionCall(action)) => {
                self.convert_action_call_with_targets(action, &[])
            }
            Some(ir::expr::Kind::FunctionCall(call)) => {
                let node_id = self.next_id("fn_call");
                let (kwargs, kwarg_exprs) = self.extract_fn_call_args(call);
                let call_expr = ir::Expr {
                    kind: Some(ir::expr::Kind::FunctionCall(call.clone())),
                    span: None,
                };
                let node = FnCallNode::new(
                    node_id.clone(),
                    call.name.clone(),
                    FnCallParams {
                        kwargs,
                        kwarg_exprs,
                        targets: None,
                        target: None,
                        assign_expr: Some(call_expr),
                        parallel_index: None,
                        aggregates_to: None,
                        function_name: self.current_function.clone(),
                    },
                );
                self.dag.add_node(node.into());
                vec![node_id]
            }
            Some(
                ir::expr::Kind::Literal(_)
                | ir::expr::Kind::Variable(_)
                | ir::expr::Kind::BinaryOp(_)
                | ir::expr::Kind::UnaryOp(_)
                | ir::expr::Kind::List(_)
                | ir::expr::Kind::Dict(_)
                | ir::expr::Kind::Index(_)
                | ir::expr::Kind::Dot(_)
                | ir::expr::Kind::ParallelExpr(_)
                | ir::expr::Kind::SpreadExpr(_),
            ) => {
                let node_id = self.next_id("expression");
                let node = ExpressionNode::new(node_id.clone(), self.current_function.clone());
                self.dag.add_node(node.into());
                vec![node_id]
            }
            None => Vec::new(),
        }
    }

    /// Convert a sleep statement into a SleepNode.
    pub fn convert_sleep(&mut self, sleep_stmt: &ir::SleepStmt) -> Vec<String> {
        let node_id = self.next_id("sleep");
        let label_hint = sleep_stmt.duration.as_ref().map(|expr| {
            let expr_str = self.expr_to_string(expr);
            format!("sleep {expr_str}")
        });
        let node = SleepNode::new(
            node_id.clone(),
            sleep_stmt.duration.clone(),
            label_hint,
            self.current_function.clone(),
        );
        self.dag.add_node(node.into());
        vec![node_id]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DAGNode;

    fn literal_int(value: i64) -> ir::Expr {
        ir::Expr {
            kind: Some(ir::expr::Kind::Literal(ir::Literal {
                value: Some(ir::literal::Value::IntValue(value)),
            })),
            span: None,
        }
    }

    #[test]
    fn test_convert_assignment_literal_happy_path() {
        let mut converter = DAGConverter::new();
        converter.current_function = Some("main".to_string());

        let assign = ir::Assignment {
            targets: vec!["x".to_string()],
            value: Some(literal_int(42)),
        };

        let nodes = converter.convert_assignment(&assign);
        assert_eq!(nodes.len(), 1);
        let node_id = &nodes[0];

        let node = converter
            .dag
            .nodes
            .get(node_id)
            .expect("assignment node missing");
        match node {
            DAGNode::Assignment(assignment) => {
                assert_eq!(assignment.targets, vec!["x".to_string()]);
                assert_eq!(assignment.target.as_deref(), Some("x"));
                assert!(assignment.assign_expr.is_some());
            }
            other => panic!("expected assignment node, got {}", other.node_type()),
        }

        assert_eq!(
            converter.current_scope_vars.get("x"),
            Some(node_id),
            "assignment should track latest definition"
        );
    }
}
