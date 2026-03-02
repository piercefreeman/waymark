//! Exception flow conversion helpers.

use waymark_proto::ast as ir;

use super::super::models::{ConvertedSubgraph, DAGEdge, DagConversionError, EXCEPTION_SCOPE_VAR};
use super::super::nodes::{AssignmentNode, JoinNode};
use super::converter::DAGConverter;

/// Convert try/except blocks into exception-aware DAG edges.
impl DAGConverter {
    /// Convert try/except blocks into exception-aware edges and optional join.
    pub fn convert_try_except(
        &mut self,
        try_except: &ir::TryExcept,
    ) -> Result<ConvertedSubgraph, DagConversionError> {
        self.try_depth += 1;
        let current_depth = self.try_depth;

        let result = (|| {
            let try_graph = if let Some(block) = &try_except.try_block {
                self.convert_block(block)?
            } else {
                ConvertedSubgraph::noop()
            };
            if try_graph.is_noop {
                return Ok(ConvertedSubgraph::noop());
            }

            let mut nodes = try_graph.nodes.clone();
            let mut handler_graphs: Vec<(Vec<String>, ConvertedSubgraph)> = Vec::new();

            for handler in &try_except.handlers {
                let mut graph = if let Some(block) = &handler.block_body {
                    self.convert_block(block)?
                } else {
                    ConvertedSubgraph::noop()
                };
                if let Some(exception_var) = handler.exception_var.as_deref()
                    && !exception_var.is_empty()
                {
                    graph = self.prepend_exception_binding(exception_var, graph);
                }
                nodes.extend(graph.nodes.clone());
                handler_graphs.push((handler.exception_types.clone(), graph));
            }

            let join_needed = !try_graph.exits.is_empty()
                || handler_graphs
                    .iter()
                    .any(|(_, graph)| graph.is_noop || !graph.exits.is_empty());

            let mut join_id: Option<String> = None;
            if join_needed {
                let join_node_id = self.next_id("join");
                let join_node = JoinNode::new(
                    join_node_id.clone(),
                    "join",
                    None,
                    None,
                    self.current_function.clone(),
                );
                self.dag.add_node(join_node.into());
                nodes.push(join_node_id.clone());
                join_id = Some(join_node_id);
            }

            if let Some(join) = join_id.as_ref() {
                for try_exit in &try_graph.exits {
                    self.dag
                        .add_edge(DAGEdge::state_machine_success(try_exit, join));
                }
            }

            let try_exception_sources = try_graph.nodes.clone();
            for (exception_types, handler_graph) in handler_graphs {
                if handler_graph.is_noop {
                    if let Some(join) = join_id.as_ref() {
                        for source in &try_exception_sources {
                            let mut edge = DAGEdge::state_machine_with_exception(
                                source,
                                join,
                                exception_types.clone(),
                            );
                            edge.exception_depth = Some(current_depth as i32);
                            self.dag.add_edge(edge);
                        }
                    }
                    continue;
                }

                if let Some(entry) = handler_graph.entry.as_ref() {
                    for source in &try_exception_sources {
                        let mut edge = DAGEdge::state_machine_with_exception(
                            source,
                            entry,
                            exception_types.clone(),
                        );
                        edge.exception_depth = Some(current_depth as i32);
                        self.dag.add_edge(edge);
                    }
                }

                if let Some(join) = join_id.as_ref() {
                    for handler_exit in &handler_graph.exits {
                        self.dag
                            .add_edge(DAGEdge::state_machine(handler_exit, join));
                    }
                }
            }

            Ok(ConvertedSubgraph {
                entry: try_graph.entry.clone(),
                exits: join_id.into_iter().collect(),
                nodes,
                is_noop: false,
            })
        })();

        self.try_depth = self.try_depth.saturating_sub(1);
        result
    }

    /// Insert an exception binding node before a handler graph.
    ///
    /// Example:
    /// - except Exception as err: ...
    ///   Inserts "err = __exception__" before the handler body.
    pub fn prepend_exception_binding(
        &mut self,
        exception_var: &str,
        graph: ConvertedSubgraph,
    ) -> ConvertedSubgraph {
        let binding_id = self.next_id("exc_bind");
        let label = format!("{exception_var} = {EXCEPTION_SCOPE_VAR}");
        let assign_expr = ir::Expr {
            kind: Some(ir::expr::Kind::Variable(ir::Variable {
                name: EXCEPTION_SCOPE_VAR.to_string(),
            })),
            span: None,
        };
        let node = AssignmentNode::new(
            binding_id.clone(),
            vec![exception_var.to_string()],
            None,
            Some(assign_expr),
            Some(label),
            self.current_function.clone(),
        );
        self.dag.add_node(node.into());
        self.track_var_definition(exception_var, &binding_id);

        if let Some(entry) = graph.entry.as_ref() {
            self.dag
                .add_edge(DAGEdge::state_machine(&binding_id, entry));
        }

        let mut nodes = Vec::with_capacity(graph.nodes.len() + 1);
        nodes.push(binding_id.clone());
        nodes.extend(graph.nodes.clone());
        let exits = if graph.is_noop {
            vec![binding_id.clone()]
        } else {
            graph.exits.clone()
        };

        ConvertedSubgraph {
            entry: Some(binding_id),
            exits,
            nodes,
            is_noop: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_helpers::build_dag_with_pointers;

    #[test]
    fn test_convert_try_except_happy_path() {
        let dag = build_dag_with_pointers(
            r#"
            fn main(input: [], output: []):
                try:
                    @work()
                except Exception as err:
                    @handle()
            "#,
        );

        assert!(
            dag.edges
                .iter()
                .any(|edge| { edge.exception_types.is_some() && edge.exception_depth == Some(1) }),
            "try/except should emit exception edges with depth metadata"
        );
        assert!(
            dag.nodes
                .values()
                .any(|node| node.node_type() == "assignment"),
            "try/except with `as err` should prepend exception binding assignment"
        );
    }
}
