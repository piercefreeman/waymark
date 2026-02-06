use std::collections::{HashMap, HashSet};

use crate::messages::ast as ir;
use crate::rappel_core::dag::{
    ActionCallNode, ActionCallParams, AggregatorNode, AssignmentNode, BranchNode, BreakNode,
    ContinueNode, DAG, DAGConverter, DAGEdge, DAGNode, EXCEPTION_SCOPE_VAR, EdgeType,
    ExpressionNode, FnCallNode, FnCallParams, InputNode, JoinNode, OutputNode, ParallelNode,
    ReturnNode,
};
use crate::rappel_core::ir_parser::{IRParser, parse_program};

fn dedent(source: &str) -> String {
    let lines: Vec<&str> = source.lines().collect();
    let mut min_indent = usize::MAX;
    for line in &lines {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let indent = line.chars().take_while(|ch| ch.is_whitespace()).count();
        min_indent = min_indent.min(indent);
    }
    if min_indent == usize::MAX {
        return String::new();
    }
    lines
        .into_iter()
        .map(|line| {
            if line.len() >= min_indent {
                line[min_indent..].to_string()
            } else {
                line.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn build_dag(source: &str) -> DAG {
    let source = dedent(source);
    let program = parse_program(source.trim()).expect("parse program");
    let mut converter = DAGConverter::new();
    converter
        .convert_with_pointers(&program)
        .expect("convert program")
}

fn parse_expr(source: &str) -> ir::Expr {
    IRParser::new("    ")
        .parse_expr(source.trim())
        .expect("parse expr")
}

fn normalize_len_calls(expr: &mut ir::Expr) {
    let kind = match expr.kind.as_mut() {
        Some(kind) => kind,
        None => return,
    };
    match kind {
        ir::expr::Kind::BinaryOp(op) => {
            if let Some(left) = op.left.as_mut() {
                normalize_len_calls(left);
            }
            if let Some(right) = op.right.as_mut() {
                normalize_len_calls(right);
            }
        }
        ir::expr::Kind::UnaryOp(op) => {
            if let Some(operand) = op.operand.as_mut() {
                normalize_len_calls(operand);
            }
        }
        ir::expr::Kind::FunctionCall(call) => {
            if call.global_function == ir::GlobalFunction::Len as i32 && call.name.is_empty() {
                call.name = "len".to_string();
            }
            for arg in call.args.iter_mut() {
                normalize_len_calls(arg);
            }
            for kwarg in call.kwargs.iter_mut() {
                if let Some(value) = kwarg.value.as_mut() {
                    normalize_len_calls(value);
                }
            }
        }
        ir::expr::Kind::List(list) => {
            for item in list.elements.iter_mut() {
                normalize_len_calls(item);
            }
        }
        ir::expr::Kind::Dict(dict_expr) => {
            for entry in dict_expr.entries.iter_mut() {
                if let Some(key) = entry.key.as_mut() {
                    normalize_len_calls(key);
                }
                if let Some(value) = entry.value.as_mut() {
                    normalize_len_calls(value);
                }
            }
        }
        ir::expr::Kind::Index(index) => {
            if let Some(object) = index.object.as_mut() {
                normalize_len_calls(object);
            }
            if let Some(index_expr) = index.index.as_mut() {
                normalize_len_calls(index_expr);
            }
        }
        ir::expr::Kind::Dot(dot) => {
            if let Some(object) = dot.object.as_mut() {
                normalize_len_calls(object);
            }
        }
        ir::expr::Kind::ActionCall(action) => {
            for kwarg in action.kwargs.iter_mut() {
                if let Some(value) = kwarg.value.as_mut() {
                    normalize_len_calls(value);
                }
            }
        }
        ir::expr::Kind::ParallelExpr(parallel) => {
            for call in parallel.calls.iter_mut() {
                match call.kind.as_mut() {
                    Some(ir::call::Kind::Action(action)) => {
                        for kwarg in action.kwargs.iter_mut() {
                            if let Some(value) = kwarg.value.as_mut() {
                                normalize_len_calls(value);
                            }
                        }
                    }
                    Some(ir::call::Kind::Function(function)) => {
                        for arg in function.args.iter_mut() {
                            normalize_len_calls(arg);
                        }
                        for kwarg in function.kwargs.iter_mut() {
                            if let Some(value) = kwarg.value.as_mut() {
                                normalize_len_calls(value);
                            }
                        }
                    }
                    None => {}
                }
            }
        }
        ir::expr::Kind::SpreadExpr(spread) => {
            if let Some(collection) = spread.collection.as_mut() {
                normalize_len_calls(collection);
            }
            if let Some(action) = spread.action.as_mut() {
                for kwarg in action.kwargs.iter_mut() {
                    if let Some(value) = kwarg.value.as_mut() {
                        normalize_len_calls(value);
                    }
                }
            }
        }
        ir::expr::Kind::Literal(_) | ir::expr::Kind::Variable(_) => {}
    }
}

fn parse_guard(source: &str) -> ir::Expr {
    let mut expr = parse_expr(source);
    normalize_len_calls(&mut expr);
    expr
}

fn input_node(node_id: &str, inputs: Vec<&str>) -> DAGNode {
    DAGNode::Input(InputNode::new(
        node_id,
        inputs.into_iter().map(|value| value.to_string()).collect(),
        Some("main".to_string()),
    ))
}

fn output_node(node_id: &str, outputs: Vec<&str>) -> DAGNode {
    DAGNode::Output(OutputNode::new(
        node_id,
        outputs.into_iter().map(|value| value.to_string()).collect(),
        Some("main".to_string()),
    ))
}

#[derive(Default)]
struct ActionNodeOptions {
    parallel_index: Option<i32>,
    aggregates_to: Option<String>,
    spread_loop_var: Option<String>,
    spread_collection_expr: Option<ir::Expr>,
}

fn action_node(
    node_id: &str,
    action_name: &str,
    targets: Option<Vec<&str>>,
    target: Option<&str>,
    options: ActionNodeOptions,
) -> DAGNode {
    let targets_vec =
        targets.map(|items| items.into_iter().map(|value| value.to_string()).collect());
    let ActionNodeOptions {
        parallel_index,
        aggregates_to,
        spread_loop_var,
        spread_collection_expr,
    } = options;
    DAGNode::ActionCall(ActionCallNode::new(
        node_id,
        action_name,
        ActionCallParams {
            module_name: None,
            kwargs: HashMap::new(),
            kwarg_exprs: HashMap::new(),
            policies: Vec::new(),
            targets: targets_vec,
            target: target.map(|value| value.to_string()),
            parallel_index,
            aggregates_to,
            spread_loop_var,
            spread_collection_expr,
            function_name: Some("main".to_string()),
        },
    ))
}

fn fn_call_node(
    node_id: &str,
    called_function: &str,
    targets: Option<Vec<&str>>,
    assign_expr: Option<ir::Expr>,
) -> DAGNode {
    let targets_vec =
        targets.map(|items| items.into_iter().map(|value| value.to_string()).collect());
    DAGNode::FnCall(FnCallNode::new(
        node_id,
        called_function,
        FnCallParams {
            kwargs: HashMap::new(),
            kwarg_exprs: HashMap::new(),
            targets: targets_vec,
            target: None,
            assign_expr,
            parallel_index: None,
            aggregates_to: None,
            function_name: Some("main".to_string()),
        },
    ))
}

fn assignment_node(
    node_id: &str,
    targets: Vec<&str>,
    assign_expr: Option<ir::Expr>,
    label_hint: Option<String>,
) -> DAGNode {
    DAGNode::Assignment(AssignmentNode::new(
        node_id,
        targets.into_iter().map(|value| value.to_string()).collect(),
        None,
        assign_expr,
        label_hint,
        Some("main".to_string()),
    ))
}

fn aggregator_node(
    node_id: &str,
    aggregates_from: &str,
    targets: Option<Vec<&str>>,
    aggregator_kind: &str,
) -> DAGNode {
    let targets_vec =
        targets.map(|items| items.into_iter().map(|value| value.to_string()).collect());
    DAGNode::Aggregator(AggregatorNode::new(
        node_id,
        aggregates_from,
        targets_vec,
        None,
        aggregator_kind,
        Some("main".to_string()),
    ))
}

fn branch_node(node_id: &str, description: &str) -> DAGNode {
    DAGNode::Branch(BranchNode::new(
        node_id,
        description,
        Some("main".to_string()),
    ))
}

fn join_node(node_id: &str, description: &str) -> DAGNode {
    DAGNode::Join(JoinNode::new(
        node_id,
        description,
        None,
        None,
        Some("main".to_string()),
    ))
}

fn return_node(node_id: &str, assign_expr: Option<ir::Expr>, target: Option<&str>) -> DAGNode {
    DAGNode::Return(ReturnNode::new(
        node_id,
        assign_expr,
        None,
        target.map(|value| value.to_string()),
        Some("main".to_string()),
    ))
}

fn expression_node(node_id: &str) -> DAGNode {
    DAGNode::Expression(ExpressionNode::new(node_id, Some("main".to_string())))
}

fn break_node(node_id: &str) -> DAGNode {
    DAGNode::Break(BreakNode::new(node_id, Some("main".to_string())))
}

fn continue_node(node_id: &str) -> DAGNode {
    DAGNode::Continue(ContinueNode::new(node_id, Some("main".to_string())))
}

fn parallel_node(node_id: &str) -> DAGNode {
    DAGNode::Parallel(ParallelNode::new(node_id, Some("main".to_string())))
}

fn assert_nodes(dag: &DAG, expected_nodes: Vec<DAGNode>) {
    assert_eq!(dag.nodes.len(), expected_nodes.len());
    for expected in expected_nodes {
        let actual = dag.nodes.get(expected.id()).expect("expected node missing");
        assert_node_eq(actual, &expected);
    }
}

fn assert_node_eq(actual: &DAGNode, expected: &DAGNode) {
    match (actual, expected) {
        (DAGNode::Input(a), DAGNode::Input(e)) => {
            assert_eq!(a.id, e.id);
            assert_eq!(a.function_name, e.function_name);
            assert_eq!(a.io_vars, e.io_vars);
        }
        (DAGNode::Output(a), DAGNode::Output(e)) => {
            assert_eq!(a.id, e.id);
            assert_eq!(a.function_name, e.function_name);
            assert_eq!(a.io_vars, e.io_vars);
        }
        (DAGNode::Assignment(a), DAGNode::Assignment(e)) => {
            assert_eq!(a.id, e.id);
            assert_eq!(a.function_name, e.function_name);
            assert_eq!(a.targets, e.targets);
            assert_eq!(a.target, e.target);
            assert_eq!(a.assign_expr, e.assign_expr);
            assert_eq!(a.label_hint, e.label_hint);
        }
        (DAGNode::ActionCall(a), DAGNode::ActionCall(e)) => {
            assert_eq!(a.id, e.id);
            assert_eq!(a.function_name, e.function_name);
            assert_eq!(a.action_name, e.action_name);
            assert_eq!(a.module_name, e.module_name);
            assert_eq!(a.kwargs, e.kwargs);
            assert_eq!(a.kwarg_exprs, e.kwarg_exprs);
            assert_eq!(a.policies, e.policies);
            assert_eq!(a.targets, e.targets);
            assert_eq!(a.target, e.target);
            assert_eq!(a.parallel_index, e.parallel_index);
            assert_eq!(a.aggregates_to, e.aggregates_to);
            assert_eq!(a.spread_loop_var, e.spread_loop_var);
            assert_eq!(a.spread_collection_expr, e.spread_collection_expr);
        }
        (DAGNode::FnCall(a), DAGNode::FnCall(e)) => {
            assert_eq!(a.id, e.id);
            assert_eq!(a.function_name, e.function_name);
            assert_eq!(a.called_function, e.called_function);
            assert_eq!(a.kwargs, e.kwargs);
            assert_eq!(a.kwarg_exprs, e.kwarg_exprs);
            assert_eq!(a.targets, e.targets);
            assert_eq!(a.target, e.target);
            assert_eq!(a.assign_expr, e.assign_expr);
            assert_eq!(a.parallel_index, e.parallel_index);
            assert_eq!(a.aggregates_to, e.aggregates_to);
        }
        (DAGNode::Parallel(a), DAGNode::Parallel(e)) => {
            assert_eq!(a.id, e.id);
            assert_eq!(a.function_name, e.function_name);
        }
        (DAGNode::Aggregator(a), DAGNode::Aggregator(e)) => {
            assert_eq!(a.id, e.id);
            assert_eq!(a.function_name, e.function_name);
            assert_eq!(a.aggregates_from, e.aggregates_from);
            assert_eq!(a.targets, e.targets);
            assert_eq!(a.target, e.target);
            assert_eq!(a.aggregator_kind, e.aggregator_kind);
        }
        (DAGNode::Branch(a), DAGNode::Branch(e)) => {
            assert_eq!(a.id, e.id);
            assert_eq!(a.function_name, e.function_name);
            assert_eq!(a.description, e.description);
        }
        (DAGNode::Join(a), DAGNode::Join(e)) => {
            assert_eq!(a.id, e.id);
            assert_eq!(a.function_name, e.function_name);
            assert_eq!(a.description, e.description);
        }
        (DAGNode::Return(a), DAGNode::Return(e)) => {
            assert_eq!(a.id, e.id);
            assert_eq!(a.function_name, e.function_name);
            assert_eq!(a.assign_expr, e.assign_expr);
            assert_eq!(a.targets, e.targets);
            assert_eq!(a.target, e.target);
        }
        (DAGNode::Break(a), DAGNode::Break(e)) => {
            assert_eq!(a.id, e.id);
            assert_eq!(a.function_name, e.function_name);
        }
        (DAGNode::Continue(a), DAGNode::Continue(e)) => {
            assert_eq!(a.id, e.id);
            assert_eq!(a.function_name, e.function_name);
        }
        (DAGNode::Sleep(a), DAGNode::Sleep(e)) => {
            assert_eq!(a.id, e.id);
            assert_eq!(a.function_name, e.function_name);
            assert_eq!(a.duration_expr, e.duration_expr);
            assert_eq!(a.label_hint, e.label_hint);
        }
        (DAGNode::Expression(a), DAGNode::Expression(e)) => {
            assert_eq!(a.id, e.id);
            assert_eq!(a.function_name, e.function_name);
        }
        _ => panic!("node type mismatch: {actual:?} != {expected:?}"),
    }
}

fn assert_edge_pairs(dag: &DAG, edge_type: EdgeType, expected_edges: Vec<(&DAGNode, &DAGNode)>) {
    let expected: HashSet<(String, String)> = expected_edges
        .into_iter()
        .map(|(source, target)| (source.id().to_string(), target.id().to_string()))
        .collect();
    let actual: HashSet<(String, String)> = dag
        .edges
        .iter()
        .filter(|edge| edge.edge_type == edge_type)
        .map(|edge| (edge.source.clone(), edge.target.clone()))
        .collect();
    assert_eq!(actual, expected);
}

fn get_edge(dag: &DAG, source: &DAGNode, target: &DAGNode, edge_type: EdgeType) -> DAGEdge {
    let matches: Vec<DAGEdge> = dag
        .edges
        .iter()
        .filter(|edge| {
            edge.edge_type == edge_type && edge.source == source.id() && edge.target == target.id()
        })
        .cloned()
        .collect();
    assert_eq!(matches.len(), 1);
    matches[0].clone()
}

#[test]
fn test_assignment_literal_builds_assignment_node() {
    let dag = build_dag(
        r#"
        fn main(input: [], output: []):
            value = 42
        "#,
    );

    let input_def = input_node("main_input_1", vec![]);
    let assign_def = assignment_node("assign_2", vec!["value"], Some(parse_expr("42")), None);
    let output_def = output_node("main_output_3", vec![]);

    assert_nodes(
        &dag,
        vec![input_def.clone(), assign_def.clone(), output_def.clone()],
    );

    let state_edges = vec![(&input_def, &assign_def), (&assign_def, &output_def)];
    assert_edge_pairs(&dag, EdgeType::StateMachine, state_edges);
    assert_edge_pairs(&dag, EdgeType::DataFlow, vec![]);
}

#[test]
fn test_assignment_action_call_builds_action_node() {
    let dag = build_dag(
        r#"
        fn main(input: [], output: [result]):
            result = @noop()
        "#,
    );

    let input_def = input_node("main_input_1", vec![]);
    let action_def = action_node(
        "action_2",
        "noop",
        Some(vec!["result"]),
        None,
        ActionNodeOptions::default(),
    );
    let output_def = output_node("main_output_3", vec!["result"]);

    assert_nodes(
        &dag,
        vec![input_def.clone(), action_def.clone(), output_def.clone()],
    );

    let state_edges = vec![(&input_def, &action_def), (&action_def, &output_def)];
    assert_edge_pairs(&dag, EdgeType::StateMachine, state_edges);
    assert_edge_pairs(&dag, EdgeType::DataFlow, vec![]);
}

#[test]
fn test_assignment_function_call_builds_fn_call_node() {
    let dag = build_dag(
        r#"
        fn main(input: [], output: [result]):
            result = helper()
        "#,
    );

    let input_def = input_node("main_input_1", vec![]);
    let fn_call_def = fn_call_node(
        "fn_call_2",
        "helper",
        Some(vec!["result"]),
        Some(parse_expr("helper()")),
    );
    let output_def = output_node("main_output_3", vec!["result"]);

    assert_nodes(
        &dag,
        vec![input_def.clone(), fn_call_def.clone(), output_def.clone()],
    );

    let state_edges = vec![(&input_def, &fn_call_def), (&fn_call_def, &output_def)];
    assert_edge_pairs(&dag, EdgeType::StateMachine, state_edges);
    assert_edge_pairs(&dag, EdgeType::DataFlow, vec![]);
}

#[test]
fn test_spread_expression_builds_action_and_aggregator() {
    let dag = build_dag(
        r#"
        fn main(input: [], output: [results]):
            results = spread [1, 2]: item -> @noop()
        "#,
    );

    let collection_expr = parse_expr("[1, 2]");
    let input_def = input_node("main_input_1", vec![]);
    let action_def = action_node(
        "spread_action_2",
        "noop",
        None,
        Some("_spread_result"),
        ActionNodeOptions {
            aggregates_to: Some("aggregator_3".to_string()),
            spread_loop_var: Some("item".to_string()),
            spread_collection_expr: Some(collection_expr),
            ..ActionNodeOptions::default()
        },
    );
    let aggregator_def = aggregator_node(
        "aggregator_3",
        "spread_action_2",
        Some(vec!["results"]),
        "aggregate",
    );
    let output_def = output_node("main_output_4", vec!["results"]);

    assert_nodes(
        &dag,
        vec![
            input_def.clone(),
            action_def.clone(),
            aggregator_def.clone(),
            output_def.clone(),
        ],
    );

    let state_edges = vec![
        (&input_def, &action_def),
        (&action_def, &aggregator_def),
        (&aggregator_def, &output_def),
    ];
    let data_edges = vec![(&action_def, &aggregator_def)];
    assert_edge_pairs(&dag, EdgeType::StateMachine, state_edges);
    assert_edge_pairs(&dag, EdgeType::DataFlow, data_edges);
    let spread_edge = get_edge(&dag, &action_def, &aggregator_def, EdgeType::DataFlow);
    assert_eq!(spread_edge.variable, Some("_spread_result".to_string()));
}

#[test]
fn test_spread_action_statement_builds_action_and_aggregator() {
    let dag = build_dag(
        r#"
        fn main(input: [], output: []):
            spread [1, 2]: item -> @noop()
        "#,
    );

    let collection_expr = parse_expr("[1, 2]");
    let input_def = input_node("main_input_1", vec![]);
    let action_def = action_node(
        "spread_action_2",
        "noop",
        None,
        Some("_spread_result"),
        ActionNodeOptions {
            aggregates_to: Some("aggregator_3".to_string()),
            spread_loop_var: Some("item".to_string()),
            spread_collection_expr: Some(collection_expr),
            ..ActionNodeOptions::default()
        },
    );
    let aggregator_def = aggregator_node("aggregator_3", "spread_action_2", None, "aggregate");
    let output_def = output_node("main_output_4", vec![]);

    assert_nodes(
        &dag,
        vec![
            input_def.clone(),
            action_def.clone(),
            aggregator_def.clone(),
            output_def.clone(),
        ],
    );

    let state_edges = vec![
        (&input_def, &action_def),
        (&action_def, &aggregator_def),
        (&aggregator_def, &output_def),
    ];
    let data_edges = vec![(&action_def, &aggregator_def)];
    assert_edge_pairs(&dag, EdgeType::StateMachine, state_edges);
    assert_edge_pairs(&dag, EdgeType::DataFlow, data_edges);
    let spread_edge = get_edge(&dag, &action_def, &aggregator_def, EdgeType::DataFlow);
    assert_eq!(spread_edge.variable, Some("_spread_result".to_string()));
}

#[test]
fn test_parallel_block_builds_parallel_graph() {
    let dag = build_dag(
        r#"
        fn main(input: [], output: []):
            parallel:
                @a()
                @b()
        "#,
    );

    let input_def = input_node("main_input_1", vec![]);
    let parallel_def = parallel_node("parallel_2");
    let action_one = action_node(
        "parallel_action_4",
        "a",
        None,
        None,
        ActionNodeOptions {
            parallel_index: Some(0),
            ..ActionNodeOptions::default()
        },
    );
    let action_two = action_node(
        "parallel_action_5",
        "b",
        None,
        None,
        ActionNodeOptions {
            parallel_index: Some(1),
            ..ActionNodeOptions::default()
        },
    );
    let aggregator_def = aggregator_node("parallel_aggregator_3", "parallel_2", None, "parallel");
    let output_def = output_node("main_output_6", vec![]);

    assert_nodes(
        &dag,
        vec![
            input_def.clone(),
            parallel_def.clone(),
            action_one.clone(),
            action_two.clone(),
            aggregator_def.clone(),
            output_def.clone(),
        ],
    );

    let state_edges = vec![
        (&input_def, &parallel_def),
        (&parallel_def, &action_one),
        (&parallel_def, &action_two),
        (&action_one, &aggregator_def),
        (&action_two, &aggregator_def),
        (&aggregator_def, &output_def),
    ];
    assert_edge_pairs(&dag, EdgeType::StateMachine, state_edges);
    assert_edge_pairs(&dag, EdgeType::DataFlow, vec![]);

    let edge_zero = get_edge(&dag, &parallel_def, &action_one, EdgeType::StateMachine);
    let edge_one = get_edge(&dag, &parallel_def, &action_two, EdgeType::StateMachine);
    assert_eq!(edge_zero.condition, Some("parallel:0".to_string()));
    assert_eq!(edge_one.condition, Some("parallel:1".to_string()));
}

#[test]
fn test_parallel_expression_builds_parallel_graph_with_targets() {
    let dag = build_dag(
        r#"
        fn main(input: [], output: [a, b]):
            a, b = parallel:
                @x()
                @y()
        "#,
    );

    let input_def = input_node("main_input_1", vec![]);
    let parallel_def = parallel_node("parallel_2");
    let action_one = action_node(
        "parallel_action_4",
        "x",
        None,
        Some("a"),
        ActionNodeOptions {
            parallel_index: Some(0),
            ..ActionNodeOptions::default()
        },
    );
    let action_two = action_node(
        "parallel_action_5",
        "y",
        None,
        Some("b"),
        ActionNodeOptions {
            parallel_index: Some(1),
            ..ActionNodeOptions::default()
        },
    );
    let aggregator_def = aggregator_node(
        "parallel_aggregator_3",
        "parallel_2",
        Some(vec!["a", "b"]),
        "parallel",
    );
    let output_def = output_node("main_output_6", vec!["a", "b"]);

    assert_nodes(
        &dag,
        vec![
            input_def.clone(),
            parallel_def.clone(),
            action_one.clone(),
            action_two.clone(),
            aggregator_def.clone(),
            output_def.clone(),
        ],
    );

    let state_edges = vec![
        (&input_def, &parallel_def),
        (&parallel_def, &action_one),
        (&parallel_def, &action_two),
        (&action_one, &aggregator_def),
        (&action_two, &aggregator_def),
        (&aggregator_def, &output_def),
    ];
    assert_edge_pairs(&dag, EdgeType::StateMachine, state_edges);
    assert_edge_pairs(&dag, EdgeType::DataFlow, vec![]);

    let edge_zero = get_edge(&dag, &parallel_def, &action_one, EdgeType::StateMachine);
    let edge_one = get_edge(&dag, &parallel_def, &action_two, EdgeType::StateMachine);
    assert_eq!(edge_zero.condition, Some("parallel:0".to_string()));
    assert_eq!(edge_one.condition, Some("parallel:1".to_string()));
}

#[test]
fn test_action_call_statement_builds_action_node() {
    let dag = build_dag(
        r#"
        fn main(input: [], output: []):
            @noop()
        "#,
    );

    let input_def = input_node("main_input_1", vec![]);
    let action_def = action_node("action_2", "noop", None, None, ActionNodeOptions::default());
    let output_def = output_node("main_output_3", vec![]);

    assert_nodes(
        &dag,
        vec![input_def.clone(), action_def.clone(), output_def.clone()],
    );
    let state_edges = vec![(&input_def, &action_def), (&action_def, &output_def)];
    assert_edge_pairs(&dag, EdgeType::StateMachine, state_edges);
    assert_edge_pairs(&dag, EdgeType::DataFlow, vec![]);
}

#[test]
fn test_expr_statement_function_call_builds_fn_call_node() {
    let dag = build_dag(
        r#"
        fn main(input: [], output: []):
            helper()
        "#,
    );

    let input_def = input_node("main_input_1", vec![]);
    let fn_call_def = fn_call_node("fn_call_2", "helper", None, Some(parse_expr("helper()")));
    let output_def = output_node("main_output_3", vec![]);

    assert_nodes(
        &dag,
        vec![input_def.clone(), fn_call_def.clone(), output_def.clone()],
    );
    let state_edges = vec![(&input_def, &fn_call_def), (&fn_call_def, &output_def)];
    assert_edge_pairs(&dag, EdgeType::StateMachine, state_edges);
    assert_edge_pairs(&dag, EdgeType::DataFlow, vec![]);
}

#[test]
fn test_expr_statement_builds_expression_node() {
    let dag = build_dag(
        r#"
        fn main(input: [], output: []):
            a + b
        "#,
    );

    let input_def = input_node("main_input_1", vec![]);
    let expr_def = expression_node("expression_2");
    let output_def = output_node("main_output_3", vec![]);

    assert_nodes(
        &dag,
        vec![input_def.clone(), expr_def.clone(), output_def.clone()],
    );
    let state_edges = vec![(&input_def, &expr_def), (&expr_def, &output_def)];
    assert_edge_pairs(&dag, EdgeType::StateMachine, state_edges);
    assert_edge_pairs(&dag, EdgeType::DataFlow, vec![]);
}

#[test]
fn test_for_loop_builds_loop_nodes() {
    let dag = build_dag(
        r#"
        fn main(input: [], output: []):
            for item in [1, 2]:
                @work()
        "#,
    );

    let loop_var = "__loop_loop_2_i";
    let continue_guard = parse_guard(&format!("{loop_var} < len(items=[1, 2])"));
    let break_guard = parse_guard(&format!("not ({loop_var} < len(items=[1, 2]))"));

    let input_def = input_node("main_input_1", vec![]);
    let init_def = assignment_node(
        "loop_init_3",
        vec![loop_var],
        Some(parse_expr("0")),
        Some(format!("{loop_var} = 0")),
    );
    let cond_def = branch_node("loop_cond_4", "for item in [1, 2]");
    let extract_def = assignment_node(
        "loop_extract_5",
        vec!["item"],
        Some(parse_expr(&format!("[1, 2][{loop_var}]"))),
        Some(format!("item = [1, 2][{loop_var}]")),
    );
    let action_def = action_node("action_8", "work", None, None, ActionNodeOptions::default());
    let incr_def = assignment_node(
        "loop_incr_7",
        vec![loop_var],
        Some(parse_expr(&format!("{loop_var} + 1"))),
        Some(format!("{loop_var} = {loop_var} + 1")),
    );
    let exit_def = join_node("loop_exit_6", "end for item");
    let output_def = output_node("main_output_9", vec![]);

    assert_nodes(
        &dag,
        vec![
            input_def.clone(),
            init_def.clone(),
            cond_def.clone(),
            extract_def.clone(),
            action_def.clone(),
            incr_def.clone(),
            exit_def.clone(),
            output_def.clone(),
        ],
    );

    let state_edges = vec![
        (&input_def, &init_def),
        (&init_def, &cond_def),
        (&cond_def, &extract_def),
        (&cond_def, &exit_def),
        (&extract_def, &action_def),
        (&action_def, &incr_def),
        (&incr_def, &cond_def),
        (&exit_def, &output_def),
    ];
    assert_edge_pairs(&dag, EdgeType::StateMachine, state_edges);
    assert_edge_pairs(&dag, EdgeType::DataFlow, vec![]);

    let guarded_edge = get_edge(&dag, &cond_def, &extract_def, EdgeType::StateMachine);
    assert_eq!(guarded_edge.condition, Some("guarded".to_string()));
    assert_eq!(guarded_edge.guard_expr, Some(continue_guard));

    let break_edge = get_edge(&dag, &cond_def, &exit_def, EdgeType::StateMachine);
    assert_eq!(break_edge.condition, Some("guarded".to_string()));
    assert_eq!(break_edge.guard_expr, Some(break_guard));

    let loop_back = get_edge(&dag, &incr_def, &cond_def, EdgeType::StateMachine);
    assert!(loop_back.is_loop_back);
}

#[test]
fn test_while_loop_builds_loop_nodes() {
    let dag = build_dag(
        r#"
        fn main(input: [], output: []):
            while True:
                @work()
        "#,
    );

    let continue_guard = parse_expr("True");
    let break_guard = parse_expr("not True");

    let input_def = input_node("main_input_1", vec![]);
    let cond_def = branch_node("loop_cond_2", "while true");
    let action_def = action_node("action_5", "work", None, None, ActionNodeOptions::default());
    let continue_def = assignment_node(
        "loop_continue_4",
        vec![],
        None,
        Some("loop_continue".to_string()),
    );
    let exit_def = join_node("loop_exit_3", "end while true");
    let output_def = output_node("main_output_6", vec![]);

    assert_nodes(
        &dag,
        vec![
            input_def.clone(),
            cond_def.clone(),
            action_def.clone(),
            continue_def.clone(),
            exit_def.clone(),
            output_def.clone(),
        ],
    );

    let state_edges = vec![
        (&input_def, &cond_def),
        (&cond_def, &action_def),
        (&action_def, &continue_def),
        (&continue_def, &cond_def),
        (&cond_def, &exit_def),
        (&exit_def, &output_def),
    ];
    assert_edge_pairs(&dag, EdgeType::StateMachine, state_edges);
    assert_edge_pairs(&dag, EdgeType::DataFlow, vec![]);

    let guarded_edge = get_edge(&dag, &cond_def, &action_def, EdgeType::StateMachine);
    assert_eq!(guarded_edge.condition, Some("guarded".to_string()));
    assert_eq!(guarded_edge.guard_expr, Some(continue_guard));

    let break_edge = get_edge(&dag, &cond_def, &exit_def, EdgeType::StateMachine);
    assert_eq!(break_edge.condition, Some("guarded".to_string()));
    assert_eq!(break_edge.guard_expr, Some(break_guard));

    let loop_back = get_edge(&dag, &continue_def, &cond_def, EdgeType::StateMachine);
    assert!(loop_back.is_loop_back);
}

#[test]
fn test_conditional_builds_branch_join_graph() {
    let dag = build_dag(
        r#"
        fn main(input: [], output: []):
            if True:
                @first()
            elif False:
                @second()
            else:
                @third()
        "#,
    );

    let if_guard = parse_expr("True");
    let elif_guard = parse_expr("not True and False");

    let input_def = input_node("main_input_1", vec![]);
    let branch_def = branch_node("branch_2", "branch");
    let first_def = action_node(
        "action_3",
        "first",
        None,
        None,
        ActionNodeOptions::default(),
    );
    let second_def = action_node(
        "action_4",
        "second",
        None,
        None,
        ActionNodeOptions::default(),
    );
    let third_def = action_node(
        "action_5",
        "third",
        None,
        None,
        ActionNodeOptions::default(),
    );
    let join_def = join_node("join_6", "join");
    let output_def = output_node("main_output_7", vec![]);

    assert_nodes(
        &dag,
        vec![
            input_def.clone(),
            branch_def.clone(),
            first_def.clone(),
            second_def.clone(),
            third_def.clone(),
            join_def.clone(),
            output_def.clone(),
        ],
    );

    let state_edges = vec![
        (&input_def, &branch_def),
        (&branch_def, &first_def),
        (&branch_def, &second_def),
        (&branch_def, &third_def),
        (&first_def, &join_def),
        (&second_def, &join_def),
        (&third_def, &join_def),
        (&join_def, &output_def),
    ];
    assert_edge_pairs(&dag, EdgeType::StateMachine, state_edges);
    assert_edge_pairs(&dag, EdgeType::DataFlow, vec![]);

    let if_edge = get_edge(&dag, &branch_def, &first_def, EdgeType::StateMachine);
    assert_eq!(if_edge.condition, Some("guarded".to_string()));
    assert_eq!(if_edge.guard_expr, Some(if_guard));

    let elif_edge = get_edge(&dag, &branch_def, &second_def, EdgeType::StateMachine);
    assert_eq!(elif_edge.condition, Some("guarded".to_string()));
    assert_eq!(elif_edge.guard_expr, Some(elif_guard));

    let else_edge = get_edge(&dag, &branch_def, &third_def, EdgeType::StateMachine);
    assert_eq!(else_edge.condition, Some("else".to_string()));
    assert!(else_edge.is_else);
}

#[test]
fn test_try_except_builds_exception_edges() {
    let dag = build_dag(
        r#"
        fn main(input: [], output: []):
            try:
                @work()
            except Exception as err:
                @handle()
        "#,
    );

    let input_def = input_node("main_input_1", vec![]);
    let try_def = action_node("action_2", "work", None, None, ActionNodeOptions::default());
    let handler_def = action_node(
        "action_3",
        "handle",
        None,
        None,
        ActionNodeOptions::default(),
    );
    let bind_def = assignment_node(
        "exc_bind_4",
        vec!["err"],
        Some(parse_expr(EXCEPTION_SCOPE_VAR)),
        Some(format!("err = {EXCEPTION_SCOPE_VAR}")),
    );
    let join_def = join_node("join_5", "join");
    let output_def = output_node("main_output_6", vec![]);

    assert_nodes(
        &dag,
        vec![
            input_def.clone(),
            try_def.clone(),
            handler_def.clone(),
            bind_def.clone(),
            join_def.clone(),
            output_def.clone(),
        ],
    );

    let state_edges = vec![
        (&input_def, &try_def),
        (&try_def, &join_def),
        (&try_def, &bind_def),
        (&bind_def, &handler_def),
        (&handler_def, &join_def),
        (&join_def, &output_def),
    ];
    assert_edge_pairs(&dag, EdgeType::StateMachine, state_edges);
    assert_edge_pairs(&dag, EdgeType::DataFlow, vec![]);

    let success_edge = get_edge(&dag, &try_def, &join_def, EdgeType::StateMachine);
    assert_eq!(success_edge.condition, Some("success".to_string()));

    let exc_edge = get_edge(&dag, &try_def, &bind_def, EdgeType::StateMachine);
    assert_eq!(exc_edge.condition, Some("except:*".to_string()));
    assert_eq!(exc_edge.exception_types, Some(Vec::new()));
    assert_eq!(exc_edge.exception_depth, Some(1));
}

#[test]
fn test_return_statement_builds_return_node() {
    let dag = build_dag(
        r#"
        fn main(input: [], output: [result]):
            return 1
        "#,
    );

    let input_def = input_node("main_input_1", vec![]);
    let return_def = return_node("return_2", Some(parse_expr("1")), Some("result"));
    let output_def = output_node("main_output_3", vec!["result"]);

    assert_nodes(
        &dag,
        vec![input_def.clone(), return_def.clone(), output_def.clone()],
    );
    let state_edges = vec![(&input_def, &return_def), (&return_def, &output_def)];
    assert_edge_pairs(&dag, EdgeType::StateMachine, state_edges);
    assert_edge_pairs(&dag, EdgeType::DataFlow, vec![]);
}

#[test]
fn test_break_statement_wires_loop_exit() {
    let dag = build_dag(
        r#"
        fn main(input: [], output: []):
            for item in [1]:
                break
        "#,
    );

    let loop_var = "__loop_loop_2_i";
    let continue_guard = parse_guard(&format!("{loop_var} < len(items=[1])"));
    let break_guard = parse_guard(&format!("not ({loop_var} < len(items=[1]))"));

    let input_def = input_node("main_input_1", vec![]);
    let init_def = assignment_node(
        "loop_init_3",
        vec![loop_var],
        Some(parse_expr("0")),
        Some(format!("{loop_var} = 0")),
    );
    let cond_def = branch_node("loop_cond_4", "for item in [1]");
    let extract_def = assignment_node(
        "loop_extract_5",
        vec!["item"],
        Some(parse_expr(&format!("[1][{loop_var}]"))),
        Some(format!("item = [1][{loop_var}]")),
    );
    let break_def = break_node("break_8");
    let incr_def = assignment_node(
        "loop_incr_7",
        vec![loop_var],
        Some(parse_expr(&format!("{loop_var} + 1"))),
        Some(format!("{loop_var} = {loop_var} + 1")),
    );
    let exit_def = join_node("loop_exit_6", "end for item");
    let output_def = output_node("main_output_9", vec![]);

    assert_nodes(
        &dag,
        vec![
            input_def.clone(),
            init_def.clone(),
            cond_def.clone(),
            extract_def.clone(),
            break_def.clone(),
            incr_def.clone(),
            exit_def.clone(),
            output_def.clone(),
        ],
    );

    let state_edges = vec![
        (&input_def, &init_def),
        (&init_def, &cond_def),
        (&cond_def, &extract_def),
        (&cond_def, &exit_def),
        (&extract_def, &break_def),
        (&break_def, &exit_def),
        (&incr_def, &cond_def),
        (&exit_def, &output_def),
    ];
    assert_edge_pairs(&dag, EdgeType::StateMachine, state_edges);
    assert_edge_pairs(&dag, EdgeType::DataFlow, vec![]);

    let continue_edge = get_edge(&dag, &cond_def, &extract_def, EdgeType::StateMachine);
    assert_eq!(continue_edge.condition, Some("guarded".to_string()));
    assert_eq!(continue_edge.guard_expr, Some(continue_guard));

    let break_edge = get_edge(&dag, &cond_def, &exit_def, EdgeType::StateMachine);
    assert_eq!(break_edge.condition, Some("guarded".to_string()));
    assert_eq!(break_edge.guard_expr, Some(break_guard));

    let loop_back = get_edge(&dag, &incr_def, &cond_def, EdgeType::StateMachine);
    assert!(loop_back.is_loop_back);
}

#[test]
fn test_continue_statement_wires_loop_continue() {
    let dag = build_dag(
        r#"
        fn main(input: [], output: []):
            while True:
                continue
        "#,
    );

    let continue_guard = parse_expr("True");
    let break_guard = parse_expr("not True");

    let input_def = input_node("main_input_1", vec![]);
    let cond_def = branch_node("loop_cond_2", "while true");
    let continue_stmt_def = continue_node("continue_5");
    let loop_continue_def = assignment_node(
        "loop_continue_4",
        vec![],
        None,
        Some("loop_continue".to_string()),
    );
    let exit_def = join_node("loop_exit_3", "end while true");
    let output_def = output_node("main_output_6", vec![]);

    assert_nodes(
        &dag,
        vec![
            input_def.clone(),
            cond_def.clone(),
            continue_stmt_def.clone(),
            loop_continue_def.clone(),
            exit_def.clone(),
            output_def.clone(),
        ],
    );

    let state_edges = vec![
        (&input_def, &cond_def),
        (&cond_def, &continue_stmt_def),
        (&continue_stmt_def, &loop_continue_def),
        (&loop_continue_def, &cond_def),
        (&cond_def, &exit_def),
        (&exit_def, &output_def),
    ];
    assert_edge_pairs(&dag, EdgeType::StateMachine, state_edges);
    assert_edge_pairs(&dag, EdgeType::DataFlow, vec![]);

    let continue_edge = get_edge(&dag, &cond_def, &continue_stmt_def, EdgeType::StateMachine);
    assert_eq!(continue_edge.condition, Some("guarded".to_string()));
    assert_eq!(continue_edge.guard_expr, Some(continue_guard));

    let break_edge = get_edge(&dag, &cond_def, &exit_def, EdgeType::StateMachine);
    assert_eq!(break_edge.condition, Some("guarded".to_string()));
    assert_eq!(break_edge.guard_expr, Some(break_guard));

    let loop_back = get_edge(&dag, &loop_continue_def, &cond_def, EdgeType::StateMachine);
    assert!(loop_back.is_loop_back);
}
