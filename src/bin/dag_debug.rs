use clap::Parser;
use prost::Message;
use rappel::{
    Database, EdgeType, WorkflowInstanceId, WorkflowVersionId,
    ast::Program,
    completion::{InlineContext, analyze_subgraph, execute_inline_subgraph},
    convert_to_dag,
    dag_state::DAGHelper,
    get_config,
};
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use std::collections::{BTreeMap, HashMap};
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(name = "dag_debug", about = "Debug DAG for a workflow version")]
struct Args {
    /// Workflow version ID to debug
    #[arg(long)]
    version_id: Uuid,
    /// Database URL override (skips migrations)
    #[arg(long)]
    database_url: Option<String>,
    /// Focus on a single node ID for edge output
    #[arg(long)]
    node_id: Option<String>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let workflow_version = if let Some(database_url) = args.database_url.as_ref() {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(database_url)
            .await
            .expect("failed to connect to database");
        let row: (Vec<u8>,) = sqlx::query_as(
            r#"
            SELECT program_proto
            FROM workflow_versions
            WHERE id = $1
            "#,
        )
        .bind(args.version_id)
        .fetch_one(&pool)
        .await
        .expect("failed to load workflow version");
        row.0
    } else {
        // Connect to database using centralized config
        let config = get_config();
        let database = Database::connect(&config.database_url)
            .await
            .expect("failed to connect to database");

        // Load workflow version
        let version_id = WorkflowVersionId(args.version_id);
        let workflow_version = database
            .get_workflow_version(version_id)
            .await
            .expect("failed to load workflow version");
        workflow_version.program_proto
    };

    let program = Program::decode(workflow_version.as_slice()).expect("decode program");
    let dag = convert_to_dag(&program);

    println!("=== Nodes ===");
    let nodes: BTreeMap<_, _> = dag.nodes.iter().collect();
    for (id, node) in nodes.iter() {
        println!(
            "{}: type={} targets={:?} kwargs={:?}",
            id, node.node_type, node.targets, node.kwargs
        );
    }

    println!("\n=== State Machine Edges ===");
    for edge in dag
        .edges
        .iter()
        .filter(|e| e.edge_type == EdgeType::StateMachine)
    {
        let guard_str = edge
            .guard_expr
            .as_ref()
            .map(rappel::ast_printer::print_expr);
        println!(
            "{} -> {} (loop_back={} is_else={} guard={:?})",
            edge.source, edge.target, edge.is_loop_back, edge.is_else, guard_str
        );
    }

    if let Some(node_id) = args.node_id.as_ref() {
        println!("\n=== Focus: {node_id} ===");
        for edge in dag
            .edges
            .iter()
            .filter(|e| e.edge_type == EdgeType::StateMachine)
            .filter(|e| e.source == *node_id)
        {
            let guard_str = edge
                .guard_expr
                .as_ref()
                .map(rappel::ast_printer::print_expr);
            println!(
                "{} -> {} (loop_back={} is_else={} guard={:?})",
                edge.source, edge.target, edge.is_loop_back, edge.is_else, guard_str
            );
        }
        let helper = DAGHelper::new(&dag);
        let subgraph = analyze_subgraph(node_id, &dag, &helper);
        println!("Inline nodes: {:?}", subgraph.inline_nodes);
        println!(
            "Frontier nodes: {:?}",
            subgraph
                .frontier_nodes
                .iter()
                .map(|f| (&f.node_id, &f.category))
                .collect::<Vec<_>>()
        );
    }

    println!("\n=== Data Flow Edges ===");
    for edge in dag
        .edges
        .iter()
        .filter(|e| e.edge_type == EdgeType::DataFlow)
    {
        println!(
            "{} -[{:?}]-> {}",
            edge.source,
            edge.variable.as_deref().unwrap_or("?"),
            edge.target
        );
    }

    let helper = DAGHelper::new(&dag);

    // Check spread_action subgraph if it exists
    if dag.nodes.contains_key("spread_action_6") {
        let spread_subgraph = analyze_subgraph("spread_action_6", &dag, &helper);
        println!(
            "\nFrontiers from spread_action_6: {:?}",
            spread_subgraph.frontier_nodes
        );
    }

    // Check aggregator subgraph if it exists
    if dag.nodes.contains_key("aggregator_7") {
        let subgraph = analyze_subgraph("aggregator_7", &dag, &helper);
        println!(
            "\nFrontiers from aggregator_7: {:?}",
            subgraph.frontier_nodes
        );
        println!(
            "Inline nodes from aggregator_7: {:?}",
            subgraph.inline_nodes
        );
        println!("All nodes from aggregator_7: {:?}", subgraph.all_node_ids);

        // Enable tracing for debugging
        use tracing_subscriber::fmt::format::FmtSpan;
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_span_events(FmtSpan::CLOSE)
            .try_init();

        let ctx = InlineContext {
            initial_scope: &HashMap::new(),
            existing_inbox: &HashMap::new(),
            spread_index: None,
        };
        let plan = execute_inline_subgraph(
            "aggregator_7",
            json!(["hash1", "hash2"]),
            ctx,
            &subgraph,
            &dag,
            WorkflowInstanceId(Uuid::nil()),
        );

        match plan {
            Ok(plan) => {
                println!(
                    "Readiness increments targets: {:?}",
                    plan.readiness_increments
                        .iter()
                        .map(|r| r.node_id.as_str())
                        .collect::<Vec<_>>()
                );
                println!(
                    "Inbox writes: {:?}",
                    plan.inbox_writes
                        .iter()
                        .map(|w| format!(
                            "{} -> {} ({})",
                            w.source_node_id, w.target_node_id, w.variable_name
                        ))
                        .collect::<Vec<_>>()
                );
            }
            Err(e) => {
                println!("execute_inline_subgraph error: {:?}", e);
            }
        }

        // Check if loop_exit_14 -> action_15 edge exists
        use rappel::completion::{find_direct_predecessor_in_path, is_direct_predecessor};
        let executed_inline = subgraph.inline_nodes.clone();
        let pred =
            find_direct_predecessor_in_path(&executed_inline, "action_15", "aggregator_7", &dag);
        println!("Predecessor for action_15: {}", pred);
        println!(
            "is_direct_predecessor(loop_exit_14, action_15): {}",
            is_direct_predecessor("loop_exit_14", "action_15", &dag)
        );
        println!(
            "is_direct_predecessor(pred, action_15): {}",
            is_direct_predecessor(&pred, "action_15", &dag)
        );
    }
}
