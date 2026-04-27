//! CLI smoke check for Python worker components.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Result, anyhow};
use clap::Parser;
use prost::Message;
use serde_json::Value;
use sha2::{Digest, Sha256};
use waymark_backend_memory::MemoryBackend;
use waymark_core_backend::QueuedInstance;
use waymark_ids::{InstanceId, LockId};
use waymark_workflow_registry_backend::{WorkflowRegistration, WorkflowRegistryBackend as _};

use waymark_dag_builder::convert_to_dag;
use waymark_dag_viz::render_dag_image;
use waymark_ir_conversions::literal_from_json_value;
use waymark_ir_format::format_program;
use waymark_proto::ast as ir;
use waymark_runloop::{RunLoop, RunLoopConfig};
use waymark_runner_state::RunnerState;
use waymark_smoke_sources::{
    build_control_flow_program, build_parallel_spread_program, build_program,
    build_try_except_program, build_while_loop_program,
};

#[derive(Parser, Debug)]
#[command(
    name = "waymark-smoke",
    about = "Smoke check Python worker components."
)]
struct SmokeArgs {
    #[arg(long, default_value_t = 5)]
    base: i64,
}

fn list_examples() -> Vec<&'static str> {
    let mut names = vec![
        "control_flow",
        "parallel_spread",
        "try_except",
        "while_loop",
    ];
    names.sort();
    names
}

#[derive(Clone)]
struct SmokeCase {
    name: String,
    program: ir::Program,
    inputs: HashMap<String, Value>,
}

fn slugify(name: &str) -> String {
    name.chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

async fn run_program_smoke<Spec>(
    case: &SmokeCase,
    worker_pool: Arc<waymark_worker_remote_pool::RemoteWorkerPool<Spec>>,
) -> Result<()>
where
    Spec: waymark_worker_process_spec::Spec + Send + Sync + 'static,
{
    println!("\nIR program ({})", case.name);
    println!("{}", format_program(&case.program));
    println!("IR inputs ({}): {:?}", case.name, case.inputs);
    let program_proto = case.program.encode_to_vec();
    let ir_hash = format!("{:x}", Sha256::digest(&program_proto));
    let dag = Arc::new(convert_to_dag(&case.program).map_err(|err| anyhow!(err.to_string()))?);
    let slug = slugify(&case.name);
    let output_path = PathBuf::from(format!("dag_smoke_{slug}.png"));
    let output_path =
        render_dag_image(&dag, &output_path).map_err(|err| anyhow!(err.to_string()))?;
    println!(
        "DAG image ({}) written to {}",
        case.name,
        output_path.display()
    );

    let mut state = RunnerState::from_dag(Arc::clone(&dag));
    let queue = Arc::new(Mutex::new(VecDeque::new()));
    let backend = MemoryBackend::with_queue(queue.clone());
    let workflow_version_id = backend
        .upsert_workflow_version(&WorkflowRegistration {
            workflow_name: case.name.clone(),
            workflow_version: ir_hash.clone(),
            ir_hash,
            program_proto,
            concurrent: false,
        })
        .await
        .map_err(|err| anyhow!(err.to_string()))?;
    for (name, value) in &case.inputs {
        let expr = literal_from_json_value(value);
        let label = format!("input {name} = {value}");
        let _ = state
            .record_assignment(vec![name.clone()], &expr, None, Some(label))
            .expect("record assignment");
    }

    let entry_node = dag
        .entry_node
        .clone()
        .ok_or_else(|| anyhow!("DAG entry node not found"))?;
    let entry_exec = state
        .queue_template_node(&entry_node, None)
        .map_err(|err| anyhow!(err.0))?;

    let runloop = RunLoop::<waymark_worker_remote_pool::RemoteWorkerPool<Spec>, _, _>::new(
        worker_pool,
        backend.clone(),
        RunLoopConfig {
            max_concurrent_instances: 25.try_into().unwrap(),
            executor_shards: 1.try_into().unwrap(),
            instance_done_batch_size: None,
            poll_interval: Some(Duration::from_secs_f64(0.05).try_into().unwrap()),
            persistence_interval: Some(Duration::from_secs_f64(0.1).try_into().unwrap()),
            lock_uuid: LockId::new_uuid_v4(),
            lock_ttl: Duration::from_secs(15).try_into().unwrap(),
            lock_heartbeat: Duration::from_secs(5).try_into().unwrap(),
            evict_sleep_threshold: Duration::from_secs(10).try_into().unwrap(),
            skip_sleep: false,
            active_instance_gauge: None,
        },
    );
    queue.lock().expect("queue lock").push_back(QueuedInstance {
        workflow_version_id,
        schedule_id: None,
        entry_node: entry_exec.node_id,
        graph: state.graph,
        action_results: HashMap::new(),
        instance_id: InstanceId::new_uuid_v4(),
        scheduled_at: None,
    });
    runloop
        .run()
        .await
        .map_err(|err| anyhow!(err.to_string()))?;
    let instances_done = backend.instances_done();
    let done = instances_done.last();
    if let Some(done) = done {
        println!(
            "Runner output: result={:?} error={:?}",
            done.result, done.error
        );
    } else {
        println!("Runner output: no completed instance found");
    }
    Ok(())
}

async fn run_smoke(base: i64) -> i32 {
    let config =
        waymark_worker_python::Config::new().with_user_module("tests.fixtures.test_actions");

    let result = waymark_worker_remote_bringup::start(
        Default::default(),
        None,
        |bridge_server_addr| waymark_worker_python::Spec {
            config,
            bridge_server_addr,
        },
        2.try_into().unwrap(),
        None,
        10.try_into().unwrap(),
    )
    .await;

    let (process_pool, bridge_server_task) = match result {
        Ok(val) => val,
        Err(err) => {
            println!("Failed to start python worker pool: {err}");
            return 1;
        }
    };
    let worker_pool = waymark_worker_remote_pool::RemoteWorkerPool::new(process_pool);
    let worker_pool = Arc::new(worker_pool);

    let mut cases = Vec::new();
    cases.push(SmokeCase {
        name: "smoke".to_string(),
        program: build_program(),
        inputs: HashMap::from([("base".to_string(), Value::Number(base.into()))]),
    });
    let examples = vec![
        ("control_flow", build_control_flow_program()),
        ("parallel_spread", build_parallel_spread_program()),
        ("try_except", build_try_except_program()),
        ("while_loop", build_while_loop_program()),
    ];
    for (name, program) in examples {
        let program = match program {
            Ok(value) => value,
            Err(err) => {
                println!("Failed to build {name} program: {err}");
                continue;
            }
        };
        let inputs = match name {
            "control_flow" => HashMap::from([("base".to_string(), Value::Number(2.into()))]),
            "parallel_spread" => HashMap::from([("base".to_string(), Value::Number(3.into()))]),
            "try_except" => HashMap::from([(
                "values".to_string(),
                Value::Array(vec![1.into(), 2.into(), 3.into()]),
            )]),
            "while_loop" => HashMap::from([("limit".to_string(), Value::Number(6.into()))]),
            _ => HashMap::new(),
        };
        cases.push(SmokeCase {
            name: name.to_string(),
            program,
            inputs,
        });
    }

    let mut failures = 0;
    for case in &cases {
        if let Err(err) = run_program_smoke(case, worker_pool.clone()).await {
            failures += 1;
            println!("Smoke case '{}' failed: {}", case.name, err);
        }
    }

    bridge_server_task.abort();
    let _ = bridge_server_task.await;

    if let Err(err) = worker_pool.shutdown_arc().await {
        println!("Failed to shut down worker pool: {err}");
    }

    println!("Examples available: {:?}", list_examples());
    if failures > 0 { 1 } else { 0 }
}

pub fn main() {
    waymark_fn_main_common::init().expect("tracing setup");

    let args = SmokeArgs::parse();
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
    let code = runtime.block_on(run_smoke(args.base));
    std::process::exit(code);
}
