use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use prost::Message;
use serde_json::Value;
use sha2::{Digest, Sha256};

use waymark_backend_fault_injection::FaultInjectingBackend;
use waymark_backend_memory::MemoryBackend;
use waymark_core_backend::{ActionAttemptStatus, CoreBackend, QueuedInstance};
use waymark_dag_builder::convert_to_dag;
use waymark_ids::{ExecutionId, InstanceId, LockId, WorkflowVersionId};
use waymark_ir_parser::parse_program;
use waymark_nonzero_duration::NonZeroDuration;
use waymark_proto::ast as ir;
use waymark_runloop::{RunLoop, RunLoopConfig, RunLoopError};
use waymark_runner::RunnerExecutor;
use waymark_runner_execution_core::NodeStatus;
use waymark_runner_state::RunnerState;
use waymark_worker_inline::ActionCallable;
use waymark_workflow_registry_backend::{WorkflowRegistration, WorkflowRegistryBackend};

fn default_test_config(lock_uuid: LockId) -> RunLoopConfig {
    RunLoopConfig {
        max_concurrent_instances: 25.try_into().unwrap(),
        executor_shards: 1.try_into().unwrap(),
        instance_done_batch_size: None,
        poll_interval: NonZeroDuration::from_millis(10),
        persistence_interval: NonZeroDuration::from_millis(10),
        lock_uuid,
        lock_ttl: NonZeroDuration::from_secs(15).unwrap(),
        lock_heartbeat: NonZeroDuration::from_secs(5).unwrap(),
        evict_sleep_threshold: NonZeroDuration::from_secs(10).unwrap(),
        skip_sleep: false,
        active_instance_gauge: None,
    }
}

#[tokio::test]
async fn test_runloop_executes_actions() {
    let source = r#"
fn main(input: [x], output: [y]):
    y = @tests.fixtures.test_actions.double(value=x)
    return y
"#;
    let program = parse_program(source.trim()).expect("parse program");
    let program_proto = program.encode_to_vec();
    let ir_hash = format!("{:x}", Sha256::digest(&program_proto));
    let dag = Arc::new(convert_to_dag(&program).expect("convert to dag"));

    let mut state = RunnerState::from_dag(Arc::clone(&dag));
    let _ = state
        .record_assignment(
            vec!["x".to_string()],
            &ir::Expr {
                kind: Some(ir::expr::Kind::Literal(ir::Literal {
                    value: Some(ir::literal::Value::IntValue(4)),
                })),
                span: None,
            },
            None,
            Some("input x = 4".to_string()),
        )
        .expect("record assignment");
    let entry_node = dag
        .entry_node
        .as_ref()
        .expect("DAG entry node not found")
        .clone();
    let entry_exec = state
        .queue_template_node(&entry_node, None)
        .expect("queue entry node");

    let queue = Arc::new(Mutex::new(VecDeque::new()));
    let backend = MemoryBackend::with_queue(queue.clone());
    let workflow_version_id = backend
        .upsert_workflow_version(&WorkflowRegistration {
            workflow_name: "test".to_string(),
            workflow_version: ir_hash.clone(),
            ir_hash,
            program_proto,
            concurrent: false,
        })
        .await
        .expect("register workflow version");

    let mut actions: HashMap<String, ActionCallable> = HashMap::new();
    actions.insert(
        "double".to_string(),
        Arc::new(|kwargs| {
            Box::pin(async move {
                let value = kwargs
                    .get("value")
                    .and_then(|value| value.as_i64())
                    .unwrap_or(0);
                Ok(Value::Number((value * 2).into()))
            })
        }),
    );
    let worker_pool = waymark_worker_inline::InlineWorkerPool::new(actions);

    let runloop = RunLoop::new(
        worker_pool,
        backend.clone(),
        RunLoopConfig {
            max_concurrent_instances: 25.try_into().unwrap(),
            executor_shards: 1.try_into().unwrap(),
            instance_done_batch_size: None,
            // TODO: do we really want no interval here?
            poll_interval: None,
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
        state,
        action_results: HashMap::new(),
        instance_id: InstanceId::new_uuid_v4(),
        scheduled_at: None,
    });

    runloop.run().await.expect("runloop");

    let instances_done = backend.instances_done();
    assert_eq!(instances_done.len(), 1);
    let done = &instances_done[0];
    let output = done.result.clone().expect("instance result");
    let Value::Object(map) = output.0 else {
        panic!("expected output object");
    };
    assert_eq!(map.get("y"), Some(&Value::Number(8.into())));
}

#[tokio::test]
async fn test_runloop_executes_multiple_instances_with_multiple_shards() {
    let source = r#"
fn main(input: [x], output: [y]):
    y = @tests.fixtures.test_actions.double(value=x)
    return y
"#;
    let program = parse_program(source.trim()).expect("parse program");
    let program_proto = program.encode_to_vec();
    let ir_hash = format!("{:x}", Sha256::digest(&program_proto));
    let dag = Arc::new(convert_to_dag(&program).expect("convert to dag"));

    let queue = Arc::new(Mutex::new(VecDeque::new()));
    let backend = MemoryBackend::with_queue(queue.clone());
    let workflow_version_id = backend
        .upsert_workflow_version(&WorkflowRegistration {
            workflow_name: "test_multishard".to_string(),
            workflow_version: ir_hash.clone(),
            ir_hash,
            program_proto,
            concurrent: false,
        })
        .await
        .expect("register workflow version");

    let mut actions: HashMap<String, ActionCallable> = HashMap::new();
    actions.insert(
        "double".to_string(),
        Arc::new(|kwargs| {
            Box::pin(async move {
                let value = kwargs
                    .get("value")
                    .and_then(|value| value.as_i64())
                    .unwrap_or(0);
                Ok(Value::Number((value * 2).into()))
            })
        }),
    );
    let worker_pool = waymark_worker_inline::InlineWorkerPool::new(actions);

    let runloop = RunLoop::new(
        worker_pool,
        backend.clone(),
        RunLoopConfig {
            max_concurrent_instances: 25.try_into().unwrap(),
            executor_shards: 2.try_into().unwrap(),
            instance_done_batch_size: None,
            // TODO: do we really want no interval here?
            poll_interval: None,
            persistence_interval: Some(Duration::from_secs_f64(0.1).try_into().unwrap()),
            lock_uuid: LockId::new_uuid_v4(),
            lock_ttl: Duration::from_secs(15).try_into().unwrap(),
            lock_heartbeat: Duration::from_secs(5).try_into().unwrap(),
            evict_sleep_threshold: Duration::from_secs(10).try_into().unwrap(),
            skip_sleep: false,
            active_instance_gauge: None,
        },
    );

    for input in [2_i64, 7_i64] {
        let mut state = RunnerState::from_dag(Arc::clone(&dag));
        let _ = state
            .record_assignment(
                vec!["x".to_string()],
                &ir::Expr {
                    kind: Some(ir::expr::Kind::Literal(ir::Literal {
                        value: Some(ir::literal::Value::IntValue(input)),
                    })),
                    span: None,
                },
                None,
                Some(format!("input x = {input}")),
            )
            .expect("record assignment");
        let entry_node = dag
            .entry_node
            .as_ref()
            .expect("DAG entry node not found")
            .clone();
        let entry_exec = state
            .queue_template_node(&entry_node, None)
            .expect("queue entry node");

        queue.lock().expect("queue lock").push_back(QueuedInstance {
            workflow_version_id,
            schedule_id: None,
            entry_node: entry_exec.node_id,
            state,
            action_results: HashMap::new(),
            instance_id: InstanceId::new_uuid_v4(),
            scheduled_at: None,
        });
    }

    runloop.run().await.expect("runloop");

    let mut outputs: Vec<i64> = backend
        .instances_done()
        .into_iter()
        .map(|done| {
            let Value::Object(map) = done.result.expect("instance result").0 else {
                panic!("expected output object");
            };
            map.get("y")
                .and_then(Value::as_i64)
                .expect("numeric y output")
        })
        .collect();
    outputs.sort_unstable();
    assert_eq!(outputs, vec![4, 14]);
}

#[tokio::test]
async fn test_runloop_executes_sleep_then_action_with_skip_sleep() {
    let source = r#"
fn main(input: [x], output: [y]):
    sleep 5
    y = @tests.fixtures.test_actions.double(value=x)
    return y
"#;
    let program = parse_program(source.trim()).expect("parse program");
    let program_proto = program.encode_to_vec();
    let ir_hash = format!("{:x}", Sha256::digest(&program_proto));
    let dag = Arc::new(convert_to_dag(&program).expect("convert to dag"));

    let mut state = RunnerState::from_dag(Arc::clone(&dag));
    let _ = state
        .record_assignment(
            vec!["x".to_string()],
            &ir::Expr {
                kind: Some(ir::expr::Kind::Literal(ir::Literal {
                    value: Some(ir::literal::Value::IntValue(3)),
                })),
                span: None,
            },
            None,
            Some("input x = 3".to_string()),
        )
        .expect("record assignment");
    let entry_node = dag
        .entry_node
        .as_ref()
        .expect("DAG entry node not found")
        .clone();
    let entry_exec = state
        .queue_template_node(&entry_node, None)
        .expect("queue entry node");

    let queue = Arc::new(Mutex::new(VecDeque::new()));
    let backend = MemoryBackend::with_queue(queue.clone());
    let workflow_version_id = backend
        .upsert_workflow_version(&WorkflowRegistration {
            workflow_name: "test_sleep_then_action".to_string(),
            workflow_version: ir_hash.clone(),
            ir_hash,
            program_proto,
            concurrent: false,
        })
        .await
        .expect("register workflow version");

    let mut actions: HashMap<String, ActionCallable> = HashMap::new();
    actions.insert(
        "double".to_string(),
        Arc::new(|kwargs| {
            Box::pin(async move {
                let value = kwargs
                    .get("value")
                    .and_then(|value| value.as_i64())
                    .unwrap_or(0);
                Ok(Value::Number((value * 2).into()))
            })
        }),
    );
    let worker_pool = waymark_worker_inline::InlineWorkerPool::new(actions);

    let runloop = RunLoop::new(
        worker_pool,
        backend.clone(),
        RunLoopConfig {
            max_concurrent_instances: 25.try_into().unwrap(),
            executor_shards: 1.try_into().unwrap(),
            instance_done_batch_size: None,
            // TODO: do we really want no interval here?
            poll_interval: None,
            persistence_interval: Some(Duration::from_secs_f64(0.1).try_into().unwrap()),
            lock_uuid: LockId::new_uuid_v4(),
            lock_ttl: Duration::from_secs(15).try_into().unwrap(),
            lock_heartbeat: Duration::from_secs(5).try_into().unwrap(),
            evict_sleep_threshold: Duration::from_secs(10).try_into().unwrap(),
            skip_sleep: true,
            active_instance_gauge: None,
        },
    );
    queue.lock().expect("queue lock").push_back(QueuedInstance {
        workflow_version_id,
        schedule_id: None,
        entry_node: entry_exec.node_id,
        state,
        action_results: HashMap::new(),
        instance_id: InstanceId::new_uuid_v4(),
        scheduled_at: None,
    });

    runloop.run().await.expect("runloop");

    let instances_done = backend.instances_done();
    assert_eq!(instances_done.len(), 1);
    let Value::Object(map) = instances_done[0].result.clone().expect("instance result").0 else {
        panic!("expected output object");
    };
    assert_eq!(map.get("y"), Some(&Value::Number(6.into())));

    let actions_done = backend.actions_done();
    assert_eq!(actions_done.len(), 1, "action after sleep should execute");
}

#[tokio::test]
async fn test_runloop_times_out_action_and_persists_timestamps() {
    let source = r#"
fn main(input: [], output: [y]):
    y = @tests.fixtures.test_actions.hang()[timeout: 1 s]
    return y
"#;
    let program = parse_program(source.trim()).expect("parse program");
    let program_proto = program.encode_to_vec();
    let ir_hash = format!("{:x}", Sha256::digest(&program_proto));
    let dag = Arc::new(convert_to_dag(&program).expect("convert to dag"));

    let mut state = RunnerState::from_dag(Arc::clone(&dag));
    let entry_node = dag
        .entry_node
        .as_ref()
        .expect("DAG entry node not found")
        .clone();
    let entry_exec = state
        .queue_template_node(&entry_node, None)
        .expect("queue entry node");

    let queue = Arc::new(Mutex::new(VecDeque::new()));
    let backend = MemoryBackend::with_queue(queue.clone());
    let workflow_version_id = backend
        .upsert_workflow_version(&WorkflowRegistration {
            workflow_name: "test_timeout".to_string(),
            workflow_version: ir_hash.clone(),
            ir_hash,
            program_proto,
            concurrent: false,
        })
        .await
        .expect("register workflow version");

    let mut actions: HashMap<String, ActionCallable> = HashMap::new();
    actions.insert(
        "hang".to_string(),
        Arc::new(|_kwargs| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_secs(5)).await;
                Ok(Value::String("late".to_string()))
            })
        }),
    );
    let worker_pool = waymark_worker_inline::InlineWorkerPool::new(actions);

    let runloop = RunLoop::new(
        worker_pool,
        backend.clone(),
        RunLoopConfig {
            max_concurrent_instances: 25.try_into().unwrap(),
            executor_shards: 1.try_into().unwrap(),
            instance_done_batch_size: None,
            // TODO: do we really want no interval here?
            poll_interval: None,
            persistence_interval: Some(Duration::from_secs_f64(0.05).try_into().unwrap()),
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
        state,
        action_results: HashMap::new(),
        instance_id: InstanceId::new_uuid_v4(),
        scheduled_at: None,
    });

    runloop.run().await.expect("runloop");

    let actions_done = backend.actions_done();
    assert_eq!(actions_done.len(), 1);
    let action_done = &actions_done[0];
    assert_eq!(action_done.status, ActionAttemptStatus::TimedOut);
    assert!(action_done.started_at.is_some());
    assert!(action_done.completed_at.is_some());
    assert!(action_done.duration_ms.is_some());

    let execution_id = action_done.execution_id;
    let graph_updates = backend.graph_updates();
    let mut saw_running_snapshot = false;
    let mut saw_failed_snapshot = false;
    for update in graph_updates {
        let Some(node) = update.nodes.get(&execution_id) else {
            continue;
        };
        if node.status == NodeStatus::Running && node.started_at.is_some() {
            saw_running_snapshot = true;
        }
        if node.status == NodeStatus::Failed
            && node.started_at.is_some()
            && node.completed_at.is_some()
        {
            saw_failed_snapshot = true;
        }
    }
    assert!(saw_running_snapshot, "expected running graph snapshot");
    assert!(saw_failed_snapshot, "expected failed graph snapshot");

    let instances_done = backend.instances_done();
    assert_eq!(instances_done.len(), 1);
    assert!(instances_done[0].result.is_none());
    let Value::Object(error_obj) = instances_done[0]
        .error
        .clone()
        .expect("instance error payload")
        .0
    else {
        panic!("expected error payload object");
    };
    assert_eq!(
        error_obj.get("type"),
        Some(&Value::String("ActionTimeout".to_string()))
    );
}

#[tokio::test]
async fn test_runloop_marks_instance_failed_on_executor_error() {
    let source = r#"
fn main(input: [x], output: [y]):
    y = @tests.fixtures.test_actions.double(value=x)
    return y
"#;
    let program = parse_program(source.trim()).expect("parse program");
    let program_proto = program.encode_to_vec();
    let ir_hash = format!("{:x}", Sha256::digest(&program_proto));
    let dag = Arc::new(convert_to_dag(&program).expect("convert to dag"));

    // Intentionally omit input assignment so action kwarg resolution fails at runtime.
    let mut state = RunnerState::from_dag(Arc::clone(&dag));
    let entry_node = dag
        .entry_node
        .as_ref()
        .expect("DAG entry node not found")
        .clone();
    let entry_exec = state
        .queue_template_node(&entry_node, None)
        .expect("queue entry node");

    let queue = Arc::new(Mutex::new(VecDeque::new()));
    let backend = MemoryBackend::with_queue(queue.clone());
    let workflow_version_id = backend
        .upsert_workflow_version(&WorkflowRegistration {
            workflow_name: "test".to_string(),
            workflow_version: ir_hash.clone(),
            ir_hash,
            program_proto,
            concurrent: false,
        })
        .await
        .expect("register workflow version");

    let worker_pool = waymark_worker_inline::InlineWorkerPool::new(HashMap::new());
    let runloop = RunLoop::new(
        worker_pool,
        backend.clone(),
        RunLoopConfig {
            max_concurrent_instances: 25.try_into().unwrap(),
            executor_shards: 1.try_into().unwrap(),
            instance_done_batch_size: None,
            // TODO: do we really want no interval here?
            poll_interval: None,
            persistence_interval: Some(Duration::from_secs_f64(0.1).try_into().unwrap()),
            lock_uuid: LockId::new_uuid_v4(),
            lock_ttl: Duration::from_secs(15).try_into().unwrap(),
            lock_heartbeat: Duration::from_secs(5).try_into().unwrap(),
            evict_sleep_threshold: Duration::from_secs(10).try_into().unwrap(),
            skip_sleep: false,
            active_instance_gauge: None,
        },
    );
    let instance_id = InstanceId::new_uuid_v4();
    queue.lock().expect("queue lock").push_back(QueuedInstance {
        workflow_version_id,
        schedule_id: None,
        entry_node: entry_exec.node_id,
        state,
        action_results: HashMap::new(),
        instance_id,
        scheduled_at: None,
    });

    runloop
        .run()
        .await
        .expect("runloop should continue after instance failure");
    let instances_done = backend.instances_done();
    assert_eq!(instances_done.len(), 1);

    let done = &instances_done[0];
    assert_eq!(done.executor_id, instance_id);
    assert!(done.result.is_none());
    let error = done.error.as_ref().expect("instance error");
    let Value::Object(error_obj) = &error.0 else {
        panic!("expected error payload object");
    };
    assert_eq!(
        error_obj.get("type"),
        Some(&Value::String("ExecutionError".to_string()))
    );
    let message = error_obj
        .get("message")
        .and_then(Value::as_str)
        .expect("error message");
    assert!(message.contains("variable not found: x"));
}

#[tokio::test]
async fn test_runloop_executes_for_loop_action_assignments() {
    let source = r#"
fn main(input: [limit], output: [result]):
    current = 0
    iterations = 0
    for _ in range(limit):
        current = @tests.fixtures.test_actions.increment(value=current)
        iterations = iterations + 1
    result = @tests.fixtures.test_actions.pack(limit=limit, final=current, iterations=iterations)
    return result
"#;
    let program = parse_program(source.trim()).expect("parse program");
    let program_proto = program.encode_to_vec();
    let ir_hash = format!("{:x}", Sha256::digest(&program_proto));
    let dag = Arc::new(convert_to_dag(&program).expect("convert to dag"));

    let mut state = RunnerState::from_dag(Arc::clone(&dag));
    let _ = state
        .record_assignment(
            vec!["limit".to_string()],
            &ir::Expr {
                kind: Some(ir::expr::Kind::Literal(ir::Literal {
                    value: Some(ir::literal::Value::IntValue(4)),
                })),
                span: None,
            },
            None,
            Some("input limit = 4".to_string()),
        )
        .expect("record assignment");
    let entry_node = dag
        .entry_node
        .as_ref()
        .expect("DAG entry node not found")
        .clone();
    let entry_exec = state
        .queue_template_node(&entry_node, None)
        .expect("queue entry node");

    let queue = Arc::new(Mutex::new(VecDeque::new()));
    let backend = MemoryBackend::with_queue(queue.clone());
    let workflow_version_id = backend
        .upsert_workflow_version(&WorkflowRegistration {
            workflow_name: "test_loop_actions".to_string(),
            workflow_version: ir_hash.clone(),
            ir_hash,
            program_proto,
            concurrent: false,
        })
        .await
        .expect("register workflow version");

    let mut actions: HashMap<String, ActionCallable> = HashMap::new();
    let increment_inputs = Arc::new(Mutex::new(Vec::new()));
    let increment_inputs_clone = Arc::clone(&increment_inputs);
    actions.insert(
        "increment".to_string(),
        Arc::new(move |kwargs| {
            let increment_inputs = Arc::clone(&increment_inputs_clone);
            Box::pin(async move {
                let value = kwargs
                    .get("value")
                    .and_then(|value| value.as_i64())
                    .unwrap_or(0);
                increment_inputs
                    .lock()
                    .expect("increment inputs lock")
                    .push(value);
                Ok(Value::Number((value + 1).into()))
            })
        }),
    );
    actions.insert(
        "pack".to_string(),
        Arc::new(|kwargs| {
            Box::pin(async move {
                let limit = kwargs.get("limit").cloned().unwrap_or(Value::Null);
                let final_value = kwargs.get("final").cloned().unwrap_or(Value::Null);
                let iterations = kwargs.get("iterations").cloned().unwrap_or(Value::Null);
                Ok(Value::Object(
                    [
                        ("limit".to_string(), limit),
                        ("final".to_string(), final_value),
                        ("iterations".to_string(), iterations),
                    ]
                    .into_iter()
                    .collect(),
                ))
            })
        }),
    );
    let worker_pool = waymark_worker_inline::InlineWorkerPool::new(actions);

    let runloop = RunLoop::new(
        worker_pool,
        backend.clone(),
        RunLoopConfig {
            max_concurrent_instances: 25.try_into().unwrap(),
            executor_shards: 1.try_into().unwrap(),
            instance_done_batch_size: None,
            // TODO: do we really want no interval here?
            poll_interval: None,
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
        state,
        action_results: HashMap::new(),
        instance_id: InstanceId::new_uuid_v4(),
        scheduled_at: None,
    });

    runloop.run().await.expect("runloop");
    let instances_done = backend.instances_done();
    assert_eq!(instances_done.len(), 1);
    let done = &instances_done[0];
    let output = done.result.clone().expect("instance result");
    let Value::Object(map) = output.0 else {
        panic!("expected output object");
    };
    let Value::Object(result_map) = map
        .get("result")
        .cloned()
        .expect("result payload should include result")
    else {
        panic!("expected nested result object");
    };
    assert_eq!(
        *increment_inputs.lock().expect("increment inputs lock"),
        vec![0, 1, 2, 3]
    );
    assert_eq!(result_map.get("limit"), Some(&Value::Number(4.into())));
    assert_eq!(result_map.get("final"), Some(&Value::Number(4.into())));
    assert_eq!(result_map.get("iterations"), Some(&Value::Number(4.into())));
}

#[tokio::test]
async fn test_runloop_reproduces_no_progress_with_continued_queue_growth() {
    let queue = Arc::new(Mutex::new(VecDeque::new()));
    let backend =
        FaultInjectingBackend::with_depth_limit_poll_failures(MemoryBackend::with_queue(queue));
    let worker_pool = waymark_worker_inline::InlineWorkerPool::new(HashMap::new());
    let shutdown_token = tokio_util::sync::CancellationToken::new();

    let runloop = RunLoop::new_with_shutdown(
        worker_pool,
        backend.clone(),
        default_test_config(LockId::new_uuid_v4()),
        shutdown_token.clone(),
    );
    let runloop = tokio::spawn(async move { runloop.run().await });

    for _ in 0..20 {
        backend
            .queue_instances(&[QueuedInstance {
                workflow_version_id: WorkflowVersionId::new_uuid_v4(),
                schedule_id: None,
                entry_node: ExecutionId::new_uuid_v4(),
                state: RunnerState::dummy(),
                action_results: HashMap::new(),
                instance_id: InstanceId::new_uuid_v4(),
                scheduled_at: None,
            }])
            .await
            .expect("queue synthetic instance");
    }

    tokio::time::sleep(Duration::from_millis(500)).await;
    shutdown_token.cancel();
    let result = tokio::time::timeout(Duration::from_secs(2), runloop)
        .await
        .expect("runloop task should stop before timeout")
        .expect("runloop task should not panic");

    let Err(error) = result else {
        panic!("expected an Err result, got {result:?}");
    };
    assert!(
        matches!(
            error,
            RunLoopError::CoreBackendPoll(
                waymark_backend_fault_injection::PollQueuedInstancesError::DepthLimitExceeded,
            )
        ),
        "expected depth limit exceeded error"
    );

    assert!(
        backend.get_queued_instances_calls() >= 1,
        "expected polling attempts during stall simulation"
    );
    assert!(
        backend
            .as_ref()
            .instance_queue()
            .as_ref()
            .map(|queue| queue.lock().expect("queue poisoned").len())
            .unwrap_or(0)
            >= 20,
        "queued work should continue to grow when poller cannot read instances"
    );
    assert_eq!(
        backend.as_ref().instances_done().len(),
        0,
        "no instances should complete while poller is failing"
    );
}

#[tokio::test]
async fn test_runloop_marks_instance_failed_when_rehydrated_state_is_missing_action_result() {
    let source = r#"
fn main(input: [x], output: [y]):
    y = @tests.fixtures.test_actions.double(value=x)
    return y
"#;
    let program = parse_program(source.trim()).expect("parse program");
    let program_proto = program.encode_to_vec();
    let ir_hash = format!("{:x}", Sha256::digest(&program_proto));
    let dag = Arc::new(convert_to_dag(&program).expect("convert to dag"));

    let mut state = RunnerState::from_dag(Arc::clone(&dag));
    let _ = state
        .record_assignment(
            vec!["x".to_string()],
            &ir::Expr {
                kind: Some(ir::expr::Kind::Literal(ir::Literal {
                    value: Some(ir::literal::Value::IntValue(4)),
                })),
                span: None,
            },
            None,
            Some("input x = 4".to_string()),
        )
        .expect("record assignment");
    let template_entry_node = dag
        .entry_node
        .as_ref()
        .expect("DAG entry node not found")
        .clone();
    let entry_exec = state
        .queue_template_node(&template_entry_node, None)
        .expect("queue entry node");
    let mut bootstrap_executor =
        RunnerExecutor::without_updates_collection(Arc::clone(&dag), state, HashMap::new());
    let bootstrap_step = bootstrap_executor
        .increment(&[entry_exec.node_id])
        .expect("bootstrap increment should materialize action node");
    let action_exec = bootstrap_step
        .actions
        .first()
        .expect("bootstrap should queue one action call")
        .clone();

    // Simulate a reclaimed instance whose graph says the action execution node
    // has finished, but action_results payload was lost.
    bootstrap_executor
        .state_mut()
        .mark_completed(action_exec.node_id)
        .expect("mark action completed");
    bootstrap_executor.state_mut().ready_queue.clear();
    assert!(
        bootstrap_executor
            .state()
            .nodes
            .get(&action_exec.node_id)
            .is_some_and(|node| node.is_action_call() && node.status == NodeStatus::Completed),
        "expected completed action execution node"
    );
    let state = bootstrap_executor.state().clone();

    let queue = Arc::new(Mutex::new(VecDeque::new()));
    let backend = MemoryBackend::with_queue(queue.clone());
    let workflow_version_id = backend
        .upsert_workflow_version(&WorkflowRegistration {
            workflow_name: "test_missing_action_result".to_string(),
            workflow_version: ir_hash.clone(),
            ir_hash,
            program_proto,
            concurrent: false,
        })
        .await
        .expect("register workflow version");

    let worker_pool = waymark_worker_inline::InlineWorkerPool::new(HashMap::new());
    let runloop = RunLoop::new(
        worker_pool,
        backend.clone(),
        default_test_config(LockId::new_uuid_v4()),
    );
    let instance_id = InstanceId::new_uuid_v4();
    queue.lock().expect("queue lock").push_back(QueuedInstance {
        workflow_version_id,
        schedule_id: None,
        entry_node: action_exec.node_id,
        state,
        action_results: HashMap::new(),
        instance_id,
        scheduled_at: None,
    });

    runloop
        .run()
        .await
        .expect("runloop should continue after instance failure");
    let instances_done = backend.instances_done();
    assert_eq!(instances_done.len(), 1);
    assert_eq!(instances_done[0].executor_id, instance_id);
    let Value::Object(error_obj) = instances_done[0]
        .error
        .clone()
        .expect("instance error payload")
        .0
    else {
        panic!("expected error payload object");
    };
    assert_eq!(
        error_obj.get("type"),
        Some(&Value::String("ExecutionError".to_string()))
    );
    let message = error_obj
        .get("message")
        .and_then(Value::as_str)
        .expect("error message");
    assert!(
        message.contains("missing action result for"),
        "expected missing action result error, got: {message}"
    );
}

#[tokio::test]
async fn test_runloop_marks_instance_failed_with_dict_key_error() {
    let source = r#"
fn main(input: [], output: [result]):
    payload = @tests.fixtures.test_actions.make_payload()
    result = payload["missing"]
    return result
"#;
    let program = parse_program(source.trim()).expect("parse program");
    let program_proto = program.encode_to_vec();
    let ir_hash = format!("{:x}", Sha256::digest(&program_proto));
    let dag = Arc::new(convert_to_dag(&program).expect("convert to dag"));

    let mut state = RunnerState::from_dag(Arc::clone(&dag));
    let entry_node = dag
        .entry_node
        .as_ref()
        .expect("DAG entry node not found")
        .clone();
    let entry_exec = state
        .queue_template_node(&entry_node, None)
        .expect("queue entry node");

    let queue = Arc::new(Mutex::new(VecDeque::new()));
    let backend = MemoryBackend::with_queue(queue.clone());
    let workflow_version_id = backend
        .upsert_workflow_version(&WorkflowRegistration {
            workflow_name: "test_dict_key_error".to_string(),
            workflow_version: ir_hash.clone(),
            ir_hash,
            program_proto,
            concurrent: false,
        })
        .await
        .expect("register workflow version");

    let mut actions: HashMap<String, ActionCallable> = HashMap::new();
    actions.insert(
        "make_payload".to_string(),
        Arc::new(|_kwargs| {
            Box::pin(async move {
                Ok(Value::Object(
                    [("present".to_string(), Value::Number(1.into()))]
                        .into_iter()
                        .collect(),
                ))
            })
        }),
    );
    let worker_pool = waymark_worker_inline::InlineWorkerPool::new(actions);
    let runloop = RunLoop::new(
        worker_pool,
        backend.clone(),
        default_test_config(LockId::new_uuid_v4()),
    );
    let instance_id = InstanceId::new_uuid_v4();
    queue.lock().expect("queue lock").push_back(QueuedInstance {
        workflow_version_id,
        schedule_id: None,
        entry_node: entry_exec.node_id,
        state,
        action_results: HashMap::new(),
        instance_id,
        scheduled_at: None,
    });

    runloop
        .run()
        .await
        .expect("runloop should continue after instance failure");
    let instances_done = backend.instances_done();
    assert_eq!(instances_done.len(), 1);
    assert_eq!(instances_done[0].executor_id, instance_id);
    let Value::Object(error_obj) = instances_done[0]
        .error
        .clone()
        .expect("instance error payload")
        .0
    else {
        panic!("expected error payload object");
    };
    let message = error_obj
        .get("message")
        .and_then(Value::as_str)
        .expect("error message");
    assert!(
        message.contains("dict has no key"),
        "expected dict key error, got: {message}"
    );
}

#[tokio::test]
async fn test_runloop_executes_list_merge_reassignment() {
    let source = r#"
fn main(input: [], output: [result]):
    left = [1, 2]
    right = [3, 4]
    left = left + right
    left = left + [5]
    result = left
    return result
"#;
    let program = parse_program(source.trim()).expect("parse program");
    let program_proto = program.encode_to_vec();
    let ir_hash = format!("{:x}", Sha256::digest(&program_proto));
    let dag = Arc::new(convert_to_dag(&program).expect("convert to dag"));

    let mut state = RunnerState::from_dag(Arc::clone(&dag));
    let entry_node = dag
        .entry_node
        .as_ref()
        .expect("DAG entry node not found")
        .clone();
    let entry_exec = state
        .queue_template_node(&entry_node, None)
        .expect("queue entry node");

    let queue = Arc::new(Mutex::new(VecDeque::new()));
    let backend = MemoryBackend::with_queue(queue.clone());
    let workflow_version_id = backend
        .upsert_workflow_version(&WorkflowRegistration {
            workflow_name: "test_list_merge_reassignment".to_string(),
            workflow_version: ir_hash.clone(),
            ir_hash,
            program_proto,
            concurrent: false,
        })
        .await
        .expect("register workflow version");

    let worker_pool = waymark_worker_inline::InlineWorkerPool::new(HashMap::new());
    let runloop = RunLoop::new(
        worker_pool,
        backend.clone(),
        default_test_config(LockId::new_uuid_v4()),
    );
    let instance_id = InstanceId::new_uuid_v4();
    queue.lock().expect("queue lock").push_back(QueuedInstance {
        workflow_version_id,
        schedule_id: None,
        entry_node: entry_exec.node_id,
        state,
        action_results: HashMap::new(),
        instance_id,
        scheduled_at: None,
    });

    runloop.run().await.expect("runloop");

    let instances_done = backend.instances_done();
    assert_eq!(instances_done.len(), 1);
    assert_eq!(instances_done[0].executor_id, instance_id);
    let output = instances_done[0].result.clone().expect("instance result");
    let Value::Object(map) = output.0 else {
        panic!("expected output object");
    };
    assert_eq!(
        map.get("result"),
        Some(&Value::Array(vec![
            Value::Number(1.into()),
            Value::Number(2.into()),
            Value::Number(3.into()),
            Value::Number(4.into()),
            Value::Number(5.into()),
        ]))
    );
}

#[tokio::test]
async fn test_runloop_marks_instance_failed_with_attribute_error() {
    let source = r#"
fn main(input: [], output: [result]):
    payload = @tests.fixtures.test_actions.make_number()
    result = payload.missing
    return result
"#;
    let program = parse_program(source.trim()).expect("parse program");
    let program_proto = program.encode_to_vec();
    let ir_hash = format!("{:x}", Sha256::digest(&program_proto));
    let dag = Arc::new(convert_to_dag(&program).expect("convert to dag"));

    let mut state = RunnerState::from_dag(Arc::clone(&dag));
    let entry_node = dag
        .entry_node
        .as_ref()
        .expect("DAG entry node not found")
        .clone();
    let entry_exec = state
        .queue_template_node(&entry_node, None)
        .expect("queue entry node");

    let queue = Arc::new(Mutex::new(VecDeque::new()));
    let backend = MemoryBackend::with_queue(queue.clone());
    let workflow_version_id = backend
        .upsert_workflow_version(&WorkflowRegistration {
            workflow_name: "test_attribute_error".to_string(),
            workflow_version: ir_hash.clone(),
            ir_hash,
            program_proto,
            concurrent: false,
        })
        .await
        .expect("register workflow version");

    let mut actions: HashMap<String, ActionCallable> = HashMap::new();
    actions.insert(
        "make_number".to_string(),
        Arc::new(|_kwargs| Box::pin(async move { Ok(Value::Number(7.into())) })),
    );
    let worker_pool = waymark_worker_inline::InlineWorkerPool::new(actions);
    let runloop = RunLoop::new(
        worker_pool,
        backend.clone(),
        default_test_config(LockId::new_uuid_v4()),
    );
    let instance_id = InstanceId::new_uuid_v4();
    queue.lock().expect("queue lock").push_back(QueuedInstance {
        workflow_version_id,
        schedule_id: None,
        entry_node: entry_exec.node_id,
        state,
        action_results: HashMap::new(),
        instance_id,
        scheduled_at: None,
    });

    runloop
        .run()
        .await
        .expect("runloop should continue after instance failure");
    let instances_done = backend.instances_done();
    assert_eq!(instances_done.len(), 1);
    assert_eq!(instances_done[0].executor_id, instance_id);
    let Value::Object(error_obj) = instances_done[0]
        .error
        .clone()
        .expect("instance error payload")
        .0
    else {
        panic!("expected error payload object");
    };
    let message = error_obj
        .get("message")
        .and_then(Value::as_str)
        .expect("error message");
    assert!(
        message.contains("attribute not found"),
        "expected attribute error, got: {message}"
    );
}
