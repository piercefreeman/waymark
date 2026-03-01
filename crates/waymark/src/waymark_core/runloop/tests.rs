use super::*;
use std::collections::{HashMap, VecDeque};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering},
};
use std::time::Duration;

use chrono::Utc;
use prost::Message;
use sha2::{Digest, Sha256};
use tonic::async_trait;

use crate::backends::{
    ActionAttemptStatus, BackendError, BackendResult, CoreBackend, GraphUpdate, InstanceDone,
    InstanceLockStatus, LockClaim, MemoryBackend, QueuedInstanceBatch, WorkflowRegistration,
    WorkflowRegistryBackend, WorkflowVersion,
};
use crate::messages::ast as ir;
use crate::waymark_core::ir_parser::parse_program;
use crate::waymark_core::runner::RunnerState;
use crate::waymark_core::runner::state::NodeStatus;
use crate::workers::ActionCallable;
use waymark_dag::convert_to_dag;

#[derive(Clone)]
struct FaultInjectingBackend {
    inner: MemoryBackend,
    fail_get_queued_instances_with_depth_limit: Arc<AtomicBool>,
    get_queued_instances_calls: Arc<AtomicUsize>,
}

impl FaultInjectingBackend {
    fn with_depth_limit_poll_failures(inner: MemoryBackend) -> Self {
        Self {
            inner,
            fail_get_queued_instances_with_depth_limit: Arc::new(AtomicBool::new(true)),
            get_queued_instances_calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn get_queued_instances_calls(&self) -> usize {
        self.get_queued_instances_calls.load(AtomicOrdering::SeqCst)
    }

    fn queue_len(&self) -> usize {
        self.inner
            .instance_queue()
            .as_ref()
            .map(|queue| queue.lock().expect("queue poisoned").len())
            .unwrap_or(0)
    }

    fn instances_done_len(&self) -> usize {
        self.inner.instances_done().len()
    }
}

#[async_trait]
impl CoreBackend for FaultInjectingBackend {
    fn clone_box(&self) -> Box<dyn CoreBackend> {
        Box::new(self.clone())
    }

    async fn save_graphs(
        &self,
        claim: LockClaim,
        graphs: &[GraphUpdate],
    ) -> BackendResult<Vec<InstanceLockStatus>> {
        self.inner.save_graphs(claim, graphs).await
    }

    async fn save_actions_done(
        &self,
        actions: &[crate::backends::ActionDone],
    ) -> BackendResult<()> {
        self.inner.save_actions_done(actions).await
    }

    async fn save_instances_done(&self, instances: &[InstanceDone]) -> BackendResult<()> {
        self.inner.save_instances_done(instances).await
    }

    async fn get_queued_instances(
        &self,
        size: usize,
        claim: LockClaim,
    ) -> BackendResult<QueuedInstanceBatch> {
        self.get_queued_instances_calls
            .fetch_add(1, AtomicOrdering::SeqCst);
        if self
            .fail_get_queued_instances_with_depth_limit
            .load(AtomicOrdering::SeqCst)
        {
            return Err(BackendError::Message("depth limit exceeded".to_string()));
        }
        self.inner.get_queued_instances(size, claim).await
    }

    async fn queue_instances(
        &self,
        instances: &[crate::backends::QueuedInstance],
    ) -> BackendResult<()> {
        self.inner.queue_instances(instances).await
    }

    async fn refresh_instance_locks(
        &self,
        claim: LockClaim,
        instance_ids: &[Uuid],
    ) -> BackendResult<Vec<InstanceLockStatus>> {
        self.inner.refresh_instance_locks(claim, instance_ids).await
    }

    async fn release_instance_locks(
        &self,
        lock_uuid: Uuid,
        instance_ids: &[Uuid],
    ) -> BackendResult<()> {
        self.inner
            .release_instance_locks(lock_uuid, instance_ids)
            .await
    }
}

#[async_trait]
impl WorkflowRegistryBackend for FaultInjectingBackend {
    async fn upsert_workflow_version(
        &self,
        registration: &WorkflowRegistration,
    ) -> BackendResult<Uuid> {
        self.inner.upsert_workflow_version(registration).await
    }

    async fn get_workflow_versions(&self, ids: &[Uuid]) -> BackendResult<Vec<WorkflowVersion>> {
        self.inner.get_workflow_versions(ids).await
    }
}

fn default_test_config(lock_uuid: Uuid) -> RunLoopSupervisorConfig {
    RunLoopSupervisorConfig {
        max_concurrent_instances: 25,
        executor_shards: 1,
        instance_done_batch_size: None,
        poll_interval: Duration::from_millis(10),
        persistence_interval: Duration::from_millis(10),
        lock_uuid,
        lock_ttl: Duration::from_secs(15),
        lock_heartbeat: Duration::from_secs(5),
        evict_sleep_threshold: Duration::from_secs(10),
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

    let mut state = RunnerState::new(Some(Arc::clone(&dag)), None, None, false);
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
    let worker_pool = crate::workers::InlineWorkerPool::new(actions);

    let mut runloop = RunLoop::new(
        worker_pool,
        backend.clone(),
        RunLoopSupervisorConfig {
            max_concurrent_instances: 25,
            executor_shards: 1,
            instance_done_batch_size: None,
            poll_interval: Duration::from_secs_f64(0.0),
            persistence_interval: Duration::from_secs_f64(0.1),
            lock_uuid: Uuid::new_v4(),
            lock_ttl: Duration::from_secs(15),
            lock_heartbeat: Duration::from_secs(5),
            evict_sleep_threshold: Duration::from_secs(10),
            skip_sleep: false,
            active_instance_gauge: None,
        },
    );
    queue.lock().expect("queue lock").push_back(QueuedInstance {
        workflow_version_id,
        schedule_id: None,
        dag: None,
        entry_node: entry_exec.node_id,
        state: Some(state),
        action_results: HashMap::new(),
        instance_id: Uuid::new_v4(),
        scheduled_at: None,
    });

    tracing::info!("1");

    runloop.run().await.expect("runloop");

    tracing::info!("1");

    let instances_done = backend.instances_done();
    assert_eq!(instances_done.len(), 1);
    let done = &instances_done[0];
    let output = done.result.clone().expect("instance result");
    let Value::Object(map) = output else {
        panic!("expected output object");
    };
    assert_eq!(map.get("y"), Some(&Value::Number(8.into())));
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

    let mut state = RunnerState::new(Some(Arc::clone(&dag)), None, None, false);
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
    let worker_pool = crate::workers::InlineWorkerPool::new(actions);

    let mut runloop = RunLoop::new(
        worker_pool,
        backend.clone(),
        RunLoopSupervisorConfig {
            max_concurrent_instances: 25,
            executor_shards: 1,
            instance_done_batch_size: None,
            poll_interval: Duration::from_secs_f64(0.0),
            persistence_interval: Duration::from_secs_f64(0.05),
            lock_uuid: Uuid::new_v4(),
            lock_ttl: Duration::from_secs(15),
            lock_heartbeat: Duration::from_secs(5),
            evict_sleep_threshold: Duration::from_secs(10),
            skip_sleep: false,
            active_instance_gauge: None,
        },
    );
    queue.lock().expect("queue lock").push_back(QueuedInstance {
        workflow_version_id,
        schedule_id: None,
        dag: None,
        entry_node: entry_exec.node_id,
        state: Some(state),
        action_results: HashMap::new(),
        instance_id: Uuid::new_v4(),
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
    let mut state = RunnerState::new(Some(Arc::clone(&dag)), None, None, false);
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

    let worker_pool = crate::workers::InlineWorkerPool::new(HashMap::new());
    let mut runloop = RunLoop::new(
        worker_pool,
        backend.clone(),
        RunLoopSupervisorConfig {
            max_concurrent_instances: 25,
            executor_shards: 1,
            instance_done_batch_size: None,
            poll_interval: Duration::from_secs_f64(0.0),
            persistence_interval: Duration::from_secs_f64(0.1),
            lock_uuid: Uuid::new_v4(),
            lock_ttl: Duration::from_secs(15),
            lock_heartbeat: Duration::from_secs(5),
            evict_sleep_threshold: Duration::from_secs(10),
            skip_sleep: false,
            active_instance_gauge: None,
        },
    );
    let instance_id = Uuid::new_v4();
    queue.lock().expect("queue lock").push_back(QueuedInstance {
        workflow_version_id,
        schedule_id: None,
        dag: None,
        entry_node: entry_exec.node_id,
        state: Some(state),
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
    let Value::Object(error_obj) = error else {
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

    let mut state = RunnerState::new(Some(Arc::clone(&dag)), None, None, false);
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
    let worker_pool = crate::workers::InlineWorkerPool::new(actions);

    let mut runloop = RunLoop::new(
        worker_pool,
        backend.clone(),
        RunLoopSupervisorConfig {
            max_concurrent_instances: 25,
            executor_shards: 1,
            instance_done_batch_size: None,
            poll_interval: Duration::from_secs_f64(0.0),
            persistence_interval: Duration::from_secs_f64(0.1),
            lock_uuid: Uuid::new_v4(),
            lock_ttl: Duration::from_secs(15),
            lock_heartbeat: Duration::from_secs(5),
            evict_sleep_threshold: Duration::from_secs(10),
            skip_sleep: false,
            active_instance_gauge: None,
        },
    );
    queue.lock().expect("queue lock").push_back(QueuedInstance {
        workflow_version_id,
        schedule_id: None,
        dag: None,
        entry_node: entry_exec.node_id,
        state: Some(state),
        action_results: HashMap::new(),
        instance_id: Uuid::new_v4(),
        scheduled_at: None,
    });

    runloop.run().await.expect("runloop");
    let instances_done = backend.instances_done();
    assert_eq!(instances_done.len(), 1);
    let done = &instances_done[0];
    let output = done.result.clone().expect("instance result");
    let Value::Object(map) = output else {
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
async fn test_instance_poller_send_unblocks_on_stop_notification() {
    let (instance_tx, mut instance_rx) = mpsc::channel::<InstanceMessage>(1);
    instance_tx
        .send(InstanceMessage::Batch {
            instances: Vec::new(),
        })
        .await
        .expect("seed channel");

    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let send_task = tokio::spawn({
        let instance_tx = instance_tx.clone();
        let shutdown_token = shutdown_token.clone();
        async move {
            send_with_stop(
                &instance_tx,
                InstanceMessage::Batch {
                    instances: Vec::new(),
                },
                shutdown_token.cancelled(),
                "instance message",
            )
            .await
        }
    });

    tokio::time::sleep(Duration::from_millis(20)).await;
    shutdown_token.cancel();
    let sent = tokio::time::timeout(Duration::from_millis(300), send_task)
        .await
        .expect("send task should complete")
        .expect("send task should not panic");
    assert!(!sent, "send should abort when stop is notified");

    let _ = instance_rx.recv().await;
}

#[tokio::test]
async fn test_instance_poller_send_succeeds_when_channel_has_capacity() {
    let (instance_tx, mut instance_rx) = mpsc::channel::<InstanceMessage>(1);
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let sent = send_with_stop(
        &instance_tx,
        InstanceMessage::Batch {
            instances: Vec::new(),
        },
        shutdown_token.cancelled(),
        "instance message",
    )
    .await;
    assert!(sent);

    let received = instance_rx.recv().await.expect("queued message");
    match received {
        InstanceMessage::Batch { instances } => assert!(instances.is_empty()),
        InstanceMessage::Error(err) => panic!("unexpected error message: {err}"),
    }
}

#[tokio::test]
async fn test_runloop_supervisor_restarts_on_depth_limit_backend_errors() {
    let queue = Arc::new(Mutex::new(VecDeque::new()));
    let backend =
        FaultInjectingBackend::with_depth_limit_poll_failures(MemoryBackend::with_queue(queue));
    let worker_pool = crate::workers::InlineWorkerPool::new(HashMap::new());
    let shutdown_token = tokio_util::sync::CancellationToken::new();

    let supervisor = tokio::spawn(runloop_supervisor(
        backend.clone(),
        worker_pool,
        default_test_config(Uuid::new_v4()),
        shutdown_token.clone(),
    ));

    tokio::time::sleep(Duration::from_millis(750)).await;
    shutdown_token.cancel();
    tokio::time::timeout(Duration::from_secs(2), supervisor)
        .await
        .expect("supervisor should stop")
        .expect("supervisor task should not panic");

    assert!(
        backend.get_queued_instances_calls() >= 2,
        "expected multiple polling attempts while supervisor restarts"
    );
}

#[tokio::test]
async fn test_runloop_supervisor_reproduces_no_progress_with_continued_queue_growth() {
    let queue = Arc::new(Mutex::new(VecDeque::new()));
    let backend =
        FaultInjectingBackend::with_depth_limit_poll_failures(MemoryBackend::with_queue(queue));
    let worker_pool = crate::workers::InlineWorkerPool::new(HashMap::new());
    let shutdown_token = tokio_util::sync::CancellationToken::new();

    let supervisor = tokio::spawn(runloop_supervisor(
        backend.clone(),
        worker_pool,
        default_test_config(Uuid::new_v4()),
        shutdown_token.clone(),
    ));

    for _ in 0..20 {
        backend
            .queue_instances(&[QueuedInstance {
                workflow_version_id: Uuid::new_v4(),
                schedule_id: None,
                dag: None,
                entry_node: Uuid::new_v4(),
                state: None,
                action_results: HashMap::new(),
                instance_id: Uuid::new_v4(),
                scheduled_at: None,
            }])
            .await
            .expect("queue synthetic instance");
    }

    tokio::time::sleep(Duration::from_millis(500)).await;
    shutdown_token.cancel();
    tokio::time::timeout(Duration::from_secs(2), supervisor)
        .await
        .expect("supervisor should stop")
        .expect("supervisor task should not panic");

    assert!(
        backend.get_queued_instances_calls() >= 1,
        "expected polling attempts during stall simulation"
    );
    assert!(
        backend.queue_len() >= 20,
        "queued work should continue to grow when poller cannot read instances"
    );
    assert_eq!(
        backend.instances_done_len(),
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

    let mut state = RunnerState::new(Some(Arc::clone(&dag)), None, None, false);
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
    let mut bootstrap_executor = RunnerExecutor::new(Arc::clone(&dag), state, HashMap::new(), None);
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

    let worker_pool = crate::workers::InlineWorkerPool::new(HashMap::new());
    let mut runloop = RunLoop::new(
        worker_pool,
        backend.clone(),
        default_test_config(Uuid::new_v4()),
    );
    let instance_id = Uuid::new_v4();
    queue.lock().expect("queue lock").push_back(QueuedInstance {
        workflow_version_id,
        schedule_id: None,
        dag: None,
        entry_node: action_exec.node_id,
        state: Some(state),
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

    let mut state = RunnerState::new(Some(Arc::clone(&dag)), None, None, false);
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
    let worker_pool = crate::workers::InlineWorkerPool::new(actions);
    let mut runloop = RunLoop::new(
        worker_pool,
        backend.clone(),
        default_test_config(Uuid::new_v4()),
    );
    let instance_id = Uuid::new_v4();
    queue.lock().expect("queue lock").push_back(QueuedInstance {
        workflow_version_id,
        schedule_id: None,
        dag: None,
        entry_node: entry_exec.node_id,
        state: Some(state),
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

    let mut state = RunnerState::new(Some(Arc::clone(&dag)), None, None, false);
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
    let worker_pool = crate::workers::InlineWorkerPool::new(actions);
    let mut runloop = RunLoop::new(
        worker_pool,
        backend.clone(),
        default_test_config(Uuid::new_v4()),
    );
    let instance_id = Uuid::new_v4();
    queue.lock().expect("queue lock").push_back(QueuedInstance {
        workflow_version_id,
        schedule_id: None,
        dag: None,
        entry_node: entry_exec.node_id,
        state: Some(state),
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

#[test]
fn test_lock_mismatches_ignores_expired_lock_with_matching_owner() {
    let backend = MemoryBackend::new();
    let worker_pool = crate::workers::InlineWorkerPool::new(HashMap::new());
    let lock_uuid = Uuid::new_v4();
    let runloop = RunLoop::new(worker_pool, backend, default_test_config(lock_uuid));

    let instance_id = Uuid::new_v4();
    let statuses = vec![InstanceLockStatus {
        instance_id,
        lock_uuid: Some(lock_uuid),
        lock_expires_at: Some(Utc::now() - chrono::Duration::seconds(60)),
    }];
    assert!(
        runloop.lock_mismatches(&statuses).is_empty(),
        "matching lock UUID should not evict solely due to stale expiry"
    );

    let mismatched = vec![InstanceLockStatus {
        instance_id,
        lock_uuid: Some(Uuid::new_v4()),
        lock_expires_at: Some(Utc::now() + chrono::Duration::seconds(60)),
    }];
    let evict_ids = runloop.lock_mismatches(&mismatched);
    assert_eq!(evict_ids, HashSet::from([instance_id]));
}
