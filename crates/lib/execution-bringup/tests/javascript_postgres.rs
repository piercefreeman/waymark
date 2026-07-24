use std::{sync::Arc, time::Duration};

use nonempty_collections::NEVec;
use serial_test::serial;
use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;
use waymark_worker_core::BaseWorkerPool as _;

struct ControlledWorkerPool {
    request_tx: mpsc::UnboundedSender<waymark_worker_core::ActionRequest>,
    completion_rx: Mutex<mpsc::UnboundedReceiver<waymark_worker_core::ActionCompletion>>,
}

impl waymark_worker_core::BaseWorkerPool for ControlledWorkerPool {
    fn queue(
        &self,
        request: waymark_worker_core::ActionRequest,
    ) -> Result<(), waymark_worker_core::WorkerPoolError> {
        self.request_tx.send(request).map_err(|_| {
            waymark_worker_core::WorkerPoolError::new(
                "ControlledWorkerPoolClosed",
                "request receiver closed",
            )
        })
    }

    async fn poll_complete(&self) -> Option<NEVec<waymark_worker_core::ActionCompletion>> {
        self.completion_rx.lock().await.recv().await.map(NEVec::new)
    }
}

fn javascript_program() -> waymark_proto::ast::Program {
    let mut program = waymark_ir_parser::parse_program(
        r#"
fn main(input: [], output: [result]):
    result = @actions.greet(name="Ada")
    return result
"#
        .trim(),
    )
    .expect("parse test IR");

    // The textual IR predates runtime annotations and deliberately defaults
    // actions to Python. This test exercises the JavaScript source-AST path,
    // so stamp the parsed action exactly where the TypeScript compiler does.
    let assignment = match program.functions[0].body.as_mut().unwrap().statements[0]
        .kind
        .as_mut()
        .unwrap()
    {
        waymark_proto::ast::statement::Kind::Assignment(assignment) => assignment,
        other => panic!("expected assignment, got {other:?}"),
    };
    let action = match assignment.value.as_mut().unwrap().kind.as_mut().unwrap() {
        waymark_proto::ast::expr::Kind::ActionCall(action) => action,
        other => panic!("expected action call, got {other:?}"),
    };
    action.runtime = waymark_proto::action::ActionRuntime::Javascript as i32;
    program
}

async fn reset_database(pool: &sqlx::PgPool) {
    sqlx::query(
        r#"
        TRUNCATE action_call_completions,
                 action_call_requests,
                 queued_instances,
                 runner_actions_done,
                 runner_instances,
                 vm_execution_results,
                 vm_executables,
                 vm_runtime_snapshots,
                 workflow_schedules,
                 workflow_versions,
                 workload_pinnings,
                 worker_status
        RESTART IDENTITY CASCADE
        "#,
    )
    .execute(pool)
    .await
    .expect("reset Postgres test tables");
}

async fn register_program(
    backend: &waymark_backend_postgres::PostgresBackend,
) -> waymark_ids::InstanceId {
    let program = javascript_program();
    let ast = waymark_vm_ast_old_proto::convert(program).expect("convert program");
    let executable_service = waymark_workflow_service_vm_executables::ExecutablesService::<
        _,
        _,
        waymark_system_vm::Spec,
        waymark_system_vm::Lowering,
    >::new(backend.clone(), waymark_vm_codec_rmp::RmpCodec);
    let (executable_id, executable, _) = executable_service
        .compile_and_store("JavaScriptPostgres", "test", &ast)
        .await
        .expect("compile and store executable");
    let runtime = waymark_system_vm::Runtime::with_conventional_entrypoint(
        waymark_system_vm::Interpreter::default(),
        Arc::new(executable),
    )
    .expect("create runtime");
    let vm_id = waymark_ids::InstanceId::new_uuid_v4();
    waymark_workflow_service_vm_runtimes::RegistrationService::new(
        backend.clone(),
        waymark_vm_codec_rmp::RmpCodec,
    )
    .register_vm(vm_id, executable_id, |serializer| {
        runtime.snapshot(serializer)
    })
    .await
    .expect("register VM");
    vm_id
}

#[tokio::test]
#[serial]
async fn javascript_action_uses_the_postgres_durable_effect_pipeline() {
    let pool = waymark_support_test::postgres_setup().await;
    reset_database(&pool).await;
    let backend = waymark_backend_postgres::PostgresBackend::new(pool);
    let vm_id = register_program(&backend).await;

    let (request_tx, mut request_rx) = mpsc::unbounded_channel();
    let (completion_tx, completion_rx) = mpsc::unbounded_channel();
    let worker_pool = Arc::new(ControlledWorkerPool {
        request_tx,
        completion_rx: Mutex::new(completion_rx),
    });
    worker_pool.launch().await.unwrap();

    let shutdown = CancellationToken::new();
    let force_shutdown = CancellationToken::new();
    let handles = waymark_execution_bringup::start(
        waymark_execution_bringup::Config {
            node_id: uuid::Uuid::new_v4(),
            action_effect_reconciler_lock_ttl: Duration::from_secs(30).try_into().unwrap(),
            action_effect_reconciler_lock_heartbeat: Duration::from_secs(5).try_into().unwrap(),
            max_pinned: 1.try_into().unwrap(),
            pinning_ttl: Duration::from_secs(30).try_into().unwrap(),
            pinning_heartbeat: Duration::from_secs(5).try_into().unwrap(),
            vm_retention: Duration::from_secs(60).try_into().unwrap(),
            vm_sweep_interval: Duration::from_secs(60).try_into().unwrap(),
            executable_retention: Duration::from_secs(60).try_into().unwrap(),
            executable_sweep_interval: Duration::from_secs(60).try_into().unwrap(),
        },
        Arc::new(backend.clone()),
        worker_pool,
        shutdown.clone(),
        force_shutdown.clone(),
    )
    .await
    .expect("start VM execution");

    let request = tokio::time::timeout(Duration::from_secs(10), request_rx.recv())
        .await
        .expect("JavaScript action dispatch timed out")
        .expect("worker request channel closed");
    assert_eq!(
        request.runtime,
        waymark_action_core::ActionRuntime::JavaScript
    );
    assert_eq!(request.action_name, "greet");
    assert_eq!(request.module_name.as_deref(), Some("actions"));
    assert_eq!(request.kwargs.get("name"), Some(&serde_json::json!("Ada")));

    let stored_requests: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM action_call_requests WHERE vm_id = $1")
            .bind(vm_id)
            .fetch_one(backend.pool())
            .await
            .expect("count durable action requests");
    assert_eq!(
        stored_requests, 1,
        "request must be durable before delivery"
    );

    completion_tx
        .send(waymark_worker_core::ActionCompletion {
            executor_id: request.executor_id,
            execution_id: request.execution_id,
            attempt_number: request.attempt_number,
            dispatch_token: request.dispatch_token,
            result: waymark_runner_executor_core::UncheckedExecutionResult(serde_json::json!(
                "Hello, Ada"
            )),
            metadata: request.metadata,
        })
        .expect("send JavaScript action completion");

    let outcome_service = waymark_workflow_service_vm_runtimes::OutcomePollingService::<
        _,
        _,
        waymark_system_vm::ReadyValue,
    >::new(backend.clone(), waymark_vm_codec_rmp::RmpCodec);
    let outcome = tokio::time::timeout(
        Duration::from_secs(10),
        outcome_service.wait_for_outcome(&vm_id, Duration::from_millis(10)),
    )
    .await
    .expect("workflow outcome timed out")
    .expect("poll workflow outcome");
    assert_eq!(
        outcome,
        waymark_workflow_completion_core::Outcome::Completion(
            waymark_system_vm::ReadyValue::String("Hello, Ada".to_owned())
        )
    );

    for _ in 0..100 {
        if backend
            .query_counts()
            .get("delete:action_call_completions_ack")
            .copied()
            .unwrap_or_default()
            > 0
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let query_counts = backend.query_counts();
    assert!(
        query_counts
            .get("insert:action_call_requests")
            .copied()
            .unwrap_or_default()
            > 0
    );
    assert!(
        query_counts
            .get("insert:action_call_completions")
            .copied()
            .unwrap_or_default()
            > 0
    );
    assert!(
        query_counts
            .get("delete:action_call_completions_ack")
            .copied()
            .unwrap_or_default()
            > 0
    );

    let pending_requests: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM action_call_requests WHERE vm_id = $1")
            .bind(vm_id)
            .fetch_one(backend.pool())
            .await
            .unwrap();
    let pending_completions: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM action_call_completions WHERE vm_id = $1")
            .bind(vm_id)
            .fetch_one(backend.pool())
            .await
            .unwrap();
    assert_eq!((pending_requests, pending_completions), (0, 0));

    shutdown.cancel();
    force_shutdown.cancel();
    for handle in [
        handles.pinning_manager,
        handles.execution_driver,
        handles.executable_sweeper,
        handles.vm_sweeper,
        handles.durable_action_completions_writer,
        handles.durable_action_completions_poller,
        handles.durable_action_completions_acker,
        handles.action_effect_reconciler_lock_renewal,
    ] {
        handle.abort();
        let _ = handle.await;
    }
}
