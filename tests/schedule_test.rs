//! Integration tests for workflow scheduling.
//!
//! These tests verify the schedule functionality:
//! 1. Schedule creation and storage
//! 2. Scheduler loop fires on due schedules
//! 3. Instance creation from schedules
//! 4. Schedule status updates (pause/resume/delete)
//! 5. ListSchedules gRPC endpoint

use std::{env, path::PathBuf, sync::Arc};

use anyhow::Result;
use chrono::Utc;
use serial_test::serial;
use tokio::time::Duration;
use tonic::transport::Channel;
use tracing::info;

use rappel::{
    DAGRunner, Database, PythonWorkerConfig, PythonWorkerPool, RunnerConfig, ScheduleId,
    ScheduleType, WorkerBridgeServer, WorkflowVersionId, ir_ast, proto,
};
use sha2::{Digest, Sha256};

mod integration_harness;

/// Clean up test data from previous runs.
async fn cleanup_database(database: &Database) -> Result<()> {
    sqlx::query("DELETE FROM action_queue")
        .execute(database.pool())
        .await?;
    sqlx::query("DELETE FROM node_inputs")
        .execute(database.pool())
        .await?;
    sqlx::query("DELETE FROM node_readiness")
        .execute(database.pool())
        .await?;
    sqlx::query("DELETE FROM instance_context")
        .execute(database.pool())
        .await?;
    sqlx::query("DELETE FROM loop_state")
        .execute(database.pool())
        .await?;
    sqlx::query("DELETE FROM workflow_schedules")
        .execute(database.pool())
        .await?;
    sqlx::query("DELETE FROM workflow_instances")
        .execute(database.pool())
        .await?;
    sqlx::query("DELETE FROM workflow_versions")
        .execute(database.pool())
        .await?;
    Ok(())
}

/// Create a minimal workflow version for testing schedules.
async fn create_test_workflow(database: &Database, name: &str) -> Result<WorkflowVersionId> {
    // Create a minimal IR program (empty program is fine for schedule testing)
    let program = ir_ast::Program::default();
    let ir_bytes = prost::Message::encode_to_vec(&program);
    let mut hasher = Sha256::new();
    hasher.update(&ir_bytes);
    let ir_hash = format!("{:x}", hasher.finalize());

    let version_id = database
        .upsert_workflow_version(name, &ir_hash, &ir_bytes, false)
        .await?;

    Ok(version_id)
}

#[tokio::test]
#[serial]
async fn test_schedule_database_operations() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    let database_url = match env::var("RAPPEL_DATABASE_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!("skipping test: RAPPEL_DATABASE_URL not set");
            return Ok(());
        }
    };

    let database = Database::connect(&database_url).await?;
    cleanup_database(&database).await?;

    // Create a test workflow
    let version_id = create_test_workflow(&database, "test_schedule_workflow").await?;
    info!(%version_id, "created test workflow");

    // Test 1: Create a cron schedule
    let next_run = Utc::now() + chrono::Duration::seconds(60);
    let schedule_id = database
        .upsert_schedule(
            "test_schedule_workflow",
            "default",
            ScheduleType::Cron,
            Some("0 * * * *"),
            None,
            0,
            None,
            next_run,
        )
        .await?;
    info!(%schedule_id, "created cron schedule");

    // Verify schedule was created
    let schedule = database
        .get_schedule_by_name("test_schedule_workflow", "default")
        .await?
        .expect("schedule should exist");
    assert_eq!(schedule.workflow_name, "test_schedule_workflow");
    assert_eq!(schedule.schedule_type, "cron");
    assert_eq!(schedule.cron_expression.as_deref(), Some("0 * * * *"));
    assert_eq!(schedule.status, "active");

    // Test 2: Update to interval schedule (upsert)
    let next_run = Utc::now() + chrono::Duration::seconds(30);
    database
        .upsert_schedule(
            "test_schedule_workflow",
            "default",
            ScheduleType::Interval,
            None,
            Some(300), // 5 minutes
            0,
            None,
            next_run,
        )
        .await?;

    let schedule = database
        .get_schedule_by_name("test_schedule_workflow", "default")
        .await?
        .expect("schedule should exist");
    assert_eq!(schedule.schedule_type, "interval");
    assert_eq!(schedule.interval_seconds, Some(300));

    // Test 3: Pause schedule
    let success = database
        .update_schedule_status("test_schedule_workflow", "default", "paused")
        .await?;
    assert!(success);

    let schedule = database
        .get_schedule_by_name("test_schedule_workflow", "default")
        .await?
        .expect("schedule should exist");
    assert_eq!(schedule.status, "paused");

    // Test 4: Resume schedule
    database
        .update_schedule_status("test_schedule_workflow", "default", "active")
        .await?;

    // Test 5: Find due schedules (schedule not yet due)
    let due = database.find_due_schedules(10).await?;
    assert!(due.is_empty(), "schedule should not be due yet");

    // Test 6: Update next_run to past, then find due schedules
    let past = Utc::now() - chrono::Duration::seconds(10);
    database
        .update_schedule_next_run(ScheduleId(schedule.id), past)
        .await?;

    let due = database.find_due_schedules(10).await?;
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].workflow_name, "test_schedule_workflow");

    // Test 7: Mark schedule executed
    let instance_id = database
        .create_instance("test_schedule_workflow", version_id, None, None)
        .await?;
    let next_run = Utc::now() + chrono::Duration::seconds(300);
    database
        .mark_schedule_executed(ScheduleId(schedule.id), instance_id, next_run)
        .await?;

    let schedule = database
        .get_schedule_by_name("test_schedule_workflow", "default")
        .await?
        .expect("schedule should exist");
    assert!(schedule.last_run_at.is_some());
    assert_eq!(schedule.last_instance_id, Some(instance_id.0));

    // Test 8: Delete schedule
    let success = database
        .delete_schedule("test_schedule_workflow", "default")
        .await?;
    assert!(success);

    let schedule = database
        .get_schedule_by_name("test_schedule_workflow", "default")
        .await?;
    assert!(schedule.is_none(), "deleted schedule should not be found");

    // Test 9: List schedules
    // Re-create schedule to test listing
    database
        .upsert_schedule(
            "test_schedule_workflow",
            "default",
            ScheduleType::Cron,
            Some("0 0 * * *"),
            None,
            0,
            None,
            Utc::now() + chrono::Duration::hours(1),
        )
        .await?;

    let schedules = database.list_schedules(None).await?;
    assert_eq!(schedules.len(), 1);

    let schedules = database.list_schedules(Some("active")).await?;
    assert_eq!(schedules.len(), 1);

    let schedules = database.list_schedules(Some("paused")).await?;
    assert!(schedules.is_empty());

    info!("all schedule database tests passed");
    Ok(())
}

/// Tests that the scheduler loop creates instances for due schedules.
#[tokio::test]
#[serial]
async fn test_scheduler_creates_instance() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    let database_url = match env::var("RAPPEL_DATABASE_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!("skipping test: RAPPEL_DATABASE_URL not set");
            return Ok(());
        }
    };

    let database = Arc::new(Database::connect(&database_url).await?);
    cleanup_database(&database).await?;

    // Create a test workflow
    let version_id = create_test_workflow(&database, "scheduled_workflow").await?;
    info!(%version_id, "created test workflow");

    // Create a schedule that's already due (next_run in the past)
    let past = Utc::now() - chrono::Duration::seconds(5);
    database
        .upsert_schedule(
            "scheduled_workflow",
            "default",
            ScheduleType::Interval,
            None,
            Some(60), // 1 minute interval
            0,
            None,
            past, // Already due!
        )
        .await?;
    info!("created due schedule");

    // Count initial instances
    let initial_instances: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM workflow_instances WHERE workflow_name = $1")
            .bind("scheduled_workflow")
            .fetch_one(database.pool())
            .await?;
    assert_eq!(initial_instances.0, 0);

    // Start worker bridge (needed for DAGRunner)
    let worker_bridge = WorkerBridgeServer::start(None).await?;

    // Use the real Python worker script (same as IntegrationHarness)
    let worker_script = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("python")
        .join(".venv")
        .join("bin")
        .join("rappel-worker");

    let worker_config = PythonWorkerConfig {
        script_path: worker_script,
        script_args: Vec::new(),
        user_modules: Vec::new(),
        extra_python_paths: Vec::new(),
    };

    // Create runner with fast schedule check interval (500ms)
    let runner_config = RunnerConfig {
        batch_size: 10,
        max_slots_per_worker: 5,
        poll_interval_ms: 100,
        timeout_check_interval_ms: 1000,
        timeout_check_batch_size: 100,
        schedule_check_interval_ms: 500, // Fast for testing
        schedule_check_batch_size: 10,
        worker_status_interval_ms: 1000,
        ..Default::default()
    };

    // Create worker pool with 1 worker (minimum required)
    let worker_pool =
        Arc::new(PythonWorkerPool::new(worker_config, 1, Arc::clone(&worker_bridge), None).await?);

    let runner = Arc::new(DAGRunner::new(
        runner_config,
        Arc::clone(&database),
        Arc::clone(&worker_pool),
    ));

    // Run the runner in a background task
    let runner_clone = Arc::clone(&runner);
    let runner_handle = tokio::spawn(async move {
        let _ = runner_clone.run().await;
    });

    // Wait for the scheduler to process the due schedule
    // We wait for last_run_at to be set, not just for the instance to exist,
    // because mark_schedule_executed is called after create_instance
    let mut schedule_executed = false;
    for _ in 0..20 {
        // Wait up to 2 seconds
        tokio::time::sleep(Duration::from_millis(100)).await;

        let schedule = database
            .get_schedule_by_name("scheduled_workflow", "default")
            .await?
            .expect("schedule should exist");

        if schedule.last_run_at.is_some() {
            schedule_executed = true;
            info!("scheduler executed schedule");
            break;
        }
    }

    // Shutdown runner
    runner.shutdown();
    let _ = runner_handle.await;

    assert!(
        schedule_executed,
        "scheduler should have executed the due schedule"
    );

    // Verify schedule was updated
    let schedule = database
        .get_schedule_by_name("scheduled_workflow", "default")
        .await?
        .expect("schedule should exist");
    assert!(
        schedule.last_run_at.is_some(),
        "last_run_at should be set after execution"
    );
    assert!(
        schedule.last_instance_id.is_some(),
        "last_instance_id should be set"
    );
    assert!(
        schedule.next_run_at.unwrap() > Utc::now(),
        "next_run_at should be in the future"
    );

    info!("scheduler integration test passed");
    Ok(())
}

/// Tests the ListSchedules gRPC endpoint.
#[tokio::test]
#[serial]
async fn test_list_schedules_grpc_endpoint() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    let database_url = match env::var("RAPPEL_DATABASE_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!("skipping test: RAPPEL_DATABASE_URL not set");
            return Ok(());
        }
    };

    let database = Database::connect(&database_url).await?;
    cleanup_database(&database).await?;

    // Create test workflows (schedules need workflow versions)
    let version1 = create_test_workflow(&database, "cron_workflow").await?;
    let version2 = create_test_workflow(&database, "interval_workflow").await?;
    info!(%version1, %version2, "created test workflows");

    // Create test schedules with different types and statuses
    let next_run = Utc::now() + chrono::Duration::hours(1);
    database
        .upsert_schedule(
            "cron_workflow",
            "default",
            ScheduleType::Cron,
            Some("0 0 * * *"),
            None,
            0,
            None,
            next_run,
        )
        .await?;
    info!("created cron schedule");

    database
        .upsert_schedule(
            "interval_workflow",
            "default",
            ScheduleType::Interval,
            None,
            Some(300),
            0,
            None,
            next_run,
        )
        .await?;
    info!("created interval schedule");

    // Pause one schedule to test status filtering
    database
        .update_schedule_status("interval_workflow", "default", "paused")
        .await?;

    // Start gRPC server
    let (grpc_addr, shutdown_tx, server_handle) =
        integration_harness::start_workflow_grpc_server(database.clone()).await?;
    info!(%grpc_addr, "gRPC server started");

    // Give server a moment to start
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Create gRPC client
    let channel = Channel::from_shared(format!("http://{}", grpc_addr))?
        .connect()
        .await?;
    let mut client = proto::workflow_service_client::WorkflowServiceClient::new(channel);

    // Test 1: List all schedules
    let response = client
        .list_schedules(proto::ListSchedulesRequest {
            status_filter: None,
        })
        .await?;
    let schedules = response.into_inner().schedules;
    assert_eq!(schedules.len(), 2, "should have 2 schedules");

    // Verify cron schedule
    let cron_schedule = schedules
        .iter()
        .find(|s| s.workflow_name == "cron_workflow")
        .expect("cron_workflow schedule should exist");
    assert_eq!(
        cron_schedule.schedule_type,
        proto::ScheduleType::Cron as i32
    );
    assert_eq!(cron_schedule.cron_expression, "0 0 * * *");
    assert_eq!(cron_schedule.status, proto::ScheduleStatus::Active as i32);

    // Verify interval schedule
    let interval_schedule = schedules
        .iter()
        .find(|s| s.workflow_name == "interval_workflow")
        .expect("interval_workflow schedule should exist");
    assert_eq!(
        interval_schedule.schedule_type,
        proto::ScheduleType::Interval as i32
    );
    assert_eq!(interval_schedule.interval_seconds, 300);
    assert_eq!(
        interval_schedule.status,
        proto::ScheduleStatus::Paused as i32
    );

    // Test 2: List only active schedules
    let response = client
        .list_schedules(proto::ListSchedulesRequest {
            status_filter: Some("active".to_string()),
        })
        .await?;
    let active_schedules = response.into_inner().schedules;
    assert_eq!(active_schedules.len(), 1, "should have 1 active schedule");
    assert_eq!(active_schedules[0].workflow_name, "cron_workflow");

    // Test 3: List only paused schedules
    let response = client
        .list_schedules(proto::ListSchedulesRequest {
            status_filter: Some("paused".to_string()),
        })
        .await?;
    let paused_schedules = response.into_inner().schedules;
    assert_eq!(paused_schedules.len(), 1, "should have 1 paused schedule");
    assert_eq!(paused_schedules[0].workflow_name, "interval_workflow");

    // Shutdown server
    let _ = shutdown_tx.send(());
    let _ = server_handle.await;

    info!("list_schedules gRPC endpoint test passed");
    Ok(())
}
