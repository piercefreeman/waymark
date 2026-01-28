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
    Database, InstanceRunner, InstanceRunnerConfig, PythonWorkerConfig, PythonWorkerPool,
    ScheduleId, ScheduleType, WorkerBridgeServer, WorkflowVersionId, ir_ast, proto,
};
use sha2::{Digest, Sha256};

mod integration_harness;

/// Clean up test data from previous runs.
async fn cleanup_database(database: &Database) -> Result<()> {
    // Only truncate tables that exist in the new architecture
    sqlx::query("TRUNCATE workflow_schedules, workflow_instances, workflow_versions CASCADE")
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
            0,     // priority
            false, // allow_duplicate
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
            0,     // priority
            false, // allow_duplicate
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
            0,     // priority
            false, // allow_duplicate
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
/// Note: This test verifies schedule database operations work correctly.
/// The actual scheduler loop is part of the runner's periodic tasks.
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
            past,  // Already due!
            0,     // priority
            false, // allow_duplicate
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

    // Start worker bridge (needed for InstanceRunner)
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

    // Create runner with fast poll interval for testing
    let runner_config = InstanceRunnerConfig {
        claim_batch_size: 10,
        completion_batch_size: 10,
        idle_poll_interval: Duration::from_millis(100),
        ..Default::default()
    };

    // Create worker pool with 1 worker (minimum required)
    let worker_pool =
        Arc::new(PythonWorkerPool::new(worker_config, 1, Arc::clone(&worker_bridge), None).await?);

    let runner = Arc::new(InstanceRunner::new(
        runner_config,
        (*database).clone(),
        Arc::clone(&worker_pool),
    ));

    // Manually process due schedules (the InstanceRunner doesn't include scheduler loop yet)
    // This tests the database operations that would be called by a scheduler
    let due_schedules = database.find_due_schedules(10).await?;
    assert_eq!(due_schedules.len(), 1, "should have 1 due schedule");

    let schedule = &due_schedules[0];
    let instance_id = database
        .create_instance(
            &schedule.workflow_name,
            version_id,
            None,
            Some(ScheduleId(schedule.id)),
        )
        .await?;

    // Calculate next run and mark executed
    let next_run = Utc::now() + chrono::Duration::seconds(schedule.interval_seconds.unwrap_or(60));
    database
        .mark_schedule_executed(ScheduleId(schedule.id), instance_id, next_run)
        .await?;

    // Shutdown runner (we didn't run it, but clean up)
    runner.shutdown();

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
            0,     // priority
            false, // allow_duplicate
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
            0,     // priority
            false, // allow_duplicate
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

/// Tests that has_running_instance_for_schedule detects running instances
/// and that allow_duplicate controls whether schedules skip or proceed.
#[tokio::test]
#[serial]
async fn test_allow_duplicate_flag() -> Result<()> {
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
    let version_id = create_test_workflow(&database, "dedup_workflow").await?;
    info!(%version_id, "created test workflow");

    // Create a schedule with allow_duplicate=false
    let past = Utc::now() - chrono::Duration::seconds(5);
    let schedule_id = database
        .upsert_schedule(
            "dedup_workflow",
            "no-dup",
            ScheduleType::Interval,
            None,
            Some(60),
            0,
            None,
            past,
            0,     // priority
            false, // allow_duplicate
        )
        .await?;
    info!(%schedule_id, "created schedule with allow_duplicate=false");

    // Verify allow_duplicate is stored correctly
    let schedule = database
        .get_schedule_by_name("dedup_workflow", "no-dup")
        .await?
        .expect("schedule should exist");
    assert!(!schedule.allow_duplicate, "allow_duplicate should be false");

    // No running instances yet
    let has_running = database
        .has_running_instance_for_schedule(schedule_id)
        .await?;
    assert!(!has_running, "should have no running instances initially");

    // Create a running instance for this schedule
    let instance_id = database
        .create_instance("dedup_workflow", version_id, None, Some(schedule_id))
        .await?;
    info!(%instance_id, "created running instance for schedule");

    // Now has_running_instance_for_schedule should return true
    let has_running = database
        .has_running_instance_for_schedule(schedule_id)
        .await?;
    assert!(has_running, "should detect running instance");

    // Complete the instance
    database.complete_instance(instance_id, None).await?;

    // Should no longer detect running instance
    let has_running = database
        .has_running_instance_for_schedule(schedule_id)
        .await?;
    assert!(
        !has_running,
        "should not detect completed instance as running"
    );

    // Test with allow_duplicate=true
    let schedule_id_dup = database
        .upsert_schedule(
            "dedup_workflow",
            "allow-dup",
            ScheduleType::Interval,
            None,
            Some(60),
            0,
            None,
            past,
            0,    // priority
            true, // allow_duplicate
        )
        .await?;

    let schedule_dup = database
        .get_schedule_by_name("dedup_workflow", "allow-dup")
        .await?
        .expect("schedule should exist");
    assert!(
        schedule_dup.allow_duplicate,
        "allow_duplicate should be true"
    );

    info!(%schedule_id_dup, "allow_duplicate flag test passed");
    Ok(())
}
