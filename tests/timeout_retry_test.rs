//! Tests for action timeout and retry handling.
//!
//! These tests verify the two-phase timeout/retry system:
//! 1. mark_timed_out_actions: Marks overdue dispatched actions as 'failed' with retry_kind='timeout'
//! 2. requeue_failed_actions: Handles ALL failed actions (timeouts + explicit failures),
//!    applying backoff logic and retry limits in one place

use std::env;

use anyhow::Result;
use chrono::{Duration, Utc};
use serial_test::serial;
use sqlx::Row;
use uuid::Uuid;

use rappel::{BackoffKind, Database, NewAction, WorkflowInstanceId, WorkflowVersionId};

/// Helper to create a test database connection.
async fn setup_db() -> Option<Database> {
    let database_url = match env::var("RAPPEL_DATABASE_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!("skipping test: RAPPEL_DATABASE_URL not set");
            return None;
        }
    };

    let db = Database::connect(&database_url).await.ok()?;
    cleanup_database(&db).await.ok()?;
    Some(db)
}

/// Clean up all tables before each test.
async fn cleanup_database(db: &Database) -> Result<()> {
    sqlx::query(
        "TRUNCATE action_queue, instance_context, loop_state, node_inputs, node_readiness, workflow_instances, workflow_versions CASCADE",
    )
    .execute(db.pool())
    .await?;
    Ok(())
}

/// Helper to create a workflow version and instance for testing.
async fn create_test_instance(db: &Database) -> Result<(WorkflowVersionId, WorkflowInstanceId)> {
    let version_id = db
        .upsert_workflow_version("test_workflow", "hash123", b"proto", false)
        .await?;

    let instance_id = db
        .create_instance("test_workflow", version_id, None)
        .await?;

    Ok((version_id, instance_id))
}

/// Helper to insert an action directly with specific parameters for testing.
#[allow(clippy::too_many_arguments)]
async fn insert_test_action(
    db: &Database,
    instance_id: WorkflowInstanceId,
    status: &str,
    attempt_number: i32,
    timeout_retry_limit: i32,
    deadline_at: Option<chrono::DateTime<Utc>>,
    backoff_kind: &str,
    backoff_base_delay_ms: i32,
) -> Result<Uuid> {
    let row = sqlx::query(
        r#"
        INSERT INTO action_queue (
            instance_id, action_seq, module_name, action_name,
            dispatch_payload, status, attempt_number, timeout_retry_limit,
            deadline_at, backoff_kind, backoff_base_delay_ms,
            delivery_token, dispatched_at
        )
        VALUES ($1, 0, 'test_module', 'test_action', $2, $3, $4, $5, $6, $7, $8, $9, NOW())
        RETURNING id
        "#,
    )
    .bind(instance_id.0)
    .bind(Vec::<u8>::new())
    .bind(status)
    .bind(attempt_number)
    .bind(timeout_retry_limit)
    .bind(deadline_at)
    .bind(backoff_kind)
    .bind(backoff_base_delay_ms)
    .bind(Uuid::new_v4()) // delivery_token
    .fetch_one(db.pool())
    .await?;

    Ok(row.get("id"))
}

/// Helper to get action status, attempt_number, and retry_kind.
async fn get_action_state(db: &Database, action_id: Uuid) -> Result<(String, i32, String)> {
    let row =
        sqlx::query("SELECT status, attempt_number, retry_kind FROM action_queue WHERE id = $1")
            .bind(action_id)
            .fetch_one(db.pool())
            .await?;

    Ok((
        row.get("status"),
        row.get("attempt_number"),
        row.get("retry_kind"),
    ))
}

/// Helper to get action scheduled_at.
async fn get_action_scheduled_at(db: &Database, action_id: Uuid) -> Result<chrono::DateTime<Utc>> {
    let row = sqlx::query("SELECT scheduled_at FROM action_queue WHERE id = $1")
        .bind(action_id)
        .fetch_one(db.pool())
        .await?;

    Ok(row.get("scheduled_at"))
}

// =============================================================================
// mark_timed_out_actions Tests
// =============================================================================

#[tokio::test]
#[serial]
async fn test_mark_timed_out_actions_marks_overdue_as_failed() -> Result<()> {
    let Some(db) = setup_db().await else {
        return Ok(());
    };

    let (_, instance_id) = create_test_instance(&db).await?;

    // Create a dispatched action with deadline in the past
    let past_deadline = Utc::now() - Duration::seconds(60);
    let action_id = insert_test_action(
        &db,
        instance_id,
        "dispatched",
        0,
        3,
        Some(past_deadline),
        "exponential",
        1000,
    )
    .await?;

    // Mark timed out actions
    let count = db.mark_timed_out_actions(100).await?;
    assert_eq!(count, 1, "should have marked 1 action");

    // Verify action state - should be 'failed' with retry_kind='timeout'
    let (status, attempt_number, retry_kind) = get_action_state(&db, action_id).await?;
    assert_eq!(status, "failed", "action should be marked as failed");
    assert_eq!(retry_kind, "timeout", "retry_kind should be timeout");
    assert_eq!(
        attempt_number, 0,
        "attempt_number should NOT be incremented yet"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_mark_timed_out_actions_ignores_non_overdue() -> Result<()> {
    let Some(db) = setup_db().await else {
        return Ok(());
    };

    let (_, instance_id) = create_test_instance(&db).await?;

    // Create a dispatched action with deadline in the FUTURE
    let future_deadline = Utc::now() + Duration::seconds(300);
    let action_id = insert_test_action(
        &db,
        instance_id,
        "dispatched",
        0,
        3,
        Some(future_deadline),
        "exponential",
        1000,
    )
    .await?;

    // Mark timed out actions
    let count = db.mark_timed_out_actions(100).await?;
    assert_eq!(count, 0, "should have marked 0 actions");

    // Verify action state unchanged
    let (status, _, _) = get_action_state(&db, action_id).await?;
    assert_eq!(status, "dispatched", "action should still be dispatched");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_mark_timed_out_actions_clears_delivery_token() -> Result<()> {
    let Some(db) = setup_db().await else {
        return Ok(());
    };

    let (_, instance_id) = create_test_instance(&db).await?;

    let past_deadline = Utc::now() - Duration::seconds(60);
    let action_id = insert_test_action(
        &db,
        instance_id,
        "dispatched",
        0,
        3,
        Some(past_deadline),
        "exponential",
        1000,
    )
    .await?;

    // Verify delivery_token was set initially
    let row = sqlx::query("SELECT delivery_token FROM action_queue WHERE id = $1")
        .bind(action_id)
        .fetch_one(db.pool())
        .await?;
    let initial_token: Option<Uuid> = row.get("delivery_token");
    assert!(
        initial_token.is_some(),
        "delivery_token should be set initially"
    );

    // Mark timed out actions
    db.mark_timed_out_actions(100).await?;

    // Verify delivery_token and deadline are cleared
    let row = sqlx::query("SELECT delivery_token, deadline_at FROM action_queue WHERE id = $1")
        .bind(action_id)
        .fetch_one(db.pool())
        .await?;
    let cleared_token: Option<Uuid> = row.get("delivery_token");
    let deadline: Option<chrono::DateTime<Utc>> = row.get("deadline_at");

    assert!(cleared_token.is_none(), "delivery_token should be cleared");
    assert!(deadline.is_none(), "deadline_at should be cleared");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_mark_timed_out_actions_respects_limit() -> Result<()> {
    let Some(db) = setup_db().await else {
        return Ok(());
    };

    let (_, instance_id) = create_test_instance(&db).await?;
    let past_deadline = Utc::now() - Duration::seconds(60);

    // Create 5 overdue actions
    for i in 0..5 {
        sqlx::query(
            r#"
            INSERT INTO action_queue (
                instance_id, action_seq, module_name, action_name,
                dispatch_payload, status, attempt_number, timeout_retry_limit,
                deadline_at, backoff_kind, backoff_base_delay_ms,
                delivery_token, dispatched_at
            )
            VALUES ($1, $2, 'test_module', 'test_action', $3, 'dispatched', 0, 3, $4, 'none', 0, $5, NOW())
            "#,
        )
        .bind(instance_id.0)
        .bind(i)
        .bind(Vec::<u8>::new())
        .bind(Some(past_deadline))
        .bind(Uuid::new_v4())
        .execute(db.pool())
        .await?;
    }

    // Mark with limit of 2
    let count = db.mark_timed_out_actions(2).await?;
    assert_eq!(count, 2, "should only mark 2 actions");

    // Count remaining dispatched actions
    let row = sqlx::query("SELECT COUNT(*) as count FROM action_queue WHERE status = 'dispatched'")
        .fetch_one(db.pool())
        .await?;
    let remaining: i64 = row.get("count");
    assert_eq!(remaining, 3, "should have 3 actions still dispatched");

    Ok(())
}

// =============================================================================
// requeue_failed_actions Tests
// =============================================================================

#[tokio::test]
#[serial]
async fn test_requeue_failed_actions_requeues_failure_with_retries() -> Result<()> {
    let Some(db) = setup_db().await else {
        return Ok(());
    };

    let (_, instance_id) = create_test_instance(&db).await?;

    // Create a failed action with retries remaining
    sqlx::query(
        r#"
        INSERT INTO action_queue (
            instance_id, action_seq, module_name, action_name,
            dispatch_payload, status, retry_kind, attempt_number, max_retries,
            backoff_kind, backoff_base_delay_ms
        )
        VALUES ($1, 0, 'test_module', 'test_action', $2, 'failed', 'failure', 0, 3, 'exponential', 1000)
        "#,
    )
    .bind(instance_id.0)
    .bind(Vec::<u8>::new())
    .execute(db.pool())
    .await?;

    // Requeue failed actions
    let (requeued, permanently_failed) = db.requeue_failed_actions(100).await?;

    assert_eq!(requeued, 1, "should have requeued 1 action");
    assert_eq!(permanently_failed, 0, "should have 0 permanently failed");

    // Verify it was requeued
    let row = sqlx::query("SELECT status, attempt_number FROM action_queue WHERE instance_id = $1")
        .bind(instance_id.0)
        .fetch_one(db.pool())
        .await?;
    let status: String = row.get("status");
    let attempt: i32 = row.get("attempt_number");

    assert_eq!(status, "queued");
    assert_eq!(attempt, 1);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_requeue_failed_actions_requeues_timeout_with_retries() -> Result<()> {
    let Some(db) = setup_db().await else {
        return Ok(());
    };

    let (_, instance_id) = create_test_instance(&db).await?;

    // Create a failed action with retry_kind='timeout' and retries remaining
    sqlx::query(
        r#"
        INSERT INTO action_queue (
            instance_id, action_seq, module_name, action_name,
            dispatch_payload, status, retry_kind, attempt_number, timeout_retry_limit,
            backoff_kind, backoff_base_delay_ms
        )
        VALUES ($1, 0, 'test_module', 'test_action', $2, 'failed', 'timeout', 0, 3, 'exponential', 1000)
        "#,
    )
    .bind(instance_id.0)
    .bind(Vec::<u8>::new())
    .execute(db.pool())
    .await?;

    // Requeue failed actions
    let (requeued, permanently_failed) = db.requeue_failed_actions(100).await?;

    assert_eq!(requeued, 1, "should have requeued 1 timeout action");
    assert_eq!(permanently_failed, 0, "should have 0 permanently failed");

    // Verify it was requeued
    let row = sqlx::query("SELECT status, attempt_number FROM action_queue WHERE instance_id = $1")
        .bind(instance_id.0)
        .fetch_one(db.pool())
        .await?;
    let status: String = row.get("status");
    let attempt: i32 = row.get("attempt_number");

    assert_eq!(status, "queued");
    assert_eq!(attempt, 1);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_requeue_failed_actions_permanently_fails_exhausted_failure() -> Result<()> {
    let Some(db) = setup_db().await else {
        return Ok(());
    };

    let (_, instance_id) = create_test_instance(&db).await?;

    // Create a failed action with NO retries remaining
    sqlx::query(
        r#"
        INSERT INTO action_queue (
            instance_id, action_seq, module_name, action_name,
            dispatch_payload, status, retry_kind, attempt_number, max_retries,
            backoff_kind, backoff_base_delay_ms
        )
        VALUES ($1, 0, 'test_module', 'test_action', $2, 'failed', 'failure', 3, 3, 'exponential', 1000)
        "#,
    )
    .bind(instance_id.0)
    .bind(Vec::<u8>::new())
    .execute(db.pool())
    .await?;

    // Requeue failed actions
    let (requeued, permanently_failed) = db.requeue_failed_actions(100).await?;

    assert_eq!(requeued, 0, "should have 0 requeued");
    assert_eq!(permanently_failed, 1, "should have 1 permanently failed");

    // Verify it's marked as 'exhausted' (terminal state for failure type)
    let row = sqlx::query("SELECT status FROM action_queue WHERE instance_id = $1")
        .bind(instance_id.0)
        .fetch_one(db.pool())
        .await?;
    let status: String = row.get("status");
    assert_eq!(status, "exhausted");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_requeue_failed_actions_permanently_fails_exhausted_timeout() -> Result<()> {
    let Some(db) = setup_db().await else {
        return Ok(());
    };

    let (_, instance_id) = create_test_instance(&db).await?;

    // Create a failed action with retry_kind='timeout' and NO retries remaining
    sqlx::query(
        r#"
        INSERT INTO action_queue (
            instance_id, action_seq, module_name, action_name,
            dispatch_payload, status, retry_kind, attempt_number, timeout_retry_limit,
            backoff_kind, backoff_base_delay_ms
        )
        VALUES ($1, 0, 'test_module', 'test_action', $2, 'failed', 'timeout', 3, 3, 'exponential', 1000)
        "#,
    )
    .bind(instance_id.0)
    .bind(Vec::<u8>::new())
    .execute(db.pool())
    .await?;

    // Requeue failed actions
    let (requeued, permanently_failed) = db.requeue_failed_actions(100).await?;

    assert_eq!(requeued, 0, "should have 0 requeued");
    assert_eq!(permanently_failed, 1, "should have 1 permanently failed");

    // Verify it's marked as 'timed_out' (terminal state for timeout type)
    let row = sqlx::query("SELECT status FROM action_queue WHERE instance_id = $1")
        .bind(instance_id.0)
        .fetch_one(db.pool())
        .await?;
    let status: String = row.get("status");
    assert_eq!(status, "timed_out");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_requeue_failed_actions_applies_exponential_backoff() -> Result<()> {
    let Some(db) = setup_db().await else {
        return Ok(());
    };

    let (_, instance_id) = create_test_instance(&db).await?;

    // Create a failed action with exponential backoff
    let action_id: Uuid = sqlx::query(
        r#"
        INSERT INTO action_queue (
            instance_id, action_seq, module_name, action_name,
            dispatch_payload, status, retry_kind, attempt_number, max_retries,
            backoff_kind, backoff_base_delay_ms
        )
        VALUES ($1, 0, 'test_module', 'test_action', $2, 'failed', 'failure', 1, 3, 'exponential', 1000)
        RETURNING id
        "#,
    )
    .bind(instance_id.0)
    .bind(Vec::<u8>::new())
    .fetch_one(db.pool())
    .await?
    .get("id");

    // Requeue failed actions
    let (requeued, _) = db.requeue_failed_actions(100).await?;
    assert_eq!(requeued, 1);

    let after_scheduled = get_action_scheduled_at(&db, action_id).await?;

    // With exponential backoff: base_delay * 2^attempt_number = 1000 * 2^1 = 2000ms
    let actual_delay = after_scheduled - Utc::now();

    // Allow some tolerance (should be close to 2000ms, but check it's at least 1500ms)
    assert!(
        actual_delay > Duration::milliseconds(1500),
        "backoff delay should be at least 1500ms, got {:?}",
        actual_delay
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_requeue_failed_actions_applies_linear_backoff() -> Result<()> {
    let Some(db) = setup_db().await else {
        return Ok(());
    };

    let (_, instance_id) = create_test_instance(&db).await?;

    // Create a failed action with linear backoff
    let action_id: Uuid = sqlx::query(
        r#"
        INSERT INTO action_queue (
            instance_id, action_seq, module_name, action_name,
            dispatch_payload, status, retry_kind, attempt_number, max_retries,
            backoff_kind, backoff_base_delay_ms
        )
        VALUES ($1, 0, 'test_module', 'test_action', $2, 'failed', 'failure', 1, 3, 'linear', 1000)
        RETURNING id
        "#,
    )
    .bind(instance_id.0)
    .bind(Vec::<u8>::new())
    .fetch_one(db.pool())
    .await?
    .get("id");

    // Requeue failed actions
    let (requeued, _) = db.requeue_failed_actions(100).await?;
    assert_eq!(requeued, 1);

    let after_scheduled = get_action_scheduled_at(&db, action_id).await?;

    // With linear backoff: base_delay * (attempt_number + 1) = 1000 * (1 + 1) = 2000ms
    let actual_delay = after_scheduled - Utc::now();

    assert!(
        actual_delay > Duration::milliseconds(1500),
        "backoff delay should be at least 1500ms, got {:?}",
        actual_delay
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_requeue_failed_actions_handles_no_backoff() -> Result<()> {
    let Some(db) = setup_db().await else {
        return Ok(());
    };

    let (_, instance_id) = create_test_instance(&db).await?;

    // Create a failed action with no backoff
    let action_id: Uuid = sqlx::query(
        r#"
        INSERT INTO action_queue (
            instance_id, action_seq, module_name, action_name,
            dispatch_payload, status, retry_kind, attempt_number, max_retries,
            backoff_kind, backoff_base_delay_ms
        )
        VALUES ($1, 0, 'test_module', 'test_action', $2, 'failed', 'failure', 0, 3, 'none', 1000)
        RETURNING id
        "#,
    )
    .bind(instance_id.0)
    .bind(Vec::<u8>::new())
    .fetch_one(db.pool())
    .await?
    .get("id");

    // Requeue failed actions
    let (requeued, _) = db.requeue_failed_actions(100).await?;
    assert_eq!(requeued, 1);

    let after_scheduled = get_action_scheduled_at(&db, action_id).await?;

    // With no backoff, scheduled_at should be very close to NOW()
    let actual_delay = after_scheduled - Utc::now();

    assert!(
        actual_delay < Duration::seconds(1),
        "with no backoff, delay should be minimal, got {:?}",
        actual_delay
    );

    Ok(())
}

// =============================================================================
// Two-Phase Integration Tests
// =============================================================================

#[tokio::test]
#[serial]
async fn test_two_phase_timeout_requeues_with_retries() -> Result<()> {
    let Some(db) = setup_db().await else {
        return Ok(());
    };

    let (_, instance_id) = create_test_instance(&db).await?;

    // Create a dispatched action with deadline in the past and retries remaining
    let past_deadline = Utc::now() - Duration::seconds(60);
    let action_id = insert_test_action(
        &db,
        instance_id,
        "dispatched",
        0, // attempt_number = 0
        3, // timeout_retry_limit = 3 (so 0 < 3, retries remaining)
        Some(past_deadline),
        "exponential",
        1000,
    )
    .await?;

    // Phase 1: Mark timed out
    let marked = db.mark_timed_out_actions(100).await?;
    assert_eq!(marked, 1);

    // Verify intermediate state
    let (status, attempt, retry_kind) = get_action_state(&db, action_id).await?;
    assert_eq!(status, "failed");
    assert_eq!(retry_kind, "timeout");
    assert_eq!(attempt, 0); // Not incremented yet

    // Phase 2: Requeue failed actions
    let (requeued, permanently_failed) = db.requeue_failed_actions(100).await?;
    assert_eq!(requeued, 1);
    assert_eq!(permanently_failed, 0);

    // Verify final state
    let (status, attempt, _) = get_action_state(&db, action_id).await?;
    assert_eq!(status, "queued");
    assert_eq!(attempt, 1); // Now incremented

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_two_phase_timeout_permanently_fails_when_exhausted() -> Result<()> {
    let Some(db) = setup_db().await else {
        return Ok(());
    };

    let (_, instance_id) = create_test_instance(&db).await?;

    // Create a dispatched action with deadline in the past and NO retries remaining
    let past_deadline = Utc::now() - Duration::seconds(60);
    let action_id = insert_test_action(
        &db,
        instance_id,
        "dispatched",
        3, // attempt_number = 3
        3, // timeout_retry_limit = 3 (so 3 >= 3, no retries remaining)
        Some(past_deadline),
        "exponential",
        1000,
    )
    .await?;

    // Phase 1: Mark timed out
    let marked = db.mark_timed_out_actions(100).await?;
    assert_eq!(marked, 1);

    // Phase 2: Requeue failed actions (should permanently fail)
    let (requeued, permanently_failed) = db.requeue_failed_actions(100).await?;
    assert_eq!(requeued, 0);
    assert_eq!(permanently_failed, 1);

    // Verify final state - should be 'timed_out' (terminal)
    let (status, attempt, _) = get_action_state(&db, action_id).await?;
    assert_eq!(status, "timed_out");
    assert_eq!(attempt, 3); // Not incremented

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_full_timeout_retry_cycle() -> Result<()> {
    let Some(db) = setup_db().await else {
        return Ok(());
    };

    let (_, instance_id) = create_test_instance(&db).await?;

    // Enqueue an action
    let action = NewAction {
        instance_id,
        module_name: "test_module".to_string(),
        action_name: "test_action".to_string(),
        dispatch_payload: vec![],
        timeout_seconds: 1, // 1 second timeout
        max_retries: 3,
        backoff_kind: BackoffKind::None,
        backoff_base_delay_ms: 0,
        node_id: Some("test_node".to_string()),
        node_type: Some("action".to_string()),
    };
    db.enqueue_action(action).await?;

    // Dispatch the action (sets deadline_at)
    let actions = db.dispatch_actions(10).await?;
    assert_eq!(actions.len(), 1);
    let action_id = actions[0].id;

    // Verify it's dispatched with a deadline
    let row = sqlx::query("SELECT status, deadline_at FROM action_queue WHERE id = $1")
        .bind(action_id)
        .fetch_one(db.pool())
        .await?;
    let status: String = row.get("status");
    let deadline: Option<chrono::DateTime<Utc>> = row.get("deadline_at");
    assert_eq!(status, "dispatched");
    assert!(deadline.is_some());

    // Simulate timeout by setting deadline to past
    sqlx::query("UPDATE action_queue SET deadline_at = NOW() - INTERVAL '1 minute' WHERE id = $1")
        .bind(action_id)
        .execute(db.pool())
        .await?;

    // Two-phase: mark then requeue
    db.mark_timed_out_actions(100).await?;
    let (requeued, permanently_failed) = db.requeue_failed_actions(100).await?;
    assert_eq!(requeued, 1);
    assert_eq!(permanently_failed, 0);

    // Verify it's requeued with incremented attempt
    let (status, attempt, _) = get_action_state(&db, action_id).await?;
    assert_eq!(status, "queued");
    assert_eq!(attempt, 1);

    // Dispatch again and repeat timeout cycle
    for expected_attempt in 2..=3 {
        db.dispatch_actions(10).await?;

        sqlx::query(
            "UPDATE action_queue SET deadline_at = NOW() - INTERVAL '1 minute' WHERE id = $1",
        )
        .bind(action_id)
        .execute(db.pool())
        .await?;

        db.mark_timed_out_actions(100).await?;
        let (requeued, permanently_failed) = db.requeue_failed_actions(100).await?;
        assert_eq!(requeued, 1);
        assert_eq!(permanently_failed, 0);

        let (status, attempt, _) = get_action_state(&db, action_id).await?;
        assert_eq!(status, "queued");
        assert_eq!(attempt, expected_attempt);
    }

    // Final dispatch and timeout - should be permanently failed
    db.dispatch_actions(10).await?;
    sqlx::query("UPDATE action_queue SET deadline_at = NOW() - INTERVAL '1 minute' WHERE id = $1")
        .bind(action_id)
        .execute(db.pool())
        .await?;

    db.mark_timed_out_actions(100).await?;
    let (requeued, permanently_failed) = db.requeue_failed_actions(100).await?;
    assert_eq!(requeued, 0);
    assert_eq!(permanently_failed, 1);

    // Verify it's permanently timed_out
    let (status, attempt, _) = get_action_state(&db, action_id).await?;
    assert_eq!(status, "timed_out");
    assert_eq!(attempt, 3); // Not incremented for permanent failure

    Ok(())
}

// =============================================================================
// fail_instances_with_exhausted_actions Tests
// =============================================================================

/// Helper to get workflow instance status.
async fn get_instance_status(db: &Database, instance_id: WorkflowInstanceId) -> Result<String> {
    let row = sqlx::query("SELECT status FROM workflow_instances WHERE id = $1")
        .bind(instance_id.0)
        .fetch_one(db.pool())
        .await?;
    Ok(row.get("status"))
}

/// Helper to insert an action that has exhausted all failure retries (in 'exhausted' status).
async fn insert_exhausted_failure_action(
    db: &Database,
    instance_id: WorkflowInstanceId,
    max_retries: i32,
) -> Result<Uuid> {
    let row = sqlx::query(
        r#"
        INSERT INTO action_queue (
            instance_id, action_seq, module_name, action_name,
            dispatch_payload, status, attempt_number, max_retries,
            retry_kind, backoff_kind
        )
        VALUES ($1, 0, 'test_module', 'test_action', $2, 'exhausted', $3, $3, 'failure', 'none')
        RETURNING id
        "#,
    )
    .bind(instance_id.0)
    .bind(Vec::<u8>::new())
    .bind(max_retries)
    .fetch_one(db.pool())
    .await?;

    Ok(row.get("id"))
}

/// Helper to insert a timed_out action with exhausted retries.
async fn insert_exhausted_timeout_action(
    db: &Database,
    instance_id: WorkflowInstanceId,
    timeout_retry_limit: i32,
) -> Result<Uuid> {
    let row = sqlx::query(
        r#"
        INSERT INTO action_queue (
            instance_id, action_seq, module_name, action_name,
            dispatch_payload, status, attempt_number, timeout_retry_limit,
            retry_kind, backoff_kind
        )
        VALUES ($1, 0, 'test_module', 'test_action', $2, 'timed_out', $3, $3, 'timeout', 'none')
        RETURNING id
        "#,
    )
    .bind(instance_id.0)
    .bind(Vec::<u8>::new())
    .bind(timeout_retry_limit)
    .fetch_one(db.pool())
    .await?;

    Ok(row.get("id"))
}

#[tokio::test]
#[serial]
async fn test_fail_instances_with_exhausted_failure_retries() -> Result<()> {
    let Some(db) = setup_db().await else {
        return Ok(());
    };

    let (_, instance_id) = create_test_instance(&db).await?;

    // Verify instance starts as running
    let status = get_instance_status(&db, instance_id).await?;
    assert_eq!(status, "running");

    // Insert an action that has exhausted failure retries (attempt_number >= max_retries)
    insert_exhausted_failure_action(&db, instance_id, 3).await?;

    // Call fail_instances_with_exhausted_actions
    let failed_count = db.fail_instances_with_exhausted_actions(100).await?;
    assert_eq!(failed_count, 1, "should have failed 1 instance");

    // Verify instance is now failed
    let status = get_instance_status(&db, instance_id).await?;
    assert_eq!(status, "failed", "instance should be marked as failed");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_fail_instances_with_exhausted_timeout_retries() -> Result<()> {
    let Some(db) = setup_db().await else {
        return Ok(());
    };

    let (_, instance_id) = create_test_instance(&db).await?;

    // Verify instance starts as running
    let status = get_instance_status(&db, instance_id).await?;
    assert_eq!(status, "running");

    // Insert an action that has exhausted timeout retries
    insert_exhausted_timeout_action(&db, instance_id, 3).await?;

    // Call fail_instances_with_exhausted_actions
    let failed_count = db.fail_instances_with_exhausted_actions(100).await?;
    assert_eq!(failed_count, 1, "should have failed 1 instance");

    // Verify instance is now failed
    let status = get_instance_status(&db, instance_id).await?;
    assert_eq!(status, "failed", "instance should be marked as failed");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_fail_instances_ignores_actions_with_retries_remaining() -> Result<()> {
    let Some(db) = setup_db().await else {
        return Ok(());
    };

    let (_, instance_id) = create_test_instance(&db).await?;

    // Insert a failed action that still has retries remaining (attempt_number < max_retries)
    sqlx::query(
        r#"
        INSERT INTO action_queue (
            instance_id, action_seq, module_name, action_name,
            dispatch_payload, status, attempt_number, max_retries,
            retry_kind, backoff_kind
        )
        VALUES ($1, 0, 'test_module', 'test_action', $2, 'failed', 1, 3, 'failure', 'none')
        "#,
    )
    .bind(instance_id.0)
    .bind(Vec::<u8>::new())
    .execute(db.pool())
    .await?;

    // Call fail_instances_with_exhausted_actions
    let failed_count = db.fail_instances_with_exhausted_actions(100).await?;
    assert_eq!(failed_count, 0, "should have failed 0 instances");

    // Verify instance is still running
    let status = get_instance_status(&db, instance_id).await?;
    assert_eq!(status, "running", "instance should still be running");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_fail_instances_ignores_already_failed_instances() -> Result<()> {
    let Some(db) = setup_db().await else {
        return Ok(());
    };

    let (_, instance_id) = create_test_instance(&db).await?;

    // Manually mark instance as failed first
    sqlx::query("UPDATE workflow_instances SET status = 'failed' WHERE id = $1")
        .bind(instance_id.0)
        .execute(db.pool())
        .await?;

    // Insert an exhausted action
    insert_exhausted_failure_action(&db, instance_id, 3).await?;

    // Call fail_instances_with_exhausted_actions
    let failed_count = db.fail_instances_with_exhausted_actions(100).await?;
    assert_eq!(
        failed_count, 0,
        "should not re-fail already failed instance"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_fail_instances_respects_limit() -> Result<()> {
    let Some(db) = setup_db().await else {
        return Ok(());
    };

    // Create 3 instances with exhausted actions
    let mut instance_ids = Vec::new();
    for _ in 0..3 {
        let (_, instance_id) = create_test_instance(&db).await?;
        insert_exhausted_failure_action(&db, instance_id, 3).await?;
        instance_ids.push(instance_id);
    }

    // Call with limit of 2
    let failed_count = db.fail_instances_with_exhausted_actions(2).await?;
    assert_eq!(failed_count, 2, "should have failed exactly 2 instances");

    // Call again to get the remaining one
    let failed_count = db.fail_instances_with_exhausted_actions(2).await?;
    assert_eq!(
        failed_count, 1,
        "should have failed the remaining 1 instance"
    );

    // All instances should now be failed
    for instance_id in instance_ids {
        let status = get_instance_status(&db, instance_id).await?;
        assert_eq!(status, "failed");
    }

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_fail_instances_sets_completed_at() -> Result<()> {
    let Some(db) = setup_db().await else {
        return Ok(());
    };

    let (_, instance_id) = create_test_instance(&db).await?;

    // Verify completed_at is NULL initially
    let row = sqlx::query("SELECT completed_at FROM workflow_instances WHERE id = $1")
        .bind(instance_id.0)
        .fetch_one(db.pool())
        .await?;
    let completed_at: Option<chrono::DateTime<Utc>> = row.get("completed_at");
    assert!(
        completed_at.is_none(),
        "completed_at should be NULL initially"
    );

    // Insert exhausted action and fail instance
    insert_exhausted_failure_action(&db, instance_id, 3).await?;
    db.fail_instances_with_exhausted_actions(100).await?;

    // Verify completed_at is now set
    let row = sqlx::query("SELECT completed_at FROM workflow_instances WHERE id = $1")
        .bind(instance_id.0)
        .fetch_one(db.pool())
        .await?;
    let completed_at: Option<chrono::DateTime<Utc>> = row.get("completed_at");
    assert!(
        completed_at.is_some(),
        "completed_at should be set after failure"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_full_retry_exhaustion_fails_workflow() -> Result<()> {
    // Integration test: simulate the full flow of action failure -> retry exhaustion -> workflow failure
    let Some(db) = setup_db().await else {
        return Ok(());
    };

    let (_, instance_id) = create_test_instance(&db).await?;

    // Create an action with max_retries=2
    let action = NewAction {
        instance_id,
        module_name: "test_module".to_string(),
        action_name: "test_action".to_string(),
        dispatch_payload: vec![],
        timeout_seconds: 30,
        max_retries: 2,
        backoff_kind: BackoffKind::None,
        backoff_base_delay_ms: 0,
        node_id: Some("action_0".to_string()),
        node_type: Some("action".to_string()),
    };
    db.enqueue_action(action).await?;

    // Simulate 3 failures (initial + 2 retries)
    for i in 0..3 {
        // Dispatch the action
        let actions = db.dispatch_actions(10).await?;
        assert_eq!(
            actions.len(),
            1,
            "iteration {}: should dispatch 1 action",
            i
        );

        // Mark it as failed
        sqlx::query(
            "UPDATE action_queue SET status = 'failed', retry_kind = 'failure' WHERE instance_id = $1 AND status = 'dispatched'",
        )
        .bind(instance_id.0)
        .execute(db.pool())
        .await?;

        // Run requeue logic
        let (requeued, permanently_failed) = db.requeue_failed_actions(100).await?;

        if i < 2 {
            // Should requeue
            assert_eq!(requeued, 1, "iteration {}: should requeue", i);
            assert_eq!(
                permanently_failed, 0,
                "iteration {}: should not permanently fail yet",
                i
            );

            // Instance should still be running
            let status = get_instance_status(&db, instance_id).await?;
            assert_eq!(
                status, "running",
                "iteration {}: instance should still be running",
                i
            );
        } else {
            // Should permanently fail
            assert_eq!(requeued, 0, "final iteration: should not requeue");
            assert_eq!(
                permanently_failed, 1,
                "final iteration: should permanently fail action"
            );
        }
    }

    // Verify action is now in 'exhausted' state
    let row = sqlx::query("SELECT status FROM action_queue WHERE instance_id = $1")
        .bind(instance_id.0)
        .fetch_one(db.pool())
        .await?;
    let action_status: String = row.get("status");
    assert_eq!(
        action_status, "exhausted",
        "action should be in exhausted state"
    );

    // Now fail the workflow instance
    let failed_count = db.fail_instances_with_exhausted_actions(100).await?;
    assert_eq!(failed_count, 1, "should fail the workflow instance");

    // Verify workflow is failed
    let status = get_instance_status(&db, instance_id).await?;
    assert_eq!(status, "failed", "workflow instance should be failed");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_exhausted_actions_not_reselected() -> Result<()> {
    // Regression test: verify that exhausted actions are not repeatedly
    // selected by requeue_failed_actions (fixes spammy log issue)
    let Some(db) = setup_db().await else {
        return Ok(());
    };

    let (_, instance_id) = create_test_instance(&db).await?;

    // Create an action that has exhausted failure retries
    sqlx::query(
        r#"
        INSERT INTO action_queue (
            instance_id, action_seq, module_name, action_name,
            dispatch_payload, status, retry_kind, attempt_number, max_retries,
            backoff_kind, backoff_base_delay_ms
        )
        VALUES ($1, 0, 'test_module', 'test_action', $2, 'failed', 'failure', 3, 3, 'exponential', 1000)
        "#,
    )
    .bind(instance_id.0)
    .bind(Vec::<u8>::new())
    .execute(db.pool())
    .await?;

    // First call should mark it as exhausted
    let (requeued, permanently_failed) = db.requeue_failed_actions(100).await?;
    assert_eq!(requeued, 0);
    assert_eq!(permanently_failed, 1);

    // Verify it's now in 'exhausted' status
    let row = sqlx::query("SELECT status FROM action_queue WHERE instance_id = $1")
        .bind(instance_id.0)
        .fetch_one(db.pool())
        .await?;
    let status: String = row.get("status");
    assert_eq!(status, "exhausted");

    // Second call should NOT pick it up again (this was the bug - it kept selecting the same records)
    let (requeued, permanently_failed) = db.requeue_failed_actions(100).await?;
    assert_eq!(requeued, 0, "should not requeue exhausted action");
    assert_eq!(
        permanently_failed, 0,
        "should not re-process already exhausted action"
    );

    // Third call - same thing
    let (requeued, permanently_failed) = db.requeue_failed_actions(100).await?;
    assert_eq!(requeued, 0);
    assert_eq!(permanently_failed, 0);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_timed_out_actions_not_reselected() -> Result<()> {
    // Similar regression test for timed_out status
    let Some(db) = setup_db().await else {
        return Ok(());
    };

    let (_, instance_id) = create_test_instance(&db).await?;

    // Create an action that has exhausted timeout retries
    sqlx::query(
        r#"
        INSERT INTO action_queue (
            instance_id, action_seq, module_name, action_name,
            dispatch_payload, status, retry_kind, attempt_number, timeout_retry_limit,
            backoff_kind, backoff_base_delay_ms
        )
        VALUES ($1, 0, 'test_module', 'test_action', $2, 'failed', 'timeout', 3, 3, 'exponential', 1000)
        "#,
    )
    .bind(instance_id.0)
    .bind(Vec::<u8>::new())
    .execute(db.pool())
    .await?;

    // First call should mark it as timed_out
    let (requeued, permanently_failed) = db.requeue_failed_actions(100).await?;
    assert_eq!(requeued, 0);
    assert_eq!(permanently_failed, 1);

    // Verify it's now in 'timed_out' status
    let row = sqlx::query("SELECT status FROM action_queue WHERE instance_id = $1")
        .bind(instance_id.0)
        .fetch_one(db.pool())
        .await?;
    let status: String = row.get("status");
    assert_eq!(status, "timed_out");

    // Second call should NOT pick it up again
    let (requeued, permanently_failed) = db.requeue_failed_actions(100).await?;
    assert_eq!(requeued, 0);
    assert_eq!(permanently_failed, 0);

    Ok(())
}
