//! Tests for garbage collection of old completed/failed workflow instances.

use std::env;

use anyhow::Result;
use serial_test::serial;
use sqlx::Row;
use uuid::Uuid;

use rappel::{Database, WorkflowInstanceId, WorkflowVersionId};

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
        "TRUNCATE action_logs, action_queue, instance_context, loop_state, node_inputs, node_readiness, workflow_instances, workflow_versions CASCADE",
    )
    .execute(db.pool())
    .await?;
    Ok(())
}

/// Helper to create a workflow version for testing.
async fn create_test_workflow(db: &Database) -> Result<WorkflowVersionId> {
    let version_id = db
        .upsert_workflow_version("test_gc_workflow", "hash123", b"proto", false)
        .await?;
    Ok(version_id)
}

/// Helper to create an instance and mark it as completed/failed with a specific completed_at time.
async fn create_instance_with_status(
    db: &Database,
    version_id: WorkflowVersionId,
    status: &str,
    hours_ago: i64,
) -> Result<WorkflowInstanceId> {
    let instance_id = db
        .create_instance("test_gc_workflow", version_id, None, None)
        .await?;

    // Update the status and set completed_at to a specific time in the past
    sqlx::query(
        r#"
        UPDATE workflow_instances
        SET status = $2, completed_at = NOW() - ($3 || ' hours')::interval
        WHERE id = $1
        "#,
    )
    .bind(instance_id.0)
    .bind(status)
    .bind(hours_ago)
    .execute(db.pool())
    .await?;

    Ok(instance_id)
}

/// Helper to add an action to an instance.
async fn add_action_to_instance(db: &Database, instance_id: WorkflowInstanceId) -> Result<Uuid> {
    let row = sqlx::query(
        r#"
        INSERT INTO action_queue (
            instance_id, action_seq, module_name, action_name,
            dispatch_payload, status
        )
        VALUES ($1, 0, 'test_module', 'test_action', $2, 'completed')
        RETURNING id
        "#,
    )
    .bind(instance_id.0)
    .bind(Vec::<u8>::new())
    .fetch_one(db.pool())
    .await?;

    Ok(row.get("id"))
}

/// Helper to add an action log entry.
async fn add_action_log(
    db: &Database,
    action_id: Uuid,
    instance_id: WorkflowInstanceId,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO action_logs (action_id, instance_id, attempt_number, dispatched_at)
        VALUES ($1, $2, 0, NOW())
        "#,
    )
    .bind(action_id)
    .bind(instance_id.0)
    .execute(db.pool())
    .await?;
    Ok(())
}

/// Helper to count instances in the database.
async fn count_instances(db: &Database) -> Result<i64> {
    let row = sqlx::query("SELECT COUNT(*) as count FROM workflow_instances")
        .fetch_one(db.pool())
        .await?;
    Ok(row.get("count"))
}

/// Helper to count actions in the database.
async fn count_actions(db: &Database) -> Result<i64> {
    let row = sqlx::query("SELECT COUNT(*) as count FROM action_queue")
        .fetch_one(db.pool())
        .await?;
    Ok(row.get("count"))
}

/// Helper to count action logs in the database.
async fn count_action_logs(db: &Database) -> Result<i64> {
    let row = sqlx::query("SELECT COUNT(*) as count FROM action_logs")
        .fetch_one(db.pool())
        .await?;
    Ok(row.get("count"))
}

/// Helper to check if a specific instance exists.
async fn instance_exists(db: &Database, instance_id: WorkflowInstanceId) -> Result<bool> {
    let row = sqlx::query("SELECT COUNT(*) as count FROM workflow_instances WHERE id = $1")
        .bind(instance_id.0)
        .fetch_one(db.pool())
        .await?;
    let count: i64 = row.get("count");
    Ok(count > 0)
}

// =============================================================================
// Tests
// =============================================================================

#[tokio::test]
#[serial]
async fn test_gc_deletes_old_completed_instances() -> Result<()> {
    let Some(db) = setup_db().await else {
        return Ok(());
    };

    let version_id = create_test_workflow(&db).await?;

    // Create an old completed instance (48 hours ago)
    let old_instance = create_instance_with_status(&db, version_id, "completed", 48).await?;
    let action_id = add_action_to_instance(&db, old_instance).await?;
    add_action_log(&db, action_id, old_instance).await?;

    // Create a recent completed instance (1 hour ago)
    let recent_instance = create_instance_with_status(&db, version_id, "completed", 1).await?;
    add_action_to_instance(&db, recent_instance).await?;

    // Verify both instances exist
    assert_eq!(count_instances(&db).await?, 2);
    assert_eq!(count_actions(&db).await?, 2);

    // Run GC with 24-hour retention (86400 seconds)
    let deleted = db.garbage_collect_instances(86400, 100).await?;

    // Should delete 1 instance (the old one)
    assert_eq!(deleted, 1);
    assert_eq!(count_instances(&db).await?, 1);
    assert_eq!(count_actions(&db).await?, 1);

    // The old instance should be gone, recent should remain
    assert!(!instance_exists(&db, old_instance).await?);
    assert!(instance_exists(&db, recent_instance).await?);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_gc_deletes_old_failed_instances() -> Result<()> {
    let Some(db) = setup_db().await else {
        return Ok(());
    };

    let version_id = create_test_workflow(&db).await?;

    // Create an old failed instance (72 hours ago)
    let old_instance = create_instance_with_status(&db, version_id, "failed", 72).await?;
    add_action_to_instance(&db, old_instance).await?;

    // Create a recent failed instance (2 hours ago)
    let recent_instance = create_instance_with_status(&db, version_id, "failed", 2).await?;
    add_action_to_instance(&db, recent_instance).await?;

    // Verify both instances exist
    assert_eq!(count_instances(&db).await?, 2);

    // Run GC with 24-hour retention
    let deleted = db.garbage_collect_instances(86400, 100).await?;

    // Should delete 1 instance (the old one)
    assert_eq!(deleted, 1);
    assert_eq!(count_instances(&db).await?, 1);

    // The old instance should be gone, recent should remain
    assert!(!instance_exists(&db, old_instance).await?);
    assert!(instance_exists(&db, recent_instance).await?);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_gc_does_not_delete_running_instances() -> Result<()> {
    let Some(db) = setup_db().await else {
        return Ok(());
    };

    let version_id = create_test_workflow(&db).await?;

    // Create a running instance (should never be deleted regardless of age)
    let running_instance = db
        .create_instance("test_gc_workflow", version_id, None, None)
        .await?;
    add_action_to_instance(&db, running_instance).await?;

    // Create an old completed instance
    let old_completed = create_instance_with_status(&db, version_id, "completed", 48).await?;
    add_action_to_instance(&db, old_completed).await?;

    assert_eq!(count_instances(&db).await?, 2);

    // Run GC with 24-hour retention
    let deleted = db.garbage_collect_instances(86400, 100).await?;

    // Should only delete the old completed instance, not the running one
    assert_eq!(deleted, 1);
    assert_eq!(count_instances(&db).await?, 1);

    // Running instance should still exist
    assert!(instance_exists(&db, running_instance).await?);
    assert!(!instance_exists(&db, old_completed).await?);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_gc_respects_batch_limit() -> Result<()> {
    let Some(db) = setup_db().await else {
        return Ok(());
    };

    let version_id = create_test_workflow(&db).await?;

    // Create 5 old completed instances
    for _ in 0..5 {
        let instance = create_instance_with_status(&db, version_id, "completed", 48).await?;
        add_action_to_instance(&db, instance).await?;
    }

    assert_eq!(count_instances(&db).await?, 5);

    // Run GC with limit of 2
    let deleted = db.garbage_collect_instances(86400, 2).await?;

    // Should only delete 2 instances
    assert_eq!(deleted, 2);
    assert_eq!(count_instances(&db).await?, 3);

    // Run again to delete more
    let deleted = db.garbage_collect_instances(86400, 2).await?;
    assert_eq!(deleted, 2);
    assert_eq!(count_instances(&db).await?, 1);

    // Run one more time
    let deleted = db.garbage_collect_instances(86400, 2).await?;
    assert_eq!(deleted, 1);
    assert_eq!(count_instances(&db).await?, 0);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_gc_deletes_associated_action_logs() -> Result<()> {
    let Some(db) = setup_db().await else {
        return Ok(());
    };

    let version_id = create_test_workflow(&db).await?;

    // Create an old completed instance with actions and logs
    let old_instance = create_instance_with_status(&db, version_id, "completed", 48).await?;
    let action_id = add_action_to_instance(&db, old_instance).await?;
    add_action_log(&db, action_id, old_instance).await?;

    // Create a recent instance with actions and logs
    let recent_instance = create_instance_with_status(&db, version_id, "completed", 1).await?;
    let recent_action_id = add_action_to_instance(&db, recent_instance).await?;
    add_action_log(&db, recent_action_id, recent_instance).await?;

    // Verify both exist
    assert_eq!(count_instances(&db).await?, 2);
    assert_eq!(count_actions(&db).await?, 2);
    assert_eq!(count_action_logs(&db).await?, 2);

    // Run GC
    let deleted = db.garbage_collect_instances(86400, 100).await?;

    assert_eq!(deleted, 1);
    assert_eq!(count_instances(&db).await?, 1);
    assert_eq!(count_actions(&db).await?, 1);
    assert_eq!(count_action_logs(&db).await?, 1);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_gc_with_no_eligible_instances() -> Result<()> {
    let Some(db) = setup_db().await else {
        return Ok(());
    };

    let version_id = create_test_workflow(&db).await?;

    // Create only recent instances
    let _recent1 = create_instance_with_status(&db, version_id, "completed", 1).await?;
    let _recent2 = create_instance_with_status(&db, version_id, "failed", 2).await?;

    assert_eq!(count_instances(&db).await?, 2);

    // Run GC with 24-hour retention
    let deleted = db.garbage_collect_instances(86400, 100).await?;

    // Should delete nothing
    assert_eq!(deleted, 0);
    assert_eq!(count_instances(&db).await?, 2);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_gc_with_short_retention() -> Result<()> {
    let Some(db) = setup_db().await else {
        return Ok(());
    };

    let version_id = create_test_workflow(&db).await?;

    // Create instances at various ages
    let _instance_2h = create_instance_with_status(&db, version_id, "completed", 2).await?;
    let _instance_4h = create_instance_with_status(&db, version_id, "completed", 4).await?;
    let _instance_6h = create_instance_with_status(&db, version_id, "completed", 6).await?;

    assert_eq!(count_instances(&db).await?, 3);

    // Run GC with 3-hour retention (10800 seconds)
    let deleted = db.garbage_collect_instances(10800, 100).await?;

    // Should delete instances older than 3 hours (4h and 6h)
    assert_eq!(deleted, 2);
    assert_eq!(count_instances(&db).await?, 1);

    Ok(())
}
