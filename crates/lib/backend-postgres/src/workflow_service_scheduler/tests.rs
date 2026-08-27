use chrono::TimeZone as _;
use serial_test::serial;

use waymark_ids::WorkflowVersionId;
use waymark_scheduler_core::ScheduleStatus;
use waymark_workflow_service_scheduler_backend::{
    DeleteSchedule, GetSchedule, ListSchedules, UpdateScheduleStatus, UpsertSchedule,
    upsert_schedule,
};

use super::super::test_helpers::{setup_backend, upsert_test_executable};

const TEST_DEFINITION: &[u8] = b"test-definition";
const TEST_INITIAL_SNAPSHOT: &[u8] = b"test-initial-snapshot";

fn at(hour: u32) -> chrono::DateTime<chrono::Utc> {
    chrono::Utc
        .with_ymd_and_hms(2026, 1, 1, hour, 0, 0)
        .unwrap()
}

async fn upsert(
    backend: &crate::PostgresBackend,
    schedule_name: &str,
    executable_id: &WorkflowVersionId,
    next_run_at: chrono::DateTime<chrono::Utc>,
) {
    backend
        .upsert_schedule(upsert_schedule::Params {
            schedule_name,
            executable_id,
            definition: TEST_DEFINITION,
            initial_snapshot: TEST_INITIAL_SNAPSHOT,
            next_run_at: &next_run_at,
        })
        .await
        .expect("upsert schedule");
}

#[serial(postgres)]
#[tokio::test]
async fn upsert_and_get_round_trip() {
    let backend = setup_backend().await;
    let executable_id = upsert_test_executable(&backend, "data_sync").await;

    upsert(&backend, "data_sync/hourly", &executable_id, at(13)).await;

    let record = backend
        .get_schedule("data_sync/hourly")
        .await
        .expect("get schedule")
        .expect("schedule exists");
    assert_eq!(record.schedule_name, "data_sync/hourly");
    assert_eq!(record.workflow_name, "data_sync");
    assert_eq!(record.definition, TEST_DEFINITION);
    assert_eq!(record.status, ScheduleStatus::Active);
    assert_eq!(record.next_run_at, at(13));
    assert_eq!(record.last_instance_id, None);
}

#[serial(postgres)]
#[tokio::test]
async fn get_missing_schedule_is_none() {
    let backend = setup_backend().await;
    assert!(
        backend
            .get_schedule("missing")
            .await
            .expect("get schedule")
            .is_none()
    );
}

#[serial(postgres)]
#[tokio::test]
async fn repointing_replaces_settings_and_reactivates() {
    let backend = setup_backend().await;
    let first_executable_id = upsert_test_executable(&backend, "first_workflow").await;
    let second_executable_id = upsert_test_executable(&backend, "second_workflow").await;

    upsert(&backend, "shared", &first_executable_id, at(13)).await;
    assert!(
        backend
            .update_schedule_status("shared", ScheduleStatus::Paused)
            .await
            .expect("pause schedule")
    );

    upsert(&backend, "shared", &second_executable_id, at(15)).await;

    let record = backend
        .get_schedule("shared")
        .await
        .expect("get schedule")
        .expect("schedule exists");
    assert_eq!(record.workflow_name, "second_workflow");
    assert_eq!(record.status, ScheduleStatus::Active);
    assert_eq!(record.next_run_at, at(15));
}

#[serial(postgres)]
#[tokio::test]
async fn update_schedule_status_round_trips_and_reports_missing() {
    let backend = setup_backend().await;
    let executable_id = upsert_test_executable(&backend, "data_sync").await;
    upsert(&backend, "data_sync/hourly", &executable_id, at(13)).await;

    assert!(
        backend
            .update_schedule_status("data_sync/hourly", ScheduleStatus::Paused)
            .await
            .expect("pause schedule")
    );
    let record = backend
        .get_schedule("data_sync/hourly")
        .await
        .expect("get schedule")
        .expect("schedule exists");
    assert_eq!(record.status, ScheduleStatus::Paused);

    assert!(
        !backend
            .update_schedule_status("missing", ScheduleStatus::Paused)
            .await
            .expect("update missing schedule")
    );
}

#[serial(postgres)]
#[tokio::test]
async fn delete_schedule_removes_the_row_and_reports_missing() {
    let backend = setup_backend().await;
    let executable_id = upsert_test_executable(&backend, "data_sync").await;
    upsert(&backend, "data_sync/hourly", &executable_id, at(13)).await;

    assert!(
        backend
            .delete_schedule("data_sync/hourly")
            .await
            .expect("delete schedule")
    );
    assert!(
        backend
            .get_schedule("data_sync/hourly")
            .await
            .expect("get schedule")
            .is_none()
    );
    assert!(
        !backend
            .delete_schedule("data_sync/hourly")
            .await
            .expect("delete missing schedule")
    );
}

#[serial(postgres)]
#[tokio::test]
async fn list_schedules_filters_by_status() {
    let backend = setup_backend().await;
    let executable_id = upsert_test_executable(&backend, "data_sync").await;
    upsert(&backend, "active-schedule", &executable_id, at(13)).await;
    upsert(&backend, "paused-schedule", &executable_id, at(13)).await;
    assert!(
        backend
            .update_schedule_status("paused-schedule", ScheduleStatus::Paused)
            .await
            .expect("pause schedule")
    );

    let all = backend.list_schedules(None).await.expect("list all");
    let names: Vec<_> = all
        .iter()
        .map(|record| record.schedule_name.as_str())
        .collect();
    assert_eq!(names, ["active-schedule", "paused-schedule"]);

    let active = backend
        .list_schedules(Some(ScheduleStatus::Active))
        .await
        .expect("list active");
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].schedule_name, "active-schedule");

    let paused = backend
        .list_schedules(Some(ScheduleStatus::Paused))
        .await
        .expect("list paused");
    assert_eq!(paused.len(), 1);
    assert_eq!(paused[0].schedule_name, "paused-schedule");
}
