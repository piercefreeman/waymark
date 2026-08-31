use std::borrow::Cow;
use std::num::NonZeroUsize;

use chrono::TimeZone as _;
use nonempty_collections::nev;
use serial_test::serial;
use sqlx::Row as _;

use waymark_ids::{InstanceId, WorkflowVersionId};
use waymark_scheduler_backend::register_scheduled_vm_runtimes::{Item, Outcome};
use waymark_scheduler_backend::{PollDueSchedules, RegisterScheduledVmRuntimes};
use waymark_workflow_service_scheduler_backend::{UpsertSchedule, upsert_schedule};

use super::super::test_helpers::{deadlock, setup_backend, upsert_test_executable};

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

async fn register_one(
    backend: &crate::PostgresBackend,
    schedule_name: &str,
    expected_next_run_at: chrono::DateTime<chrono::Utc>,
    vm_id: InstanceId,
    new_next_run_at: chrono::DateTime<chrono::Utc>,
    check_overlap: bool,
) -> Outcome {
    let item = Item {
        schedule_name: Cow::Borrowed(schedule_name),
        expected_next_run_at: Cow::Owned(expected_next_run_at),
        vm_id: Cow::Owned(vm_id),
        new_next_run_at: Cow::Owned(new_next_run_at),
        check_overlap,
    };
    let outcomes = backend
        .register_scheduled_vm_runtimes(nev![item].as_nonempty_slice())
        .await
        .expect("register scheduled vm runtimes");
    assert_eq!(outcomes.len().get(), 1);
    *outcomes.first()
}

async fn schedule_row(
    backend: &crate::PostgresBackend,
    schedule_name: &str,
) -> (chrono::DateTime<chrono::Utc>, Option<InstanceId>) {
    let row =
        sqlx::query("SELECT next_run_at, last_instance_id FROM schedules WHERE schedule_name = $1")
            .bind(schedule_name)
            .fetch_one(backend.pool())
            .await
            .expect("fetch schedule row");
    (row.get("next_run_at"), row.get("last_instance_id"))
}

async fn vm_runtime_registered(backend: &crate::PostgresBackend, vm_id: InstanceId) -> bool {
    let snapshot: Option<(Vec<u8>,)> =
        sqlx::query_as("SELECT snapshot FROM vm_runtime_snapshots WHERE vm_id = $1")
            .bind(vm_id)
            .fetch_optional(backend.pool())
            .await
            .expect("fetch snapshot row");
    let workload = sqlx::query("SELECT 1 FROM runnable_workloads WHERE workload_id = $1")
        .bind(vm_id)
        .fetch_optional(backend.pool())
        .await
        .expect("fetch workload row");
    match (snapshot, workload) {
        (Some((snapshot,)), Some(_)) => {
            assert_eq!(snapshot, TEST_INITIAL_SNAPSHOT);
            true
        }
        (None, None) => false,
        (snapshot, workload) => panic!(
            "half-registered VM runtime: snapshot={:?} workload={:?}",
            snapshot.is_some(),
            workload.is_some()
        ),
    }
}

async fn record_execution_result(backend: &crate::PostgresBackend, vm_id: InstanceId) {
    sqlx::query("INSERT INTO vm_execution_results (vm_id, result) VALUES ($1, $2)")
        .bind(vm_id)
        .bind(&b"test-result"[..])
        .execute(backend.pool())
        .await
        .expect("insert execution result");
}

#[serial(postgres)]
#[tokio::test]
async fn poll_returns_only_due_active_schedules() {
    let backend = setup_backend().await;
    let executable_id = upsert_test_executable(&backend, "data_sync").await;
    upsert(&backend, "due", &executable_id, at(12)).await;
    upsert(&backend, "future", &executable_id, at(18)).await;
    upsert(&backend, "paused-due", &executable_id, at(12)).await;
    assert!(
        waymark_workflow_service_scheduler_backend::UpdateScheduleStatus::update_schedule_status(
            &backend,
            "paused-due",
            waymark_scheduler_core::ScheduleStatus::Paused,
        )
        .await
        .expect("pause schedule")
    );

    let due = backend
        .poll_due_schedules(at(13), NonZeroUsize::new(10).unwrap())
        .await
        .expect("poll due schedules")
        .expect("one due schedule");
    assert_eq!(due.len().get(), 1);
    let row = due.first();
    assert_eq!(row.schedule_name, "due");
    assert_eq!(row.definition, TEST_DEFINITION);
    assert_eq!(row.next_run_at, at(12));
    assert_eq!(row.last_instance_id, None);

    let none_due = backend
        .poll_due_schedules(at(11), NonZeroUsize::new(10).unwrap())
        .await
        .expect("poll before dueness");
    assert!(none_due.is_none());
}

#[serial(postgres)]
#[tokio::test]
async fn registering_spawns_the_vm_and_advances_the_cursor() {
    let backend = setup_backend().await;
    let executable_id = upsert_test_executable(&backend, "data_sync").await;
    upsert(&backend, "due", &executable_id, at(12)).await;
    let vm_id = InstanceId::new_uuid_v4();

    let outcome = register_one(&backend, "due", at(12), vm_id, at(13), true).await;

    assert_eq!(outcome, Outcome::Registered);
    assert_eq!(schedule_row(&backend, "due").await, (at(13), Some(vm_id)));
    assert!(vm_runtime_registered(&backend, vm_id).await);
}

#[serial(postgres)]
#[tokio::test]
async fn a_stale_fence_is_superseded_and_writes_nothing() {
    let backend = setup_backend().await;
    let executable_id = upsert_test_executable(&backend, "data_sync").await;
    upsert(&backend, "due", &executable_id, at(12)).await;
    let vm_id = InstanceId::new_uuid_v4();

    let outcome = register_one(&backend, "due", at(11), vm_id, at(13), true).await;

    assert_eq!(outcome, Outcome::Superseded);
    assert_eq!(schedule_row(&backend, "due").await, (at(12), None));
    assert!(!vm_runtime_registered(&backend, vm_id).await);
}

#[serial(postgres)]
#[tokio::test]
async fn the_overlap_gate_skips_while_the_previous_instance_runs() {
    let backend = setup_backend().await;
    let executable_id = upsert_test_executable(&backend, "data_sync").await;
    upsert(&backend, "due", &executable_id, at(12)).await;
    let first_vm_id = InstanceId::new_uuid_v4();
    let outcome = register_one(&backend, "due", at(12), first_vm_id, at(13), true).await;
    assert_eq!(outcome, Outcome::Registered);

    // Re-registering the schedule makes it due again while preserving
    // the last-spawned-instance marker; the first instance still has its
    // snapshot and no execution result, so it counts as running.
    upsert(&backend, "due", &executable_id, at(13)).await;
    let second_vm_id = InstanceId::new_uuid_v4();
    let outcome = register_one(&backend, "due", at(13), second_vm_id, at(14), true).await;

    assert_eq!(outcome, Outcome::SkippedOverlap);
    assert_eq!(
        schedule_row(&backend, "due").await,
        (at(14), Some(first_vm_id))
    );
    assert!(!vm_runtime_registered(&backend, second_vm_id).await);
}

#[serial(postgres)]
#[tokio::test]
async fn the_overlap_gate_releases_once_the_previous_instance_completed() {
    let backend = setup_backend().await;
    let executable_id = upsert_test_executable(&backend, "data_sync").await;
    upsert(&backend, "due", &executable_id, at(12)).await;
    let first_vm_id = InstanceId::new_uuid_v4();
    let outcome = register_one(&backend, "due", at(12), first_vm_id, at(13), true).await;
    assert_eq!(outcome, Outcome::Registered);
    record_execution_result(&backend, first_vm_id).await;

    upsert(&backend, "due", &executable_id, at(13)).await;
    let second_vm_id = InstanceId::new_uuid_v4();
    let outcome = register_one(&backend, "due", at(13), second_vm_id, at(14), true).await;

    assert_eq!(outcome, Outcome::Registered);
    assert_eq!(
        schedule_row(&backend, "due").await,
        (at(14), Some(second_vm_id))
    );
    assert!(vm_runtime_registered(&backend, second_vm_id).await);
}

#[serial(postgres)]
#[tokio::test]
async fn allowed_duplicates_spawn_over_a_running_instance() {
    let backend = setup_backend().await;
    let executable_id = upsert_test_executable(&backend, "data_sync").await;
    upsert(&backend, "due", &executable_id, at(12)).await;
    let first_vm_id = InstanceId::new_uuid_v4();
    let outcome = register_one(&backend, "due", at(12), first_vm_id, at(13), false).await;
    assert_eq!(outcome, Outcome::Registered);

    upsert(&backend, "due", &executable_id, at(13)).await;
    let second_vm_id = InstanceId::new_uuid_v4();
    let outcome = register_one(&backend, "due", at(13), second_vm_id, at(14), false).await;

    assert_eq!(outcome, Outcome::Registered);
    assert_eq!(
        schedule_row(&backend, "due").await,
        (at(14), Some(second_vm_id))
    );
    assert!(vm_runtime_registered(&backend, second_vm_id).await);
}

#[serial(postgres)]
#[tokio::test]
async fn a_batch_reports_per_row_outcomes_in_input_order() {
    let backend = setup_backend().await;
    let executable_id = upsert_test_executable(&backend, "data_sync").await;
    upsert(&backend, "fresh", &executable_id, at(12)).await;
    upsert(&backend, "stale", &executable_id, at(12)).await;
    let fresh_vm_id = InstanceId::new_uuid_v4();
    let stale_vm_id = InstanceId::new_uuid_v4();

    let items = nev![
        Item {
            schedule_name: Cow::Borrowed("fresh"),
            expected_next_run_at: Cow::Owned(at(12)),
            vm_id: Cow::Owned(fresh_vm_id),
            new_next_run_at: Cow::Owned(at(13)),
            check_overlap: true,
        },
        Item {
            schedule_name: Cow::Borrowed("stale"),
            expected_next_run_at: Cow::Owned(at(11)),
            vm_id: Cow::Owned(stale_vm_id),
            new_next_run_at: Cow::Owned(at(13)),
            check_overlap: true,
        },
    ];
    let outcomes = backend
        .register_scheduled_vm_runtimes(items.as_nonempty_slice())
        .await
        .expect("register scheduled vm runtimes");

    assert_eq!(outcomes, nev![Outcome::Registered, Outcome::Superseded]);
    assert!(vm_runtime_registered(&backend, fresh_vm_id).await);
    assert!(!vm_runtime_registered(&backend, stale_vm_id).await);
}

/// One choreographed collision of two concurrent registrar batches over
/// the same two due schedules — the schedules-table member of the
/// multi-row-writer class behind the 2026-08-31 deadlock outage.
/// Racing registrars are the DESIGNED normal case here (the loser's
/// rows come back Superseded), so opposite batch orders must queue, not
/// cycle.
///
/// The staging, plan-shape seeding, and contract follow
/// `complete_and_renew_across_a_staged_row` in
/// `action_call_requests::tests`: the leader's first key is pre-held so
/// it blocks holding nothing, the follower locks the other row and
/// queues behind it.  Both batches run the same statement text, so the
/// follower's blocked state is staged by waiter count.
async fn register_against_register_across_a_staged_row(leader_order: [&'static str; 2]) {
    let backend = setup_backend().await;
    let executable_id = upsert_test_executable(&backend, "choreography").await;
    upsert(&backend, "sched-a", &executable_id, at(1)).await;
    upsert(&backend, "sched-b", &executable_id, at(1)).await;

    // Plan shape, as in the swept-table choreographies: enough analyzed
    // rows that the registrar statement probes the primary key in input
    // order instead of seq-scanning both contenders into one order.
    sqlx::query(
        r#"
        INSERT INTO schedules
            (schedule_name, executable_id, definition, initial_snapshot, status, next_run_at)
        SELECT 'filler-' || n, $1, 'd', 's', 'active', NOW() + interval '1 hour'
        FROM generate_series(1, 10000) AS n
        "#,
    )
    .bind(executable_id)
    .execute(backend.pool())
    .await
    .expect("seed filler schedules");
    sqlx::query("ANALYZE schedules")
        .execute(backend.pool())
        .await
        .expect("analyze");

    // Pre-hold the leader batch's first schedule row.
    let mut staging = backend.pool().begin().await.expect("begin staging");
    let held = sqlx::query("SELECT 1 FROM schedules WHERE schedule_name = $1 FOR UPDATE")
        .bind(leader_order[0])
        .execute(&mut *staging)
        .await
        .expect("hold staged row");
    assert_eq!(held.rows_affected(), 1, "the staged schedule must exist");

    fn spawn_register(
        backend: &crate::PostgresBackend,
        order: [&'static str; 2],
    ) -> tokio::task::JoinHandle<
        Result<
            nonempty_collections::NEVec<Outcome>,
            <crate::PostgresBackend as RegisterScheduledVmRuntimes>::Error,
        >,
    > {
        let backend = backend.clone();
        tokio::spawn(async move {
            let items = nev![
                Item {
                    schedule_name: Cow::Borrowed(order[0]),
                    expected_next_run_at: Cow::Owned(at(1)),
                    vm_id: Cow::Owned(InstanceId::new_uuid_v4()),
                    new_next_run_at: Cow::Owned(at(2)),
                    check_overlap: false,
                },
                Item {
                    schedule_name: Cow::Borrowed(order[1]),
                    expected_next_run_at: Cow::Owned(at(1)),
                    vm_id: Cow::Owned(InstanceId::new_uuid_v4()),
                    new_next_run_at: Cow::Owned(at(2)),
                    check_overlap: false,
                },
            ];
            backend
                .register_scheduled_vm_runtimes(items.as_nonempty_slice())
                .await
        })
    }
    // `input_position` appears only in the registrar statement.
    let register_pattern = "%input_position%";

    let leader = spawn_register(&backend, leader_order);
    deadlock::wait_until_lock_blocked(backend.pool(), register_pattern).await;
    let follower = spawn_register(&backend, [leader_order[1], leader_order[0]]);
    deadlock::wait_until_lock_blocked_at_least(
        backend.pool(),
        register_pattern,
        std::num::NonZeroI64::new(2).unwrap(),
    )
    .await;

    staging.rollback().await.expect("release staged row");

    leader
        .await
        .expect("join leader")
        .expect("a registrar batch must not deadlock against a competing registrar");
    follower
        .await
        .expect("join follower")
        .expect("a registrar batch must not deadlock against a competing registrar");
}

#[serial(postgres)]
#[tokio::test]
async fn concurrent_registrar_batches_do_not_deadlock() {
    // Both leader orders, with the staged row tracking the leader's
    // first key, so an ordering discipline missing from the registrar
    // statement fails one of the runs.
    register_against_register_across_a_staged_row(["sched-b", "sched-a"]).await;
    register_against_register_across_a_staged_row(["sched-a", "sched-b"]).await;
}
