//! Shared machinery for the lock-order deadlock choreographies.
//!
//! The choreography tests (see `complete_and_renew_across_a_staged_row`
//! in `action_call_requests::tests` for the canonical write-up) stage
//! two multi-row statements into a would-be deadlock cycle around a
//! pre-held row and assert both succeed under the canonical row-lock
//! order.  These helpers carry the pieces every choreography shares —
//! and the load-bearing constants (the filler-row count, the wait
//! deadline) live here so no test copy can drift on its own.

use waymark_ids::InstanceId;

/// A trigger-swept table a lock-order choreography stages rows in.
#[derive(Debug, Clone, Copy)]
pub(crate) enum SweptTable {
    ActionCallRequests,
    ActionCallCompletions,
    SleepRequests,
}

/// Wait until a backend whose current query matches `query_pattern` is
/// blocked waiting on a lock, so lock-order choreographies can stage
/// each statement's position in the row-lock queues deterministically.
///
/// `pg_stat_activity.query` is truncated at `track_activity_query_size`
/// (~1KB), so match on text near the FRONT of the statement, never on a
/// fragment that sits past the first kilobyte.
pub(crate) async fn wait_until_lock_blocked(pool: &sqlx::PgPool, query_pattern: &str) {
    wait_until_lock_blocked_at_least(pool, query_pattern, std::num::NonZeroI64::new(1).unwrap())
        .await;
}

/// [`wait_until_lock_blocked`] for `count` concurrently blocked
/// statements matching the pattern — for choreographies whose two
/// contenders run the same statement text.
pub(crate) async fn wait_until_lock_blocked_at_least(
    pool: &sqlx::PgPool,
    query_pattern: &str,
    count: std::num::NonZeroI64,
) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let waiting: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*) FROM pg_stat_activity
            WHERE wait_event_type = 'Lock'
                AND datname = current_database()
                AND query LIKE $1
            "#,
        )
        .bind(query_pattern)
        .fetch_one(pool)
        .await
        .expect("poll pg_stat_activity");
        if waiting >= count.get() {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "no statement matching {query_pattern:?} became lock-blocked within the \
             deadline — the statement the caller staged either never ran, finished \
             without contending the pre-held row, its plan no longer takes row \
             locks in the order the caller's choreography assumes, or the match \
             fragment fell past pg_stat_activity's ~1KB query truncation"
        );
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

/// Seed filler rows and refresh statistics so a lock-order choreography
/// gets the production plan shape: against a large analyzed table the
/// planner drives the contending statements through per-key primary-key
/// probes, making their lock order follow their input; a tiny
/// unanalyzed table walks both statements through the same seq scan,
/// aligning their lock orders and silently neutering the test.  The row
/// count lives here so no test copy can drift below the plan-flip
/// threshold on its own.
pub(crate) async fn seed_filler_rows(pool: &sqlx::PgPool, table: SweptTable) {
    let insert = match table {
        SweptTable::ActionCallRequests => {
            r#"
            INSERT INTO action_call_requests
                (vm_id, promise_state_id, effect_number, request)
            SELECT gen_random_uuid(), n, n, 'filler'
            FROM generate_series(1, 10000) AS n
            "#
        }
        SweptTable::ActionCallCompletions => {
            r#"
            INSERT INTO action_call_completions
                (vm_id, promise_state_id, effect_number, execution_result)
            SELECT gen_random_uuid(), n, n, 'filler'
            FROM generate_series(1, 10000) AS n
            "#
        }
        SweptTable::SleepRequests => {
            r#"
            INSERT INTO sleep_requests
                (vm_id, promise_state_id, effect_number, wake_at)
            SELECT gen_random_uuid(), n, n, NOW()
            FROM generate_series(1, 10000) AS n
            "#
        }
    };
    sqlx::query(insert)
        .execute(pool)
        .await
        .expect("seed filler rows");
    sqlx::query(
        "ANALYZE action_call_requests, action_call_completions, \
         sleep_requests, vm_runtime_snapshots",
    )
    .execute(pool)
    .await
    .expect("analyze");
}

/// Open a transaction holding a `FOR UPDATE` lock on one choreography
/// row; dropping or rolling back the returned transaction releases it.
pub(crate) async fn hold_row_for_update(
    pool: &sqlx::PgPool,
    table: SweptTable,
    vm_id: InstanceId,
    promise_state_id: usize,
) -> sqlx::Transaction<'_, sqlx::Postgres> {
    let select = match table {
        SweptTable::ActionCallRequests => {
            "SELECT 1 FROM action_call_requests \
             WHERE vm_id = $1 AND promise_state_id = $2 FOR UPDATE"
        }
        SweptTable::ActionCallCompletions => {
            "SELECT 1 FROM action_call_completions \
             WHERE vm_id = $1 AND promise_state_id = $2 FOR UPDATE"
        }
        SweptTable::SleepRequests => {
            "SELECT 1 FROM sleep_requests \
             WHERE vm_id = $1 AND promise_state_id = $2 FOR UPDATE"
        }
    };
    let mut staging = pool.begin().await.expect("begin staging");
    let held = sqlx::query(select)
        .bind(vm_id)
        .bind(i64::try_from(promise_state_id).expect("promise state id fits"))
        .execute(&mut *staging)
        .await
        .expect("hold staged row");
    assert_eq!(
        held.rows_affected(),
        1,
        "the staged row ({table:?}, {vm_id:?}, {promise_state_id}) does not exist — \
         the choreography would hold nothing and time out blaming plan shapes"
    );
    staging
}

/// The `pg_stat_activity` match pattern for a blocked [`spawn_snapshot_sweep`]
/// statement.
pub(crate) const SNAPSHOT_SWEEP_PATTERN: &str = "%DELETE FROM vm_runtime_snapshots%";

/// Spawn the VMs' terminal snapshot delete, firing the cleanup trigger
/// whose per-table sweeps the choreographies contend with; wait for its
/// blocked state via [`SNAPSHOT_SWEEP_PATTERN`].
pub(crate) fn spawn_snapshot_sweep(
    pool: &sqlx::PgPool,
    vm_ids: Vec<InstanceId>,
) -> tokio::task::JoinHandle<Result<sqlx::postgres::PgQueryResult, sqlx::Error>> {
    let pool = pool.clone();
    tokio::spawn(async move {
        sqlx::query("DELETE FROM vm_runtime_snapshots WHERE vm_id = ANY($1)")
            .bind(vm_ids)
            .execute(&pool)
            .await
    })
}

/// The shared tail of the single-VM op-versus-sweep choreographies: the
/// caller has spawned the op task with the staged row as its first
/// contact; this waits for it to become the staged row's first waiter,
/// queues the VM's snapshot sweep behind it, releases the staged row so
/// the two advance into each other, and requires both to succeed.
pub(crate) async fn contend_op_with_snapshot_sweep<T, OpError: core::fmt::Debug>(
    pool: &sqlx::PgPool,
    staging: sqlx::Transaction<'_, sqlx::Postgres>,
    op_name: &str,
    op_task: tokio::task::JoinHandle<Result<T, OpError>>,
    op_pattern: &str,
    sweep_vm: InstanceId,
) {
    wait_until_lock_blocked(pool, op_pattern).await;

    let sweep_task = spawn_snapshot_sweep(pool, vec![sweep_vm]);
    wait_until_lock_blocked(pool, SNAPSHOT_SWEEP_PATTERN).await;

    staging.rollback().await.expect("release staged row");

    if let Err(error) = op_task.await.expect("join the op") {
        panic!("the {op_name} must not deadlock against the snapshot sweep: {error:?}");
    }
    sweep_task
        .await
        .expect("join the sweep")
        .expect("the snapshot sweep must not deadlock against the op");
}
