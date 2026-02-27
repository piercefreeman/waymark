use sqlx::PgPool;

use super::PostgresBackend;
use waymark_test_support::postgres_setup;

pub(super) async fn setup_backend() -> PostgresBackend {
    let pool = postgres_setup().await;
    reset_database(&pool).await;
    PostgresBackend::new(pool)
}

pub(super) async fn reset_database(pool: &PgPool) {
    sqlx::query(
        r#"
        TRUNCATE runner_actions_done,
                 queued_instances,
                 runner_instances,
                 workflow_versions,
                 workflow_schedules,
                 worker_status
        RESTART IDENTITY CASCADE
        "#,
    )
    .execute(pool)
    .await
    .expect("truncate postgres tables");
}
