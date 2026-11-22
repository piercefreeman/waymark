//! Tests of the migration's operations against the local test Postgres,
//! each over a snapshots table of its own schema, filled with the
//! vendored snapshots.

use waymark_migrations_engine_migration::{
    PostValidateMigration as _, PreValidateMigration as _, RunMigration as _,
};

use super::migration;
use crate::test_fixtures::{empty_after, empty_before, populated_after, populated_before};

/// A connection scoped to `schema`, made afresh with an empty snapshots
/// table in it.
async fn connection_with_snapshots_table(schema: &str) -> sqlx::PgConnection {
    let bootstrap = waymark_support_test::postgres_setup().await;

    sqlx::query(sqlx::AssertSqlSafe(format!(
        r#"
DROP SCHEMA IF EXISTS "{schema}" CASCADE
        "#
    )))
    .execute(&bootstrap)
    .await
    .expect("drop leftover test schema");

    sqlx::query(sqlx::AssertSqlSafe(format!(
        r#"
CREATE SCHEMA "{schema}"
        "#
    )))
    .execute(&bootstrap)
    .await
    .expect("create test schema");

    let mut connection = <sqlx::PgConnection as sqlx::Connection>::connect(
        waymark_support_integration::LOCAL_POSTGRES_DSN.expose_secret(),
    )
    .await
    .expect("connect");

    sqlx::query(sqlx::AssertSqlSafe(format!(
        r#"
SET search_path TO "{schema}"
        "#
    )))
    .execute(&mut connection)
    .await
    .expect("scope the connection to the test schema");

    sqlx::query(
        r#"
CREATE TABLE vm_runtime_snapshots (
    vm_id UUID PRIMARY KEY,
    snapshot BYTEA NOT NULL
)
        "#,
    )
    .execute(&mut connection)
    .await
    .expect("create the snapshots table");

    connection
}

/// Drop the test `schema` and close `connection`.
async fn drop_schema(mut connection: sqlx::PgConnection, schema: &str) {
    sqlx::query(sqlx::AssertSqlSafe(format!(
        r#"
DROP SCHEMA "{schema}" CASCADE
        "#
    )))
    .execute(&mut connection)
    .await
    .expect("drop test schema");

    sqlx::Connection::close(connection)
        .await
        .expect("close the connection");
}

/// Store `snapshot` for the VM whose id is `vm`.
async fn store(connection: &mut sqlx::PgConnection, vm: u128, snapshot: &[u8]) {
    sqlx::query(
        r#"
INSERT INTO vm_runtime_snapshots (vm_id, snapshot)
VALUES ($1, $2)
        "#,
    )
    .bind(uuid::Uuid::from_u128(vm))
    .bind(snapshot)
    .execute(connection)
    .await
    .expect("store the snapshot");
}

/// The stored snapshots, in the order of their VM ids.
async fn stored(connection: &mut sqlx::PgConnection) -> Vec<Vec<u8>> {
    sqlx::query_scalar(
        r#"
SELECT snapshot
FROM vm_runtime_snapshots
ORDER BY vm_id
        "#,
    )
    .fetch_all(connection)
    .await
    .expect("read the snapshots")
}

#[tokio::test]
async fn run_rewrites_every_snapshot_across_batches() {
    let schema = "migration_0027_run";
    let mut connection = connection_with_snapshots_table(schema).await;
    let migration = migration();

    // Two full batches and a part of a third, the two fixtures taking
    // turns.
    let mut expected = Vec::new();
    for vm in 0..150u128 {
        if vm % 2 == 0 {
            store(&mut connection, vm, &populated_before()).await;
            expected.push(populated_after());
        } else {
            store(&mut connection, vm, &empty_before()).await;
            expected.push(empty_after());
        }
    }

    migration
        .pre_validate_migration(&mut connection)
        .await
        .expect("the pre-validation passes");
    migration
        .run_migration(&mut connection)
        .await
        .expect("the run rewrites the snapshots");
    migration
        .post_validate_migration(&mut connection)
        .await
        .expect("the post-validation passes");

    assert_eq!(stored(&mut connection).await, expected);

    drop_schema(connection, schema).await;
}

#[tokio::test]
async fn run_without_snapshots_changes_nothing() {
    let schema = "migration_0027_run_empty";
    let mut connection = connection_with_snapshots_table(schema).await;
    let migration = migration();

    migration
        .run_migration(&mut connection)
        .await
        .expect("the run passes");
    migration
        .post_validate_migration(&mut connection)
        .await
        .expect("the post-validation passes");

    assert!(stored(&mut connection).await.is_empty());

    drop_schema(connection, schema).await;
}

#[tokio::test]
async fn run_that_refuses_a_snapshot_leaves_every_snapshot_as_it_was() {
    let schema = "migration_0027_run_refused";
    let mut connection = connection_with_snapshots_table(schema).await;
    let migration = migration();

    // The refused snapshot comes in the second batch, after the first one
    // was written.
    let mut expected = Vec::new();
    for vm in 0..70u128 {
        store(&mut connection, vm, &populated_before()).await;
        expected.push(populated_before());
    }
    store(&mut connection, 70, &populated_after()).await;
    expected.push(populated_after());

    let refused = migration
        .run_migration(&mut connection)
        .await
        .expect_err("the run refuses the snapshot that is already rewritten");

    assert_eq!(
        refused.to_string(),
        "rewriting the snapshots: the snapshot of VM 00000000-0000-0000-0000-000000000046: the \
         promise states table is not a sequence of promise states"
    );
    assert_eq!(stored(&mut connection).await, expected);

    drop_schema(connection, schema).await;
}

#[tokio::test]
async fn run_inside_a_transaction_is_rolled_back_with_it() {
    let schema = "migration_0027_run_in_transaction";
    let mut connection = connection_with_snapshots_table(schema).await;
    let migration = migration();

    store(&mut connection, 1, &populated_before()).await;

    let mut transaction = sqlx::Connection::begin(&mut connection)
        .await
        .expect("begin the outer transaction");
    migration
        .run_migration(&mut transaction)
        .await
        .expect("the run rewrites the snapshot");
    let inside = stored(&mut transaction).await;
    transaction
        .rollback()
        .await
        .expect("roll the outer transaction back");

    assert_eq!(inside, [populated_after()]);
    assert_eq!(stored(&mut connection).await, [populated_before()]);

    drop_schema(connection, schema).await;
}

#[tokio::test]
async fn post_validation_refuses_a_snapshot_that_is_not_rewritten() {
    let schema = "migration_0027_post_validation";
    let mut connection = connection_with_snapshots_table(schema).await;
    let migration = migration();

    store(&mut connection, 1, &populated_after()).await;
    store(&mut connection, 2, &empty_before()).await;

    let refused = migration
        .post_validate_migration(&mut connection)
        .await
        .expect_err("the post-validation refuses the snapshot in the shape before");

    assert_eq!(
        refused.to_string(),
        "the snapshot of VM 00000000-0000-0000-0000-000000000002: the promise states table is \
         not an id map of promise states"
    );

    drop_schema(connection, schema).await;
}
