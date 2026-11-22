//! Tests of the observability store's migrations array.

use std::collections::BTreeSet;
use std::num::NonZeroU64;

use waymark_migrations_engine::run_migrations::Outcome;
use waymark_migrations_engine::{MigrationsLockTiming, MigrationsRunTransactions};
use waymark_migrations_engine_backend::migrations_lock;
use waymark_migrations_engine_migration::HasMigrationIdentity as _;
use waymark_nonzero_duration::NonZeroDuration;

waymark_migrations_source::source_checksums_tests!(sql_files("migrations"));

#[test]
fn array_is_in_the_order_of_its_versions() {
    waymark_migrations_engine::validate_migrations_array(super::migrations())
        .expect("each migration has a higher version than the one before it");
}

#[test]
fn array_has_every_sql_file_of_the_migrations_directory_and_no_other() {
    let directory = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");

    let file_versions: BTreeSet<u32> = std::fs::read_dir(&directory)
        .expect("read the migrations directory")
        .map(|entry| {
            let file_name = entry
                .expect("read an entry of the migrations directory")
                .file_name();
            let file_name = file_name.to_str().expect("the file name is UTF-8");

            let (version, _description) = file_name
                .split_once('_')
                .expect("the file name is <version>_<description>.sql");

            version.parse().expect("the version is a number")
        })
        .collect();

    let array_versions: BTreeSet<u32> = super::migrations()
        .iter()
        .map(|migration| migration.migration_identity().version.to_raw())
        .collect();

    assert_eq!(array_versions, file_versions);
}

#[tokio::test]
async fn migrations_run_on_an_empty_schema_and_then_check_as_applied() {
    let schema = "observability_store_postgres_migrations_run_then_check";
    let bootstrap = waymark_support_test::postgres_setup().await;

    sqlx::query(sqlx::AssertSqlSafe(format!(
        r#"
DROP SCHEMA IF EXISTS "{schema}" CASCADE
        "#
    )))
    .execute(&bootstrap)
    .await
    .expect("drop leftover test schema");

    let connection_config = waymark_postgres_config::connection::Config {
        url: waymark_support_integration::LOCAL_POSTGRES_DSN.into(),
        statement_timeout: NonZeroDuration::from_secs(60),
    };
    let millis =
        |millis| NonZeroDuration::from_nonzero_millis(NonZeroU64::new(millis).expect("non-zero"));
    let migrations_lock_timing = MigrationsLockTiming {
        heartbeat: millis(1_000),
        time_to_live: millis(15_000),
        fencing_margin: millis(1_000),
    };
    let migrations_lock = waymark_postgres_config::migrations_lock::Config {
        timing: migrations_lock_timing
            .validate()
            .expect("the timing is valid"),
        statement_timeout: Some(millis(3_000)),
    };
    let assertions = super::assertions();
    let migrations = waymark_migrations_engine::validate_migrations_array(super::migrations())
        .expect("the array is in the order of its versions");

    let outcome =
        waymark_postgres_bringup::migrate_and_release_migrations_lock::<super::Tables, _, _, _>(
            waymark_postgres_bringup::migrate_and_release_migrations_lock::Params {
                migrations_connection: &connection_config,
                migrations_lock: &migrations_lock,
                schema: Some(schema.into()),
                holder_id: migrations_lock::HolderId::from_raw(uuid::Uuid::from_u128(1)),
                assertions: &assertions,
                migrations: &migrations,
                transactions: MigrationsRunTransactions::WholeRun,
            },
        )
        .await
        .expect("every migration runs, inside one transaction");

    let Outcome::Applied {
        migrations: applied,
    } = outcome
    else {
        panic!("the run applies the migrations");
    };
    assert_eq!(applied.get(), migrations.as_slice().len());

    // A second run reads the ledger the first one left: it finds the
    // store migrated exactly to the array, with nothing pending.
    let outcome_of_a_second_run =
        waymark_postgres_bringup::migrate_and_release_migrations_lock::<super::Tables, _, _, _>(
            waymark_postgres_bringup::migrate_and_release_migrations_lock::Params {
                migrations_connection: &connection_config,
                migrations_lock: &migrations_lock,
                schema: Some(schema.into()),
                holder_id: migrations_lock::HolderId::from_raw(uuid::Uuid::from_u128(2)),
                assertions: &assertions,
                migrations: &migrations,
                transactions: MigrationsRunTransactions::WholeRun,
            },
        )
        .await
        .expect("the ledger does not conflict with the array");

    assert!(matches!(outcome_of_a_second_run, Outcome::NothingPending));

    sqlx::query(sqlx::AssertSqlSafe(format!(
        r#"
DROP SCHEMA "{schema}" CASCADE
        "#
    )))
    .execute(&bootstrap)
    .await
    .expect("drop test schema");
}
