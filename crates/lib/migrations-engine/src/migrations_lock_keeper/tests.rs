//! Tests of the migrations lock keeper, over a mocked backend and paused
//! time.

use std::sync::Arc;
use std::time::Duration;

use waymark_migrations_engine_backend::migrations_lock;
use waymark_nonzero_duration::NonZeroDuration;

use super::{
    ConvertToSharedError, KeepOutcome, MigrationsLockKeeper, TakeExclusiveError, TakeSharedError,
};
use crate::test_helpers::{MockBackendOperations, TestBackend, TestBackendError};
use crate::{MigrationsLockTiming, ValidMigrationsLockTiming};

/// The holder of every test.
const HOLDER: uuid::Uuid = uuid::Uuid::from_u128(7);

/// A holder id of [`HOLDER`].
fn holder_id() -> migrations_lock::HolderId {
    migrations_lock::HolderId::from_raw(HOLDER)
}

/// A heartbeat of 5 s, a time to live of 15 s and a fencing margin of
/// 1 s: the holder relies on the lock for 14 s after a renewal.
fn timing() -> ValidMigrationsLockTiming {
    let seconds = |seconds| NonZeroDuration::from_secs(seconds).expect("seconds are not zero");

    MigrationsLockTiming {
        heartbeat: seconds(5),
        time_to_live: seconds(15),
        fencing_margin: seconds(1),
    }
    .validate()
    .expect("the timing is valid")
}

/// Operations that grant the exclusive lock once, to [`HOLDER`] with the
/// time to live of [`timing`].
fn granting_the_exclusive_lock() -> MockBackendOperations {
    let mut operations = MockBackendOperations::new();

    operations
        .expect_take_exclusive_migrations_lock()
        .times(1)
        .withf(|holder_id, expiry| {
            holder_id.as_raw() == &HOLDER && expiry.time_to_live.get() == Duration::from_secs(15)
        })
        .returning(|_, _| {
            Ok(waymark_migrations_engine_backend::take_exclusive_migrations_lock::Outcome::Taken)
        });

    operations
}

/// Operations that grant a shared lock once, to [`HOLDER`] with the time
/// to live of [`timing`].
fn granting_a_shared_lock() -> MockBackendOperations {
    let mut operations = MockBackendOperations::new();

    operations
        .expect_take_shared_migrations_lock()
        .times(1)
        .withf(|holder_id, expiry| {
            holder_id.as_raw() == &HOLDER && expiry.time_to_live.get() == Duration::from_secs(15)
        })
        .returning(|_, _| {
            Ok(waymark_migrations_engine_backend::take_shared_migrations_lock::Outcome::Taken)
        });

    operations
}

/// The seconds of paused time each renewal reached the backend at.
type RenewalTimes = Arc<std::sync::Mutex<Vec<u64>>>;

/// Expect renewals for [`HOLDER`], each recorded in the returned list and
/// answered by `answer`.
fn expect_renewals<Answer>(operations: &mut MockBackendOperations, answer: Answer) -> RenewalTimes
where
    Answer: Fn() -> Result<
            waymark_migrations_engine_backend::renew_migrations_lock::Outcome,
            TestBackendError,
        > + Send
        + 'static,
{
    let started_at = tokio::time::Instant::now();
    let renewal_times = RenewalTimes::default();
    let recorded = Arc::clone(&renewal_times);

    operations
        .expect_renew_migrations_lock()
        .withf(|holder_id, expiry| {
            holder_id.as_raw() == &HOLDER && expiry.time_to_live.get() == Duration::from_secs(15)
        })
        .returning(move |_, _| {
            let at = started_at.elapsed().as_secs();
            recorded.lock().expect("the list is not poisoned").push(at);

            answer()
        });

    renewal_times
}

/// The times in `renewal_times`.
fn renewal_times(renewal_times: &RenewalTimes) -> Vec<u64> {
    renewal_times
        .lock()
        .expect("the list is not poisoned")
        .clone()
}

#[tokio::test(start_paused = true)]
async fn take_exclusive_gives_a_lock_that_is_not_fenced() {
    let backend = TestBackend::new(granting_the_exclusive_lock());

    let (mut keeper, lock) = MigrationsLockKeeper::take_exclusive(backend, holder_id(), timing())
        .await
        .expect("the lock is taken");

    assert!(!lock.is_migrations_lock_fenced());

    keeper.backend.operations.checkpoint();
}

#[tokio::test(start_paused = true)]
async fn take_exclusive_refused_with_an_exclusive_lock_found_is_a_migration_in_progress() {
    let mut operations = MockBackendOperations::new();
    operations
        .expect_take_exclusive_migrations_lock()
        .times(1)
        .returning(|_, _| {
            Ok(waymark_migrations_engine_backend::take_exclusive_migrations_lock::Outcome::Refused)
        });
    operations
        .expect_find_exclusive_migrations_lock()
        .times(1)
        .returning(|| {
            Ok(waymark_migrations_engine_backend::find_exclusive_migrations_lock::Outcome::Found)
        });

    let refused =
        MigrationsLockKeeper::take_exclusive(TestBackend::new(operations), holder_id(), timing())
            .await;

    assert!(matches!(
        refused,
        Err(TakeExclusiveError::MigrationInProgress)
    ));
}

#[tokio::test(start_paused = true)]
async fn take_exclusive_refused_with_no_exclusive_lock_found_is_a_database_in_use() {
    let mut operations = MockBackendOperations::new();
    operations
        .expect_take_exclusive_migrations_lock()
        .times(1)
        .returning(|_, _| {
            Ok(waymark_migrations_engine_backend::take_exclusive_migrations_lock::Outcome::Refused)
        });
    operations
        .expect_find_exclusive_migrations_lock()
        .times(1)
        .returning(|| {
            Ok(waymark_migrations_engine_backend::find_exclusive_migrations_lock::Outcome::NotFound)
        });

    let refused =
        MigrationsLockKeeper::take_exclusive(TestBackend::new(operations), holder_id(), timing())
            .await;

    assert!(matches!(refused, Err(TakeExclusiveError::DatabaseInUse)));
}

#[tokio::test(start_paused = true)]
async fn take_exclusive_backend_errors_are_take_and_find_errors() {
    let mut failing_take = MockBackendOperations::new();
    failing_take
        .expect_take_exclusive_migrations_lock()
        .times(1)
        .returning(|_, _| Err(TestBackendError));

    let take_failed =
        MigrationsLockKeeper::take_exclusive(TestBackend::new(failing_take), holder_id(), timing())
            .await;

    assert!(matches!(
        take_failed,
        Err(TakeExclusiveError::Take(TestBackendError))
    ));

    let mut failing_find = MockBackendOperations::new();
    failing_find
        .expect_take_exclusive_migrations_lock()
        .times(1)
        .returning(|_, _| {
            Ok(waymark_migrations_engine_backend::take_exclusive_migrations_lock::Outcome::Refused)
        });
    failing_find
        .expect_find_exclusive_migrations_lock()
        .times(1)
        .returning(|| Err(TestBackendError));

    let find_failed =
        MigrationsLockKeeper::take_exclusive(TestBackend::new(failing_find), holder_id(), timing())
            .await;

    assert!(matches!(
        find_failed,
        Err(TakeExclusiveError::Find(TestBackendError))
    ));
}

#[tokio::test(start_paused = true)]
async fn take_shared_gives_a_lock_that_is_not_fenced() {
    let backend = TestBackend::new(granting_a_shared_lock());

    let (mut keeper, lock) = MigrationsLockKeeper::take_shared(backend, holder_id(), timing())
        .await
        .expect("the lock is taken");

    assert!(!lock.is_migrations_lock_fenced());

    keeper.backend.operations.checkpoint();
}

#[tokio::test(start_paused = true)]
async fn take_shared_refused_is_a_migration_in_progress() {
    let mut operations = MockBackendOperations::new();
    operations
        .expect_take_shared_migrations_lock()
        .times(1)
        .returning(|_, _| {
            Ok(waymark_migrations_engine_backend::take_shared_migrations_lock::Outcome::Refused)
        });

    let refused =
        MigrationsLockKeeper::take_shared(TestBackend::new(operations), holder_id(), timing())
            .await;

    assert!(matches!(refused, Err(TakeSharedError::MigrationInProgress)));
}

#[tokio::test(start_paused = true)]
async fn keep_renews_a_heartbeat_apart_until_stopped() {
    let mut operations = granting_a_shared_lock();
    let renewals = expect_renewals(&mut operations, || {
        Ok(waymark_migrations_engine_backend::renew_migrations_lock::Outcome::Renewed)
    });

    let (mut keeper, lock) =
        MigrationsLockKeeper::take_shared(TestBackend::new(operations), holder_id(), timing())
            .await
            .expect("the lock is taken");

    // Far past the 14 s the lock is relied on for after the take alone.
    let outcome = keeper
        .keep(tokio::time::sleep(Duration::from_secs(32)))
        .await;

    assert!(matches!(outcome, KeepOutcome::Stopped));
    assert!(!lock.is_migrations_lock_fenced());
    assert_eq!(renewal_times(&renewals), [5, 10, 15, 20, 25, 30]);

    keeper.backend.operations.checkpoint();
}

#[tokio::test(start_paused = true)]
async fn keep_picks_the_renewals_up_where_it_stopped() {
    let mut operations = granting_a_shared_lock();
    let renewals = expect_renewals(&mut operations, || {
        Ok(waymark_migrations_engine_backend::renew_migrations_lock::Outcome::Renewed)
    });

    let (mut keeper, _lock) =
        MigrationsLockKeeper::take_shared(TestBackend::new(operations), holder_id(), timing())
            .await
            .expect("the lock is taken");

    let first = keeper
        .keep(tokio::time::sleep(Duration::from_secs(7)))
        .await;
    let second = keeper
        .keep(tokio::time::sleep(Duration::from_secs(4)))
        .await;

    assert!(matches!(first, KeepOutcome::Stopped));
    assert!(matches!(second, KeepOutcome::Stopped));
    assert_eq!(renewal_times(&renewals), [5, 10]);

    keeper.backend.operations.checkpoint();
}

#[tokio::test(start_paused = true)]
async fn keep_fences_a_lock_a_renewal_finds_lost() {
    let mut operations = granting_a_shared_lock();
    let renewals = expect_renewals(&mut operations, || {
        Ok(waymark_migrations_engine_backend::renew_migrations_lock::Outcome::Lost)
    });

    let (mut keeper, lock) =
        MigrationsLockKeeper::take_shared(TestBackend::new(operations), holder_id(), timing())
            .await
            .expect("the lock is taken");

    let outcome = keeper.keep(std::future::pending()).await;

    assert!(matches!(outcome, KeepOutcome::Lost));
    assert!(lock.is_migrations_lock_fenced());
    assert_eq!(renewal_times(&renewals), [5]);

    keeper.backend.operations.checkpoint();
}

#[tokio::test(start_paused = true)]
async fn keep_fences_a_lock_no_renewal_confirms_in_time() {
    let mut operations = granting_a_shared_lock();
    let renewals = expect_renewals(&mut operations, || Err(TestBackendError));

    let (mut keeper, lock) =
        MigrationsLockKeeper::take_shared(TestBackend::new(operations), holder_id(), timing())
            .await
            .expect("the lock is taken");

    let started_at = tokio::time::Instant::now();
    let outcome = keeper.keep(std::future::pending()).await;

    // Each failed renewal waits for its own next heartbeat; the lock is
    // fenced 14 s after the take, the last instant it was confirmed at.
    assert!(matches!(outcome, KeepOutcome::Lapsed));
    assert!(lock.is_migrations_lock_fenced());
    assert_eq!(renewal_times(&renewals), [5, 10]);
    assert_eq!(started_at.elapsed(), Duration::from_secs(14));

    keeper.backend.operations.checkpoint();
}

#[tokio::test(start_paused = true)]
async fn keep_fences_at_the_lapse_while_a_renewal_is_on_its_way_and_waits_for_it() {
    let mut operations = granting_a_shared_lock();
    let renewals = expect_renewals(&mut operations, || {
        Ok(waymark_migrations_engine_backend::renew_migrations_lock::Outcome::Renewed)
    });

    let mut backend = TestBackend::new(operations);
    backend.renewal_takes = Duration::from_secs(20);

    let (mut keeper, lock) = MigrationsLockKeeper::take_shared(backend, holder_id(), timing())
        .await
        .expect("the lock is taken");

    let started_at = tokio::time::Instant::now();

    let (outcome, fenced_after) = tokio::join!(keeper.keep(std::future::pending()), async {
        lock.migrations_lock_fenced().await;

        started_at.elapsed()
    });

    // The renewal sent at 5 s reaches the backend at 25 s; the lock is
    // fenced at 14 s meanwhile, and the late confirmation does not lift
    // the fence.
    assert_eq!(fenced_after, Duration::from_secs(14));
    assert!(matches!(outcome, KeepOutcome::Lapsed));
    assert!(lock.is_migrations_lock_fenced());
    assert_eq!(renewal_times(&renewals), [25]);
    assert_eq!(started_at.elapsed(), Duration::from_secs(25));

    keeper.backend.operations.checkpoint();
}

#[tokio::test(start_paused = true)]
async fn keep_looks_at_the_stop_only_once_a_renewal_on_its_way_returned() {
    let mut operations = granting_a_shared_lock();
    let renewals = expect_renewals(&mut operations, || {
        Ok(waymark_migrations_engine_backend::renew_migrations_lock::Outcome::Renewed)
    });

    let mut backend = TestBackend::new(operations);
    backend.renewal_takes = Duration::from_secs(3);

    let (mut keeper, lock) = MigrationsLockKeeper::take_shared(backend, holder_id(), timing())
        .await
        .expect("the lock is taken");

    let started_at = tokio::time::Instant::now();

    // The stop completes at 6 s, while the renewal sent at 5 s is on its
    // way until 8 s.
    let outcome = keeper
        .keep(tokio::time::sleep(Duration::from_secs(6)))
        .await;

    assert!(matches!(outcome, KeepOutcome::Stopped));
    assert!(!lock.is_migrations_lock_fenced());
    assert_eq!(renewal_times(&renewals), [8]);
    assert_eq!(started_at.elapsed(), Duration::from_secs(8));

    keeper.backend.operations.checkpoint();
}

#[tokio::test(start_paused = true)]
async fn convert_to_shared_moves_the_lock_to_the_shared_backend() {
    let exclusive_backend = TestBackend::new(granting_the_exclusive_lock());

    let (keeper, exclusive_lock) =
        MigrationsLockKeeper::take_exclusive(exclusive_backend, holder_id(), timing())
            .await
            .expect("the lock is taken");

    let mut shared_operations = MockBackendOperations::new();
    shared_operations
        .expect_convert_migrations_lock_to_shared()
        .times(1)
        .withf(|holder_id, expiry| {
            holder_id.as_raw() == &HOLDER && expiry.time_to_live.get() == Duration::from_secs(15)
        })
        .returning(|_, _| {
            Ok(
                waymark_migrations_engine_backend::convert_migrations_lock_to_shared::Outcome::Converted,
            )
        });
    let renewals = expect_renewals(&mut shared_operations, || {
        Ok(waymark_migrations_engine_backend::renew_migrations_lock::Outcome::Renewed)
    });

    tokio::time::sleep(Duration::from_secs(9)).await;

    let mut converted = keeper
        .convert_to_shared(exclusive_lock, TestBackend::new(shared_operations))
        .await
        .expect("the lock is converted");

    // The shared lock is counted from the conversion at 9 s, so its first
    // renewal goes out a heartbeat later, at 14 s, through the shared
    // backend.
    let outcome = converted
        .keeper
        .keep(tokio::time::sleep(Duration::from_secs(7)))
        .await;

    assert!(matches!(outcome, KeepOutcome::Stopped));
    assert!(!converted.lock.is_migrations_lock_fenced());
    assert_eq!(renewal_times(&renewals), [14]);

    converted.keeper.backend.operations.checkpoint();
    converted.exclusive_backend.operations.checkpoint();
}

#[tokio::test(start_paused = true)]
async fn convert_to_shared_of_a_lapsed_lock_sends_nothing() {
    let (keeper, exclusive_lock) = MigrationsLockKeeper::take_exclusive(
        TestBackend::new(granting_the_exclusive_lock()),
        holder_id(),
        timing(),
    )
    .await
    .expect("the lock is taken");

    tokio::time::sleep(Duration::from_secs(14)).await;

    // No conversion is expected: the mock fails the test on any call.
    let refused = keeper
        .convert_to_shared(
            exclusive_lock,
            TestBackend::new(MockBackendOperations::new()),
        )
        .await;

    assert!(matches!(refused, Err(ConvertToSharedError::Fenced)));
}

#[tokio::test(start_paused = true)]
async fn convert_to_shared_that_finds_the_lock_lost_is_an_error() {
    let (keeper, exclusive_lock) = MigrationsLockKeeper::take_exclusive(
        TestBackend::new(granting_the_exclusive_lock()),
        holder_id(),
        timing(),
    )
    .await
    .expect("the lock is taken");

    let mut shared_operations = MockBackendOperations::new();
    shared_operations
        .expect_convert_migrations_lock_to_shared()
        .times(1)
        .returning(|_, _| {
            Ok(waymark_migrations_engine_backend::convert_migrations_lock_to_shared::Outcome::Lost)
        });

    let lost = keeper
        .convert_to_shared(exclusive_lock, TestBackend::new(shared_operations))
        .await;

    assert!(matches!(lost, Err(ConvertToSharedError::Lost)));
}

#[tokio::test(start_paused = true)]
async fn convert_to_shared_backend_error_is_a_convert_error() {
    let (keeper, exclusive_lock) = MigrationsLockKeeper::take_exclusive(
        TestBackend::new(granting_the_exclusive_lock()),
        holder_id(),
        timing(),
    )
    .await
    .expect("the lock is taken");

    let mut shared_operations = MockBackendOperations::new();
    shared_operations
        .expect_convert_migrations_lock_to_shared()
        .times(1)
        .returning(|_, _| Err(TestBackendError));

    let failed = keeper
        .convert_to_shared(exclusive_lock, TestBackend::new(shared_operations))
        .await;

    assert!(matches!(
        failed,
        Err(ConvertToSharedError::Convert(TestBackendError))
    ));
}

#[tokio::test(start_paused = true)]
async fn release_fences_the_lock_and_hands_the_backend_back() {
    let mut operations = granting_a_shared_lock();
    operations
        .expect_release_migrations_lock()
        .times(1)
        .withf(|holder_id| holder_id.as_raw() == &HOLDER)
        .returning(|_| Ok(()));

    let (keeper, lock) =
        MigrationsLockKeeper::take_shared(TestBackend::new(operations), holder_id(), timing())
            .await
            .expect("the lock is taken");

    let (mut backend, released) = keeper.release().await;

    assert!(released.is_ok());
    assert!(lock.is_migrations_lock_fenced());

    backend.operations.checkpoint();
}

#[tokio::test(start_paused = true)]
async fn release_that_fails_still_fences_the_lock_and_hands_the_backend_back() {
    let mut operations = granting_a_shared_lock();
    operations
        .expect_release_migrations_lock()
        .times(1)
        .returning(|_| Err(TestBackendError));

    let (keeper, lock) =
        MigrationsLockKeeper::take_shared(TestBackend::new(operations), holder_id(), timing())
            .await
            .expect("the lock is taken");

    let (mut backend, released) = keeper.release().await;

    assert!(matches!(released, Err(TestBackendError)));
    assert!(lock.is_migrations_lock_fenced());

    backend.operations.checkpoint();
}
