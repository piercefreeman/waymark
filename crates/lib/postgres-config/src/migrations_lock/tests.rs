use std::num::NonZeroU64;

use super::*;

// No WAYMARK_POSTGRES_CONFIG_TEST_* variable is set in the test
// environment.

/// A migrations lock profile whose variables are never set, with a valid
/// timing.
struct TestMigrationsLock;

impl Profile for TestMigrationsLock {
    const PREFIX: &'static str = "POSTGRES_CONFIG_TEST_MIGRATIONS_LOCK";

    const HEARTBEAT_DEFAULT: NonZeroDuration =
        NonZeroDuration::from_nonzero_secs(NonZeroU64::new(5).unwrap());

    const TIME_TO_LIVE_DEFAULT: NonZeroDuration =
        NonZeroDuration::from_nonzero_secs(NonZeroU64::new(15).unwrap());

    const FENCING_MARGIN_DEFAULT: NonZeroDuration =
        NonZeroDuration::from_nonzero_secs(NonZeroU64::new(1).unwrap());

    const STATEMENT_TIMEOUT_DEFAULT: Option<NonZeroDuration> = NonZeroDuration::from_secs(3);
}

/// A migrations lock profile whose variables are never set, with a
/// heartbeat as long as its time to live.
struct TestMigrationsLockWithNoRoom;

impl Profile for TestMigrationsLockWithNoRoom {
    const PREFIX: &'static str = "POSTGRES_CONFIG_TEST_MIGRATIONS_LOCK_NO_ROOM";

    const HEARTBEAT_DEFAULT: NonZeroDuration =
        NonZeroDuration::from_nonzero_secs(NonZeroU64::new(15).unwrap());

    const TIME_TO_LIVE_DEFAULT: NonZeroDuration =
        NonZeroDuration::from_nonzero_secs(NonZeroU64::new(15).unwrap());

    const FENCING_MARGIN_DEFAULT: NonZeroDuration =
        NonZeroDuration::from_nonzero_secs(NonZeroU64::new(1).unwrap());

    const STATEMENT_TIMEOUT_DEFAULT: Option<NonZeroDuration> = None;
}

#[test]
fn a_migrations_lock_takes_the_profile_defaults() {
    let config = Config::from_env::<TestMigrationsLock>().expect("the defaults are valid");

    assert_eq!(
        config.timing.heartbeat().get(),
        std::time::Duration::from_secs(5)
    );
    assert_eq!(
        config.timing.time_to_live().get(),
        std::time::Duration::from_secs(15)
    );
    assert_eq!(
        config.timing.fencing_margin().get(),
        std::time::Duration::from_secs(1)
    );
    assert_eq!(
        config.timing.relied_on_for(),
        std::time::Duration::from_secs(14)
    );
    assert_eq!(
        config.statement_timeout.map(NonZeroDuration::get),
        Some(std::time::Duration::from_secs(3))
    );
}

#[test]
fn a_migrations_lock_with_no_room_for_a_renewal_is_refused() {
    let refused = Config::from_env::<TestMigrationsLockWithNoRoom>();

    let Err(error @ FromEnvError::Timing(_)) = refused else {
        panic!("the timing is refused");
    };
    assert_eq!(
        error.to_string(),
        "timing: heartbeat (15s) plus fencing margin (1s) must be less than time to live (15s)"
    );
}
