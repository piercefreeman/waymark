//! Tests of the migrations lock timing's validation.

use std::time::Duration;

use waymark_nonzero_duration::NonZeroDuration;

use super::{MigrationsLockTiming, ValidateError};

/// A timing of whole seconds.
fn timing(heartbeat: u64, time_to_live: u64, fencing_margin: u64) -> MigrationsLockTiming {
    let seconds = |seconds| NonZeroDuration::from_secs(seconds).expect("seconds are not zero");

    MigrationsLockTiming {
        heartbeat: seconds(heartbeat),
        time_to_live: seconds(time_to_live),
        fencing_margin: seconds(fencing_margin),
    }
}

#[test]
fn timing_with_room_for_a_renewal_is_valid() {
    let valid = timing(5, 15, 1).validate().expect("the timing is valid");

    assert_eq!(valid.heartbeat().get(), Duration::from_secs(5));
    assert_eq!(valid.time_to_live().get(), Duration::from_secs(15));
    assert_eq!(valid.relied_on_for(), Duration::from_secs(14));
}

#[test]
fn heartbeat_just_under_the_time_relied_on_is_valid() {
    let valid = timing(13, 15, 1).validate().expect("the timing is valid");

    assert_eq!(valid.relied_on_for(), Duration::from_secs(14));
}

#[test]
fn heartbeat_equal_to_the_time_relied_on_is_refused() {
    let refused = timing(14, 15, 1).validate();

    assert!(matches!(
        refused,
        Err(ValidateError::NoRoomForARenewal { .. })
    ));
}

#[test]
fn fencing_margin_longer_than_the_time_to_live_is_refused() {
    let refused = timing(1, 15, 20).validate();

    assert!(matches!(
        refused,
        Err(ValidateError::NoRoomForARenewal { .. })
    ));
}

#[test]
fn refusal_shows_the_three_durations() {
    let refused = timing(14, 15, 1)
        .validate()
        .expect_err("the timing is refused");

    assert_eq!(
        refused.to_string(),
        "heartbeat (14s) plus fencing margin (1s) must be less than time to live (15s)"
    );
}
