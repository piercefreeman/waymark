use std::time::{Duration, Instant};

use super::*;

#[test]
fn the_first_reading_becomes_the_baseline() {
    let mut rate = CounterRate::new();
    let start = Instant::now();
    assert!(matches!(
        rate.observe(start, 10_u64),
        Ok(Observation::First)
    ));
    assert!(matches!(
        rate.observe(start, 10_u64),
        Ok(Observation::Unchanged)
    ));
}

#[test]
fn an_advanced_reading_yields_the_window_and_moves_the_baseline() {
    let mut rate = CounterRate::new();
    let start = Instant::now();
    rate.observe(start, 10_u64).expect("first reading");
    let later = start + Duration::from_secs(2);
    let Ok(Observation::Advanced(window)) = rate.observe(later, 16_u64) else {
        panic!("a newer reading yields a window");
    };
    assert_eq!(window.delta, 6);
    assert!(
        (window.per_second - 3.0).abs() < 1e-9,
        "{}",
        window.per_second
    );
    assert!(matches!(
        rate.observe(later, 16_u64),
        Ok(Observation::Unchanged)
    ));
}

#[test]
fn an_older_reading_is_refused_and_the_baseline_stays() {
    let mut rate = CounterRate::new();
    let start = Instant::now();
    let later = start + Duration::from_secs(1);
    rate.observe(later, 10_u64).expect("first reading");
    assert!(matches!(
        rate.observe(start, 12_u64),
        Err(ObserveError::Regressed)
    ));
    assert!(matches!(
        rate.observe(later, 10_u64),
        Ok(Observation::Unchanged)
    ));
}

#[test]
fn a_counter_reset_reads_as_zero_growth() {
    let mut rate = CounterRate::new();
    let start = Instant::now();
    rate.observe(start, 10_u64).expect("first reading");
    let later = start + Duration::from_secs(1);
    let Ok(Observation::Advanced(window)) = rate.observe(later, 3_u64) else {
        panic!("a newer reading yields a window");
    };
    assert_eq!(window.delta, 0);
    assert!(window.per_second.abs() < 1e-9, "{}", window.per_second);
}

#[cfg(feature = "chrono")]
#[test]
fn a_zero_second_window_is_floored_rather_than_divided_by() {
    let mut rate = CounterRate::new();
    // 2016-12-31T23:59:60Z, the leap second, then 2017-01-01T00:00:00Z:
    // the second compares as later, and the two are zero seconds apart.
    let leap_second = chrono::DateTime::<chrono::Utc>::from_timestamp(1_483_228_799, 1_000_000_000)
        .expect("a leap second, on a second ending in :59");
    let next_day =
        chrono::DateTime::<chrono::Utc>::from_timestamp(1_483_228_800, 0).expect("in range");
    rate.observe(leap_second, 0_u64).expect("first reading");

    let Ok(Observation::Advanced(window)) = rate.observe(next_day, 1_u64) else {
        panic!("a later reading yields a window");
    };

    assert_eq!(window.delta, 1);
    assert_eq!(window.per_second, 1.0 / f64::EPSILON);
}

#[cfg(feature = "chrono")]
#[test]
fn a_sub_millisecond_window_measures_its_real_spread() {
    let mut rate = CounterRate::new();
    let start =
        chrono::DateTime::<chrono::Utc>::from_timestamp(1_700_000_000, 0).expect("in range");
    rate.observe(start, 0_u64).expect("first reading");
    let later = start + chrono::TimeDelta::microseconds(500);
    let Ok(Observation::Advanced(window)) = rate.observe(later, 1_u64) else {
        panic!("a newer reading yields a window");
    };
    assert!(
        (window.per_second - 2000.0).abs() < 1e-6,
        "{}",
        window.per_second
    );
}
