use super::*;

fn at(seconds: i64, nanos: u32) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp(seconds, nanos).expect("within chrono's range")
}

#[test]
fn the_range_is_inclusive_at_both_ends() {
    let floor = Timestamp::new(at(-62_167_219_200, 0)).expect("the floor");
    assert_eq!(floor.get().timestamp(), -62_167_219_200);

    let ceiling = Timestamp::new(at(253_402_300_799, 999_999_000)).expect("the ceiling");
    assert_eq!(ceiling.get().timestamp_subsec_micros(), 999_999);
}

#[test]
fn the_instants_just_outside_the_range_are_refused() {
    let before = Timestamp::new(at(-62_167_219_201, 0)).expect_err("before the floor");
    assert_eq!(before.instant.timestamp(), -62_167_219_201);

    let after = Timestamp::new(at(253_402_300_800, 0)).expect_err("past the ceiling");
    assert_eq!(after.instant.timestamp(), 253_402_300_800);
}

#[test]
fn instants_compare_and_order_as_instants() {
    let earlier = Timestamp::new(at(1_704_067_200, 0)).unwrap();
    let later = Timestamp::new(at(1_704_067_201, 0)).unwrap();
    let same = Timestamp::new(at(1_704_067_200, 0)).unwrap();

    assert_eq!(earlier, same);
    assert!(earlier < later);
    assert_eq!(earlier.max(later), later);
}

#[test]
fn the_error_names_the_range_and_the_instant() {
    let error =
        Timestamp::new(chrono::DateTime::<chrono::Utc>::MIN_UTC).expect_err("chrono's minimum");
    let text = error.to_string();
    assert!(
        text.contains("0000-01-01T00:00:00Z..=9999-12-31T23:59:59.999999Z"),
        "{text}"
    );
    assert!(text.contains("-262143"), "{text}");
}

#[cfg(feature = "serde")]
#[test]
fn serde_reads_rfc_3339_and_refuses_the_out_of_range() {
    let timestamp: Timestamp =
        serde_json::from_value(serde_json::json!("2024-01-01T00:00:00Z")).expect("instant");
    assert_eq!(timestamp.get().timestamp(), 1_704_067_200);
    assert_eq!(
        serde_json::to_value(timestamp).expect("json"),
        serde_json::json!("2024-01-01T00:00:00Z")
    );

    let error = serde_json::from_value::<Timestamp>(serde_json::json!("-262143-01-01T00:00:00Z"))
        .expect_err("chrono's minimum");
    assert!(error.to_string().contains("0000-01-01"), "{error}");
}

#[cfg(feature = "now")]
#[test]
fn now_is_the_present() {
    let before = chrono::Utc::now();
    let now = Timestamp::now();
    let after = chrono::Utc::now();

    assert!(before <= now.get() && now.get() <= after);
}
