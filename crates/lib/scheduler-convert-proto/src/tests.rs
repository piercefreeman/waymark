use std::num::NonZeroU64;

use waymark_convert_core::TryConvert as _;
use waymark_proto::messages as proto;

use super::{Converter, ScheduleDefinitionError};

fn wire_definition(
    schedule: proto::schedule_definition::Schedule,
    jitter_seconds: i64,
    allow_duplicate: bool,
) -> proto::ScheduleDefinition {
    proto::ScheduleDefinition {
        schedule: Some(schedule),
        jitter_seconds,
        allow_duplicate,
    }
}

#[test]
fn converts_a_cron_definition() {
    let wire = wire_definition(
        proto::schedule_definition::Schedule::CronExpression("0 * * * *".to_owned()),
        5,
        true,
    );
    let definition: waymark_scheduler_core::ScheduleDefinition =
        Converter::try_convert(&wire).unwrap();
    let waymark_scheduler_core::Schedule::CronExpression(expression) = &definition.schedule else {
        panic!("expected a cron schedule");
    };
    assert_eq!(expression.as_str(), "0 * * * *");
    assert_eq!(definition.jitter_seconds, 5);
    assert!(definition.allow_duplicate);
}

#[test]
fn converts_an_interval_definition() {
    let wire = wire_definition(
        proto::schedule_definition::Schedule::IntervalSeconds(3600),
        0,
        false,
    );
    let definition: waymark_scheduler_core::ScheduleDefinition =
        Converter::try_convert(&wire).unwrap();
    assert_eq!(
        definition.schedule,
        waymark_scheduler_core::Schedule::IntervalSeconds(NonZeroU64::new(3600).unwrap())
    );
}

#[test]
fn rejects_a_missing_schedule() {
    let wire = proto::ScheduleDefinition {
        schedule: None,
        jitter_seconds: 0,
        allow_duplicate: false,
    };
    let result: Result<waymark_scheduler_core::ScheduleDefinition, _> =
        Converter::try_convert(&wire);
    assert!(matches!(
        result,
        Err(ScheduleDefinitionError::MissingSchedule)
    ));
}

#[test]
fn rejects_bad_cron_and_bad_numbers() {
    let bad_cron = wire_definition(
        proto::schedule_definition::Schedule::CronExpression("nope".to_owned()),
        0,
        false,
    );
    let result: Result<waymark_scheduler_core::ScheduleDefinition, _> =
        Converter::try_convert(&bad_cron);
    assert!(matches!(
        result,
        Err(ScheduleDefinitionError::InvalidCronExpression(_))
    ));

    for seconds in [0, -1] {
        let bad_interval = wire_definition(
            proto::schedule_definition::Schedule::IntervalSeconds(seconds),
            0,
            false,
        );
        let result: Result<waymark_scheduler_core::ScheduleDefinition, _> =
            Converter::try_convert(&bad_interval);
        assert!(matches!(
            result,
            Err(ScheduleDefinitionError::NonPositiveIntervalSeconds { .. })
        ));
    }

    let bad_jitter = wire_definition(
        proto::schedule_definition::Schedule::IntervalSeconds(60),
        -1,
        false,
    );
    let result: Result<waymark_scheduler_core::ScheduleDefinition, _> =
        Converter::try_convert(&bad_jitter);
    assert!(matches!(
        result,
        Err(ScheduleDefinitionError::NegativeJitterSeconds { .. })
    ));
}

#[test]
fn definitions_round_trip_through_the_wire() {
    for schedule in [
        proto::schedule_definition::Schedule::CronExpression("*/15 * * * *".to_owned()),
        proto::schedule_definition::Schedule::IntervalSeconds(90),
    ] {
        let wire = wire_definition(schedule, 7, true);
        let definition: waymark_scheduler_core::ScheduleDefinition =
            Converter::try_convert(&wire).unwrap();
        let round_tripped: proto::ScheduleDefinition = Converter::try_convert(&definition).unwrap();
        assert_eq!(round_tripped, wire);
    }
}

#[test]
fn statuses_convert_both_ways() {
    for (wire, domain) in [
        (
            proto::ScheduleStatus::Active,
            waymark_scheduler_core::ScheduleStatus::Active,
        ),
        (
            proto::ScheduleStatus::Paused,
            waymark_scheduler_core::ScheduleStatus::Paused,
        ),
    ] {
        let converted: waymark_scheduler_core::ScheduleStatus =
            Converter::try_convert(wire).unwrap();
        assert_eq!(converted, domain);
        let back: proto::ScheduleStatus = Converter::try_convert(domain).unwrap();
        assert_eq!(back, wire);
    }

    let unspecified: Result<waymark_scheduler_core::ScheduleStatus, _> =
        Converter::try_convert(proto::ScheduleStatus::Unspecified);
    assert!(unspecified.is_err());
}

#[test]
fn timestamps_carry_seconds_and_nanos() {
    let instant = chrono::DateTime::from_timestamp(1_700_000_000, 123_456_789).unwrap();
    let timestamp: prost_wkt_types::Timestamp = Converter::try_convert(instant).unwrap();
    assert_eq!(timestamp.seconds, 1_700_000_000);
    assert_eq!(timestamp.nanos, 123_456_789);
}
