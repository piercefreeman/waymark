use super::*;
use crate::test_helpers::TaskError;

fn breaker_and_loop() -> Report<TaskError> {
    Report {
        ended: vec![
            End {
                name: "breaker",
                result: Err(Cause::Error(TaskError::Broke)),
                before_shutdown: true,
            },
            End {
                name: "loop",
                result: Err(Cause::Error(TaskError::Shutdown)),
                before_shutdown: false,
            },
        ],
    }
}

fn loop_only() -> Report<TaskError> {
    Report {
        ended: vec![End {
            name: "loop",
            result: Err(Cause::Error(TaskError::Shutdown)),
            before_shutdown: false,
        }],
    }
}

#[test]
fn display_lists_every_end() {
    insta::assert_snapshot!(breaker_and_loop().to_string(), @r"
    2 tasks ended, 1 of them before shutdown was requested
      breaker (before shutdown): broke
      loop (after shutdown): shutdown observed
    ");
}

#[test]
fn display_shows_a_return() {
    let report = Report::<TaskError> {
        ended: vec![End {
            name: "early",
            result: Ok(()),
            before_shutdown: true,
        }],
    };

    insta::assert_snapshot!(report.to_string(), @r"
    1 tasks ended, 1 of them before shutdown was requested
      early (before shutdown): returned
    ");
}

#[test]
fn into_result_follows_any_before_shutdown() {
    assert!(!loop_only().any_before_shutdown());
    assert!(loop_only().into_result().is_ok());

    assert!(breaker_and_loop().any_before_shutdown());
    assert!(breaker_and_loop().into_result().is_err());
}
