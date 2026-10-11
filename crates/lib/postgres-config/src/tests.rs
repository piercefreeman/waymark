use super::*;

#[test]
fn a_zero_statement_timeout_turns_the_timeout_off() {
    assert!(statement_timeout_from_millis(0).is_none());
    assert_eq!(
        statement_timeout_from_millis(2_500).map(NonZeroDuration::get),
        Some(std::time::Duration::from_millis(2_500)),
    );
}
