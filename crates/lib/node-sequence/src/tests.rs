use super::*;

#[test]
fn counter_starts_at_zero_and_advances_by_one() {
    let counter = NodeSequenceCounter::new();
    let first = counter.next();
    let second = counter.next();
    let third = counter.next();
    assert_eq!(first.get(), 0);
    assert_eq!(second.get(), 1);
    assert_eq!(third.get(), 2);
    assert!(first < second && second < third);
}

#[test]
fn counters_are_independent_streams() {
    let one = NodeSequenceCounter::new();
    let other = NodeSequenceCounter::new();
    let _ = one.next();
    assert_eq!(one.next().get(), 1);
    assert_eq!(other.next().get(), 0);
}

#[test]
fn a_persisted_position_reads_back_as_itself() {
    let minted = NodeSequenceCounter::new().next();
    let persisted = NodeSequence::from_persisted(minted.get());
    assert_eq!(persisted, minted);
}
