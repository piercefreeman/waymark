use super::*;

fn count(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("test counts are non-zero")
}

#[test]
fn a_new_counter_has_one_owner() {
    let counter = AtomicOwnersCounter::new();

    assert_eq!(counter.strong(), count(1));
    assert_eq!(counter.weak(), 0);
}

#[test]
fn cloning_adds_an_owner_and_dropping_removes_it() {
    let counter = AtomicOwnersCounter::new();
    let other = counter.clone();
    assert_eq!(counter.strong(), count(2));
    assert_eq!(other.strong(), count(2));

    drop(other);
    assert_eq!(counter.strong(), count(1));
}

#[test]
fn a_weak_observer_sees_the_owners_without_being_one() {
    let counter = AtomicOwnersCounter::new();
    let weak = counter.downgrade();

    assert_eq!(counter.strong(), count(1));
    assert_eq!(counter.weak(), 1);
    assert_eq!(weak.strong(), 1);
    assert_eq!(weak.clone().weak(), 2);
}

#[test]
fn upgrading_adds_an_owner_while_one_remains() {
    let counter = AtomicOwnersCounter::new();
    let weak = counter.downgrade();

    let upgraded = weak.upgrade().expect("an owner remains");
    assert_eq!(counter.strong(), count(2));

    drop(upgraded);
    drop(counter);
    assert_eq!(weak.strong(), 0);
    assert!(weak.upgrade().is_none());
}
