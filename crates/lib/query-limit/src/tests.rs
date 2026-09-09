use super::*;

#[test]
fn within_the_cap_is_a_limit() {
    assert_eq!(Limit::<10>::new(1).expect("one").get(), 1);
    assert_eq!(Limit::<10>::new(10).expect("the cap").get(), 10);
    assert_eq!(
        Limit::<10>::new(10).expect("the cap").get_nonzero().get(),
        10
    );
}

#[test]
fn zero_and_above_the_cap_are_errors() {
    let error = Limit::<10>::new(0).expect_err("zero");
    assert_eq!(error.value, 0);
    assert_eq!(error.to_string(), "limit must be within 1..=10, got 0");

    let error = Limit::<10>::new(11).expect_err("above the cap");
    assert_eq!(error.value, 11);
    assert_eq!(error.to_string(), "limit must be within 1..=10, got 11");
}
