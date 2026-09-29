use super::{BucketedHistogram, Quantile};

/// A three-bound ladder for the arithmetic.
const BOUNDS: [f64; 3] = [1.0, 10.0, 100.0];

/// Three counts at or below the bounds, then the total.
fn histogram(counts: [u64; 4]) -> BucketedHistogram<4> {
    BucketedHistogram { counts, sum: 0.0 }
}

#[test]
fn the_median_interpolates_within_its_bucket() {
    // 2 observations at or below 1, 8 at or below 10: the 5th of 10 sits
    // halfway through the second bucket.
    let median = histogram([2, 8, 10, 10]).quantile(&BOUNDS, Quantile::MEDIAN);

    assert_eq!(median, Some(1.0 + (10.0 - 1.0) * 0.5));
}

#[test]
fn the_first_bucket_interpolates_from_zero() {
    // Every observation is at or below the first bound: the median is
    // halfway from zero to it.
    let median = histogram([10, 10, 10, 10]).quantile(&BOUNDS, Quantile::MEDIAN);

    assert_eq!(median, Some(0.5));
}

#[test]
fn a_quantile_above_the_last_bound_is_unknown() {
    // Nine of ten observations are above the last bound: that bucket has
    // no upper edge to interpolate towards.
    let median = histogram([1, 1, 1, 10]).quantile(&BOUNDS, Quantile::MEDIAN);

    assert_eq!(median, None);
}

#[test]
fn nothing_observed_has_no_quantile() {
    let median = histogram([0, 0, 0, 0]).quantile(&BOUNDS, Quantile::MEDIAN);

    assert_eq!(median, None);
}

#[test]
fn the_last_bound_is_reached_exactly() {
    // The whole sample sits in the last bounded bucket: the top quantile
    // lands on that bound exactly, never past it.
    let top = histogram([0, 0, 10, 10]).quantile(&BOUNDS, Quantile::new(1.0).unwrap());

    assert_eq!(top, Some(100.0));
}

#[test]
fn a_fraction_outside_the_range_is_no_quantile() {
    assert!(Quantile::new(0.0).is_none());
    assert!(Quantile::new(1.0 + f64::EPSILON).is_none());
    assert!(Quantile::new(f64::NAN).is_none());
    assert!(Quantile::new(1.0).is_some());
}
