//! Internal helpers for Python-style arithmetic semantics.

use typed_floats::NonNaNFinite;

pub(crate) fn checked_floor_div_i64(left: i64, right: i64) -> Option<i64> {
    let left = i128::from(left);
    let right = i128::from(right);

    let mut quotient = left / right;
    let remainder = left % right;

    if remainder != 0 && (remainder > 0) != (right > 0) {
        quotient -= 1;
    }

    quotient.try_into().ok()
}

pub(crate) fn checked_modulo_i64(left: i64, right: i64) -> Option<i64> {
    let left = i128::from(left);
    let right = i128::from(right);

    let mut remainder = left % right;

    if remainder != 0 && (remainder > 0) != (right > 0) {
        remainder += right;
    }

    remainder.try_into().ok()
}

pub(crate) fn checked_floor_div_float(
    left: NonNaNFinite,
    right: NonNaNFinite,
) -> Option<NonNaNFinite> {
    (left.get() / right.get()).floor().try_into().ok()
}

pub(crate) fn checked_modulo_float(
    left: NonNaNFinite,
    right: NonNaNFinite,
) -> Option<NonNaNFinite> {
    let right = right.get();
    let mut remainder = left.get() % right;

    if remainder != 0.0 && (remainder > 0.0) != (right > 0.0) {
        remainder += right;
    }

    remainder.try_into().ok()
}
