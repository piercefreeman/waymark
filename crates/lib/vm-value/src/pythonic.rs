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

pub(crate) fn normalized_index(index: i64, len: usize) -> Option<usize> {
    if index >= 0 {
        let index = usize::try_from(index).ok()?;
        return (index < len).then_some(index);
    }

    let distance_from_end = usize::try_from(index.unsigned_abs()).ok()?;
    len.checked_sub(distance_from_end)
}

#[cfg(test)]
mod tests {
    use super::normalized_index;

    #[test]
    fn normalized_index_matches_python_style_indexing() {
        assert_eq!(normalized_index(0, 3), Some(0));
        assert_eq!(normalized_index(2, 3), Some(2));
        assert_eq!(normalized_index(3, 3), None);
        assert_eq!(normalized_index(-1, 3), Some(2));
        assert_eq!(normalized_index(-3, 3), Some(0));
        assert_eq!(normalized_index(-4, 3), None);
        assert_eq!(normalized_index(0, 0), None);
        assert_eq!(normalized_index(i64::MIN, 3), None);
    }
}
