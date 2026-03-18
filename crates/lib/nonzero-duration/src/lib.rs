use std::error::Error;
use std::fmt;
use std::num::NonZeroU64;
use std::ops::{Add, Mul};
use std::time::Duration;

/// A [`Duration`] guaranteed to be strictly greater than zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct NonZeroDuration(Duration);

impl NonZeroDuration {
    /// Smallest representable non-zero duration (`1ns`).
    pub const MIN: Self = Self(Duration::from_nanos(1));

    /// Creates a new [`NonZeroDuration`] if `duration` is strictly positive.
    pub const fn new(duration: Duration) -> Option<Self> {
        if duration.is_zero() {
            None
        } else {
            Some(Self(duration))
        }
    }

    /// Creates a [`NonZeroDuration`] from whole seconds.
    pub const fn from_secs(secs: u64) -> Option<Self> {
        Self::new(Duration::from_secs(secs))
    }

    /// Creates a [`NonZeroDuration`] from non-zero whole seconds.
    pub const fn from_nonzero_secs(secs: NonZeroU64) -> Self {
        Self(Duration::from_secs(secs.get()))
    }

    /// Creates a [`NonZeroDuration`] from whole milliseconds.
    pub const fn from_millis(millis: u64) -> Option<Self> {
        Self::new(Duration::from_millis(millis))
    }

    /// Creates a [`NonZeroDuration`] from non-zero whole milliseconds.
    pub const fn from_nonzero_millis(millis: NonZeroU64) -> Self {
        Self(Duration::from_millis(millis.get()))
    }

    /// Creates a [`NonZeroDuration`] from whole microseconds.
    pub const fn from_micros(micros: u64) -> Option<Self> {
        Self::new(Duration::from_micros(micros))
    }

    /// Creates a [`NonZeroDuration`] from non-zero whole microseconds.
    pub const fn from_nonzero_micros(micros: NonZeroU64) -> Self {
        Self(Duration::from_micros(micros.get()))
    }

    /// Creates a [`NonZeroDuration`] from whole nanoseconds.
    pub const fn from_nanos(nanos: u64) -> Option<Self> {
        Self::new(Duration::from_nanos(nanos))
    }

    /// Creates a [`NonZeroDuration`] from non-zero whole nanoseconds.
    pub const fn from_nonzero_nanos(nanos: NonZeroU64) -> Self {
        Self(Duration::from_nanos(nanos.get()))
    }

    /// Returns the wrapped [`Duration`].
    pub const fn get(self) -> Duration {
        self.0
    }

    /// Returns this duration as whole milliseconds.
    pub const fn as_millis(self) -> u128 {
        self.0.as_millis()
    }

    /// Returns this duration as whole microseconds.
    pub const fn as_micros(self) -> u128 {
        self.0.as_micros()
    }

    /// Returns this duration as whole nanoseconds.
    pub const fn as_nanos(self) -> u128 {
        self.0.as_nanos()
    }

    /// Returns this duration as seconds in `f64`.
    pub fn as_secs_f64(self) -> f64 {
        self.0.as_secs_f64()
    }

    /// Checked multiplication that preserves the non-zero invariant.
    pub fn checked_mul(self, rhs: u32) -> Option<Self> {
        if rhs == 0 {
            return None;
        }

        self.0.checked_mul(rhs).map(Self)
    }
}

impl TryFrom<Duration> for NonZeroDuration {
    type Error = ZeroDurationError;

    fn try_from(value: Duration) -> Result<Self, Self::Error> {
        Self::new(value).ok_or(ZeroDurationError)
    }
}

impl From<NonZeroDuration> for Duration {
    fn from(value: NonZeroDuration) -> Self {
        value.0
    }
}

impl AsRef<Duration> for NonZeroDuration {
    fn as_ref(&self) -> &Duration {
        &self.0
    }
}

impl Add<Duration> for NonZeroDuration {
    type Output = NonZeroDuration;

    fn add(self, rhs: Duration) -> Self::Output {
        NonZeroDuration(self.0 + rhs)
    }
}

impl Add<NonZeroDuration> for NonZeroDuration {
    type Output = NonZeroDuration;

    fn add(self, rhs: NonZeroDuration) -> Self::Output {
        NonZeroDuration(self.0 + rhs.0)
    }
}

impl Mul<u32> for NonZeroDuration {
    type Output = Duration;

    fn mul(self, rhs: u32) -> Self::Output {
        self.0 * rhs
    }
}

/// Error returned when attempting to construct a [`NonZeroDuration`] from zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ZeroDurationError;

impl fmt::Display for ZeroDurationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("duration must be non-zero")
    }
}

impl Error for ZeroDurationError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_rejects_zero() {
        assert_eq!(NonZeroDuration::new(Duration::ZERO), None);
    }

    #[test]
    fn new_accepts_positive_duration() {
        let value = NonZeroDuration::new(Duration::from_millis(1));
        assert_eq!(
            value.map(NonZeroDuration::get),
            Some(Duration::from_millis(1))
        );
    }

    #[test]
    fn min_is_one_nanosecond() {
        assert_eq!(NonZeroDuration::MIN.get(), Duration::from_nanos(1));
    }

    #[test]
    fn from_parts_builders_preserve_invariant() {
        assert_eq!(NonZeroDuration::from_secs(0), None);
        assert_eq!(NonZeroDuration::from_millis(0), None);
        assert_eq!(NonZeroDuration::from_micros(0), None);
        assert_eq!(NonZeroDuration::from_nanos(0), None);

        assert_eq!(
            NonZeroDuration::from_secs(2).map(NonZeroDuration::get),
            Some(Duration::from_secs(2))
        );
        assert_eq!(
            NonZeroDuration::from_millis(2).map(NonZeroDuration::get),
            Some(Duration::from_millis(2))
        );
        assert_eq!(
            NonZeroDuration::from_micros(2).map(NonZeroDuration::get),
            Some(Duration::from_micros(2))
        );
        assert_eq!(
            NonZeroDuration::from_nanos(2).map(NonZeroDuration::get),
            Some(Duration::from_nanos(2))
        );
    }

    #[test]
    fn nonzero_numeric_builders_are_infallible() {
        let secs = NonZeroU64::new(2).expect("non-zero");
        let millis = NonZeroU64::new(3).expect("non-zero");
        let micros = NonZeroU64::new(4).expect("non-zero");
        let nanos = NonZeroU64::new(5).expect("non-zero");

        assert_eq!(
            NonZeroDuration::from_nonzero_secs(secs).get(),
            Duration::from_secs(2)
        );
        assert_eq!(
            NonZeroDuration::from_nonzero_millis(millis).get(),
            Duration::from_millis(3)
        );
        assert_eq!(
            NonZeroDuration::from_nonzero_micros(micros).get(),
            Duration::from_micros(4)
        );
        assert_eq!(
            NonZeroDuration::from_nonzero_nanos(nanos).get(),
            Duration::from_nanos(5)
        );
    }

    #[test]
    fn try_from_duration() {
        assert_eq!(
            NonZeroDuration::try_from(Duration::from_secs(1)),
            Ok(NonZeroDuration(Duration::from_secs(1)))
        );
        assert_eq!(
            NonZeroDuration::try_from(Duration::ZERO),
            Err(ZeroDurationError)
        );
    }

    #[test]
    fn checked_mul_rejects_zero_multiplier() {
        let input = NonZeroDuration::from_millis(5).expect("non-zero duration");
        assert_eq!(input.checked_mul(0), None);
    }

    #[test]
    fn checked_mul_keeps_nonzero_result() {
        let input = NonZeroDuration::from_millis(5).expect("non-zero duration");
        let out = input.checked_mul(3).expect("non-zero duration");
        assert_eq!(out.get(), Duration::from_millis(15));
    }

    #[test]
    fn add_duration_preserves_nonzero() {
        let input = NonZeroDuration::from_millis(5).expect("non-zero duration");
        let out = input + Duration::from_millis(10);
        assert_eq!(out.get(), Duration::from_millis(15));
    }

    #[test]
    fn add_nonzero_duration_preserves_nonzero() {
        let left = NonZeroDuration::from_millis(5).expect("non-zero duration");
        let right = NonZeroDuration::from_millis(10).expect("non-zero duration");
        let out = left + right;
        assert_eq!(out.get(), Duration::from_millis(15));
    }
}
