use core::str::FromStr;
use std::time::Duration;

use waymark_nonzero_duration::NonZeroDuration;

#[derive(Default)]
pub struct CommaSeparated<T>(pub Vec<T>);

impl<T> FromStr for CommaSeparated<T>
where
    T: FromStr,
{
    type Err = T::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let vecs: Vec<_> = s
            .split(',')
            .map(|item| item.trim())
            .filter(|item| !item.is_empty())
            .map(|item| T::from_str(item))
            .collect::<Result<_, _>>()?;

        Ok(Self(vecs))
    }
}

pub struct FromMillis<T>(pub T);

impl FromStr for FromMillis<Duration> {
    type Err = core::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(Duration::from_millis(s.parse()?)))
    }
}

impl FromStr for FromMillis<NonZeroDuration> {
    type Err = core::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(NonZeroDuration::from_nonzero_millis(s.parse()?)))
    }
}

pub struct FromNanos<T>(pub T);

impl FromStr for FromNanos<NonZeroDuration> {
    type Err = core::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(NonZeroDuration::from_nonzero_nanos(s.parse()?)))
    }
}

impl<T> From<T> for FromMillis<T> {
    fn from(value: T) -> Self {
        Self(value)
    }
}
