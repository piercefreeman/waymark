use core::str::FromStr;
use std::time::Duration;

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

pub struct FromMillisMin<T, const MIN: u64>(pub T);

impl<const MIN: u64> FromStr for FromMillisMin<Duration, MIN> {
    type Err = core::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let ms: u64 = s.parse()?;
        let ms = ms.max(MIN);
        Ok(Self(Duration::from_millis(ms)))
    }
}

impl<T> From<T> for FromMillis<T> {
    fn from(value: T) -> Self {
        Self(value)
    }
}

impl<T, const MIN: u64> From<T> for FromMillisMin<T, MIN> {
    fn from(value: T) -> Self {
        Self(value)
    }
}
