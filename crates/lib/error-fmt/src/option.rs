//! An optional source in an error message.

/// The `: {source}` tail of an error message for `source`, or nothing when
/// it is `None`.
///
/// ```
/// let some = Some("connection refused");
/// let none: Option<&str> = None;
/// assert_eq!(
///     format!("timed out{}", waymark_error_fmt::option::suffix(&some)),
///     "timed out: connection refused"
/// );
/// assert_eq!(format!("timed out{}", waymark_error_fmt::option::suffix(&none)), "timed out");
/// ```
pub fn suffix<Source>(source: &Option<Source>) -> Suffix<'_, Source> {
    Suffix(source)
}

/// What [`suffix`] renders.
pub struct Suffix<'a, Source>(&'a Option<Source>);

impl<Source> std::fmt::Display for Suffix<'_, Source>
where
    Source: std::fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some(source) => write!(f, ": {source}"),
            None => Ok(()),
        }
    }
}
