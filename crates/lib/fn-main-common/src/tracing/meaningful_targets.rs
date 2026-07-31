//! `RUST_LOG`-style filter targets that only parse from meaningful
//! values.

/// [`Targets`](tracing_subscriber::filter::Targets) that parse only from
/// a meaningful value: empty or whitespace-only input is rejected — bare
/// `Targets` would "parse" the empty string into a bare-ERROR filter
/// that near-silently disables the fmt output.
#[derive(Debug)]
pub struct MeaningfulTargets(pub tracing_subscriber::filter::Targets);

impl MeaningfulTargets {
    /// The default-INFO filter, for when the variable is not set.
    pub fn info_default() -> Self {
        Self(
            tracing_subscriber::filter::Targets::new()
                .with_default(tracing_subscriber::filter::LevelFilter::INFO),
        )
    }
}

/// Error parsing [`MeaningfulTargets`].
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    /// The value is empty or whitespace-only.
    #[error("the value is empty")]
    Empty,

    /// The value could not be parsed as filter targets.
    #[error("{0}")]
    Targets(#[source] <tracing_subscriber::filter::Targets as std::str::FromStr>::Err),
}

impl std::str::FromStr for MeaningfulTargets {
    type Err = ParseError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        if raw.trim().is_empty() {
            return Err(ParseError::Empty);
        }
        raw.parse().map(Self).map_err(ParseError::Targets)
    }
}
