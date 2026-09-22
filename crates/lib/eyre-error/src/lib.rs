//! The eyre report as an `Error`.

#![warn(missing_docs)]

/// A report as an error: transparently the error the report holds, for
/// wherever an `Error` is required, which the report itself is not.
#[derive(Debug)]
pub struct ReportError(pub color_eyre::eyre::Report);

impl std::fmt::Display for ReportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl std::error::Error for ReportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        AsRef::<dyn std::error::Error + Send + Sync>::as_ref(&self.0).source()
    }
}

impl From<color_eyre::eyre::Report> for ReportError {
    fn from(report: color_eyre::eyre::Report) -> Self {
        Self(report)
    }
}

#[cfg(test)]
mod tests;
