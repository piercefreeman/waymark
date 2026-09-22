//! The eyre report's `Error` form: [`waymark_eyre_error::ReportError`].

impl crate::IntoError for color_eyre::eyre::Report {
    type Error = waymark_eyre_error::ReportError;

    fn as_dyn_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
        self.as_ref()
    }

    fn into_error(self) -> waymark_eyre_error::ReportError {
        waymark_eyre_error::ReportError(self)
    }
}

#[cfg(test)]
mod tests;
