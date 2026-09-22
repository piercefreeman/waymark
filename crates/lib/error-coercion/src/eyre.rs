//! The eyre report's `Error` forms: [`waymark_eyre_error::ReportError`]
//! by value, the report's own error by reference.

impl crate::IntoError for color_eyre::eyre::Report {
    type Error = waymark_eyre_error::ReportError;

    fn into_error(self) -> waymark_eyre_error::ReportError {
        waymark_eyre_error::ReportError(self)
    }
}

impl crate::AsDynError for color_eyre::eyre::Report {
    fn as_dyn_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
        self.as_ref()
    }
}

#[cfg(test)]
mod tests;
