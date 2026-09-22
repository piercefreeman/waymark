use super::*;

#[derive(Debug, thiserror::Error)]
#[error("connection refused")]
struct Refused;

#[test]
fn a_report_is_transparently_the_error_it_holds() {
    let report = color_eyre::eyre::Report::new(Refused).wrap_err("connect the database");
    let error = ReportError::from(report);

    assert_eq!(error.to_string(), "connect the database");
    let source = std::error::Error::source(&error).expect("the wrapped error");
    assert_eq!(source.to_string(), "connection refused");
    assert!(std::error::Error::source(source).is_none());
}
