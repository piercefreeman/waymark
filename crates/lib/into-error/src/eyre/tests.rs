use crate::IntoError as _;

#[derive(Debug, thiserror::Error)]
#[error("connection refused")]
struct Refused;

#[test]
fn a_report_is_seen_as_its_top_error_and_made_into_its_error_form() {
    let report = color_eyre::eyre::Report::new(Refused);
    assert!(report.as_dyn_error().downcast_ref::<Refused>().is_some());

    let wrapped = report.wrap_err("connect the database");
    assert!(wrapped.as_dyn_error().downcast_ref::<Refused>().is_none());
    assert_eq!(wrapped.into_error().to_string(), "connect the database");
}
