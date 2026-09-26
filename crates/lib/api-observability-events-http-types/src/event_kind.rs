//! The name of the event kinds' component.

/// The name of the component listing the event kinds: `EventKind`.
//
// The transports serve whatever payload their backend has, and every
// payload's kinds go under this one name — so it is a marker chosen
// here, not a property of a kind type.
#[derive(Debug)]
pub struct EventKind;

impl waymark_http_api_types::TypeName for EventKind {
    const NAME: &'static str = "EventKind";
}
