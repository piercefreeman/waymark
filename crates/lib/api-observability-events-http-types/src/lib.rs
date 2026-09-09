//! Wire types of the observability events shared by the API HTTP
//! transports: what more than one transport serves about an event, so
//! that it is served, and documented, the same way everywhere.
//!
//! The doc comments on the wire types are the schema descriptions in the
//! OpenAPI document, so they speak to the API's consumer; what an
//! implementer needs to know is in plain comments.

#![warn(missing_docs)]

mod event_kind;
mod kind_tag;

pub use self::event_kind::*;
pub use self::kind_tag::*;
