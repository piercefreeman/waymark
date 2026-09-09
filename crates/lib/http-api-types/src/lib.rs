//! Wire types shared by the API HTTP transports: the shapes every domain
//! serves the same way, so that they are served, and documented, the same
//! way.
//!
//! A transport keeps its own domain types and reaches here for the
//! proxies that put a domain value on the wire — an id, a duration —
//! without the domain crate knowing the wire.
//!
//! The doc comments on the wire types and their fields are the schema
//! descriptions in the OpenAPI document, so they speak to the API's
//! consumer; what an implementer needs to know is in plain comments.

#![warn(missing_docs)]

mod nonzero_seconds;
mod type_name;
mod uuid_id;

pub use self::nonzero_seconds::*;
pub use self::type_name::*;
pub use self::uuid_id::*;

#[cfg(test)]
mod test_helpers;
