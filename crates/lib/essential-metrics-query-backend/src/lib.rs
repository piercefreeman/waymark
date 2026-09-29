//! Backend traits for querying the essential-metrics store.

#![warn(missing_docs)]

mod common;
pub mod latest;
pub mod series;

pub use self::common::*;

pub use self::latest::Latest;
pub use self::series::Series;
