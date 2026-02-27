pub use waymark_observability_macros::obs;

#[doc(hidden)]
pub mod __inner {
    pub mod tracing {
        pub use tracing::instrument;
    }
}
