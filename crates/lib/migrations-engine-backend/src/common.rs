//! Traits shared by the backend interface.

/// Common base: the backend is associated with the connection type the
/// migrations run through.
pub trait HasConnection {
    /// The connection type the migrations run through.
    type Connection;
}
