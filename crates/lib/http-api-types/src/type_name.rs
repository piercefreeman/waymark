//! A name carried by a type.

/// A name, as a type: what a schema component is called, passed where a
/// type parameter can go and a string can not.
//
// Const generics admit no strings on stable, so a wire type that is
// generic over what it carries takes its component's name as a marker
// type implementing this, chosen by the transport that serves it.
pub trait TypeName {
    /// The name.
    const NAME: &'static str;
}
