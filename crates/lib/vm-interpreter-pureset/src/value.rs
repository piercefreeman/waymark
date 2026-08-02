//! Value requirements.

mod capture_copy;
mod dict;
mod dot;
mod exception;
mod index;
mod length;
mod list;
mod load_const;
mod scalar;

pub use self::capture_copy::*;
pub use self::dict::*;
pub use self::dot::*;
pub use self::exception::*;
pub use self::index::*;
pub use self::length::*;
pub use self::list::*;
pub use self::load_const::*;
pub use self::scalar::*;

/// A unifying trait for all value requirements.
pub trait Value:
    waymark_vm_runtime_value::RootValueAccess<RootValue = Self>
    + CaptureCopy
    + AsScalar
    + MakeList
    + ListAppend
    + AsDictKey
    + MakeDict
    + AsExceptionTypeId
    + MakeException
    + Length
    + IndexOp
    + DotOp
{
}

impl<T> Value for T where
    T: waymark_vm_runtime_value::RootValueAccess<RootValue = Self>
        + CaptureCopy
        + AsScalar
        + MakeList
        + ListAppend
        + AsDictKey
        + MakeDict
        + AsExceptionTypeId
        + MakeException
        + Length
        + IndexOp
        + DotOp
{
}
