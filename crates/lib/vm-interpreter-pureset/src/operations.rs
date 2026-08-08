//! Operations requirements.

use waymark_vm_runtime_exception::IntermediateDetailsOf;

mod capture_copy;
mod dict;
mod dot;
mod exception;
mod index;
mod length;
mod list;
mod load_const;
mod scalar;
mod typed_exceptions;

pub use self::capture_copy::*;
pub use self::dict::*;
pub use self::dot::*;
pub use self::exception::*;
pub use self::index::*;
pub use self::length::*;
pub use self::list::*;
pub use self::load_const::*;
pub use self::scalar::*;

/// A unifying trait for all operations requirements.
#[waymark_blanket_impl_macros::blanket_impl]
pub trait Operations<Value>:
    CaptureCopy<Value>
    + AsScalarValue<Value>
    + BinaryOps<<Self as AsScalarValue<Value>>::ScalarValue>
    + UnaryOps<<Self as AsScalarValue<Value>>::ScalarValue>
    + MakeList<Value>
    + ListAppend<Value>
    + AsDictKey<Value>
    + MakeDict<Value>
    + AsExceptionTypeId<Value>
    + MakeException<Value>
    + Length<Value>
    + IndexOp<Value>
    + DotOp<Value>
where
    Value: waymark_vm_runtime_value::RootValueAccess,
{
}

/// The exception model the operations have to satisfy for the errors of
/// the failing operations to be raised as runtime exceptions.
///
/// A separate demand from [`Operations`], which users bound explicitly
/// alongside it — not every user of the operations raises: every error
/// the interpreter raises has to be a typed exception. Converting the
/// details payload into the value domain is the value's affair
/// ([`waymark_vm_runtime_exception::ExceptionFromIntermediate`]), bound
/// at the raise sites.
///
/// The operation traits leave their error types unconstrained — what an
/// error *is* belongs to the operations that define it.
#[waymark_blanket_impl_macros::blanket_impl]
pub trait Exceptions<Value>:
    AsScalarValue<Value, Error: waymark_vm_runtime_exception::TypedException>
    + BinaryOps<
        <Self as AsScalarValue<Value>>::ScalarValue,
        Error: waymark_vm_runtime_exception::TypedException,
    > + UnaryOps<
        <Self as AsScalarValue<Value>>::ScalarValue,
        Error: waymark_vm_runtime_exception::TypedException,
    > + AsDictKey<Value, Error: waymark_vm_runtime_exception::TypedException>
    + Length<
        Value,
        Error: waymark_vm_runtime_exception::TypedException,
        FromLengthError: waymark_vm_runtime_exception::TypedException,
    > + IndexOp<Value, Error: waymark_vm_runtime_exception::TypedException>
    + DotOp<Value, Error: waymark_vm_runtime_exception::TypedException>
where
    Value: waymark_vm_runtime_value::RootValueAccess,
{
}

/// The exception model the value has to satisfy for the errors of the
/// failing operations to be raised as runtime exceptions: the value
/// constructs the runtime exception from each raisable error's
/// intermediate details payload.
#[waymark_blanket_impl_macros::blanket_impl]
pub trait ExceptionValue<Operations>:
    waymark_vm_runtime_exception::ExceptionFromIntermediate<
        IntermediateDetailsOf<AsScalarValueErrorFor<Operations, Self>>,
    > + waymark_vm_runtime_exception::ExceptionFromIntermediate<
        IntermediateDetailsOf<BinaryOpsErrorFor<Operations, Self>>,
    > + waymark_vm_runtime_exception::ExceptionFromIntermediate<
        IntermediateDetailsOf<UnaryOpsErrorFor<Operations, Self>>,
    > + waymark_vm_runtime_exception::ExceptionFromIntermediate<
        IntermediateDetailsOf<AsDictKeyErrorFor<Operations, Self>>,
    > + waymark_vm_runtime_exception::ExceptionFromIntermediate<
        IntermediateDetailsOf<LengthErrorFor<Operations, Self>>,
    > + waymark_vm_runtime_exception::ExceptionFromIntermediate<
        IntermediateDetailsOf<FromLengthErrorFor<Operations, Self>>,
    > + waymark_vm_runtime_exception::ExceptionFromIntermediate<
        IntermediateDetailsOf<IndexOpErrorFor<Operations, Self>>,
    > + waymark_vm_runtime_exception::ExceptionFromIntermediate<
        IntermediateDetailsOf<DotOpErrorFor<Operations, Self>>,
    >
    // The provided operations carry concrete errors of their own; the
    // interpreter raises those too.
    + waymark_vm_runtime_exception::ExceptionFromIntermediate<
        IntermediateDetailsOf<MakeListError>,
    > + waymark_vm_runtime_exception::ExceptionFromIntermediate<
        IntermediateDetailsOf<ListAppendError>,
    > + waymark_vm_runtime_exception::ExceptionFromIntermediate<
        IntermediateDetailsOf<MakeDictError>,
    >
where
    Self: Sized,
    Operations: Exceptions<Self>,
{
}
