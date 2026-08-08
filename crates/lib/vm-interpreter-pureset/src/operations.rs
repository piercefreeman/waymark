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
/// alongside it — not every user of the operations raises: every error the interpreter raises has to be a
/// typed exception, and the operations have to convert its details
/// payload into the value domain. The payload type stays the
/// operations' choice.
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
    + waymark_vm_runtime_exception::ExceptionFromIntermediate<
            IntermediateDetailsOf<AsScalarValueErrorFor<Self, Value>>,
            Value,
        > + waymark_vm_runtime_exception::ExceptionFromIntermediate<
            IntermediateDetailsOf<BinaryOpsErrorFor<Self, Value>>,
            Value,
        > + waymark_vm_runtime_exception::ExceptionFromIntermediate<
            IntermediateDetailsOf<UnaryOpsErrorFor<Self, Value>>,
            Value,
        > + waymark_vm_runtime_exception::ExceptionFromIntermediate<
            IntermediateDetailsOf<AsDictKeyErrorFor<Self, Value>>,
            Value,
        > + waymark_vm_runtime_exception::ExceptionFromIntermediate<
            IntermediateDetailsOf<LengthErrorFor<Self, Value>>,
            Value,
        > + waymark_vm_runtime_exception::ExceptionFromIntermediate<
            IntermediateDetailsOf<FromLengthErrorFor<Self, Value>>,
            Value,
        > + waymark_vm_runtime_exception::ExceptionFromIntermediate<
            IntermediateDetailsOf<IndexOpErrorFor<Self, Value>>,
            Value,
        > + waymark_vm_runtime_exception::ExceptionFromIntermediate<
            IntermediateDetailsOf<DotOpErrorFor<Self, Value>>,
            Value,
        >
    // The provided operations carry concrete errors of their own; the
    // interpreter raises those too.
    + waymark_vm_runtime_exception::ExceptionFromIntermediate<
            IntermediateDetailsOf<MakeListError>,
            Value,
        > + waymark_vm_runtime_exception::ExceptionFromIntermediate<
            IntermediateDetailsOf<ListAppendError>,
            Value,
        > + waymark_vm_runtime_exception::ExceptionFromIntermediate<
            IntermediateDetailsOf<MakeDictError>,
            Value,
        >
where
    Value: waymark_vm_runtime_value::RootValueAccess,
{
}
