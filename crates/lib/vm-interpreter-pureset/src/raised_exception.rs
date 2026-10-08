//! Raised exception requirements.
//!
//! A pure operation that fails raises: the failure is a typed error, and
//! how it renders as the composition's raised exception - which class, with
//! which bases, carrying what - is that exception type's own business. The
//! interpreter asks only that each of its errors converts.

use crate::value::{
    AsDictKeyError, AsScalarError, BinaryOperationError, DotOperationError, FromLengthError,
    IndexOperationError, LengthError, ListAppendError, MakeDictError, MakeListError,
    UnaryOperationError,
};

/// A unifying trait for all raised exception requirements.
#[waymark_blanket_impl_macros::blanket_impl]
pub trait RaisedException:
    From<AsScalarError>
    + From<BinaryOperationError>
    + From<UnaryOperationError>
    + From<LengthError>
    + From<FromLengthError>
    + From<MakeListError>
    + From<ListAppendError>
    + From<AsDictKeyError>
    + From<MakeDictError>
    + From<IndexOperationError>
    + From<DotOperationError>
{
}
