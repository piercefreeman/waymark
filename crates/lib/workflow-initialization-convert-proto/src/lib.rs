//! [`Convert`](waymark_convert_core::Convert) implementation for turning a
//! [`WorkflowRegistration`]'s opaque `arguments` payload into the entry
//! call spec.
//!
//! [`Converter`] is generic over a wrapped value converter: the payload
//! decoding — including the flavor's initiation convention of matching
//! named arguments to inputs and defaulting the rest — is delegated
//! through a [`TryConvert`] bound on the `ValueConverter` parameter,
//! and this crate keeps only what is language-free: resolving the entry
//! function's input names from the compiler metadata and assembling the
//! [`CallSpec`](waymark_vm_runtime::CallSpec).
//!
//! [`WorkflowRegistration`]: waymark_proto::messages::WorkflowRegistration

#![warn(missing_docs)]

use waymark_convert_core::{ConvertErrorFor, TryConvert};

/// Stateless converter that builds the entry call spec from an encoded
/// keyword-argument payload and the entry function's declared inputs.
pub struct Converter<ValueConverter> {
    _value_converter: core::marker::PhantomData<ValueConverter>,
}

impl<'a, ValueConverter, Value, FunctionId>
    TryConvert<
        (&'a [u8], &'a [String], FunctionId),
        waymark_vm_runtime::CallSpec<FunctionId, Value>,
    > for Converter<ValueConverter>
where
    ValueConverter: TryConvert<(&'a [u8], &'a [String]), Vec<Value>>,
{
    type Error = ConvertErrorFor<ValueConverter, (&'a [u8], &'a [String]), Vec<Value>>;

    fn try_convert(
        (arguments, input_names, func): (&'a [u8], &'a [String], FunctionId),
    ) -> Result<waymark_vm_runtime::CallSpec<FunctionId, Value>, Self::Error> {
        let args = ValueConverter::try_convert((arguments, input_names))?;
        Ok(waymark_vm_runtime::CallSpec { func, args })
    }
}

impl<'a, ValueConverter, Value, FunctionId>
    TryConvert<(&'a [u8], &'a [String]), waymark_vm_runtime::CallSpec<FunctionId, Value>>
    for Converter<ValueConverter>
where
    ValueConverter: TryConvert<(&'a [u8], &'a [String]), Vec<Value>>,
    FunctionId: Default,
{
    type Error = ConvertErrorFor<ValueConverter, (&'a [u8], &'a [String]), Vec<Value>>;

    fn try_convert(
        (arguments, input_names): (&'a [u8], &'a [String]),
    ) -> Result<waymark_vm_runtime::CallSpec<FunctionId, Value>, Self::Error> {
        Self::try_convert((arguments, input_names, FunctionId::default()))
    }
}

impl<'a, ValueConverter, Value, FunctionId>
    TryConvert<
        (
            &'a [u8],
            &'a waymark_vm_compiler_metadata::Metadata<FunctionId>,
            FunctionId,
        ),
        waymark_vm_runtime::CallSpec<FunctionId, Value>,
    > for Converter<ValueConverter>
where
    ValueConverter: TryConvert<(&'a [u8], &'a [String]), Vec<Value>>,
    FunctionId: index_type::IndexType,
{
    type Error = ConvertErrorFor<ValueConverter, (&'a [u8], &'a [String]), Vec<Value>>;

    fn try_convert(
        (arguments, metadata, func): (
            &'a [u8],
            &'a waymark_vm_compiler_metadata::Metadata<FunctionId>,
            FunctionId,
        ),
    ) -> Result<waymark_vm_runtime::CallSpec<FunctionId, Value>, Self::Error> {
        let entry_input_names = metadata.input_names(func).unwrap_or_default();
        Self::try_convert((arguments, entry_input_names, func))
    }
}

impl<'a, ValueConverter, Value, FunctionId>
    TryConvert<
        (
            &'a [u8],
            &'a waymark_vm_compiler_metadata::Metadata<FunctionId>,
        ),
        waymark_vm_runtime::CallSpec<FunctionId, Value>,
    > for Converter<ValueConverter>
where
    ValueConverter: TryConvert<(&'a [u8], &'a [String]), Vec<Value>>,
    FunctionId: Default + index_type::IndexType,
{
    type Error = ConvertErrorFor<ValueConverter, (&'a [u8], &'a [String]), Vec<Value>>;

    fn try_convert(
        (arguments, metadata): (
            &'a [u8],
            &'a waymark_vm_compiler_metadata::Metadata<FunctionId>,
        ),
    ) -> Result<waymark_vm_runtime::CallSpec<FunctionId, Value>, Self::Error> {
        Self::try_convert((arguments, metadata, FunctionId::default()))
    }
}
