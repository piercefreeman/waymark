use waymark_action_runtime_core::ActionCallRequest;
use waymark_convert_core::{ConvertErrorFor, TryConvert};

use crate::Converter;

/// Convert an action call request into the worker protocol's dispatch
/// message.
///
/// The dispatch itself is the flavor's to assemble — the action's
/// identity and the arguments payload are its calling convention — so
/// the whole ref and the argument values are handed through.  What is
/// the envelope's here is the correlation metadata: encoded into the
/// opaque bytes the worker echoes back untouched, and passed along for
/// the flavor to carry.
///
/// The dispatch carries no deadline or attempt bookkeeping: retries and
/// timeouts are lowered in the VM rather than delegated to the worker.
impl<ValueConverter, Argument, Metadata>
    TryConvert<ActionCallRequest<Argument, Metadata>, waymark_proto::messages::ActionDispatch>
    for Converter<ValueConverter>
where
    ValueConverter: TryConvert<
            (waymark_action_core::ActionRef, Vec<Argument>, Vec<u8>),
            waymark_proto::messages::ActionDispatch,
        >,
    Metadata: waymark_action_runtime_metadata_codec::Encode,
{
    type Error = ConvertErrorFor<
        ValueConverter,
        (waymark_action_core::ActionRef, Vec<Argument>, Vec<u8>),
        waymark_proto::messages::ActionDispatch,
    >;

    fn try_convert(
        request: ActionCallRequest<Argument, Metadata>,
    ) -> Result<waymark_proto::messages::ActionDispatch, Self::Error> {
        let mut encoded_metadata = Vec::new();
        request.metadata.encode(&mut encoded_metadata);

        ValueConverter::try_convert((request.action_ref, request.arguments, encoded_metadata))
    }
}
