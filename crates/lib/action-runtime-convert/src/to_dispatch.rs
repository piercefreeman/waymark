use waymark_action_runtime_core::ActionCallRequest;
use waymark_convert_core::{ConvertErrorFor, TryConvert};

use crate::Converter;

/// Convert an action call request into the worker protocol's dispatch
/// message.
///
/// Every transport builds the same dispatch: the action identity and
/// its arguments as one opaque encoded payload, plus the correlation
/// metadata encoded into the opaque bytes the worker echoes back
/// untouched.  How the arguments are shaped inside the payload — and
/// that no arguments encode as no bytes — is the value converter's
/// calling convention; the framing carries the payload without reading
/// it.
///
/// The dispatch carries no deadline or attempt bookkeeping: retries and
/// timeouts are lowered in the VM rather than delegated to the worker.
impl<ValueConverter, Argument, Metadata>
    TryConvert<ActionCallRequest<Argument, Metadata>, waymark_proto::messages::ActionDispatch>
    for Converter<ValueConverter>
where
    ValueConverter: TryConvert<(Vec<String>, Vec<Argument>), Vec<u8>>,
    Metadata: waymark_action_runtime_metadata_codec::Encode,
{
    type Error = ConvertErrorFor<ValueConverter, (Vec<String>, Vec<Argument>), Vec<u8>>;

    fn try_convert(
        request: ActionCallRequest<Argument, Metadata>,
    ) -> Result<waymark_proto::messages::ActionDispatch, Self::Error> {
        let arguments =
            ValueConverter::try_convert((request.action_ref.call_args, request.arguments))?;

        let mut encoded_metadata = Vec::new();
        request.metadata.encode(&mut encoded_metadata);

        Ok(waymark_proto::messages::ActionDispatch {
            action_name: request.action_ref.action_name,
            module_name: request.action_ref.module_name.unwrap_or_default(),
            arguments,
            metadata: encoded_metadata,
        })
    }
}
