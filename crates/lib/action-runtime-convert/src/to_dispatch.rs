use waymark_action_runtime_core::ActionCallRequest;
use waymark_convert_core::TryConvert;
use waymark_vm_value_convert_core::PendingPromiseError;

use crate::Converter;

/// Convert an action call request into the worker protocol's dispatch
/// message.
///
/// Every transport builds the same dispatch: the action identity and
/// its arguments as framing-level kwargs, plus the correlation metadata
/// encoded into the opaque bytes the worker echoes back untouched.
///
/// The dispatch carries no deadline or attempt bookkeeping: retries and
/// timeouts are lowered in the VM rather than delegated to the worker.
impl<Metadata>
    TryConvert<
        ActionCallRequest<waymark_vm_value_python::ReadyValue, Metadata>,
        waymark_proto::messages::ActionDispatch,
    > for Converter
where
    Metadata: waymark_action_runtime_metadata_codec::Encode,
{
    type Error = PendingPromiseError;

    fn try_convert(
        request: ActionCallRequest<waymark_vm_value_python::ReadyValue, Metadata>,
    ) -> Result<waymark_proto::messages::ActionDispatch, Self::Error> {
        let kwargs: Option<waymark_proto::messages::WorkflowArguments> =
            Self::try_convert((&request.action_ref.call_args[..], &request.arguments[..]))?;

        let mut encoded_metadata = Vec::new();
        request.metadata.encode(&mut encoded_metadata);

        Ok(waymark_proto::messages::ActionDispatch {
            action_name: request.action_ref.action_name,
            module_name: request.action_ref.module_name.unwrap_or_default(),
            kwargs,
            metadata: encoded_metadata,
        })
    }
}
