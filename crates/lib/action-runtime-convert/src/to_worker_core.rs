use std::collections::HashMap;

use waymark_convert_core::TryConvert;
use waymark_vm_value_convert_core::PendingPromiseError;

use crate::Converter;

/// Convert a pair of call-argument names and values into kwargs as consumed by
/// the [`worker_core`] crate.
///
/// This is a temporary conversion, since we might as well eliminate
/// the intermediate layer of [`worker_core`] request.
/// TODO: address this after we have rework surface API of the workers to expose
/// raw `Sender`.
impl
    TryConvert<
        (&[String], &[waymark_vm_value_python::ReadyValue]),
        HashMap<String, serde_json::Value>,
    > for Converter
{
    type Error = PendingPromiseError;

    fn try_convert(
        (names, values): (&[String], &[waymark_vm_value_python::ReadyValue]),
    ) -> Result<HashMap<String, serde_json::Value>, PendingPromiseError> {
        let mut arguments = HashMap::with_capacity(names.len());
        for (name, value) in names.iter().zip(values.iter()) {
            // Skip `None`-valued parameters (dependency markers such as
            // `Annotated[T, Depend(…)]` are serialized as `None` by the
            // VM).  The Python side (`provide_dependencies`) will resolve
            // them from the function signature instead.
            if matches!(value, waymark_vm_value_python::ReadyValue::None) {
                continue;
            }
            arguments.insert(
                name.clone(),
                waymark_vm_value_convert_json::Converter::try_convert(value.clone())?,
            );
        }
        Ok(arguments)
    }
}
