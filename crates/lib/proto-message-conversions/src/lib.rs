//! Framing-level protocol buffer message conversion utilities.
//!
//! Operates on the transport framing — named arguments carrying opaque
//! encoded value documents.  The documents themselves are the business
//! of [`waymark_proto_python_value_conversions`].

use waymark_proto::messages as proto;
use waymark_proto_python_value_conversions::{
    decode_workflow_argument_value, workflow_argument_value_to_json,
};

/// Error decoding an embedded value document out of a framing-level
/// [`proto::WorkflowArgument`]'s value bytes.
#[derive(Debug, thiserror::Error)]
#[error("decoding workflow argument value bytes for key {key:?}")]
pub struct DecodeArgumentError {
    /// The framing-level argument name the bytes belonged to.
    pub key: String,

    /// The decode failure.
    #[source]
    pub source: prost::DecodeError,
}

/// Convert framing-level workflow arguments into a JSON object, decoding
/// each entry's embedded value document.
pub fn workflow_arguments_to_json(
    args: &proto::WorkflowArguments,
) -> Result<serde_json::Value, DecodeArgumentError> {
    let mut map = serde_json::Map::new();
    for arg in &args.arguments {
        let value =
            decode_workflow_argument_value(&arg.value).map_err(|source| DecodeArgumentError {
                key: arg.key.clone(),
                source,
            })?;
        map.insert(arg.key.clone(), workflow_argument_value_to_json(&value));
    }

    Ok(serde_json::Value::Object(map))
}
