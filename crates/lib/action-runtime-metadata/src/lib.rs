//! Correlation metadata carried alongside action calls.
//!
//! The action runtime request/completion types
//! (`waymark-action-runtime-core`) are generic over a metadata type; this
//! crate provides the concrete metadata shapes and the accessor traits that
//! let metadata-agnostic consumers read only the fields they need.

#![warn(missing_docs)]

use waymark_action_runtime_metadata_codec::{Decode, Encode};
use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

/// The correlation carried alongside an action call.
///
/// This pairs the effect that triggered a call with the promise state it
/// fulfills — the minimum needed to route a completion back to the promise
/// that awaits it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActionCallCorrelation {
    /// The sequential number of the effect that triggered the call.
    pub effect_number: EffectNumber,

    /// The id of the promise state the call fulfills.
    pub promise_state_id: PromiseStateId,
}

/// Wraps action-call metadata with the identifier of the VM instance that
/// owns the call.
///
/// Deployments that multiplex many VMs over a shared requester/completions
/// pipeline use this so completions can be routed back to the originating VM.
/// The inner metadata is preserved unchanged, so all correlation continues to
/// work through the trait impls below.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WithVmId<VmId, Metadata> {
    /// The VM instance that owns the call.
    pub vm_id: VmId,

    /// The wrapped metadata.
    pub inner: Metadata,
}

/// Metadata from which the action-call correlation can be recovered.
///
/// Consumers that only need to settle a promise (e.g. the reconciler) bound on
/// this trait rather than a concrete metadata type, so they work with any
/// metadata shape — including [`WithVmId`]-wrapped ones.
pub trait ActionCallCorrelated {
    /// Recover the correlation pair for this metadata.
    fn call_correlation(&self) -> ActionCallCorrelation;
}

impl ActionCallCorrelated for ActionCallCorrelation {
    fn call_correlation(&self) -> ActionCallCorrelation {
        *self
    }
}

impl<VmId, Metadata: ActionCallCorrelated> ActionCallCorrelated for WithVmId<VmId, Metadata> {
    fn call_correlation(&self) -> ActionCallCorrelation {
        self.inner.call_correlation()
    }
}

/// Metadata that identifies the VM instance owning the call.
///
/// Only VM-scoped metadata (i.e. [`WithVmId`]) implements this, so routing
/// code that bounds on it will not accept metadata that lacks a VM id.
pub trait VmScoped {
    /// The type of VM identifier carried by this metadata.
    type VmId: Copy;

    /// The VM instance that owns the call.
    fn vm_id(&self) -> Self::VmId;
}

impl<VmId: Copy, Metadata> VmScoped for WithVmId<VmId, Metadata> {
    type VmId = VmId;

    fn vm_id(&self) -> VmId {
        self.vm_id
    }
}

// ---------------------------------------------------------------------------
// Encode / Decode
// ---------------------------------------------------------------------------

impl Encode for ActionCallCorrelation {
    fn encode(&self, writer: &mut Vec<u8>) {
        writer.extend_from_slice(
            &u64::try_from(self.effect_number.0)
                .unwrap_or(u64::MAX)
                .to_be_bytes(),
        );
        writer.extend_from_slice(
            &u64::try_from(self.promise_state_id.0)
                .unwrap_or(u64::MAX)
                .to_be_bytes(),
        );
    }
}

/// Error returned when decoding an [`ActionCallCorrelation`] fails because
/// the input bytes are too short.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActionCallCorrelationDecodeError;

impl core::fmt::Display for ActionCallCorrelationDecodeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "not enough bytes to decode ActionCallCorrelation")
    }
}

impl std::error::Error for ActionCallCorrelationDecodeError {}

impl Decode for ActionCallCorrelation {
    type Error = ActionCallCorrelationDecodeError;

    fn decode(input: &mut &[u8]) -> Result<Self, Self::Error> {
        if input.len() < 16 {
            return Err(ActionCallCorrelationDecodeError);
        }
        let effect_number_bytes: [u8; 8] = input[..8].try_into().unwrap();
        let promise_state_id_bytes: [u8; 8] = input[8..16].try_into().unwrap();
        *input = &input[16..];
        let effect_number = EffectNumber(u64::from_be_bytes(effect_number_bytes) as usize);
        let promise_state_id = PromiseStateId(u64::from_be_bytes(promise_state_id_bytes) as usize);
        Ok(ActionCallCorrelation {
            effect_number,
            promise_state_id,
        })
    }
}

// ---------------------------------------------------------------------------
// Encode / Decode for WithVmId
// ---------------------------------------------------------------------------

impl<VmId: Encode, Metadata: Encode> Encode for WithVmId<VmId, Metadata> {
    fn encode(&self, writer: &mut Vec<u8>) {
        self.vm_id.encode(writer);
        self.inner.encode(writer);
    }
}

/// Error returned when decoding a [`WithVmId`] fails.
#[derive(Debug)]
pub enum WithVmIdDecodeError<VmIdError> {
    /// The VM identifier could not be decoded.
    VmId(VmIdError),
    /// The inner correlation metadata could not be decoded.
    Correlation(ActionCallCorrelationDecodeError),
}

impl<VmIdError: core::fmt::Display> core::fmt::Display for WithVmIdDecodeError<VmIdError> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::VmId(e) => write!(f, "vm id: {e}"),
            Self::Correlation(e) => write!(f, "correlation: {e}"),
        }
    }
}

impl<VmIdError: core::fmt::Debug + core::fmt::Display> std::error::Error
    for WithVmIdDecodeError<VmIdError>
{
}

impl<VmId: Decode, Metadata: Decode<Error = ActionCallCorrelationDecodeError>> Decode
    for WithVmId<VmId, Metadata>
{
    type Error = WithVmIdDecodeError<VmId::Error>;

    fn decode(input: &mut &[u8]) -> Result<Self, Self::Error> {
        let vm_id = VmId::decode(input).map_err(WithVmIdDecodeError::VmId)?;
        let inner = Metadata::decode(input).map_err(WithVmIdDecodeError::Correlation)?;
        Ok(WithVmId { vm_id, inner })
    }
}
