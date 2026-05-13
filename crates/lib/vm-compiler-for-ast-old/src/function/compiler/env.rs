//! Local-variable and definite-initialization tracking.

mod flow_state;
mod local_frame;
mod local_slot;
mod locals;

#[cfg(test)]
mod tests;

pub use self::flow_state::FlowState;
pub use self::local_frame::{LocalFrame, RegisterHandle};
pub use self::local_slot::{AssignmentTargetMarker, InitializedLocalMarker, LocalSlot};

#[cfg(test)]
pub(super) use self::locals::Locals;
