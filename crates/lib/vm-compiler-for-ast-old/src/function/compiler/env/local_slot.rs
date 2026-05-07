//! Marker-wrapped local-slot helpers.

use waymark_vm_runtime_core::RegisterId;

use crate::Marked;

use super::{LocalFrame, locals::LocalId};

/// A local variable paired with the register currently allocated for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalSlot {
    /// Stable local id for this slot.
    local: LocalId,

    /// Register currently backing the local.
    register: RegisterId,
}

/// Marker showing a local has definitely been initialized on the current path.
pub struct InitializedLocalMarker;

/// Marker showing a local slot is being used as an assignment target.
pub struct AssignmentTargetMarker;

impl LocalSlot {
    /// Creates a slot for the given local id and backing register.
    pub(super) fn new(local: LocalId, register: RegisterId) -> Self {
        Self { local, register }
    }

    /// Returns the register currently assigned to this local.
    pub fn register(&self) -> RegisterId {
        self.register
    }
}

impl From<LocalSlot> for LocalId {
    fn from(local: LocalSlot) -> Self {
        local.local
    }
}

impl Marked<LocalSlot, AssignmentTargetMarker> {
    /// Resolves `name` to an assignment target, declaring a local if needed.
    pub fn get_or_declare(
        local_frame: &mut LocalFrame,
        flow_state: &mut super::flow_state::FlowState,
        name: &str,
    ) -> Self {
        Self::mark(local_frame.get_or_declare_local(name, flow_state))
    }

    /// Marks the assignment target as initialized after its value is produced.
    pub fn mark_initialized(self, flow_state: &mut super::flow_state::FlowState) {
        let slot = Marked::unmark(self);
        flow_state.mark_initialized(slot.local)
    }
}
