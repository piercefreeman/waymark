//! Name-to-local identifier storage.

use std::collections::HashMap;

use index_type::{IndexTooBigError, IndexType, typed_vec::TypedVec};
use waymark_vm_runtime_core::RegisterId;

/// Identifier for a local variable within one function frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, IndexType)]
#[index_type(error = LocalIdTooBigError)]
pub struct LocalId(pub usize);

/// Error returned when a local id exceeds the supported index range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, IndexTooBigError)]
#[index_too_big_error(msg = "local id")]
pub struct LocalIdTooBigError;

/// Register metadata stored for a declared local.
#[derive(Clone, Copy)]
struct LocalInfo {
    /// The register allocated for this local.
    register: RegisterId,
}

/// Storage for declared locals keyed by both name and local id.
pub struct Locals {
    /// Mapping from local names to stable local ids.
    ids_by_name: HashMap<String, LocalId>,

    /// Register metadata indexed by local id.
    info_by_id: TypedVec<LocalId, LocalInfo>,
}

impl Locals {
    /// Creates an empty local table.
    pub fn new() -> Self {
        Self {
            ids_by_name: HashMap::new(),
            info_by_id: TypedVec::new(),
        }
    }

    /// Looks up a local id by source name.
    pub fn lookup(&self, name: &str) -> Option<LocalId> {
        self.ids_by_name.get(name).copied()
    }

    /// Returns the register assigned to a local id.
    pub fn register(&self, local: LocalId) -> Option<RegisterId> {
        let info = self.info_by_id.get(local)?;
        Some(info.register)
    }

    /// Declares a local when the caller has already checked for duplicates.
    pub(super) fn declare_known_vacant(&mut self, name: String, register: RegisterId) -> LocalId {
        let local = self.info_by_id.len();
        self.info_by_id.push(LocalInfo { register });
        self.ids_by_name.insert(name, local);
        local
    }
}

#[cfg(test)]
impl Locals {
    /// Declares a local for tests if the name is not already taken.
    pub fn declare(&mut self, name: String, register: RegisterId) -> Option<LocalId> {
        if self.ids_by_name.contains_key(&name) {
            return None;
        }

        Some(self.declare_known_vacant(name, register))
    }
}
