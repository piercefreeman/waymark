//! Definite-initialization tracking for locals.

use index_type::{IndexType, typed_vec::TypedVec};
use nonempty_collections::{IntoNonEmptyIterator as _, NEVec, NonEmptyIterator as _};

use super::locals::LocalId;

/// Tracks which locals are definitely initialized at a control-flow point.
#[derive(Clone)]
pub struct FlowState {
    /// Initialization bits indexed by local id.
    initialized_by_local: TypedVec<LocalId, bool>,
}

impl FlowState {
    /// Creates an empty flow state with no declared locals.
    pub fn new() -> Self {
        Self {
            initialized_by_local: TypedVec::new(),
        }
    }

    /// Returns whether the local is definitely initialized.
    pub fn is_initialized(&self, local: impl Into<LocalId>) -> bool {
        let local = local.into();

        self.initialized_by_local
            .get(local)
            .copied()
            .unwrap_or(false)
    }

    /// Ensures the local exists in the flow state without marking it initialized.
    pub fn declare_local(&mut self, local: impl Into<LocalId>) {
        let local = local.into();
        self.ensure_slot(local);
    }

    /// Marks the local as definitely initialized.
    pub fn mark_initialized(&mut self, local: impl Into<LocalId>) {
        let local = local.into();
        self.ensure_slot(local);
        self.initialized_by_local[local] = true;
    }

    /// Intersects branch flow states to keep only definitely initialized locals.
    pub fn intersect_branches(branches: NEVec<Self>) -> Self {
        Self::merge_branches(branches, |current_initialized, other_initialized| {
            current_initialized && other_initialized
        })
    }

    /// Unions branch flow states to keep any local initialized by any branch.
    pub fn union_branches(branches: NEVec<Self>) -> Self {
        Self::merge_branches(branches, |current_initialized, other_initialized| {
            current_initialized || other_initialized
        })
    }

    /// Merges branch flow states with the specified predicate.
    fn merge_branches(branches: NEVec<Self>, is_initialized: impl Fn(bool, bool) -> bool) -> Self {
        let (mut merged, branches) = branches.into_nonempty_iter().next();

        for branch in branches {
            merged.merge_with(&branch, &is_initialized);
        }

        merged
    }

    /// Merges this flow state with another branch flow state using
    /// the initialization check predicate.
    fn merge_with(&mut self, other: &Self, is_initialized: impl Fn(bool, bool) -> bool) {
        let merged_len = self
            .initialized_by_local
            .len()
            .to_scalar()
            .max(other.initialized_by_local.len().to_scalar());
        self.extend_to_len(merged_len);

        for index in 0..merged_len {
            let local = LocalId(index);
            let is_self_initialized = self.is_initialized(local);
            let is_other_initialized = other.is_initialized(local);
            self.initialized_by_local[local] =
                is_initialized(is_self_initialized, is_other_initialized);
        }
    }

    /// Ensures storage exists for the given local id.
    fn ensure_slot(&mut self, local: LocalId) {
        self.extend_to_len(local.to_scalar() + 1);
    }

    /// Extends the local-initialization storage to `len` entries.
    fn extend_to_len(&mut self, len: usize) {
        while self.initialized_by_local.len().to_scalar() < len {
            self.initialized_by_local.push(false);
        }
    }
}

#[cfg(test)]
impl FlowState {
    /// Returns the length of the internal initialization bitset.
    pub(super) fn initialized_by_local_len(&self) -> usize {
        self.initialized_by_local.len().to_scalar()
    }
}
