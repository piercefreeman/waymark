//! Shared by the reads.

use nonempty_collections::NEVec;

/// One non-empty page of instances; a read with nothing to give returns
/// no page at all, so a page always has instances and always has a
/// position.
#[derive(Debug)]
pub struct Page<Cursor> {
    /// The instances of this page, in the read's order.
    pub instances: NEVec<waymark_observability_state_core::InstanceState>,

    /// Where to resume from for what follows this page in the read's
    /// order: the position of the page's last instance.
    pub next: Cursor,
}

/// The page type of a backend's list read, for its cursor.
pub type PageFor<Backend> = Page<<Backend as crate::ListInstances>::Cursor>;
