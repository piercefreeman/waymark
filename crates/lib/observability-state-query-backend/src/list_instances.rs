//! Every VM last active in a time range, most recently active first.

use crate::PageFor;

/// Parameters of a list read.
#[derive(Debug)]
pub struct Params<Cursor> {
    /// Inclusive start of the time range a VM's last activity must fall
    /// in to be listed.
    pub from: chrono::DateTime<chrono::Utc>,

    /// Exclusive end of that time range.
    pub to: chrono::DateTime<chrono::Utc>,

    /// At most this many instances, within the cap the type names.
    pub limit: waymark_query_limit::Limit<100>,

    /// The last position already returned; the read resumes past it in
    /// its own order, so towards VMs less recently active in the range.
    /// `None` starts from the most recently active.
    pub after: Option<Cursor>,
}

/// Read the instances last active in a time range, most recently active
/// first.
///
/// A VM is in the list when its last event falls in the range, and that
/// event places it. What the read serves is the state as of the last
/// flush of the events: exact at that point, no fresher.
///
/// The order is total — by the time of the VM's last event, then by the
/// VM — so a page never repeats an instance. A VM active between two
/// page reads moves ahead of the cursor: this walk does not see it
/// again, a fresh walk does.
pub trait ListInstances {
    /// A position in this read's order, in the backend's own shape, with
    /// the codec every cursor must have.
    type Cursor: waymark_cursor_core::Cursor + Send;

    /// Error type for the read.
    type Error: std::fmt::Debug;

    /// One page of instances; `None` when nothing (more) is in range.
    fn list_instances(
        &self,
        params: Params<Self::Cursor>,
    ) -> impl Future<Output = Result<Option<PageFor<Self>>, Self::Error>> + Send + '_;
}
