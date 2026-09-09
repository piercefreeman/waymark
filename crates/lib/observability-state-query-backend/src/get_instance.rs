//! One VM, by id.

/// Read one VM's instance state.
pub trait GetInstance {
    /// Error type for the read.
    type Error: std::fmt::Debug;

    /// The VM's state; `None` when the events know no such VM.
    fn get_instance(
        &self,
        vm_id: waymark_ids::InstanceId,
    ) -> impl Future<
        Output = Result<Option<waymark_observability_state_core::InstanceState>, Self::Error>,
    > + Send
    + '_;
}
