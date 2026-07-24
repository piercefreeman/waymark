pub trait Spec {
    /// Runtime provided by workers spawned from this specification.
    fn action_runtime() -> waymark_action_core::ActionRuntime;

    fn prepare_spawn_params(
        &self,
        reservation_id: waymark_worker_reservation::Id,
    ) -> waymark_worker_process::SpawnParams;
}
