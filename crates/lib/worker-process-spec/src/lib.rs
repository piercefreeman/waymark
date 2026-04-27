pub trait Spec {
    fn prepare_spawn_params(
        &self,
        reservation_id: waymark_worker_reservation::Id,
    ) -> waymark_worker_process::SpawnParams;
}
