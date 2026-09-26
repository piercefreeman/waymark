use waymark_vm_driver_core::PromiseResolution;
use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

mockall::mock! {
    pub Hooks {}

    impl waymark_vm_driver_hooks::effect_emitted::HasEffect for Hooks {
        type Effect = &'static str;
    }

    impl waymark_vm_driver_hooks::promise_settled::HasValue for Hooks {
        type Value = u8;
    }

    impl waymark_vm_driver_hooks::vm_stopped::HasError for Hooks {
        type Error = &'static str;
    }

    impl waymark_vm_driver_hooks::VmStarted for Hooks {
        fn vm_started(&self);
    }

    impl waymark_vm_driver_hooks::EffectEmitted for Hooks {
        fn effect_emitted(&self, number: EffectNumber, effect: &&'static str);
    }

    impl waymark_vm_driver_hooks::PromiseSettled for Hooks {
        fn promise_settled(
            &self,
            promise_state_id: PromiseStateId,
            resolution: &PromiseResolution<u8>,
        );
    }

    impl waymark_vm_driver_hooks::SnapshotPersisted for Hooks {
        fn snapshot_persisted(&self, size_in_bytes: usize);
    }

    impl waymark_vm_driver_hooks::VmStopped for Hooks {
        fn vm_stopped(&self, error: &&'static str);
    }
}

/// Fire every hook once, in run order.
pub fn drive_all<Hooks>(hooks: &Hooks)
where
    Hooks: waymark_vm_driver_hooks::VmStarted
        + waymark_vm_driver_hooks::EffectEmitted<Effect = &'static str>
        + waymark_vm_driver_hooks::PromiseSettled<Value = u8>
        + waymark_vm_driver_hooks::SnapshotPersisted
        + waymark_vm_driver_hooks::VmStopped<Error = &'static str>,
{
    hooks.vm_started();
    hooks.effect_emitted(EffectNumber(3), &"tick");
    hooks.promise_settled(PromiseStateId(7), &PromiseResolution::Resolved(42));
    hooks.snapshot_persisted(128);
    hooks.vm_stopped(&"done");
}

/// Expect exactly the calls of [`drive_all`], once each, in its order.
pub fn expect_drive_all(hooks: &mut MockHooks) {
    let mut sequence = mockall::Sequence::new();

    hooks
        .expect_vm_started()
        .times(1)
        .in_sequence(&mut sequence)
        .return_const(());
    hooks
        .expect_effect_emitted()
        .withf(|number, effect| *number == EffectNumber(3) && *effect == "tick")
        .times(1)
        .in_sequence(&mut sequence)
        .return_const(());
    hooks
        .expect_promise_settled()
        .withf(|promise_state_id, resolution| {
            *promise_state_id == PromiseStateId(7)
                && matches!(resolution, PromiseResolution::Resolved(42))
        })
        .times(1)
        .in_sequence(&mut sequence)
        .return_const(());
    hooks
        .expect_snapshot_persisted()
        .withf(|size_in_bytes| *size_in_bytes == 128)
        .times(1)
        .in_sequence(&mut sequence)
        .return_const(());
    hooks
        .expect_vm_stopped()
        .withf(|error| *error == "done")
        .times(1)
        .in_sequence(&mut sequence)
        .return_const(());
}
