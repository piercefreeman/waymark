use std::cell::RefCell;

use waymark_vm_driver_core::PromiseResolution;
use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

/// Hooks that count their calls.
struct Counting(RefCell<u8>);

impl waymark_vm_driver_hooks::effect_emitted::HasEffect for Counting {
    type Effect = ();
}

impl waymark_vm_driver_hooks::promise_settled::HasValue for Counting {
    type Value = ();
}

impl waymark_vm_driver_hooks::vm_stopped::HasError for Counting {
    type Error = ();
}

impl waymark_vm_driver_hooks::VmStarted for Counting {
    fn vm_started(&self) {
        *self.0.borrow_mut() += 1;
    }
}

impl waymark_vm_driver_hooks::EffectEmitted for Counting {
    fn effect_emitted(&self, _number: EffectNumber, _effect: &Self::Effect) {
        *self.0.borrow_mut() += 1;
    }
}

impl waymark_vm_driver_hooks::PromiseSettled for Counting {
    fn promise_settled(
        &self,
        _promise_state_id: PromiseStateId,
        _resolution: &PromiseResolution<Self::Value>,
    ) {
        *self.0.borrow_mut() += 1;
    }
}

impl waymark_vm_driver_hooks::SnapshotPersisted for Counting {
    fn snapshot_persisted(&self, _size_in_bytes: usize) {
        *self.0.borrow_mut() += 1;
    }
}

impl waymark_vm_driver_hooks::VmStopped for Counting {
    fn vm_stopped(&self, _error: &Self::Error) {
        *self.0.borrow_mut() += 1;
    }
}

fn drive_all<Hooks>(hooks: &Hooks)
where
    Hooks: waymark_vm_driver_hooks::VmStarted
        + waymark_vm_driver_hooks::EffectEmitted<Effect = ()>
        + waymark_vm_driver_hooks::PromiseSettled<Value = ()>
        + waymark_vm_driver_hooks::SnapshotPersisted
        + waymark_vm_driver_hooks::VmStopped<Error = ()>,
{
    hooks.vm_started();
    hooks.effect_emitted(EffectNumber(0), &());
    hooks.promise_settled(PromiseStateId(0), &PromiseResolution::Resolved(()));
    hooks.snapshot_persisted(0);
    hooks.vm_stopped(&());
}

#[test]
fn a_present_hook_observes_every_call() {
    let hooks = Some(Counting(RefCell::new(0)));

    drive_all(&hooks);

    assert_eq!(*hooks.as_ref().expect("present").0.borrow(), 5);
}

#[test]
fn an_absent_hook_is_a_hook_that_observes_nothing() {
    let hooks: Option<Counting> = None;

    drive_all(&hooks);
}
