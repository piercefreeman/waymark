use std::cell::RefCell;

use waymark_vm_driver_core::PromiseResolution;
use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

/// Hooks that record which hook fired, tagged with their own name.
struct Recording {
    name: &'static str,
    log: RefCell<Vec<String>>,
}

impl Recording {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            log: RefCell::new(Vec::new()),
        }
    }

    fn record(&self, hook: &str) {
        self.log.borrow_mut().push(format!("{}:{hook}", self.name));
    }
}

impl waymark_vm_driver_hooks::effect_emitted::HasEffect for Recording {
    type Effect = &'static str;
}

impl waymark_vm_driver_hooks::promise_settled::HasValue for Recording {
    type Value = u8;
}

impl waymark_vm_driver_hooks::vm_stopped::HasError for Recording {
    type Error = &'static str;
}

impl waymark_vm_driver_hooks::VmStarted for Recording {
    fn vm_started(&self) {
        self.record("vm_started");
    }
}

impl waymark_vm_driver_hooks::EffectEmitted for Recording {
    fn effect_emitted(&self, number: EffectNumber, effect: &Self::Effect) {
        self.record(&format!("effect_emitted:{number}:{effect}"));
    }
}

impl waymark_vm_driver_hooks::PromiseSettled for Recording {
    fn promise_settled(
        &self,
        promise_state_id: PromiseStateId,
        resolution: &PromiseResolution<Self::Value>,
    ) {
        let value = match resolution {
            PromiseResolution::Resolved(value) => *value,
            PromiseResolution::Rejected(_) => unreachable!("the test only resolves"),
        };

        self.record(&format!("promise_settled:{promise_state_id:?}:{value}"));
    }
}

impl waymark_vm_driver_hooks::SnapshotPersisted for Recording {
    fn snapshot_persisted(&self, size_in_bytes: usize) {
        self.record(&format!("snapshot_persisted:{size_in_bytes}"));
    }
}

impl waymark_vm_driver_hooks::VmStopped for Recording {
    fn vm_stopped(&self, error: &Self::Error) {
        self.record(&format!("vm_stopped:{error}"));
    }
}

fn drive_all<Hooks>(hooks: &Hooks)
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

fn expected(name: &str) -> Vec<String> {
    [
        "vm_started",
        "effect_emitted:3:tick",
        "promise_settled:PromiseStateId(7):42",
        "snapshot_persisted:128",
        "vm_stopped:done",
    ]
    .into_iter()
    .map(|hook| format!("{name}:{hook}"))
    .collect()
}

#[test]
fn a_pair_observes_with_both_components_in_order() {
    let hooks = (Recording::new("a"), Recording::new("b"));

    drive_all(&hooks);

    assert_eq!(*hooks.0.log.borrow(), expected("a"));
    assert_eq!(*hooks.1.log.borrow(), expected("b"));
}

#[test]
fn a_triple_observes_with_all_components() {
    let hooks = (
        Recording::new("a"),
        Recording::new("b"),
        Recording::new("c"),
    );

    drive_all(&hooks);

    assert_eq!(*hooks.0.log.borrow(), expected("a"));
    assert_eq!(*hooks.1.log.borrow(), expected("b"));
    assert_eq!(*hooks.2.log.borrow(), expected("c"));
}

#[test]
fn components_fire_in_tuple_order() {
    let order = RefCell::new(Vec::new());
    struct Ordered<'a>(&'static str, &'a RefCell<Vec<&'static str>>);
    impl waymark_vm_driver_hooks::VmStarted for Ordered<'_> {
        fn vm_started(&self) {
            self.1.borrow_mut().push(self.0);
        }
    }

    let hooks = (
        Ordered("first", &order),
        Ordered("second", &order),
        Ordered("third", &order),
    );

    waymark_vm_driver_hooks::VmStarted::vm_started(&hooks);

    assert_eq!(*order.borrow(), ["first", "second", "third"]);
}
