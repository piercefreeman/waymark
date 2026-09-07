mod common;

use common::MockHooks;

/// A mock expecting exactly the calls of [`common::drive_all`].
fn expecting_drive_all() -> MockHooks {
    let mut hooks = MockHooks::new();
    common::expect_drive_all(&mut hooks);
    hooks
}

#[test]
fn a_pair_observes_with_both_components() {
    let mut hooks = (expecting_drive_all(), expecting_drive_all());

    common::drive_all(&hooks);

    hooks.0.checkpoint();
    hooks.1.checkpoint();
}

#[test]
fn a_triple_observes_with_all_components() {
    let mut hooks = (
        expecting_drive_all(),
        expecting_drive_all(),
        expecting_drive_all(),
    );

    common::drive_all(&hooks);

    hooks.0.checkpoint();
    hooks.1.checkpoint();
    hooks.2.checkpoint();
}

#[test]
fn components_fire_in_tuple_order() {
    let mut sequence = mockall::Sequence::new();
    let mut expecting_vm_started_in_turn = || {
        let mut hooks = MockHooks::new();
        hooks
            .expect_vm_started()
            .times(1)
            .in_sequence(&mut sequence)
            .return_const(());
        hooks
    };

    let mut hooks = (
        expecting_vm_started_in_turn(),
        expecting_vm_started_in_turn(),
        expecting_vm_started_in_turn(),
    );

    waymark_vm_driver_hooks::VmStarted::vm_started(&hooks);

    hooks.0.checkpoint();
    hooks.1.checkpoint();
    hooks.2.checkpoint();
}
