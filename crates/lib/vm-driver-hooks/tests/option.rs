mod common;

use common::MockHooks;

#[test]
fn a_present_hook_observes_every_call() {
    let mut hooks = MockHooks::new();
    common::expect_drive_all(&mut hooks);
    let mut hooks = Some(hooks);

    common::drive_all(&hooks);

    hooks.as_mut().expect("present").checkpoint();
}

#[test]
fn an_absent_hook_is_a_hook_that_observes_nothing() {
    let hooks: Option<MockHooks> = None;

    common::drive_all(&hooks);
}
