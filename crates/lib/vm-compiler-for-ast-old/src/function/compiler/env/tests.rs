use nonempty_collections::NEVec;
use waymark_vm_runtime_core::RegisterId;

use super::*;

#[test]
fn flow_state_merges_by_intersection() {
    let mut locals = Locals::new();
    let always = locals
        .declare("always".to_owned(), RegisterId(0))
        .expect("always local should declare");
    let branch_only = locals
        .declare("branch_only".to_owned(), RegisterId(1))
        .expect("branch local should declare");

    let mut left_branch = FlowState::new();
    left_branch.mark_initialized(always);
    left_branch.mark_initialized(branch_only);

    let mut right_branch = FlowState::new();
    right_branch.mark_initialized(always);

    let mut branches = NEVec::new(left_branch);
    branches.push(right_branch);

    let merged = FlowState::intersect_branches(branches);

    assert!(merged.is_initialized(always));
    assert!(!merged.is_initialized(branch_only));
}

#[test]
fn flow_state_intersection_of_single_branch_is_identity() {
    let mut locals = Locals::new();
    let only = locals
        .declare("only".to_owned(), RegisterId(0))
        .expect("only local should declare");

    let mut branch = FlowState::new();
    branch.mark_initialized(only);

    let merged = FlowState::intersect_branches(NEVec::new(branch));

    assert!(merged.is_initialized(only));
}

#[test]
fn flow_state_unions_branches() {
    let mut locals = Locals::new();
    let left_only = locals
        .declare("left_only".to_owned(), RegisterId(0))
        .expect("left_only local should declare");
    let right_only = locals
        .declare("right_only".to_owned(), RegisterId(1))
        .expect("right_only local should declare");

    let mut left_branch = FlowState::new();
    left_branch.mark_initialized(left_only);

    let mut right_branch = FlowState::new();
    right_branch.mark_initialized(right_only);

    let mut branches = NEVec::new(left_branch);
    branches.push(right_branch);

    let merged = FlowState::union_branches(branches);

    assert!(merged.is_initialized(left_only));
    assert!(merged.is_initialized(right_only));
}

#[test]
fn declared_locals_start_uninitialized() {
    let mut locals = Locals::new();
    let local = locals
        .declare("value".to_owned(), RegisterId(0))
        .expect("local should declare");

    let mut flow_state = FlowState::new();
    flow_state.declare_local(local);

    assert!(!flow_state.is_initialized(local));
}

#[test]
fn local_frame_assigns_input_registers_in_order() {
    let mut frame = LocalFrame::new();
    let mut flow_state = FlowState::new();

    let first = frame
        .declare_input(&mut flow_state, "first".to_owned())
        .expect("first input should declare");
    let second = frame
        .declare_input(&mut flow_state, "second".to_owned())
        .expect("second input should declare");

    assert_eq!(first.register(), RegisterId(0));
    assert_eq!(second.register(), RegisterId(1));
    assert!(flow_state.is_initialized(*first));
    assert!(flow_state.is_initialized(*second));
    assert_eq!(frame.num_registers(), 2);
}

#[test]
fn duplicate_input_declarations_do_not_consume_registers() {
    let mut frame = LocalFrame::new();
    let mut flow_state = FlowState::new();

    let first = frame
        .declare_input(&mut flow_state, "value".to_owned())
        .expect("first input should declare");
    assert!(
        frame
            .declare_input(&mut flow_state, "value".to_owned())
            .is_none()
    );

    let second = frame
        .declare_input(&mut flow_state, "other".to_owned())
        .expect("distinct input should declare");

    assert_eq!(first.register(), RegisterId(0));
    assert_eq!(second.register(), RegisterId(1));
    assert_eq!(frame.num_registers(), 2);
}

#[test]
fn local_frame_reuses_released_temporary_registers() {
    let mut frame = LocalFrame::new();

    let first = frame.allocate_temporary_register();
    let second = frame.allocate_temporary_register();
    let first_register = first.register();
    let second_register = second.register();
    drop(second);
    drop(first);

    let reused_first = frame.allocate_temporary_register();
    let reused_second = frame.allocate_temporary_register();

    assert_eq!(first_register, RegisterId(0));
    assert_eq!(second_register, RegisterId(1));
    assert_eq!(reused_first.register(), RegisterId(0));
    assert_eq!(reused_second.register(), RegisterId(1));
    assert_eq!(frame.num_registers(), 2);
}

#[test]
fn local_frame_reuses_existing_local_bindings() {
    let mut frame = LocalFrame::new();
    let mut flow_state = FlowState::new();

    let first = frame.get_or_declare_local("value", &mut flow_state);
    let second = frame.get_or_declare_local("value", &mut flow_state);

    assert_eq!(first, second);
    assert_eq!(first.register(), RegisterId(0));
    assert!(!flow_state.is_initialized(first));
    assert_eq!(frame.num_registers(), 1);
}

#[test]
fn local_frame_syncs_existing_locals_into_new_flow_states() {
    let mut frame = LocalFrame::new();
    let mut initialized_inputs = FlowState::new();
    frame
        .declare_input(&mut initialized_inputs, "first".to_owned())
        .expect("first input should declare");
    let second = frame
        .declare_input(&mut initialized_inputs, "second".to_owned())
        .expect("second input should declare");

    let mut flow_state = FlowState::new();
    let local = frame.get_or_declare_local("second", &mut flow_state);

    assert_eq!(local.register(), second.register());
    assert_eq!(flow_state.initialized_by_local_len(), 2);
    assert!(!flow_state.is_initialized(local));
}

#[test]
fn local_frame_only_resolves_initialized_locals() {
    let mut frame = LocalFrame::new();
    let mut flow_state = FlowState::new();
    let local = frame.get_or_declare_local("value", &mut flow_state);

    assert!(
        frame
            .resolve_initialized_local("value", &flow_state)
            .is_none()
    );

    flow_state.mark_initialized(local);

    let resolved = frame
        .resolve_initialized_local("value", &flow_state)
        .expect("value should resolve after initialization");

    assert_eq!(resolved.register(), local.register());
}

#[test]
fn local_frame_does_not_resolve_missing_names() {
    let frame = LocalFrame::new();
    let flow_state = FlowState::new();

    assert!(
        frame
            .resolve_initialized_local("missing", &flow_state)
            .is_none()
    );
}
