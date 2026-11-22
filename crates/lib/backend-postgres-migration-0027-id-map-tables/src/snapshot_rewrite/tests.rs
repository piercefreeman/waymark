use super::*;
use crate::test_fixtures::{empty_after, empty_before, populated_after, populated_before};

/// A settled promise state: `{"Settled": {"Resolved": {"Int": 42}}}`.
const SETTLED_PROMISE_STATE: [u8; 24] = [
    0x81, 0xa7, 0x53, 0x65, 0x74, 0x74, 0x6c, 0x65, 0x64, 0x81, 0xa8, 0x52, 0x65, 0x73, 0x6f, 0x6c,
    0x76, 0x65, 0x64, 0x81, 0xa3, 0x49, 0x6e, 0x74,
];

/// [`SETTLED_PROMISE_STATE`] with its value.
fn settled_promise_state() -> Vec<u8> {
    let mut state = SETTLED_PROMISE_STATE.to_vec();
    state.push(0x2a);

    state
}

#[test]
fn populated_snapshot_is_rewritten_to_what_the_runtime_writes() {
    let rewritten = rewrite_snapshot(&populated_before()).expect("the snapshot is rewritten");

    assert_eq!(rewritten, populated_after());
}

#[test]
fn empty_snapshot_is_rewritten() {
    let rewritten = rewrite_snapshot(&empty_before()).expect("the snapshot is rewritten");

    assert_eq!(rewritten, empty_after());
}

#[test]
fn sixteen_promise_states_are_rewritten_under_the_wider_headers() {
    // 16 is the first count that no longer fits the one-byte headers.
    let mut before = vec![0x94, 0x90, 0xdc, 0x00, 0x10];
    let mut after = vec![0x94, 0x90, 0x92, 0x10, 0xde, 0x00, 0x10];

    for id in 0..16u8 {
        before.extend(settled_promise_state());

        after.push(id);
        after.extend(settled_promise_state());
    }

    before.extend([0x92, 0x00, 0x80, 0x00]);
    after.extend([0x91, 0x92, 0x00, 0x80, 0x00]);

    let rewritten = rewrite_snapshot(&before).expect("the snapshot is rewritten");

    assert_eq!(rewritten, after);
    check_rewritten_snapshot(&rewritten).expect("the rewritten snapshot passes the check");
}

#[test]
fn rewritten_snapshots_pass_the_check() {
    check_rewritten_snapshot(&populated_after()).expect("the populated snapshot passes");
    check_rewritten_snapshot(&empty_after()).expect("the empty snapshot passes");
}

#[test]
fn snapshot_in_the_shape_before_fails_the_check() {
    let populated = check_rewritten_snapshot(&populated_before());
    let empty = check_rewritten_snapshot(&empty_before());

    assert!(matches!(
        populated,
        Err(CheckRewrittenSnapshotError::PromiseStatesNotAnIdMapOfStates)
    ));
    assert!(matches!(
        empty,
        Err(CheckRewrittenSnapshotError::PromiseStatesNotAnIdMapOfStates)
    ));
}

#[test]
fn snapshot_already_rewritten_is_refused() {
    let populated = rewrite_snapshot(&populated_after());
    let empty = rewrite_snapshot(&empty_after());

    assert!(matches!(
        populated,
        Err(RewriteSnapshotError::PromiseStatesNotASequenceOfStates)
    ));
    assert!(matches!(
        empty,
        Err(RewriteSnapshotError::PromiseStatesNotASequenceOfStates)
    ));
}

#[test]
fn promise_states_table_with_something_else_than_a_state_is_refused() {
    // The table's one element is nil.
    let refused = rewrite_snapshot(&[0x94, 0x90, 0x91, 0xc0, 0x92, 0x00, 0x80, 0x00]);

    assert!(matches!(
        refused,
        Err(RewriteSnapshotError::PromiseStatesNotASequenceOfStates)
    ));
}

#[test]
fn select_states_table_with_an_id_at_its_next_id_is_refused() {
    // Next id 0, with a continuation under id 0.
    let refused = rewrite_snapshot(&[0x94, 0x90, 0x90, 0x92, 0x00, 0x81, 0x00, 0xc0, 0x00]);

    assert!(matches!(
        refused,
        Err(RewriteSnapshotError::SelectStatesNotANextIdWithContinuations)
    ));
}

#[test]
fn select_states_table_already_rewritten_fails_the_rewrite_and_one_not_rewritten_the_check() {
    // The promise states table in the shape before, the select states
    // table in the shape after.
    let refused = rewrite_snapshot(&[0x94, 0x90, 0x90, 0x91, 0x92, 0x00, 0x80, 0x00]);

    // The promise states table in the shape after, the select states table
    // in the shape before.
    let failed = check_rewritten_snapshot(&[0x94, 0x90, 0x92, 0x00, 0x80, 0x92, 0x00, 0x80, 0x00]);

    assert!(matches!(
        refused,
        Err(RewriteSnapshotError::SelectStatesNotANextIdWithContinuations)
    ));
    assert!(matches!(
        failed,
        Err(CheckRewrittenSnapshotError::SelectStatesNotAnIdMapOfContinuations)
    ));
}

#[test]
fn bytes_after_the_value_are_refused() {
    let mut snapshot = empty_before();
    snapshot.push(0x00);

    let refused = rewrite_snapshot(&snapshot);

    assert!(matches!(
        refused,
        Err(RewriteSnapshotError::Read(
            ReadSnapshotError::TrailingBytes(1)
        ))
    ));
}

#[test]
fn value_that_is_not_a_runtime_state_is_refused() {
    let nil = rewrite_snapshot(&[0xc0]);
    let three_elements = rewrite_snapshot(&[0x93, 0x90, 0x90, 0x00]);

    assert!(matches!(
        nil,
        Err(RewriteSnapshotError::Read(
            ReadSnapshotError::NotARuntimeState
        ))
    ));
    assert!(matches!(
        three_elements,
        Err(RewriteSnapshotError::Read(
            ReadSnapshotError::NotARuntimeState
        ))
    ));
}

#[test]
fn truncated_snapshot_is_refused() {
    let snapshot = populated_before();
    let truncated = snapshot
        .get(..snapshot.len() - 1)
        .expect("the fixture has bytes");

    let refused = rewrite_snapshot(truncated);
    let failed = check_rewritten_snapshot(truncated);

    assert!(matches!(
        refused,
        Err(RewriteSnapshotError::Read(ReadSnapshotError::Decode(_)))
    ));
    assert!(matches!(
        failed,
        Err(CheckRewrittenSnapshotError::Read(
            ReadSnapshotError::Decode(_)
        ))
    ));
}
