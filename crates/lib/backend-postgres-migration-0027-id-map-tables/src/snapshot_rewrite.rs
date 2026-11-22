//! The rewrite of one snapshot from the shape before the migration to the
//! shape after it, and the check of a snapshot in the shape after it.
//!
//! Both shapes are frozen here: nothing in this module follows the
//! runtime's own types.

/// How many elements a runtime state has: the ready frames, the promise
/// states table, the select states table and the effect counter.
const RUNTIME_STATE_ELEMENTS: usize = 4;

/// The variant keys a promise state is written under.
const PROMISE_STATE_VARIANTS: [&str; 2] = ["Waiting", "Settled"];

/// Error returned by [`rewrite_snapshot`].
#[derive(Debug, thiserror::Error)]
pub enum RewriteSnapshotError {
    /// The snapshot is not a runtime state in the shape before the
    /// migration.
    #[error("reading the snapshot: {0}")]
    Read(#[source] ReadSnapshotError),

    /// The promise states table is not a sequence of promise states.
    #[error("the promise states table is not a sequence of promise states")]
    PromiseStatesNotASequenceOfStates,

    /// The select states table is not a next id with the continuations by
    /// their ids.
    #[error("the select states table is not a next id with the continuations by their ids")]
    SelectStatesNotANextIdWithContinuations,

    /// The rewritten snapshot could not be written.
    #[error("writing the rewritten snapshot: {0}")]
    Write(#[source] rmpv::encode::Error),
}

/// Error returned by [`check_rewritten_snapshot`].
#[derive(Debug, thiserror::Error)]
pub enum CheckRewrittenSnapshotError {
    /// The snapshot is not a runtime state.
    #[error("reading the snapshot: {0}")]
    Read(#[source] ReadSnapshotError),

    /// The promise states table is not an id map of promise states.
    #[error("the promise states table is not an id map of promise states")]
    PromiseStatesNotAnIdMapOfStates,

    /// The select states table is not an id map of continuations.
    #[error("the select states table is not an id map of continuations")]
    SelectStatesNotAnIdMapOfContinuations,
}

/// Error of reading a snapshot as a runtime state.
#[derive(Debug, thiserror::Error)]
pub enum ReadSnapshotError {
    /// The snapshot is not MessagePack.
    #[error("decoding: {0}")]
    Decode(#[source] rmpv::decode::Error),

    /// Bytes follow the snapshot's one value.
    #[error("{0} bytes follow the value")]
    TrailingBytes(usize),

    /// The value is not a runtime state: a sequence of the ready frames,
    /// the promise states table, the select states table and the effect
    /// counter.
    #[error("the value is not a sequence of {RUNTIME_STATE_ELEMENTS} elements")]
    NotARuntimeState,
}

/// The elements of a runtime state.
struct RuntimeStateElements {
    /// The ready frames, passed through as they are.
    ready: rmpv::Value,

    /// The promise states table.
    promise_states: rmpv::Value,

    /// The select states table.
    select_states: rmpv::Value,

    /// The effect counter, passed through as it is.
    effect_counter: rmpv::Value,
}

/// Read `snapshot` as a runtime state.
fn read_snapshot(snapshot: &[u8]) -> Result<RuntimeStateElements, ReadSnapshotError> {
    let mut rest = snapshot;

    let value = rmpv::decode::read_value(&mut rest).map_err(ReadSnapshotError::Decode)?;

    if !rest.is_empty() {
        return Err(ReadSnapshotError::TrailingBytes(rest.len()));
    }

    let rmpv::Value::Array(elements) = value else {
        return Err(ReadSnapshotError::NotARuntimeState);
    };

    let elements: [rmpv::Value; RUNTIME_STATE_ELEMENTS] = elements
        .try_into()
        .map_err(|_| ReadSnapshotError::NotARuntimeState)?;

    let [ready, promise_states, select_states, effect_counter] = elements;

    Ok(RuntimeStateElements {
        ready,
        promise_states,
        select_states,
        effect_counter,
    })
}

/// Whether `value` is a promise state: one of its variants, under the
/// variant's key.
fn is_promise_state(value: &rmpv::Value) -> bool {
    let rmpv::Value::Map(entries) = value else {
        return false;
    };

    let [(rmpv::Value::String(variant), _content)] = entries.as_slice() else {
        return false;
    };

    variant
        .as_str()
        .is_some_and(|variant| PROMISE_STATE_VARIANTS.contains(&variant))
}

/// The next id and the entries of `value` when it is an id map: a next id
/// with the entries by their ids, every id below the next id.
fn as_id_map(value: &rmpv::Value) -> Option<(u64, &[(rmpv::Value, rmpv::Value)])> {
    let rmpv::Value::Array(fields) = value else {
        return None;
    };

    let [next_id, rmpv::Value::Map(entries)] = fields.as_slice() else {
        return None;
    };

    let next_id = next_id.as_u64()?;

    let ids_are_below_the_next_id = entries
        .iter()
        .all(|(id, _entry)| id.as_u64().is_some_and(|id| id < next_id));

    if !ids_are_below_the_next_id {
        return None;
    }

    Some((next_id, entries))
}

/// Rewrite `snapshot` from the shape before the migration to the shape
/// after it.
///
/// Before, the promise states table is the sequence of the promise states,
/// each one's id its position, and the select states table is the next id
/// with the continuations by their ids. After, the promise states table is
/// an id map: the next id — the count of the states — with the states by
/// their ids. The select states table is a structure of one field, an id
/// map: the next id with the continuations by their ids that the table was
/// before, as they are.
///
/// The ready frames, the effect counter, each promise state and each
/// continuation pass through as they are.
///
/// A snapshot that is not in the shape before the migration is refused,
/// one already in the shape after it included.
pub fn rewrite_snapshot(snapshot: &[u8]) -> Result<Vec<u8>, RewriteSnapshotError> {
    let RuntimeStateElements {
        ready,
        promise_states,
        select_states,
        effect_counter,
    } = read_snapshot(snapshot).map_err(RewriteSnapshotError::Read)?;

    let rmpv::Value::Array(states) = promise_states else {
        return Err(RewriteSnapshotError::PromiseStatesNotASequenceOfStates);
    };

    if !states.iter().all(is_promise_state) {
        return Err(RewriteSnapshotError::PromiseStatesNotASequenceOfStates);
    }

    if as_id_map(&select_states).is_none() {
        return Err(RewriteSnapshotError::SelectStatesNotANextIdWithContinuations);
    }

    let mut next_promise_state_id: u64 = 0;
    let mut promise_states_by_id = Vec::with_capacity(states.len());

    for state in states {
        promise_states_by_id.push((rmpv::Value::from(next_promise_state_id), state));

        next_promise_state_id += 1;
    }

    let rewritten = rmpv::Value::Array(vec![
        ready,
        rmpv::Value::Array(vec![
            rmpv::Value::from(next_promise_state_id),
            rmpv::Value::Map(promise_states_by_id),
        ]),
        rmpv::Value::Array(vec![select_states]),
        effect_counter,
    ]);

    let mut written = Vec::with_capacity(snapshot.len() + 16);

    rmpv::encode::write_value(&mut written, &rewritten).map_err(RewriteSnapshotError::Write)?;

    Ok(written)
}

/// Check that `snapshot` is in the shape after the migration: its promise
/// states table an id map of promise states, and its select states table a
/// one-field structure of an id map.
pub fn check_rewritten_snapshot(snapshot: &[u8]) -> Result<(), CheckRewrittenSnapshotError> {
    let RuntimeStateElements {
        ready: _,
        promise_states,
        select_states,
        effect_counter: _,
    } = read_snapshot(snapshot).map_err(CheckRewrittenSnapshotError::Read)?;

    let Some((_next_id, states_by_id)) = as_id_map(&promise_states) else {
        return Err(CheckRewrittenSnapshotError::PromiseStatesNotAnIdMapOfStates);
    };

    if !states_by_id
        .iter()
        .all(|(_id, state)| is_promise_state(state))
    {
        return Err(CheckRewrittenSnapshotError::PromiseStatesNotAnIdMapOfStates);
    }

    let rmpv::Value::Array(select_states_fields) = &select_states else {
        return Err(CheckRewrittenSnapshotError::SelectStatesNotAnIdMapOfContinuations);
    };

    let [continuations] = select_states_fields.as_slice() else {
        return Err(CheckRewrittenSnapshotError::SelectStatesNotAnIdMapOfContinuations);
    };

    if as_id_map(continuations).is_none() {
        return Err(CheckRewrittenSnapshotError::SelectStatesNotAnIdMapOfContinuations);
    }

    Ok(())
}

#[cfg(test)]
mod tests;
