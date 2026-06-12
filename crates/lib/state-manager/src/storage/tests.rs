use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use core::hash::Hash;

use waymark_nonzero_duration::NonZeroDuration;

use super::{Entry, Maps};
use crate::State;

fn retention_10ms() -> NonZeroDuration {
    NonZeroDuration::from_millis(10).expect("10ms is non-zero")
}

/// White-box helpers used by the tests to set up and inspect internal state.
///
/// These live here (rather than behind `#[cfg(test)]` in `storage`) so no test
/// scaffolding leaks into the production modules.
impl<Key, Value> Maps<Key, Value>
where
    Key: Eq + Hash,
{
    /// Insert an entry holding an uninitialized value with the given
    /// ref-count, to simulate other holders.
    ///
    /// The fabricated refs have no owning `Guard`s, so the key must not be
    /// touched by an in-flight `get` — a stale guard releasing against a
    /// replaced entry would corrupt the ref-count.  Refuses to replace an
    /// existing entry for that reason.
    fn insert_uninitialized_for_test(&self, key: Key, refs: usize) {
        let previous = self.entries.insert(
            key,
            Entry {
                value: std::sync::Arc::new(tokio::sync::OnceCell::new()),
                refs,
                orphaned_since: None,
            },
        );
        assert!(
            previous.is_none(),
            "test helper must not replace an existing entry"
        );
    }

    /// The ref-count of the entry for `key`, or `None` if the entry is absent.
    fn refs_of(&self, key: &Key) -> Option<usize> {
        self.entries.get(key).map(|entry| entry.refs)
    }

    /// Whether `key` currently has a pending eviction scheduled.
    fn is_pending_eviction(&self, key: &Key) -> bool {
        self.pending_evictions.contains_key(key)
    }
}

// ------------------------------------------------------------------
// Factory failure when other refs exist
// ------------------------------------------------------------------

/// A factory that fails once for each key, then succeeds.
struct FlakyFactory {
    failures: dashmap::DashMap<u64, usize>,
}

impl waymark_state_manager_core::Factory for FlakyFactory {
    type Key = u64;
    type Value = u64;
    type Error = &'static str;

    async fn produce(&self, key: &Self::Key) -> Result<Self::Value, Self::Error> {
        let mut count = self.failures.entry(*key).or_insert(0);
        *count += 1;
        if *count == 1 {
            Err("factory failed")
        } else {
            Ok(*key * 10)
        }
    }
}

/// When the factory fails but another holder keeps the entry alive
/// (refs > 1), cleanup must decrement refs back to the original count
/// rather than removing the entry.  This path cannot be tested via
/// integration because a failing factory never produces a Handle.
#[tokio::test]
async fn factory_failure_keeps_entry_when_other_handle_exists() {
    let factory = FlakyFactory {
        failures: dashmap::DashMap::new(),
    };
    let (state, _sweeper) = State::<u64, u64, _>::new(retention_10ms(), factory);

    // Pre-populate an entry with refs=2 to simulate another holder keeping
    // the entry alive.
    state.maps.insert_uninitialized_for_test(2, 2);

    let result = state.get(2).await;
    assert!(result.is_err());
    assert_eq!(result.err().unwrap(), "factory failed");

    assert_eq!(
        state.maps.refs_of(&2),
        Some(2),
        "refs should be back to original count (other refs held)"
    );
}

// ------------------------------------------------------------------
// Cancellation safety of `get`
// ------------------------------------------------------------------

/// A factory that pauses at a gate until signalled, then fails.
struct GateFactory {
    started: Arc<AtomicBool>,
    gate: Arc<tokio::sync::Notify>,
}

impl waymark_state_manager_core::Factory for GateFactory {
    type Key = u64;
    type Value = u64;
    type Error = &'static str;

    async fn produce(&self, _key: &Self::Key) -> Result<Self::Value, Self::Error> {
        self.started.store(true, Ordering::SeqCst);
        self.gate.notified().await;
        Err("gated failure")
    }
}

/// When the `get` future is dropped while the factory is still producing
/// (e.g. a `timeout` fires, or a `select!` branch loses), the ref bump the
/// call performed before awaiting must be undone.  Otherwise `refs` never
/// returns to zero, `orphaned_since` is never set, and the `Sweeper` can
/// never evict the entry — a permanent leak.
#[tokio::test]
async fn cancelled_get_does_not_leak_ref() {
    let started = Arc::new(AtomicBool::new(false));
    // Never signalled in this test: the factory stays parked, so the only
    // way `get` returns is by being cancelled.
    let gate = Arc::new(tokio::sync::Notify::new());

    let factory = GateFactory {
        started: started.clone(),
        gate: gate.clone(),
    };
    let (state, _sweeper) = State::<u64, u64, _>::new(retention_10ms(), factory);
    let state = Arc::new(state);

    // Drive `get` on a task so we can cancel it by aborting.
    let state_clone = Arc::clone(&state);
    let handle = tokio::spawn(async move { state_clone.get(7).await });

    // Wait until the factory is entered — the ref has been bumped and the
    // future is now parked at the `get_or_try_init` await.
    while !started.load(Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    // The entry exists with the bumped ref while the factory runs.
    assert_eq!(
        state.maps.refs_of(&7),
        Some(1),
        "ref should be bumped while the factory produces"
    );

    // Cancel the `get` future mid-produce.
    handle.abort();
    let _ = handle.await;

    // The guard must have undone the bump; as the only ref, the entry is
    // removed entirely so a later `get` starts fresh.
    assert_eq!(
        state.maps.refs_of(&7),
        None,
        "cancelled get must not leak the entry"
    );
    assert!(
        !state.maps.is_pending_eviction(&7),
        "cancelled get must not leave a pending eviction"
    );
}

/// A cancelled `get` on an entry another holder keeps alive (refs > 1) must
/// decrement the ref back rather than removing the shared entry.
#[tokio::test]
async fn cancelled_get_keeps_entry_when_other_handle_exists() {
    let started = Arc::new(AtomicBool::new(false));
    let gate = Arc::new(tokio::sync::Notify::new());

    let factory = GateFactory {
        started: started.clone(),
        gate: gate.clone(),
    };
    let (state, _sweeper) = State::<u64, u64, _>::new(retention_10ms(), factory);
    let state = Arc::new(state);

    // A first `get` parked at the factory's gate acts as the other holder:
    // it owns a real guard-backed reference for as long as it stays parked.
    let state_clone = Arc::clone(&state);
    let holder = tokio::spawn(async move { state_clone.get(8).await });

    while !started.load(Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert_eq!(
        state.maps.refs_of(&8),
        Some(1),
        "holder should own one ref while parked in the factory"
    );

    // The second `get` bumps the ref-count and parks awaiting the same
    // `OnceCell` (the holder's factory call is already in flight).
    let state_clone = Arc::clone(&state);
    let handle = tokio::spawn(async move { state_clone.get(8).await });

    while state.maps.refs_of(&8) != Some(2) {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    handle.abort();
    let _ = handle.await;

    assert_eq!(
        state.maps.refs_of(&8),
        Some(1),
        "cancelled get must decrement back to the other holder's ref"
    );

    // Release the holder through the gate — its factory fails, `get` returns
    // the error, and the guard releases the last ref on the way out.
    gate.notify_one();
    let holder_result = holder.await.expect("holder task must not panic");
    assert_eq!(holder_result.err(), Some("gated failure"));

    assert_eq!(
        state.maps.refs_of(&8),
        None,
        "releasing the last ref must remove the never-produced entry"
    );
}
