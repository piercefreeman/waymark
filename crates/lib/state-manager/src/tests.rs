use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use waymark_nonzero_duration::NonZeroDuration;
use waymark_state_manager_core::Factory;

use crate::{Entry, State};

fn retention_10ms() -> NonZeroDuration {
    NonZeroDuration::from_millis(10).expect("10ms is non-zero")
}

// ------------------------------------------------------------------
// Factory failure when other refs exist
// ------------------------------------------------------------------

/// A factory that fails once for each key, then succeeds.
struct FlakyFactory {
    failures: dashmap::DashMap<u64, usize>,
}

impl Factory for FlakyFactory {
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

    // Pre-populate an entry with an empty OnceCell and refs=2
    // to simulate another holder keeping the entry alive.
    let oncecell = Arc::new(tokio::sync::OnceCell::new());
    state.maps.entries.insert(
        2,
        Entry {
            value: Arc::clone(&oncecell),
            refs: 2,
            orphaned_since: None,
        },
    );

    let result = state.get(2).await;
    assert!(result.is_err());
    assert_eq!(result.err().unwrap(), "factory failed");

    let entry = state
        .maps
        .entries
        .get(&2)
        .expect("entry should still exist (other refs held)");
    assert_eq!(entry.refs, 2, "refs should be back to original count");
}

// ------------------------------------------------------------------
// Arc::ptr_eq guard: rotated OnceCell
// ------------------------------------------------------------------

/// A factory that pauses at a gate until signalled, then fails.
struct GateFactory {
    started: Arc<AtomicBool>,
    gate: Arc<tokio::sync::Notify>,
}

impl Factory for GateFactory {
    type Key = u64;
    type Value = u64;
    type Error = &'static str;

    async fn produce(&self, _key: &Self::Key) -> Result<Self::Value, Self::Error> {
        self.started.store(true, Ordering::SeqCst);
        self.gate.notified().await;
        Err("gated failure")
    }
}

/// When `get` captures a OnceCell from an entry, but that entry's
/// OnceCell is replaced before cleanup runs, the `Arc::ptr_eq`
/// guard must prevent cleanup from touching the new entry's refs.
#[tokio::test]
async fn cleanup_skips_when_oncecell_rotated() {
    let started = Arc::new(AtomicBool::new(false));
    let gate = Arc::new(tokio::sync::Notify::new());

    let factory = GateFactory {
        started: started.clone(),
        gate: gate.clone(),
    };
    let (state, _sweeper) = State::<u64, u64, _>::new(retention_10ms(), factory);
    let state = Arc::new(state);

    // Spawn a task that calls `get`. It will block inside the factory.
    let state_clone = Arc::clone(&state);
    let handle = tokio::spawn(async move { state_clone.get(1).await });

    // Wait until the factory has been entered.
    while !started.load(Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    // Now the spawned task is blocked in get_or_try_init.
    // The entry exists with the OnceCell that `get` captured.
    // Replace it with a different, pre-initialised OnceCell.
    let new_oncecell = Arc::new(tokio::sync::OnceCell::new());
    new_oncecell.set(999u64).ok();

    {
        let mut occupied = state.maps.entries.get_mut(&1).expect("entry must exist");
        let _old = std::mem::replace(&mut occupied.value, new_oncecell);
        // `_old` is the OnceCell the spawned task is waiting on.
        // refs is still 1 here.
    }

    // Release the factory → it returns Err.
    gate.notify_one();

    // Wait for the spawned task to finish.
    let result = handle.await.expect("spawned task should not panic");
    assert!(result.is_err(), "get should fail after gated failure");

    // The entry should still exist with refs=1, untouched by cleanup
    // because the OnceCell was rotated (ptr_eq guard skipped the
    // decrement).
    let entry = state
        .maps
        .entries
        .get(&1)
        .expect("entry should still exist after rotated cleanup");
    assert_eq!(
        entry.refs, 1,
        "refs should be untouched — ptr_eq guard skipped the decrement"
    );
    assert_eq!(
        entry.value.get(),
        Some(&999),
        "rotated OnceCell should still hold its value"
    );
}
