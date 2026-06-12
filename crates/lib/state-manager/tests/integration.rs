use std::convert::Infallible;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use waymark_nonzero_duration::NonZeroDuration;
use waymark_state_manager::{Handle, State};
use waymark_state_manager_core::Factory;

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

/// A [`Factory`] that returns a fixed value and counts invocations.
struct CountingFactory<K, V> {
    value: V,
    call_count: Arc<AtomicUsize>,
    _phantom: std::marker::PhantomData<K>,
}

impl<K, V> Factory for CountingFactory<K, V>
where
    K: Hash + Eq + Sync,
    V: Clone + Sync + Send,
{
    type Key = K;
    type Value = V;
    type Error = Infallible;

    async fn produce(&self, _key: &Self::Key) -> Result<Self::Value, Self::Error> {
        self.call_count.fetch_add(1, Ordering::Relaxed);
        Ok(self.value.clone())
    }
}

/// A [`Factory`] that produces `key * 10` for `u64` keys.
struct MultiplyFactory {
    call_count: Arc<AtomicUsize>,
}

impl Factory for MultiplyFactory {
    type Key = u64;
    type Value = u64;
    type Error = Infallible;

    async fn produce(&self, key: &Self::Key) -> Result<Self::Value, Self::Error> {
        self.call_count.fetch_add(1, Ordering::Relaxed);
        Ok(key * 10)
    }
}

/// A [`Factory`] that prepends "value-for-" to string keys.
struct PrefixFactory {
    call_count: Arc<AtomicUsize>,
}

impl Factory for PrefixFactory {
    type Key = String;
    type Value = String;
    type Error = Infallible;

    async fn produce(&self, key: &Self::Key) -> Result<Self::Value, Self::Error> {
        self.call_count.fetch_add(1, Ordering::Relaxed);
        Ok(format!("value-for-{key}"))
    }
}

/// A value that tracks how many times it has been dropped.
#[derive(Clone)]
struct DropCounter {
    drops: Arc<AtomicUsize>,
}

impl DropCounter {
    fn new(drops: Arc<AtomicUsize>) -> Self {
        Self { drops }
    }
}

impl Drop for DropCounter {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}

/// A [`Factory`] that produces [`DropCounter`] values.
struct DropCounterFactory {
    drops: Arc<AtomicUsize>,
}

impl Factory for DropCounterFactory {
    type Key = u64;
    type Value = DropCounter;
    type Error = Infallible;

    async fn produce(&self, _key: &Self::Key) -> Result<Self::Value, Self::Error> {
        Ok(DropCounter::new(self.drops.clone()))
    }
}

fn retention_10ms() -> NonZeroDuration {
    NonZeroDuration::from_millis(10).expect("10ms is non-zero")
}

fn retention_1s() -> NonZeroDuration {
    NonZeroDuration::from_secs(1).expect("1s is non-zero")
}

// ---------------------------------------------------------------------------
// Basic Handle lifecycle
// ---------------------------------------------------------------------------

#[tokio::test]
async fn get_and_deref() {
    let call_count = Arc::new(AtomicUsize::new(0));
    let factory = CountingFactory {
        value: "hello".to_string(),
        call_count: call_count.clone(),
        _phantom: std::marker::PhantomData,
    };
    let (state, _sweeper) = State::<u64, String, _>::new(retention_1s(), factory);

    let handle = state.get(42).await.unwrap();
    assert_eq!(*handle, "hello");
    assert_eq!(call_count.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn handle_key_access() {
    let call_count = Arc::new(AtomicUsize::new(0));
    let factory = CountingFactory {
        value: 100u64,
        call_count: call_count.clone(),
        _phantom: std::marker::PhantomData,
    };
    let (state, _sweeper) = State::<u64, u64, _>::new(retention_1s(), factory);

    let handle = state.get(99).await.unwrap();
    assert_eq!(*Handle::key(&handle), 99);
    assert_eq!(*handle, 100);
}

#[tokio::test]
async fn factory_called_only_once_for_same_key() {
    let call_count = Arc::new(AtomicUsize::new(0));
    let factory = CountingFactory {
        value: 42u64,
        call_count: call_count.clone(),
        _phantom: std::marker::PhantomData,
    };
    let (state, _sweeper) = State::<u64, u64, _>::new(retention_1s(), factory);

    let _h1 = state.get(1).await.unwrap();
    let _h2 = state.get(1).await.unwrap();

    assert_eq!(call_count.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn factory_receives_key() {
    let call_count = Arc::new(AtomicUsize::new(0));
    let factory = PrefixFactory {
        call_count: call_count.clone(),
    };
    let (state, _sweeper) = State::<String, String, _>::new(retention_1s(), factory);

    let handle = state.get("my-key".to_string()).await.unwrap();
    assert_eq!(*handle, "value-for-my-key");
}

// ---------------------------------------------------------------------------
// Eviction
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sweep_evicts_after_retention() {
    let drops = Arc::new(AtomicUsize::new(0));
    let factory = DropCounterFactory {
        drops: drops.clone(),
    };
    let (state, mut sweeper) = State::<u64, DropCounter, _>::new(retention_10ms(), factory);

    {
        let _handle = state.get(1).await.unwrap();
    }
    sweeper.sweep();

    tokio::time::sleep(Duration::from_millis(20)).await;

    let evicted = Arc::new(AtomicUsize::new(0));
    let evicted_clone = evicted.clone();
    sweeper.sweep_with_handler(move |key, cell| {
        assert_eq!(key, 1);
        let _value = cell.get().expect("OnceCell should be initialised");
        evicted_clone.fetch_add(1, Ordering::Relaxed);
    });
    assert_eq!(evicted.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn sweep_skips_when_handle_still_held() {
    let drops = Arc::new(AtomicUsize::new(0));
    let factory = DropCounterFactory {
        drops: drops.clone(),
    };
    let (state, mut sweeper) = State::<u64, DropCounter, _>::new(retention_10ms(), factory);

    let _handle = state.get(1).await.unwrap();

    tokio::time::sleep(Duration::from_millis(20)).await;

    let evicted = Arc::new(AtomicUsize::new(0));
    let evicted_clone = evicted.clone();
    sweeper.sweep_with_handler(move |_key, _cell| {
        evicted_clone.fetch_add(1, Ordering::Relaxed);
    });
    assert_eq!(evicted.load(Ordering::Relaxed), 0);
}

// ---------------------------------------------------------------------------
// Multiple handles
// ---------------------------------------------------------------------------

#[tokio::test]
async fn multiple_handles_delay_eviction_until_all_dropped() {
    let drops = Arc::new(AtomicUsize::new(0));
    let factory = DropCounterFactory {
        drops: drops.clone(),
    };
    let (state, mut sweeper) = State::<u64, DropCounter, _>::new(retention_10ms(), factory);

    let h1 = state.get(1).await.unwrap();
    let _h2 = state.get(1).await.unwrap();

    drop(h1);
    tokio::time::sleep(Duration::from_millis(20)).await;

    let evicted = Arc::new(AtomicUsize::new(0));
    let evicted_clone = evicted.clone();
    sweeper.sweep_with_handler(move |_key, _cell| {
        evicted_clone.fetch_add(1, Ordering::Relaxed);
    });
    assert_eq!(evicted.load(Ordering::Relaxed), 0);

    drop(_h2);
    tokio::time::sleep(Duration::from_millis(20)).await;

    let evicted = Arc::new(AtomicUsize::new(0));
    let evicted_clone = evicted.clone();
    sweeper.sweep_with_handler(move |key, cell| {
        assert_eq!(key, 1);
        let _value = cell.get().expect("OnceCell should be initialised");
        evicted_clone.fetch_add(1, Ordering::Relaxed);
    });
    assert_eq!(evicted.load(Ordering::Relaxed), 1);
}

// ---------------------------------------------------------------------------
// Concurrent access
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_get_same_key() {
    let call_count = Arc::new(AtomicUsize::new(0));
    let factory = CountingFactory {
        value: 42u64,
        call_count: call_count.clone(),
        _phantom: std::marker::PhantomData,
    };
    let (state, _sweeper) = State::<u64, u64, _>::new(retention_1s(), factory);
    let state = Arc::new(state);

    let mut handles = Vec::new();
    for _ in 0..10 {
        let state = state.clone();
        let task = tokio::task::spawn(async move {
            let h = state.get(1).await.unwrap();
            *h
        });
        handles.push(task);
    }

    for handle in handles {
        let value = handle.await.unwrap();
        assert_eq!(value, 42);
    }
    assert_eq!(call_count.load(Ordering::Relaxed), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_get_different_keys() {
    let call_count = Arc::new(AtomicUsize::new(0));
    let factory = MultiplyFactory {
        call_count: call_count.clone(),
    };
    let (state, _sweeper) = State::<u64, u64, _>::new(retention_1s(), factory);
    let state = Arc::new(state);

    let mut handles = Vec::new();
    for i in 0..10 {
        let state = state.clone();
        let task = tokio::task::spawn(async move {
            let h = state.get(i).await.unwrap();
            *h
        });
        handles.push(task);
    }

    for (i, handle) in handles.into_iter().enumerate() {
        let value = handle.await.unwrap();
        assert_eq!(value, (i as u64) * 10);
    }
    assert_eq!(call_count.load(Ordering::Relaxed), 10);
}

// ---------------------------------------------------------------------------
// Edge cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn empty_sweep_does_nothing() {
    let call_count = Arc::new(AtomicUsize::new(0));
    let factory = CountingFactory {
        value: "unused".to_string(),
        call_count,
        _phantom: std::marker::PhantomData::<String>,
    };
    let (_state, mut sweeper) = State::<String, String, _>::new(retention_1s(), factory);
    sweeper.sweep();
}

#[tokio::test]
async fn sweep_after_state_dropped() {
    let call_count = Arc::new(AtomicUsize::new(0));
    let factory = CountingFactory {
        value: "value".to_string(),
        call_count,
        _phantom: std::marker::PhantomData,
    };
    let (state, mut sweeper) = State::<u64, String, _>::new(retention_1s(), factory);

    {
        let _handle = state.get(1).await.unwrap();
    }
    drop(state);
    sweeper.sweep();
}

#[tokio::test]
async fn get_after_handle_drop_before_sweep() {
    let call_count = Arc::new(AtomicUsize::new(0));
    let factory = CountingFactory {
        value: 100u64,
        call_count: call_count.clone(),
        _phantom: std::marker::PhantomData,
    };
    let (state, mut sweeper) = State::<u64, u64, _>::new(retention_10ms(), factory);

    {
        let _h = state.get(1).await.unwrap();
    }

    let h2 = state.get(1).await.unwrap();
    assert_eq!(*h2, 100);
    assert_eq!(call_count.load(Ordering::Relaxed), 1);

    tokio::time::sleep(Duration::from_millis(20)).await;

    let evicted = Arc::new(AtomicUsize::new(0));
    let evicted_clone = evicted.clone();
    sweeper.sweep_with_handler(move |_key, _cell| {
        evicted_clone.fetch_add(1, Ordering::Relaxed);
    });
    assert_eq!(evicted.load(Ordering::Relaxed), 0);
}

// ---------------------------------------------------------------------------
// Multiple keys
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sweep_evicts_multiple_stale_entries() {
    let call_count = Arc::new(AtomicUsize::new(0));
    let factory = MultiplyFactory {
        call_count: call_count.clone(),
    };
    let (state, mut sweeper) = State::<u64, u64, _>::new(retention_10ms(), factory);

    for i in 0..5 {
        let _h = state.get(i).await.unwrap();
    }

    tokio::time::sleep(Duration::from_millis(20)).await;

    let evicted = Arc::new(AtomicUsize::new(0));
    let evicted_clone = evicted.clone();
    sweeper.sweep_with_handler(move |key, cell| {
        let value = *cell.get().expect("OnceCell should be initialised");
        assert_eq!(value, key * 10);
        evicted_clone.fetch_add(1, Ordering::Relaxed);
    });
    assert_eq!(evicted.load(Ordering::Relaxed), 5);
}

#[tokio::test]
async fn sweep_evicts_only_stale_entries() {
    let call_count = Arc::new(AtomicUsize::new(0));
    let factory = CountingFactory {
        value: 100u64,
        call_count: call_count.clone(),
        _phantom: std::marker::PhantomData,
    };
    let (state, mut sweeper) = State::<u64, u64, _>::new(retention_10ms(), factory);

    {
        let _h1 = state.get(1).await.unwrap();
    }

    tokio::time::sleep(Duration::from_millis(20)).await;

    let _h2 = state.get(2).await.unwrap();

    let evicted = Arc::new(AtomicUsize::new(0));
    let evicted_clone = evicted.clone();
    sweeper.sweep_with_handler(move |key, _cell| {
        assert_eq!(key, 1);
        evicted_clone.fetch_add(1, Ordering::Relaxed);
    });
    assert_eq!(evicted.load(Ordering::Relaxed), 1);
}

// ---------------------------------------------------------------------------
// Sweeper introspection
// ---------------------------------------------------------------------------

#[tokio::test]
async fn associated_state_exists() {
    let call_count = Arc::new(AtomicUsize::new(0));
    let factory = CountingFactory {
        value: 0u64,
        call_count,
        _phantom: std::marker::PhantomData::<u64>,
    };
    let (state, sweeper) = State::<u64, u64, _>::new(retention_1s(), factory);
    assert!(sweeper.associated_state_exists());
    drop(state);
    assert!(!sweeper.associated_state_exists());
}

// ---------------------------------------------------------------------------
// OnceCell guarantees
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn once_cell_guarantees_single_init() {
    let call_count = Arc::new(AtomicUsize::new(0));
    let factory = CountingFactory {
        value: 42u64,
        call_count: call_count.clone(),
        _phantom: std::marker::PhantomData,
    };
    let (state, _sweeper) = State::<u64, u64, _>::new(retention_1s(), factory);
    let state = Arc::new(state);

    let mut handles = Vec::new();
    for _ in 0..20 {
        let state = state.clone();
        let task = tokio::task::spawn(async move {
            let h = state.get(1).await.unwrap();
            *h
        });
        handles.push(task);
    }

    for handle in handles {
        let value = handle.await.unwrap();
        assert_eq!(value, 42);
    }
    assert_eq!(call_count.load(Ordering::Relaxed), 1);
}
