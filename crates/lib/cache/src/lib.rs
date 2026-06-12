//! Generic cache with delayed eviction control.

#![warn(missing_docs)]

mod arc;

use core::hash::Hash;
use std::sync::Arc;

use papaya::{Compute, HashMap, Operation};
use waymark_nonzero_duration::NonZeroDuration;

use self::arc::*;

/// The cache that hold `Key`/`Value` pairs.
///
/// `Value`s are only exposed as read-only to the consumers.
pub struct Cache<Key, Value> {
    /// A map of keys to [`Arc`]-values.
    values: HashMap<Key, ControlledArc<Value>>,

    /// The eviction queue, shared with the [`Handle`]s.
    eviction_queue: Arc<HashMap<Key, std::time::Instant>>,
}

/// The sweeper for a particular cache that performs the eviction for stale
/// entries.
///
/// Upon eviction, an owned `Key`/`Value` pair is provided.
pub struct Sweeper<Key, Value> {
    retention: NonZeroDuration,
    cache: std::sync::Weak<Cache<Key, Value>>,
}

/// A handle to an entry in the cache.
///
/// See [`Cache::get_or_create`].
pub struct Handle<Key, Value>
where
    Key: Eq + Hash + Clone,
{
    key: Key,

    /// The value.
    ///
    /// SAFETY: there are safety invariants that depend on this field.
    value: Option<RestrictedArc<Value>>,

    eviction_queue: std::sync::Weak<HashMap<Key, std::time::Instant>>,
}

impl<Key, Value> std::ops::Deref for Handle<Key, Value>
where
    Key: Eq + Hash + Clone,
{
    type Target = Value;

    fn deref(&self) -> &Value {
        let value_ref = self.value.as_ref();

        // SAFETY: we only clear out the `Option` at drop, which
        // consumes the `Handle` by-value, thus guaranteeing there are no
        // possible live `Deref`s to the `Handle`.
        // Therefore, `deref` can only be called while the `drop` has not been
        // called, and therefore the `self.value` will always be present here.
        unsafe { value_ref.unwrap_unchecked() }
    }
}

impl<Key, Value> Handle<Key, Value>
where
    Key: Eq + Hash + Clone,
{
    /// Return a ref to the key for this entry.
    pub fn key(this: &Self) -> &Key {
        &this.key
    }
}

impl<Key, Value> Drop for Handle<Key, Value>
where
    Key: Eq + Hash + Clone,
{
    #[expect(
        unused_unsafe,
        reason = "distant invariant enforcement via safe-only fn calls"
    )]
    fn drop(&mut self) {
        let Some(eviction_queue) = self.eviction_queue.upgrade() else {
            return;
        };

        // SAFETY: this `take` affects `std::ops::Deref` implementation.
        // The invariant here is: this is the only place we make
        // the `self.value` option `None`.
        let value = unsafe { self.value.take() };

        let Some(value) = value else {
            // `drop` can't be called twice.
            unreachable!();
        };

        // If the map holds the only remaining strong reference after this
        // handle is dropped, register the key for potential eviction.

        // Decrement strong count but retain access to read it.
        let strong_count_access = RestrictedArc::into_strong_count_access(value);

        if strong_count_access.strong_count() == 1 {
            // Only the cache's `values` map itself holds a `ControlledArc`
            // to the value.
            let queue = eviction_queue.pin();
            let previous = queue.insert(self.key.clone(), std::time::Instant::now());

            if previous.is_some() {
                // Found a race! Nothing special to do really, but we'll log
                // it just in case - if users see a lot of these it would
                // warrant an investigation, as we should be able to make this
                // even safer and more reliable.
                // Or it could be caused by a newly introduced bug.
                tracing::warn!(
                    "detected race at cache handle drop; \
                    this is probably safe as it anticipated in the design - \
                    but still logged cause it  should be rare and notable \
                    occurrence"
                );
            }
        }
    }
}

impl<Key, Value> Cache<Key, Value>
where
    Key: Eq + Hash,
{
    /// Create a new [`Cache`] with the specified `retention`.
    ///
    /// Returns the [`Arc`] of a newly created cache, and a [`Sweeper`] for
    /// this cache to allow eviction of stale entries.
    pub fn new(retention: NonZeroDuration) -> (Arc<Self>, Sweeper<Key, Value>) {
        let cache = Self {
            values: HashMap::new(),
            eviction_queue: Arc::new(HashMap::new()),
        };

        let cache = Arc::new(cache);
        let sweeper = Sweeper {
            retention,
            cache: Arc::downgrade(&cache),
        };

        (cache, sweeper)
    }
}

impl<Key, Value> Cache<Key, Value>
where
    Key: Eq + Hash + Clone,
{
    /// Get or create a new value in the cache, and return a [`Handle`] to it.
    ///
    /// [`Handle`] provides read-only access to the value, and while
    /// the [`Handle`] is held the value will not be removed from the cache.
    ///
    /// After the last [`Handle`] to a value is dropped, the value
    pub fn get_or_create<Factory>(&self, key: Key, factory: Factory) -> Handle<Key, Value>
    where
        Factory: for<'a> FnOnce(&'a Key) -> Value,
    {
        // `OnceCell` lets us smuggle the `RestrictedArc` out of the
        // `compute` closure, which is the only papaya operation that
        // atomically pairs the strong-count bump with the get/insert.
        // `get_or_insert_with` cannot do this — the sweeper can remove
        // the entry between the get and the clone, causing spurious
        // re-creation of values that should still be alive.
        let cell = std::cell::OnceCell::new();

        let values = self.values.pin();
        let queue = self.eviction_queue.pin();

        // `compute` requires `FnMut`, but `factory` is `FnOnce`.  Wrap
        // it in an `Option` so we can take it out inside the closure.
        let mut factory = Some(factory);

        let _compute = values.compute(key.clone(), |entry| match entry {
            Some((_k, existing)) => {
                let handle = ControlledArc::restricted_clone(existing);
                queue.remove(&key);
                let _ = cell.set(handle);
                Operation::Abort(())
            }
            None => {
                let f = factory
                    .take()
                    .expect("compute closure is called at most once");
                let value = ControlledArc::new(f(&key));
                let handle = ControlledArc::restricted_clone(&value);
                queue.remove(&key);
                let _ = cell.set(handle);
                Operation::Insert(value)
            }
        });

        let value = cell
            .into_inner()
            .expect("compute closure must populate the OnceCell");

        Handle {
            key,
            value: Some(value),
            eviction_queue: Arc::downgrade(&self.eviction_queue),
        }
    }
}

impl<Key, Value> Sweeper<Key, Value>
where
    Key: Eq + Hash + Clone,
{
    /// Perform one sweep on the eviction queue, evicting all the entries
    /// that have not been held on to for longer than the configured
    /// retention duration.
    pub fn sweep(&mut self, mut on_eviction: impl FnMut(Key, Value)) {
        let Some(cache) = self.cache.upgrade() else {
            return;
        };

        let retention = self.retention.get();
        let now = std::time::Instant::now();

        // Phase 1 — inside papaya pin scope: remove stale entries from
        // both the eviction queue and the values map, collecting a
        // `RestrictedArc` of each removed value.  The epoch garbage holds
        // the original `ControlledArc` alive while the pin is held, so our
        // clone brings the strong count to 2.
        let evictions: Vec<(Key, RestrictedArc<Value>)> = {
            let queue = cache.eviction_queue.pin();
            let mut cleanup_queue = Vec::with_capacity(queue.len());
            queue.retain(|key, orphaned_since| {
                let retain = now.duration_since(*orphaned_since) <= retention;
                if !retain {
                    cleanup_queue.push(key.clone());
                }
                retain
            });
            drop(queue);

            let mut evictions = Vec::new();
            for key in cleanup_queue {
                let values = cache.values.pin();
                let compute_result = values.compute(key.clone(), |entry| match entry {
                    Some((_k, v)) if ControlledArc::strong_count(v) == 1 => Operation::Remove,
                    _ => Operation::Abort(()),
                });

                if let Compute::Removed(_k, v) = compute_result {
                    evictions.push((key, ControlledArc::restricted_clone(v)));
                }
            }

            evictions
        }; // All papaya pins dropped — epoch releases originals.

        // Phase 2 — no pins held: `try_into_inner` is the atomic gate.
        // If the `RestrictedArc` is the sole owner (strong_count == 1
        // after epoch cleanup), we extract the owned `Value`.  If
        // another handle still references the value, `try_into_inner`
        // returns `None` and we skip — the entry was removed from the
        // map in phase 1, so no new handles can be created for this
        // key; the existing handles keep the value alive until they
        // are dropped.
        for (key, arc) in evictions {
            if let Some(value) = arc.try_into_inner() {
                on_eviction(key, value);
            }
        }
    }
}
