//! Generic cache with delayed eviction control.

#![warn(missing_docs)]

mod arc;

use core::hash::Hash;
use std::sync::Arc;

use dashmap::DashMap;
use waymark_nonzero_duration::NonZeroDuration;

use self::arc::*;

/// The cache that hold `Key`/`Value` pairs.
///
/// `Value`s are only exposed as read-only to the consumers.
pub struct Cache<Key, Value> {
    /// A map of keys to [`Arc`]-values.
    values: DashMap<Key, ControlledArc<Value>>,

    /// The eviction queue, shared with the [`Handle`]s.
    eviction_queue: Arc<DashMap<Key, std::time::Instant>>,
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
    /// SAFETY: there are safety invariants that depend in this field.
    value: Option<RestrictedArc<Value>>,

    eviction_queue: std::sync::Weak<DashMap<Key, std::time::Instant>>,
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

        // Decrement strong count but retain access to read it.
        let strong_count_access = RestrictedArc::into_strong_count_access(value);

        if strong_count_access.strong_count() == 1 {
            // Only the cache's `values` map itself holds an `Arc` to the value.
            // If there was another entry at the eviction queue due to some
            // sort of a race - the last one wins, so overwrite the preexisting
            // one with a new one.
            let previous = eviction_queue.insert(self.key.clone(), std::time::Instant::now());

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
            values: DashMap::new(),
            eviction_queue: Arc::new(DashMap::new()),
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
        let value: RestrictedArc<Value> = match self.values.entry(key.clone()) {
            dashmap::Entry::Occupied(occupied) => {
                // Bump value-`Arc` strong count before removing from
                // the eviction queue!
                let handle_value = ControlledArc::restricted_clone(occupied.get());
                self.eviction_queue.remove(&key);

                handle_value
            }
            dashmap::Entry::Vacant(vacant) => {
                let value = ControlledArc::new(factory(&key));

                // Bump value-`Arc` string count before inserting into
                // the `values` map.
                let handle_value = ControlledArc::restricted_clone(&value);
                vacant.insert(value);

                // Remove possibly lingering stale eviction requests.
                self.eviction_queue.remove(&key);

                handle_value
            }
        };

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

        let mut cleanup_queue = Vec::with_capacity(cache.eviction_queue.len());
        cache.eviction_queue.retain(|key, orphaned_since| {
            let retain = now.duration_since(*orphaned_since) <= retention;
            if !retain {
                cleanup_queue.push((*key).clone())
            }
            retain
        });

        for key in cleanup_queue {
            let Some((key, value)) = cache.values.remove(&key) else {
                // The value was already evicted - this can happen due to a
                // legitimate race between `sweep` and
                // `Handle::drop`/`get_or_create`:
                //
                // 1. `retain` above removes key K from the eviction queue and
                //    adds it to `cleanup_queue`.
                // 2. `get_or_create(K)` runs concurrently: the occupied
                //    branch bumps the strong count and calls
                //    `eviction_queue.remove(&K)` - a no-op since K was
                //    already stripped in step 1. A fresh `Handle` is
                //    returned.
                // 3. The `Handle` is dropped. The strong count is now 1
                //    (only the `values` map still holds a `ControlledArc`),
                //    so `Handle::drop` inserts K *back* into the eviction
                //    queue with a new timestamp.
                // 4. The sweep loop reaches K and removes it from
                //    `cache.values`; `into_inner` succeeds and
                //    `on_eviction` fires.
                // 5. On a subsequent sweep, K is discovered in the
                //    eviction queue again but is no longer in
                //    `cache.values` - we hit this branch.
                //
                // This is harmless: the value has already been evicted,
                // so we simply skip it.
                continue;
            };

            let Some(value) = ControlledArc::into_inner(value) else {
                // The failure to unwrap the value in here means there was
                // a race where some `Handle` came into existence while we were
                // doing the clean-up; so we just skip this value -
                // the newly created `Handle` with do its own cleanup later.
                continue;
            };

            on_eviction(key, value);
        }
    }
}
