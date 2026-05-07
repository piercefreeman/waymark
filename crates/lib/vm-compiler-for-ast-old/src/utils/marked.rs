//! Phantom-marker wrapper for type-safe state tagging.

/// A zero-cost wrapper that tags a value with a phantom marker type.
pub struct Marked<T, Marker> {
    /// Marker carried at the type level only.
    phantom_data: core::marker::PhantomData<Marker>,

    /// Wrapped value.
    value: T,
}

impl<T, Marker> Marked<T, Marker> {
    /// Wraps `value` with the given marker type.
    pub fn mark(value: T) -> Self {
        Self {
            phantom_data: std::marker::PhantomData,
            value,
        }
    }

    /// Removes the marker wrapper and returns the underlying value.
    pub fn unmark(marked: Self) -> T {
        marked.value
    }
}

impl<T, Marker> core::ops::Deref for Marked<T, Marker> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T, Marker> core::ops::DerefMut for Marked<T, Marker> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

impl<T: Clone, Marker> Clone for Marked<T, Marker> {
    fn clone(&self) -> Self {
        Self {
            phantom_data: core::marker::PhantomData,
            value: self.value.clone(),
        }
    }
}

impl<T: Copy, Marker> Copy for Marked<T, Marker> {}

impl<T: core::fmt::Debug, Marker> core::fmt::Debug for Marked<T, Marker> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple(&format!("Marked<{}>", core::any::type_name::<T>()))
            .field(&self.value)
            .finish()
    }
}

impl<T: PartialEq, Marker> PartialEq for Marked<T, Marker> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl<T: Eq, Marker> Eq for Marked<T, Marker> {}

impl<T: core::hash::Hash, Marker> core::hash::Hash for Marked<T, Marker> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.value.hash(state);
    }
}
