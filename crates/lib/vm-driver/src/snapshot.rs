use std::ops::Deref;

/// Reusable buffer for serializing runtime snapshots.
///
/// The buffer keeps its allocation across ticks so snapshot size growth
/// is amortized.
#[derive(Default)]
pub struct Buffer {
    reusable_buffer: Vec<u8>,
}

impl Buffer {
    /// Run `serialize` with the internal byte buffer, then return a
    /// [`WriteGuard`] that provides `&[u8]` access to the result.
    ///
    /// When the guard is dropped the buffer is cleared, ensuring
    /// serialized state does not linger in memory.
    pub fn write_with<E>(
        &mut self,
        serialize: impl FnOnce(&mut Vec<u8>) -> Result<(), E>,
    ) -> Result<WriteGuard<'_>, E> {
        serialize(&mut self.reusable_buffer)?;
        Ok(WriteGuard { buffer: self })
    }
}

/// RAII guard returned by [`Buffer::write_with`].
///
/// While alive the guard provides `&[u8]` access to the serialized data.
/// On drop the internal buffer is cleared.
pub struct WriteGuard<'a> {
    buffer: &'a mut Buffer,
}

impl Deref for WriteGuard<'_> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.buffer.reusable_buffer
    }
}

impl Drop for WriteGuard<'_> {
    fn drop(&mut self) {
        self.buffer.reusable_buffer.clear();
    }
}

impl core::fmt::Debug for Buffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Buffer")
            .field("data_used", &self.reusable_buffer.len())
            .field("capacity", &self.reusable_buffer.capacity())
            .finish()
    }
}
