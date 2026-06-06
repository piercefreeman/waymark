#[derive(Default)]
pub struct Buffer {
    reusable_buffer: Vec<u8>,
}

impl Buffer {
    pub fn r#use(&mut self) -> InUseBuffer<'_> {
        self.reusable_buffer.clear();
        let serializer = rmp_serde::Serializer::new(&mut self.reusable_buffer);
        InUseBuffer { serializer }
    }
}

pub struct InUseBuffer<'buffer> {
    serializer: rmp_serde::Serializer<&'buffer mut Vec<u8>>,
}

impl<'buffer> InUseBuffer<'buffer> {
    pub fn serializer(&mut self) -> &mut rmp_serde::Serializer<&'buffer mut Vec<u8>> {
        &mut self.serializer
    }

    pub fn data(&self) -> &Vec<u8> {
        self.serializer.get_ref()
    }
}

impl<'buffer> Drop for InUseBuffer<'buffer> {
    fn drop(&mut self) {
        self.serializer.get_mut().clear();
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
