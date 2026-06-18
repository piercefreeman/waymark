//! Executable loading and retrieval, backed by
//! [`waymark_state_vm_executables_backend`].
//!
//! Provides an [`ExecutablesFactory`] that implements
//! [`waymark_state_manager_core::Factory`] to load VM executables
//! from a backend on demand.

#![warn(missing_docs)]

use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;

/// Errors returned by [`ExecutablesFactory`].
#[derive(Debug, thiserror::Error)]
pub enum LoadError<BackendError, CodecError> {
    /// The backend failed to load the executable bytes.
    #[error("backend load error: {0}")]
    Backend(#[source] BackendError),

    /// The codec failed to deserialize the executable.
    #[error("deserialize error: {0}")]
    Deserialize(#[source] CodecError),
}

/// A [`Factory`](waymark_state_manager_core::Factory) that loads VM executables
/// from a [`waymark_state_vm_executables_backend::LoadExecutable`] backend,
/// deserializing them with the given [`Codec`].
pub struct ExecutablesFactory<Backend, Codec, Executable> {
    backend: Arc<Backend>,
    codec: Arc<Codec>,
    _phantom: PhantomData<Executable>,
}

impl<Backend, Codec, Executable> ExecutablesFactory<Backend, Codec, Executable> {
    /// Create a new executables factory wrapping the given backend and codec.
    pub fn new(backend: Arc<Backend>, codec: Arc<Codec>) -> Self {
        Self {
            backend,
            codec,
            _phantom: PhantomData,
        }
    }
}

impl<Backend, Codec, Executable> waymark_state_manager_core::Factory
    for ExecutablesFactory<Backend, Codec, Executable>
where
    Backend: waymark_state_vm_executables_backend::LoadExecutable,
    Backend: Send + Sync + 'static,
    Backend::ExecutableId: Hash + Eq + Send + Sync,
    <Backend as waymark_state_vm_executables_backend::LoadExecutable>::Error: Send + 'static,
    Codec: waymark_vm_codec_core::DeserializerProvider,
    Codec: Send + Sync + 'static,
    Codec::Error: Send + 'static,
    Executable: for<'de> serde::Deserialize<'de>,
    Executable: Send + Sync + 'static,
{
    type Key = Backend::ExecutableId;
    type Value = Arc<Executable>;
    type Error = LoadError<
        <Backend as waymark_state_vm_executables_backend::LoadExecutable>::Error,
        Codec::Error,
    >;

    async fn produce(&self, key: &Self::Key) -> Result<Self::Value, Self::Error> {
        let bytes = self
            .backend
            .load_executable(key)
            .await
            .map_err(LoadError::Backend)?;
        let value = self
            .codec
            .with_deserializer(&bytes, |de| serde::Deserialize::deserialize(de))
            .map_err(LoadError::Deserialize)?;
        Ok(Arc::new(value))
    }
}
