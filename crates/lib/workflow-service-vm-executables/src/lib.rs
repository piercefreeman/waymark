//! VM compiler service — compiles ast-old programs and stores the
//! resulting bytecode via a
//! [`waymark_workflow_service_vm_executables_backend::UpsertExecutable`] backend.

#![warn(missing_docs)]

use std::marker::PhantomData;

use serde::Serialize;

/// Errors returned by [`ExecutablesService::compile_and_store`].
#[derive(Debug, thiserror::Error)]
pub enum CompileAndStoreError<CompileError, SerializeError, StoreError> {
    /// The compilation failed.
    #[error("compilation: {0}")]
    Compile(#[source] CompileError),

    /// The bytecode serialization failed.
    #[error("serialize bytecode: {0:?}")]
    Serialize(#[source] SerializeError),

    /// The backend store operation failed.
    #[error("store bytecode: {0:?}")]
    Store(#[source] StoreError),
}

/// Service that compiles ast-old programs to bytecode and stores them.
pub struct ExecutablesService<Backend, Codec, Spec, Lowering> {
    backend: Backend,
    codec: Codec,
    _phantom: PhantomData<(Spec, Lowering)>,
}

impl<Backend, Codec, Spec, Lowering> ExecutablesService<Backend, Codec, Spec, Lowering> {
    /// Create a new compiler service wrapping the given backend and codec.
    pub fn new(backend: Backend, codec: Codec) -> Self {
        Self {
            backend,
            codec,
            _phantom: PhantomData,
        }
    }
}

impl<Backend, Codec, Spec, Lowering> ExecutablesService<Backend, Codec, Spec, Lowering>
where
    Backend: waymark_workflow_service_vm_executables_backend::UpsertExecutable,
    Backend: Send + Sync + 'static,
    Backend::ExecutableId: Send + Sync,
    Codec: waymark_vm_codec_core::SerializerProvider,
    Codec: Send + Sync + 'static,
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements + Sync,
    Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec> + Sync,
    waymark_vm_compiler_for_ast_old::CompileErrorFor<Spec, Lowering>: Send,
    waymark_vm_compiler_for_ast_old_core::ExecutableFor<Spec>: serde::Serialize,
{
    /// Compile an ast-old program and atomically store it with deduplication.
    ///
    /// Returns the stored executable's ID and the program
    /// [`Metadata`](waymark_vm_compiler_for_ast_old_core::Metadata).
    pub async fn compile_and_store(
        &self,
        name: &str,
        version: &str,
        program: &waymark_vm_ast_old::Program,
    ) -> Result<
        (
            Backend::ExecutableId,
            waymark_vm_compiler_for_ast_old_core::Metadata,
        ),
        CompileAndStoreError<
            waymark_vm_compiler_for_ast_old::CompileErrorFor<Spec, Lowering>,
            Codec::Error,
            Backend::Error,
        >,
    > {
        let (executable, metadata) =
            waymark_vm_compiler_for_ast_old::compile_with_metadata::<Spec, Lowering>(program)
                .map_err(CompileAndStoreError::Compile)?;

        let mut bytes = Vec::new();
        self.codec
            .with_serializer(&mut bytes, |ser| executable.serialize(ser))
            .map_err(CompileAndStoreError::Serialize)?;

        let id = self
            .backend
            .upsert_executable(name, version, &bytes)
            .await
            .map_err(CompileAndStoreError::Store)?;

        Ok((id, metadata))
    }
}
