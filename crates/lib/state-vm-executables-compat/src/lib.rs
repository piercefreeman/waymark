//! Compatibility layer for loading VM executables from the legacy
//! workflow-registry IR storage.
//!
//! This crate bridges the old IR-based workflow storage
//! ([`WorkflowRegistryBackend`]) and the old AST compiler
//! ([`vm_compiler_for_ast_old`]) to produce a
//! [`Factory`](waymark_state_manager_core::Factory) that can be used
//! with [`State`](waymark_state_manager::State) for lazy, caching
//! executable loading.
//!
//! # Architecture
//!
//! ```text
//! Factory::produce(workflow_version_id)
//!   → WorkflowRegistryBackend::get_workflow_versions(id)
//!   → decode program_proto bytes into ir::Program
//!   → vm_ast_old_proto::convert() → vm_ast_old::Program
//!   → vm_compiler_for_ast_old::compile() → Executable
//! ```

#![warn(missing_docs)]

use std::marker::PhantomData;
use std::sync::Arc;

use prost::Message as _;
use waymark_backends_core::BackendError;
use waymark_ids::WorkflowVersionId;
use waymark_state_manager_core::Factory;
use waymark_vm_compiler_for_ast_old_core::ExecutableFor;
use waymark_vm_compiler_for_ast_old_core::SpecRequirements;
use waymark_vm_compiler_for_ast_old_core::lowering;
use waymark_workflow_registry_backend::WorkflowRegistryBackend;

/// Errors produced by [`CompatExecutableFactory`] when loading an
/// executable through the compatibility pipeline.
#[derive(Debug, thiserror::Error)]
pub enum CompatError<CompilerError> {
    /// The backend failed to retrieve the workflow version.
    #[error("registry backend error: {0}")]
    Registry(BackendError),

    /// The workflow version was not found in the registry.
    #[error("workflow version not found: {0:?}")]
    NotFound(WorkflowVersionId),

    /// The IR protobuf could not be decoded.
    #[error("protobuf decode error: {0}")]
    ProtobufDecode(#[from] prost::DecodeError),

    /// The IR-to-AST conversion failed.
    #[error("ir-to-ast conversion error: {0}")]
    Converter(waymark_vm_ast_old_proto::ConvertError),

    /// The bytecode compilation failed.
    #[error("compilation error: {0}")]
    Compiler(CompilerError),
}

/// A [`Factory`] that fetches stored IR protobuf bytes via a
/// [`WorkflowRegistryBackend`], converts them to the legacy AST via
/// [`waymark_vm_ast_old_proto`], and compiles them into bytecode with
/// the old [`vm_compiler_for_ast_old`] compiler.
///
/// # Type parameters
///
/// * `Backend` — a [`WorkflowRegistryBackend`] implementation (e.g. Postgres,
///   in-memory).
/// * `Spec` — the instruction-set spec (implements [`SpecRequirements`]).
/// * `Lowering` — the lowering strategy that maps AST literals/actions to
///   the spec's const values and action references.
pub struct CompatExecutableFactory<Backend, Spec, Lowering> {
    backend: Backend,
    _phantom: PhantomData<(Spec, Lowering)>,
}

impl<Backend, Spec, Lowering> CompatExecutableFactory<Backend, Spec, Lowering> {
    /// Create a new compatibility factory wrapping the given registry backend.
    pub fn new(backend: Backend) -> Self {
        Self {
            backend,
            _phantom: PhantomData,
        }
    }
}

impl<Backend, Spec, Lowering> Factory for CompatExecutableFactory<Backend, Spec, Lowering>
where
    Backend: WorkflowRegistryBackend + Sync,
    Spec: SpecRequirements + Sync,
    Lowering: lowering::FullSet<Spec> + Sync,
    waymark_vm_compiler_for_ast_old::CompileErrorFor<Spec, Lowering>: Send,
{
    type Key = WorkflowVersionId;
    type Value = Arc<ExecutableFor<Spec>>;
    type Error = CompatError<waymark_vm_compiler_for_ast_old::CompileErrorFor<Spec, Lowering>>;

    async fn produce(&self, key: &Self::Key) -> Result<Self::Value, Self::Error> {
        let versions = self
            .backend
            .get_workflow_versions(&[*key])
            .await
            .map_err(CompatError::Registry)?;

        let version = versions
            .into_iter()
            .next()
            .ok_or(CompatError::NotFound(*key))?;

        let program = waymark_proto::ast::Program::decode(&version.program_proto[..])?;

        let program = waymark_vm_ast_old_proto::convert(program).map_err(CompatError::Converter)?;

        let executable = waymark_vm_compiler_for_ast_old::compile::<Spec, Lowering>(&program)
            .map_err(CompatError::Compiler)?;

        let executable = Arc::new(executable);

        Ok(executable)
    }
}
