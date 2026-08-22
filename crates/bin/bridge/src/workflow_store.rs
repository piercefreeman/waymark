use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use prost::Message as _;

use waymark_backend_postgres::PostgresBackend;
use waymark_convert_core::TryConvert as _;
use waymark_ids::{InstanceId, WorkflowVersionId};
use waymark_proto::messages as proto;
use waymark_secret_string::SecretStr;

pub struct WorkflowStore {
    registration: waymark_workflow_service_vm_runtimes::RegistrationService<
        PostgresBackend,
        waymark_vm_codec_rmp::RmpCodec,
    >,
    executables: waymark_workflow_service_vm_executables::ExecutablesService<
        PostgresBackend,
        waymark_vm_codec_rmp::RmpCodec,
        waymark_system_vm::Spec,
        waymark_system_vm::Lowering,
    >,
    outcome_polling: waymark_workflow_service_vm_runtimes::OutcomePollingService<
        PostgresBackend,
        waymark_vm_codec_rmp::RmpCodec,
        waymark_system_vm::ReadyValue,
    >,
}

impl WorkflowStore {
    pub async fn connect(dsn: &SecretStr) -> Result<Self, color_eyre::eyre::Report> {
        let pool = sqlx::PgPool::connect(dsn.expose_secret()).await?;
        waymark_backend_postgres_migrations::run(&pool).await?;
        let backend = PostgresBackend::new(pool);
        let codec = waymark_vm_codec_rmp::RmpCodec;
        let executables = waymark_workflow_service_vm_executables::ExecutablesService::new(
            backend.clone(),
            codec,
        );
        let registration =
            waymark_workflow_service_vm_runtimes::RegistrationService::new(backend.clone(), codec);
        let outcome_polling = waymark_workflow_service_vm_runtimes::OutcomePollingService::new(
            backend.clone(),
            codec,
        );
        Ok(Self {
            registration,
            executables,
            outcome_polling,
        })
    }

    pub async fn compile_and_store(
        &self,
        registration: &proto::WorkflowRegistration,
    ) -> Result<
        (
            WorkflowVersionId,
            Arc<waymark_system_vm::Executable>,
            Vec<String>,
        ),
        color_eyre::eyre::Report,
    > {
        let workflow_version = if registration.workflow_version.is_empty() {
            registration.ir_hash.clone()
        } else {
            registration.workflow_version.clone()
        };

        let ir_program = waymark_proto::ast::Program::decode(&registration.ir[..])
            .map_err(|err| color_eyre::eyre::eyre!("decode IR: {err}"))?;
        let ast_program = waymark_vm_ast_old_proto::convert(ir_program)
            .map_err(|err| color_eyre::eyre::eyre!("convert IR to AST: {err}"))?;

        let (id, executable, metadata) = self
            .executables
            .compile_and_store(&registration.workflow_name, &workflow_version, &ast_program)
            .await
            .map_err(|err| color_eyre::eyre::eyre!("compile and store: {err}"))?;

        let entry_input_names = metadata
            .input_names(Default::default())
            .map(<[String]>::to_vec)
            .unwrap_or_default();

        Ok((id, Arc::new(executable), entry_input_names))
    }

    pub async fn register_vm_runtime(
        &self,
        vm_id: InstanceId,
        executable_id: WorkflowVersionId,
        executable: Arc<waymark_system_vm::Executable>,
        call_spec: waymark_vm_runtime::CallSpec<
            <waymark_system_vm::Executable as waymark_vm_executable::Functions>::FunctionId,
            waymark_system_vm::Value,
        >,
    ) -> Result<(), color_eyre::eyre::Report> {
        let interpreter = waymark_vm_interpreter_fullset::FullSetInterpreter::<
            waymark_system_vm::Spec,
            Arc<waymark_system_vm::Executable>,
            waymark_system_vm::Value,
        >::default();

        let runtime: waymark_vm_runtime::Runtime<_, _, waymark_system_vm::Value> =
            waymark_vm_runtime::Runtime::with_custom_entrypoint(interpreter, executable, call_spec)
                .map_err(|err| color_eyre::eyre::eyre!("create runtime: {err}"))?;

        self.registration
            .register_vm(vm_id, executable_id, |ser| runtime.snapshot(ser))
            .await
            .map_err(|err| color_eyre::eyre::eyre!("register vm: {err}"))
    }

    /// Register all instances, in chunks of at most `batch_max`.
    ///
    /// Each chunk is one atomic backend batch, but the call as a whole is
    /// not atomic: if a chunk fails, the preceding chunks stay durably
    /// registered.
    pub async fn register_vm_runtimes(
        &self,
        executable_id: WorkflowVersionId,
        executable: Arc<waymark_system_vm::Executable>,
        batch_max: NonZeroUsize,
        vms: nonempty_collections::NEVec<(
            InstanceId,
            waymark_vm_runtime::CallSpec<
                <waymark_system_vm::Executable as waymark_vm_executable::Functions>::FunctionId,
                waymark_system_vm::Value,
            >,
        )>,
    ) -> Result<(), color_eyre::eyre::Report> {
        let mut vms = vms.into_iter();
        loop {
            let chunk: Vec<_> = vms.by_ref().take(batch_max.get()).collect();
            if chunk.is_empty() {
                break;
            }

            let mut batch = Vec::with_capacity(chunk.len());
            for (vm_id, call_spec) in chunk {
                let interpreter = waymark_vm_interpreter_fullset::FullSetInterpreter::<
                    waymark_system_vm::Spec,
                    Arc<waymark_system_vm::Executable>,
                    waymark_system_vm::Value,
                >::default();
                let runtime: waymark_vm_runtime::Runtime<_, _, waymark_system_vm::Value> =
                    waymark_vm_runtime::Runtime::with_custom_entrypoint(
                        interpreter,
                        Arc::clone(&executable),
                        call_spec,
                    )
                    .map_err(|err| color_eyre::eyre::eyre!("create runtime: {err}"))?;
                batch.push((vm_id, executable_id, runtime));
            }

            let success = self
                .registration
                .register_vms(
                    nonempty_collections::NEVec::try_from_vec(batch)
                        .expect("chunks of a non-empty list are non-empty"),
                    |runtime, serializer| runtime.snapshot(serializer),
                )
                .await
                .map_err(|err| color_eyre::eyre::eyre!("register vm runtimes: {err}"))?;
            assert!(
                matches!(
                    success,
                    waymark_workflow_service_vm_runtimes_backend::register_vm_runtimes::RegistrationSuccess::AllRegistered,
                ),
                "freshly minted instance ids can never be already registered",
            );
        }

        Ok(())
    }

    /// Poll for the outcome of a workflow instance.
    ///
    /// Blocks until an outcome is recorded or the caller cancels. Returns
    /// `Ok(None)` if no runtime was ever registered for `instance_id`.
    pub async fn wait_for_instance(
        &self,
        instance_id: InstanceId,
        poll_interval: Duration,
    ) -> Result<Option<Vec<u8>>, color_eyre::eyre::Report> {
        let outcome = match self
            .outcome_polling
            .wait_for_outcome(&instance_id, poll_interval)
            .await
        {
            Ok(outcome) => outcome,
            Err(waymark_workflow_service_vm_runtimes::WaitForOutcomeError::NotFound) => {
                return Ok(None);
            }
            Err(err) => return Err(color_eyre::eyre::eyre!("wait for outcome: {err}")),
        };

        let payload: Vec<u8> =
            waymark_vm_value_python_convert_proto::Converter::try_convert(outcome)
                .map_err(|err| color_eyre::eyre::eyre!("convert workflow outcome: {err}"))?;

        Ok(Some(payload))
    }
}
