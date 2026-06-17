use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use prost::Message as _;
use serde::Deserialize as _;

use waymark_backend_postgres::PostgresBackend;
use waymark_convert_core::TryConvert as _;
use waymark_ids::{InstanceId, WorkflowVersionId};
use waymark_proto::messages as proto;
use waymark_secret_string::SecretStr;
use waymark_state_vm_executables_backend::LoadExecutable as _;
use waymark_vm_codec_core::DeserializerProvider as _;

pub struct WorkflowStore {
    pub backend: PostgresBackend,
    codec: waymark_vm_codec_rmp::RmpCodec,
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
    pub async fn connect(dsn: &SecretStr) -> Result<Self> {
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
            backend,
            codec,
            registration,
            executables,
            outcome_polling,
        })
    }

    pub async fn compile_and_store(
        &self,
        registration: &proto::WorkflowRegistration,
    ) -> Result<(WorkflowVersionId, Vec<String>)> {
        let workflow_version = if registration.workflow_version.is_empty() {
            registration.ir_hash.clone()
        } else {
            registration.workflow_version.clone()
        };

        let ir_program = waymark_proto::ast::Program::decode(&registration.ir[..])
            .map_err(|err| anyhow::anyhow!("decode IR: {err}"))?;
        let ast_program = waymark_vm_ast_old_proto::convert(ir_program)
            .map_err(|err| anyhow::anyhow!("convert IR to AST: {err}"))?;

        let (id, metadata) = self
            .executables
            .compile_and_store(&registration.workflow_name, &workflow_version, &ast_program)
            .await
            .map_err(|err| anyhow::anyhow!("compile and store: {err}"))?;

        let entry_input_names = metadata
            .input_names(Default::default())
            .map(<[String]>::to_vec)
            .unwrap_or_default();

        Ok((id, entry_input_names))
    }

    pub async fn register_vm_runtime(
        &self,
        vm_id: InstanceId,
        executable_id: WorkflowVersionId,
        call_spec: waymark_vm_runtime::CallSpec<
            <waymark_system_vm::Executable as waymark_vm_executable::Functions>::FunctionId,
            waymark_system_vm::Value,
        >,
    ) -> Result<()> {
        let bytes = self
            .backend
            .load_executable(&executable_id)
            .await
            .map_err(|err| anyhow::anyhow!("load executable: {err:?}"))?;

        let executable = self
            .codec
            .with_deserializer(&bytes, |de| waymark_system_vm::Executable::deserialize(de))
            .map_err(|err| anyhow::anyhow!("deserialize executable: {err:?}"))?;

        let interpreter = waymark_vm_interpreter_fullset::FullSetInterpreter::<
            waymark_system_vm::Spec,
            Arc<waymark_system_vm::Executable>,
            waymark_system_vm::Value,
        >::default();

        let runtime: waymark_vm_runtime::Runtime<_, _, waymark_system_vm::Value> =
            waymark_vm_runtime::Runtime::with_custom_entrypoint(interpreter, executable, call_spec)
                .map_err(|err| anyhow::anyhow!("create runtime: {err}"))?;

        self.registration
            .register_vm(vm_id, executable_id, |ser| runtime.snapshot(ser))
            .await
            .map_err(|err| anyhow::anyhow!("register vm: {err}"))
    }

    /// Poll for the outcome of a workflow instance.
    ///
    /// Blocks until an outcome is recorded or the caller cancels.
    pub async fn wait_for_instance(
        &self,
        instance_id: InstanceId,
        poll_interval: Duration,
    ) -> Result<Option<proto::WorkflowArguments>> {
        let outcome = self
            .outcome_polling
            .wait_for_outcome(&instance_id, poll_interval)
            .await
            .map_err(|err| anyhow::anyhow!("wait for outcome: {err}"))?;

        let arguments = match outcome {
            waymark_workflow_service_vm_runtimes::Outcome::Completion(value) => {
                waymark_workflow_completion_convert_proto::Converter::try_convert(value)
                    .map_err(|err| anyhow::anyhow!("convert completion: {err}"))?
            }
            waymark_workflow_service_vm_runtimes::Outcome::Exception(exception) => {
                waymark_workflow_completion_convert_proto::Converter::try_convert(exception)
                    .map_err(|err| anyhow::anyhow!("convert exception: {err}"))?
            }
        };

        Ok(Some(arguments))
    }
}
