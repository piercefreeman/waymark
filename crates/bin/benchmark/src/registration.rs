//! Compiling the benchmark cases and registering their VM instances.

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

use color_eyre::eyre::WrapErr as _;
use rand::seq::SliceRandom;
use waymark_backend_postgres::PostgresBackend;

use crate::cases::BenchmarkCase;

struct CompiledCase {
    executable_id: waymark_ids::WorkflowVersionId,
    executable: Arc<waymark_system_vm::Executable>,
    metadata: waymark_vm_compiler_for_ast_old_core::Metadata,
}

pub async fn register_benchmark_vms(
    executables: &waymark_workflow_service_vm_executables::ExecutablesService<
        PostgresBackend,
        waymark_vm_codec_rmp::RmpCodec,
        waymark_system_vm::Spec,
        waymark_system_vm::Lowering,
    >,
    registration: &waymark_workflow_service_vm_runtimes::RegistrationService<
        PostgresBackend,
        waymark_vm_codec_rmp::RmpCodec,
    >,
    cases: &HashMap<String, BenchmarkCase>,
    count_per_case: NonZeroUsize,
    registration_batch_max: NonZeroUsize,
) -> Result<usize, color_eyre::eyre::Report> {
    let mut compiled = HashMap::new();
    for (name, case) in cases {
        match executables
            .compile_and_store(name, &case.ir_hash, &case.program)
            .await
        {
            Ok((executable_id, executable, metadata)) => {
                compiled.insert(
                    name.clone(),
                    CompiledCase {
                        executable_id,
                        executable: Arc::new(executable),
                        metadata,
                    },
                );
            }
            Err(err) => {
                eprintln!("Skipping IR job '{name}': compilation failed: {err}");
            }
        }
    }

    let mut case_names = Vec::new();
    for name in compiled.keys() {
        for _ in 0..count_per_case.get() {
            case_names.push(name.clone());
        }
    }
    case_names.shuffle(&mut rand::rng());

    for chunk in case_names.chunks(registration_batch_max.get()) {
        let mut vms = Vec::with_capacity(chunk.len());
        for name in chunk {
            let case = cases.get(name).expect("case");
            let compiled_case = compiled.get(name).expect("compiled case");

            let call_spec = waymark_vm_runtime_builder::builder(&compiled_case.metadata)
                .first_fn()
                .wrap_err_with(|| format!("select entry function for case '{name}'"))?
                .args(case.inputs.clone())
                .wrap_err_with(|| format!("match entry function arguments for case '{name}'"))?;
            let runtime = waymark_system_vm::Runtime::with_custom_entrypoint(
                waymark_system_vm::Interpreter::default(),
                Arc::clone(&compiled_case.executable),
                call_spec,
            )
            .wrap_err_with(|| format!("create VM runtime for case '{name}'"))?;

            vms.push((
                waymark_ids::InstanceId::new_uuid_v4(),
                compiled_case.executable_id,
                runtime,
            ));
        }

        let success = registration
            .register_vms(
                nonempty_collections::NEVec::try_from_vec(vms)
                    .expect("chunks of a non-empty list are non-empty"),
                |runtime, serializer| runtime.snapshot(serializer),
            )
            .await
            .wrap_err("register VMs")?;
        assert!(
            matches!(
                success,
                waymark_workflow_service_vm_runtimes_backend::register_vm_runtimes::RegistrationSuccess::AllRegistered,
            ),
            "freshly minted instance ids can never be already registered",
        );
    }

    Ok(case_names.len())
}
