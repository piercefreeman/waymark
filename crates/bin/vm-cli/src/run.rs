use crate::integration;
use waymark_vm_codec_rmp::RmpCodec;
use waymark_vm_driver_core::{PromiseResolution, PromiseSettlement};

#[derive(Debug, thiserror::Error)]
pub enum RunError {
    #[error("the program entrypoint is invalid: {0}")]
    InvalidEntryPoint(
        waymark_vm_runtime::FunctionNotFoundError<waymark_vm_bytecode_core::FunctionId>,
    ),

    #[error("the runtime task has crashed before completing")]
    RuntimeTaskCrashed,

    #[error("execution completed with an unhandled exception: {}", .0.type_id)]
    UnhandledException(waymark_vm_runtime_exception::Exception<integration::SampleReadyValue>),
}

pub async fn run(
    executable: integration::Executable,
) -> Result<integration::SampleReadyValue, RunError> {
    let interpreter = waymark_vm_interpreter_fullset::FullSetInterpreter::<
        integration::SampleSpec,
        integration::Executable,
        integration::SampleValue,
    >::default();

    let runtime =
        waymark_vm_runtime::Runtime::with_conventional_entrypoint(interpreter, executable)
            .map_err(RunError::InvalidEntryPoint)?;

    let (effects_tx, mut effects_rx) = tokio::sync::mpsc::channel(1);
    let (promise_resolutions_tx, promise_resolutions_rx) =
        tokio::sync::mpsc::channel::<PromiseSettlement<integration::SampleReadyValue, ()>>(1);

    let mut tasks = tokio::task::JoinSet::new();

    tasks.spawn({
        let params = waymark_vm_driver::Params {
            runtime,
            effector: (effects_tx, promise_resolutions_rx),
            persister: (),
            codec: RmpCodec,
            cancel: Default::default(),
        };
        async move {
            let Err(error) = waymark_vm_driver::run(params).await;
            tracing::info!(?error, "vm driver terminated");
        }
    });

    let (completion_tx, completion_rx) = tokio::sync::oneshot::channel::<
        Result<
            integration::SampleReadyValue,
            waymark_vm_runtime_exception::Exception<integration::SampleReadyValue>,
        >,
    >();

    tasks.spawn({
        async move {
            loop {
                let Some(emitted_effect) = effects_rx.recv().await else {
                    break;
                };

                match emitted_effect.effect {
                    waymark_vm_interpreter_fullset::Effect::CoreSet(effect) => match effect {
                        waymark_vm_interpreter_coreset::Effect::Complete(value) => {
                            completion_tx.send(Ok(value)).unwrap();
                            break;
                        }
                        waymark_vm_interpreter_coreset::Effect::UnhandledException(exception) => {
                            completion_tx.send(Err(exception)).unwrap();
                            break;
                        }
                    },
                    waymark_vm_interpreter_fullset::Effect::ExtCallSet(effect) => match effect {
                        waymark_vm_interpreter_extcallset::Effect::ActionCall {
                            promise_state_id,
                            action_ref,
                            args,
                        } => {
                            tracing::info!(
                                effect_number = %emitted_effect.number,
                                ?action_ref,
                                ?promise_state_id,
                                ?args,
                                "extcall received"
                            );

                            tokio::spawn({
                                let promise_resolutions_tx = promise_resolutions_tx.clone();
                                async move {
                                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

                                    let value = integration::SampleReadyValue::Int(42);
                                    tracing::info!(
                                        effect_number = %emitted_effect.number,
                                        ?action_ref,
                                        ?promise_state_id,
                                        ?args,
                                        ?value,
                                        "resolving extcall"
                                    );
                                    promise_resolutions_tx
                                        .send(PromiseSettlement {
                                            promise_state_id,
                                            resolution: PromiseResolution::Resolved(value),
                                            ack: (),
                                        })
                                        .await
                                        .unwrap();
                                }
                            });
                        }
                        waymark_vm_interpreter_extcallset::Effect::Sleep {
                            promise_state_id,
                            duration,
                        } => {
                            tracing::info!(
                                effect_number = %emitted_effect.number,
                                ?promise_state_id,
                                ?duration,
                                "sleep received"
                            );

                            tokio::spawn({
                                let promise_resolutions_tx = promise_resolutions_tx.clone();
                                async move {
                                    tokio::time::sleep(duration.get()).await;

                                    let value = integration::SampleReadyValue::None;
                                    tracing::info!(
                                        effect_number = %emitted_effect.number,
                                        ?promise_state_id,
                                        ?value,
                                        "resolving sleep"
                                    );
                                    promise_resolutions_tx
                                        .send(PromiseSettlement {
                                            promise_state_id,
                                            resolution: PromiseResolution::Resolved(value),
                                            ack: (),
                                        })
                                        .await
                                        .unwrap();
                                }
                            });
                        }
                    },
                }
            }
        }
    });

    let result = completion_rx
        .await
        .map_err(|_| RunError::RuntimeTaskCrashed)?;
    let value = result.map_err(RunError::UnhandledException)?;

    Ok(value)
}
