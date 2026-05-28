use crate::integration;

pub async fn run(
    executable: integration::Executable,
) -> Result<integration::SampleReadyValue, waymark_fn_main_common::Error> {
    let interpreter = waymark_vm_interpreter_fullset::FullSetInterpreter::<
        integration::SampleSpec,
        integration::Executable,
        integration::SampleValue,
    >::default();

    let runtime =
        waymark_vm_runtime::Runtime::with_conventional_entrypoint(interpreter, executable)?;

    let (effects_tx, mut effects_rx) = tokio::sync::mpsc::channel(1);
    let (promise_resolutions_tx, promise_resolutions_rx) = tokio::sync::mpsc::channel(1);

    let mut tasks = tokio::task::JoinSet::new();

    tasks.spawn({
        let params = waymark_vm_driver::Params {
            runtime,
            effects_tx,
            promise_resolutions_rx,
        };
        async move {
            let Err(error) = waymark_vm_driver::run(params).await;
            tracing::info!(?error, "vm driver terminated");
        }
    });

    let (completion_tx, completion_rx) = tokio::sync::oneshot::channel();

    tasks.spawn({
        async move {
            loop {
                let Some(effect) = effects_rx.recv().await else {
                    break;
                };

                match effect {
                    waymark_vm_interpreter_fullset::Effect::CoreSet(effect) => match effect {
                        waymark_vm_interpreter_coreset::Effect::Complete(value) => {
                            completion_tx.send(value).unwrap();
                            break;
                        }
                    },
                    waymark_vm_interpreter_fullset::Effect::ExcSet(effect) => match effect {},
                    waymark_vm_interpreter_fullset::Effect::ExtCallSet(effect) => match effect {
                        waymark_vm_interpreter_extcallset::Effect::ActionCall {
                            promise_state_id,
                            action_ref,
                            args,
                        } => {
                            tracing::info!(
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
                                        ?action_ref,
                                        ?promise_state_id,
                                        ?args,
                                        ?value,
                                        "resolving extcall"
                                    );
                                    promise_resolutions_tx
                                        .send((promise_state_id, value))
                                        .await
                                        .unwrap();
                                }
                            });
                        }
                        waymark_vm_interpreter_extcallset::Effect::Sleep {
                            promise_state_id,
                            duration,
                        } => {
                            tracing::info!(?promise_state_id, ?duration, "sleep received");

                            tokio::spawn({
                                let promise_resolutions_tx = promise_resolutions_tx.clone();
                                async move {
                                    tokio::time::sleep(duration.get()).await;

                                    let value = integration::SampleReadyValue::None;
                                    tracing::info!(?promise_state_id, ?value, "resolving sleep");
                                    promise_resolutions_tx
                                        .send((promise_state_id, value))
                                        .await
                                        .unwrap();
                                }
                            });
                        }
                    },
                    waymark_vm_interpreter_fullset::Effect::PureSet(effect) => match effect {},
                }
            }
        }
    });

    match completion_rx.await? {
        Ok(value) => Ok(value),
        Err(exception) => Err(waymark_fn_main_common::Error::msg(format!(
            "VM completed with an exception: {exception:?}"
        ))),
    }
}
