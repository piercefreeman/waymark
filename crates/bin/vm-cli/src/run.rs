use crate::integration;

pub async fn run(
    executable: integration::Executable,
) -> Result<integration::SampleValue, waymark_fn_main_common::Error> {
    let interpreter = waymark_vm_interpreter_fullset::FullSetInterpreter::default();

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
                    waymark_vm_interpreter_fullset::Effect::ExtCallSet(effect) => match effect {
                        waymark_vm_interpreter_extcallset::Effect::ExtCall {
                            promise_state_id,
                            extcall_id,
                            args,
                        } => {
                            tracing::info!(
                                ?extcall_id,
                                ?promise_state_id,
                                ?args,
                                "extcall received"
                            );

                            tokio::spawn({
                                let promise_resolutions_tx = promise_resolutions_tx.clone();
                                async move {
                                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

                                    let value = integration::SampleValue::Usize(42);
                                    tracing::info!(
                                        ?extcall_id,
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
                    },
                }
            }
        }
    });

    let value = completion_rx.await?;

    Ok(value)
}
