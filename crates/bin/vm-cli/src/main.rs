mod integration;

#[tokio::main]
async fn main() -> Result<(), waymark_fn_main_common::Error> {
    waymark_fn_main_common::init()?;

    let executable = sample_executable();

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
                        waymark_vm_interpreter_coreset::Effect::ExtCall {
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

    let expected = (42 + 5) * 2;

    tracing::info!(?value, ?expected, "program complete");
    Ok(())
}

fn sample_executable() -> integration::Executable {
    use self::integration::*;
    use index_type::{IndexType, typed_vec};
    use waymark_vm_bytecode::*;
    use waymark_vm_bytecode_core::*;
    use waymark_vm_instructions_coreset::CoreSet;
    use waymark_vm_instructions_pureset::PureSet;
    use waymark_vm_runtime_core::RegisterId;

    // function 1: f() = await ExtCall(SampleExtCallId(42)) + 5
    let f1 = Function {
        num_regs: 3,
        states: typed_vec![
            State {
                instructions: typed_vec![
                    CoreSet::ExtCall {
                        dst: RegisterId::from_scalar(0),
                        extcall_id: SampleExtCallId(42),
                        args: vec![],
                        resume: StateId::from_scalar(1),
                    }
                    .into(),
                ]
            },
            State {
                instructions: typed_vec![
                    CoreSet::Await {
                        dst: RegisterId::from_scalar(1),
                        src: RegisterId::from_scalar(0),
                        resume: StateId::from_scalar(2),
                    }
                    .into(),
                ],
            },
            State {
                instructions: typed_vec![
                    PureSet::LoadConst {
                        dst: RegisterId::from_scalar(2),
                        value: SampleConstValue::Usize(5),
                    }
                    .into(),
                    PureSet::Add {
                        dst: RegisterId::from_scalar(2),
                        a: RegisterId::from_scalar(1),
                        b: RegisterId::from_scalar(2),
                    }
                    .into(),
                    CoreSet::Return {
                        src: RegisterId::from_scalar(2),
                    }
                    .into()
                ],
            },
        ],
    };

    // main: run f twice concurrently and sum
    let main_fn = Function {
        num_regs: 5,
        states: typed_vec![
            State {
                instructions: typed_vec![
                    CoreSet::Call {
                        dst: RegisterId::from_scalar(0),
                        function_id: FunctionId::from_scalar(1),
                        args: vec![]
                    }
                    .into(),
                    CoreSet::Call {
                        dst: RegisterId::from_scalar(1),
                        function_id: FunctionId::from_scalar(1),
                        args: vec![]
                    }
                    .into(),
                    CoreSet::Await {
                        dst: RegisterId::from_scalar(2),
                        src: RegisterId::from_scalar(0),
                        resume: StateId::from_scalar(1),
                    }
                    .into(),
                ],
            },
            State {
                instructions: typed_vec![
                    CoreSet::Await {
                        dst: RegisterId::from_scalar(3),
                        src: RegisterId::from_scalar(1),
                        resume: StateId::from_scalar(2),
                    }
                    .into()
                ],
            },
            State {
                instructions: typed_vec![
                    PureSet::Add {
                        dst: RegisterId::from_scalar(4),
                        a: RegisterId::from_scalar(2),
                        b: RegisterId::from_scalar(3)
                    }
                    .into(),
                    CoreSet::Return {
                        src: RegisterId::from_scalar(4)
                    }
                    .into()
                ],
            },
        ],
    };

    waymark_vm_bytecode::Executable {
        functions: typed_vec![main_fn, f1],
    }
}
