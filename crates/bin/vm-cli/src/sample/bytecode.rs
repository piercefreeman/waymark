pub fn executable() -> waymark_system_vm::Executable {
    use index_type::{IndexType, typed_vec};
    use waymark_system_vm::ConstValue;
    use waymark_vm_bytecode::*;
    use waymark_vm_bytecode_core::*;
    use waymark_vm_instructions_coreset::CoreSet;
    use waymark_vm_instructions_extcallset::ExtCallSet;
    use waymark_vm_instructions_pureset::PureSet;
    use waymark_vm_runtime_core::RegisterId;

    // function 1: f() = await double(value=21) + 5
    let f1 = Function {
        num_regs: 5,
        states: typed_vec![
            State {
                instructions: typed_vec![
                    PureSet::LoadConst {
                        dst: RegisterId::from_scalar(0),
                        value: ConstValue::Int(21),
                    }
                    .into(),
                    ExtCallSet::ActionCall {
                        dst: RegisterId::from_scalar(1),
                        action_ref: waymark_action_core::ActionRef {
                            action_name: "double".to_owned(),
                            module_name: Some("tests.fixtures.test_actions".to_owned()),
                            call_args: vec!["value".to_owned()],
                        },
                        args: vec![RegisterId::from_scalar(0)],
                        resume: StateId::from_scalar(1),
                    }
                    .into(),
                ]
            },
            State {
                instructions: typed_vec![
                    CoreSet::Await {
                        dst: RegisterId::from_scalar(2),
                        src: RegisterId::from_scalar(1),
                        resume: StateId::from_scalar(2),
                    }
                    .into(),
                ],
            },
            State {
                instructions: typed_vec![
                    PureSet::LoadConst {
                        dst: RegisterId::from_scalar(3),
                        value: ConstValue::Int(5),
                    }
                    .into(),
                    PureSet::Binary {
                        kind: waymark_vm_instructions_pureset::BinaryOpKind::Add,
                        op: waymark_vm_instructions_pureset::BinaryOp {
                            dst: RegisterId::from_scalar(4),
                            a: RegisterId::from_scalar(2),
                            b: RegisterId::from_scalar(3),
                        },
                    }
                    .into(),
                    CoreSet::Return {
                        src: RegisterId::from_scalar(4),
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
                    PureSet::Binary {
                        kind: waymark_vm_instructions_pureset::BinaryOpKind::Add,
                        op: waymark_vm_instructions_pureset::BinaryOp {
                            dst: RegisterId::from_scalar(4),
                            a: RegisterId::from_scalar(2),
                            b: RegisterId::from_scalar(3)
                        },
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
