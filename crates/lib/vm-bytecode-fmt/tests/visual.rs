use index_type::typed_vec;
use waymark_vm_bytecode::{Executable, Function, State};
use waymark_vm_bytecode_core::StateId;

#[allow(dead_code)]
#[derive(Debug)]
enum DummyInstruction {
    LoadConst7,
    Jump { target: StateId },
    Return,
}

#[test]
fn fmt_display_prints_dummy_instruction_set() {
    let executable = Executable {
        functions: typed_vec![Function {
            states: typed_vec![
                State {
                    instructions: typed_vec![
                        DummyInstruction::LoadConst7,
                        DummyInstruction::Jump { target: StateId(1) },
                    ],
                },
                State {
                    instructions: typed_vec![DummyInstruction::Return,],
                }
            ],
            num_regs: 123,
        }],
    };

    insta::assert_snapshot!(waymark_vm_bytecode_fmt::display(&executable));
}
