use waymark_vm_bytecode::{FunctionId, StateId};

use crate::{PromiseStateId, Registers};

pub struct Frame<Value> {
    pub func: FunctionId,
    pub state: StateId,
    pub regs: Registers<Value>,
    pub kind: FrameKind,
}

pub enum FrameKind {
    TopLevel,
    FnCall { ret: PromiseStateId },
}
