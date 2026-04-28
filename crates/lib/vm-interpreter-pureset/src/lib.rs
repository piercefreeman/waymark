use waymark_vm_runtime_core::{Frame, Promise};

pub struct PureSetInterpreter<Value> {
    phantom_data: core::marker::PhantomData<Value>,
}

impl<Value> Default for PureSetInterpreter<Value> {
    fn default() -> Self {
        Self {
            phantom_data: Default::default(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("add: {0}")]
    Add(AddError),
}

#[derive(Debug, thiserror::Error)]
pub enum AddError {
    #[error("adding non-addable values")]
    NotAddable,

    #[error("addition result is out of bounds")]
    ResultOutOfBounds,
}

pub trait Value: Sized {
    fn add(a: &Self, b: &Self) -> Result<Self, AddError>;
}

impl<Spec> waymark_vm_interpreter::Interpreter for PureSetInterpreter<Spec>
where
    Spec: waymark_vm_instructions_pureset::Spec<RegisterId = waymark_vm_runtime_core::RegisterId>
        + 'static,
    Spec::Value: Clone + 'static,
    Spec::Value: Value,
{
    type RuntimeView<'r> = ();
    type Frame = Frame<Promise<Spec::Value>>;
    type Instruction = waymark_vm_instructions_pureset::PureSet<Spec>;
    type Error = Error;
    type Effect = core::convert::Infallible;

    fn execute<'r>(
        &self,
        _runtime_view: Self::RuntimeView<'r>,
        mut frame: Frame<Promise<Spec::Value>>,
        instruction: &Self::Instruction,
    ) -> Result<
        waymark_vm_interpreter::ExecutionOutcome<Frame<Promise<Spec::Value>>, Self::Effect>,
        Self::Error,
    > {
        match instruction {
            waymark_vm_instructions_pureset::PureSet::LoadConst { dst, value } => {
                frame.regs[*dst] = Promise::Resolved(value.clone());
            }
            waymark_vm_instructions_pureset::PureSet::Add { dst, a, b } => {
                let x = frame.regs[*a].require_resolved_ref().unwrap();
                let y = frame.regs[*b].require_resolved_ref().unwrap();
                let value = Spec::Value::add(x, y).map_err(Error::Add)?;
                frame.regs[*dst] = Promise::Resolved(value);
            }
        }

        Ok(waymark_vm_interpreter::ExecutionOutcome::Continue(frame))
    }
}
