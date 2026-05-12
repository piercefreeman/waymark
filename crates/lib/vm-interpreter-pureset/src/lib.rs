//! The interpreter for the "pure" instructions set.

#![warn(missing_docs)]

mod error;
pub mod value;

use derive_where::derive_where;
use waymark_vm_interpreter::ExecutionOutcome;
use waymark_vm_interpreter_utils::register_values::with_register_values;
use waymark_vm_runtime_core::{Frame, Promise};

pub use self::error::*;
pub use self::value::Value;

/// An interpreter for the "pure" instructions set.
#[derive_where(Default)]
pub struct PureSetInterpreter<Spec, FunctionId, StateId, Value> {
    phantom_data: core::marker::PhantomData<(Spec, FunctionId, StateId, Value)>,
}

impl<Spec, FunctionId, StateId, Value> waymark_vm_interpreter::Interpreter
    for PureSetInterpreter<Spec, FunctionId, StateId, Value>
where
    Spec: waymark_vm_instructions_pureset::Spec<RegisterId = waymark_vm_runtime_core::RegisterId>
        + 'static,
    Spec::ConstValue: Clone + Into<Value>,
    Value: Clone + 'static,
    Value: value::Value,
{
    type RuntimeView<'r> = ();
    type Frame = Frame<FunctionId, StateId, Promise<Value>>;
    type Instruction = waymark_vm_instructions_pureset::PureSet<Spec>;
    type Error = Error;
    type Effect = core::convert::Infallible;

    fn execute<'r>(
        &self,
        _runtime_view: Self::RuntimeView<'r>,
        mut frame: Frame<FunctionId, StateId, Promise<Value>>,
        instruction: &Self::Instruction,
    ) -> Result<
        ExecutionOutcome<Frame<FunctionId, StateId, Promise<Value>>, Self::Effect>,
        Self::Error,
    > {
        match instruction {
            waymark_vm_instructions_pureset::PureSet::LoadConst { dst, value } => {
                frame
                    .regs
                    .set(*dst, Promise::Resolved(value.clone().into()));
            }
            waymark_vm_instructions_pureset::PureSet::Copy { dst, src } => {
                let value = frame
                    .regs
                    .get(*src)
                    .ok_or(Error::MissingCopySource { register: *src })?;
                frame.regs.set(*dst, value.clone());
            }
            waymark_vm_instructions_pureset::PureSet::Add { dst, a, b } => {
                let x = frame.regs.get(*a).ok_or(Error::MissingAddOperand {
                    operand_pos: BinaryOperandPosition::First,
                    register: *a,
                })?;
                let x = x
                    .require_resolved_ref()
                    .map_err(|source| Error::UnresolvedAddOperand {
                        operand_pos: BinaryOperandPosition::First,
                        source,
                    })?;

                let y = frame.regs.get(*b).ok_or(Error::MissingAddOperand {
                    operand_pos: BinaryOperandPosition::Second,
                    register: *b,
                })?;
                let y = y
                    .require_resolved_ref()
                    .map_err(|source| Error::UnresolvedAddOperand {
                        operand_pos: BinaryOperandPosition::Second,
                        source,
                    })?;

                let value = Value::add(x, y).map_err(Error::Add)?;
                frame.regs.set(*dst, Promise::Resolved(value));
            }
            waymark_vm_instructions_pureset::PureSet::MakeList { dst, items } => {
                let make_list_result = with_register_values(
                    items.iter().copied(),
                    |item_pos, register| {
                        let value = frame
                            .regs
                            .get(register)
                            .ok_or(Error::MissingListItem { item_pos, register })?;
                        let value = value
                            .require_resolved_ref()
                            .map_err(|source| Error::UnresolvedListItem { item_pos, source })?;

                        Ok(value.clone())
                    },
                    |items| Value::make_list(items.by_ref()),
                )?;

                let list = make_list_result.map_err(Error::MakeList)?;
                frame.regs.set(*dst, Promise::Resolved(list));
            }
        }

        Ok(ExecutionOutcome::Continue(frame))
    }
}

impl<Spec, Executable, FunctionId, StateId, Value>
    waymark_vm_runtime_core::CaptureRuntimeView<Executable, FunctionId, StateId, Value>
    for PureSetInterpreter<Spec, FunctionId, StateId, Value>
{
    type RuntimeView<'v>
        = ()
    where
        Executable: 'v,
        FunctionId: 'v,
        StateId: 'v,
        Value: 'v;

    fn capture_runtime_view<'r>(
        _view: waymark_vm_runtime_core::FullRuntimeView<'r, Executable, FunctionId, StateId, Value>,
    ) -> Self::RuntimeView<'r> {
    }
}
