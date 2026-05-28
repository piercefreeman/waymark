//! The interpreter for the "excset" instructions set.
//!
//! This crate executes exception-oriented VM instructions and defines the
//! value conversions needed to inspect exception values.
//! [`ExcSetInterpreter`] evaluates exception type checks and copies exception
//! details payloads into destination registers.

#![warn(missing_docs)]

mod error;
pub mod value;

use derive_where::derive_where;
use waymark_vm_interpreter::ExecutionOutcome;
use waymark_vm_runtime_core::Frame;

pub use self::error::*;
pub use self::value::Value;

/// An interpreter for the "excset" instructions set.
#[derive_where(Default)]
pub struct ExcSetInterpreter<Spec, FunctionId, StateId, Value> {
    phantom_data: core::marker::PhantomData<(Spec, FunctionId, StateId, Value)>,
}

impl<Spec, FunctionId, StateId, Value> waymark_vm_interpreter::Interpreter
    for ExcSetInterpreter<Spec, FunctionId, StateId, Value>
where
    Spec: waymark_vm_instructions_excset::Spec<RegisterId = waymark_vm_runtime_core::RegisterId>,
    Value: self::Value,
    Value: 'static,
{
    type RuntimeView<'r> = ();
    type Frame = Frame<FunctionId, StateId, Value>;
    type Instruction = waymark_vm_instructions_excset::ExcSet<Spec>;
    type Error = Error;
    type Effect = core::convert::Infallible;

    fn execute<'r>(
        &self,
        _runtime_view: Self::RuntimeView<'r>,
        mut frame: Frame<FunctionId, StateId, Value>,
        instruction: &Self::Instruction,
    ) -> Result<ExecutionOutcome<Frame<FunctionId, StateId, Value>, Self::Effect>, Self::Error>
    {
        match instruction {
            waymark_vm_instructions_excset::ExcSet::IsException {
                dst,
                src,
                exception_type_id,
            } => {
                let val = &frame.regs[*src];
                let is_exception = match val.as_exception() {
                    Err(waymark_vm_runtime_exception::NotAnExceptionError) => false,
                    Ok(exception) => match exception_type_id {
                        None => true,
                        Some(exception_type_id) => {
                            let type_id = &frame.regs[*exception_type_id];
                            let type_id =
                                type_id.as_exception_type_id().map_err(Error::IsException)?;

                            exception.type_id == type_id
                        }
                    },
                };

                frame.regs.set(*dst, Value::from_is_exception(is_exception));
            }

            waymark_vm_instructions_excset::ExcSet::ExceptionDetails { dst, src } => {
                let val = &frame.regs[*src];
                let exception = val.as_exception().map_err(Error::ExceptionDetails)?;

                frame
                    .regs
                    .set(*dst, Value::from_exception_details(&exception.details))
            }
        }

        Ok(ExecutionOutcome::Continue(frame))
    }
}

impl<Spec, Executable, FunctionId, StateId, Value>
    waymark_vm_runtime_core::CaptureRuntimeView<Executable, FunctionId, StateId, Value>
    for ExcSetInterpreter<Spec, FunctionId, StateId, Value>
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
