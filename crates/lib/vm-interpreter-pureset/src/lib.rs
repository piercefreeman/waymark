//! The interpreter for the "pure" instructions set.

#![warn(missing_docs)]

mod error;
pub mod operations;

use derive_where::derive_where;
use waymark_vm_interpreter::ExecutionOutcome;
use waymark_vm_interpreter_utils::register_values::with_register_values;
use waymark_vm_runtime_core::Frame;

pub use self::error::*;
pub use self::operations::Operations;

use waymark_vm_instructions_pureset::{BinaryOpKind, UnaryOpKind};

/// An interpreter for the "pure" instructions set.
#[derive_where(Default)]
pub struct PureSetInterpreter<Spec, FunctionId, StateId, Operations, Value> {
    phantom_data: core::marker::PhantomData<(Spec, FunctionId, StateId, Operations, Value)>,
}

impl<Spec, FunctionId, StateId, Operations, Value> waymark_vm_interpreter::Interpreter
    for PureSetInterpreter<Spec, FunctionId, StateId, Operations, Value>
where
    Spec: waymark_vm_instructions_pureset::Spec<RegisterId = waymark_vm_runtime_core::RegisterId>
        + 'static,
    Operations: self::Operations<Value>,
    Operations: self::operations::Exceptions<Value>,
    Operations: for<'a> self::operations::LoadConst<Value, &'a Spec::ConstValue>,
    Operations: 'static,
    Value: waymark_vm_runtime_value::RootValueAccess<RootValue = Value>,
    Value: self::operations::ExceptionValue<Operations>,
    Value: 'static,
{
    type RuntimeView<'r> = ();
    type Frame = Frame<FunctionId, StateId, Value>;
    type Instruction = waymark_vm_instructions_pureset::PureSet<Spec>;
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
            waymark_vm_instructions_pureset::PureSet::LoadConst { dst, value } => {
                frame.regs.set(*dst, Operations::load_const(value));
            }
            waymark_vm_instructions_pureset::PureSet::Copy { dst, src } => {
                let value = frame
                    .regs
                    .get(*src)
                    .ok_or(Error::MissingCopySource { register: *src })?;
                frame.regs.set(*dst, Operations::capture_copy(value));
            }
            waymark_vm_instructions_pureset::PureSet::Binary {
                kind,
                op: waymark_vm_instructions_pureset::BinaryOp { dst, a, b },
            } => {
                Self::execute_binary_operation(&mut frame, *dst, *a, *b, *kind)?;
            }
            waymark_vm_instructions_pureset::PureSet::Unary {
                kind,
                op: waymark_vm_instructions_pureset::UnaryOp { dst, src },
            } => {
                Self::execute_unary_operation(&mut frame, *dst, *src, *kind)?;
            }
            waymark_vm_instructions_pureset::PureSet::Length { dst, src } => {
                Self::execute_length(&mut frame, *dst, *src)?;
            }
            waymark_vm_instructions_pureset::PureSet::Index { dst, object, index } => {
                Self::execute_index_operation(&mut frame, *dst, *object, *index)?;
            }
            waymark_vm_instructions_pureset::PureSet::Dot {
                dst,
                object,
                attribute,
            } => {
                Self::execute_dot_operation(&mut frame, *dst, *object, attribute)?;
            }
            waymark_vm_instructions_pureset::PureSet::MakeList { dst, items } => {
                let make_list_result = with_register_values(
                    items.iter().copied(),
                    |item_pos, register| {
                        let value = frame
                            .regs
                            .get(register)
                            .ok_or(Error::MissingListItem { item_pos, register })?;
                        Ok(Operations::capture_copy(value))
                    },
                    |items| Operations::make_list(items.by_ref()),
                )?;

                match make_list_result {
                    Ok(list) => frame.regs.set(*dst, list),
                    Err(error) => frame.raise_typed_exception(error),
                }
            }
            waymark_vm_instructions_pureset::PureSet::ListAppend { dst, list, item } => {
                let list_value = frame
                    .regs
                    .get(*list)
                    .ok_or(Error::MissingListAppendList { register: *list })?;
                let item_value = frame
                    .regs
                    .get(*item)
                    .ok_or(Error::MissingListAppendItem { register: *item })?;
                match Operations::list_append(list_value, Operations::capture_copy(item_value)) {
                    Ok(grown) => frame.regs.set(*dst, grown),
                    Err(error) => frame.raise_typed_exception(error),
                }
            }
            waymark_vm_instructions_pureset::PureSet::MakeDict { dst, entries } => {
                let mut resolved_entries = Vec::with_capacity(entries.len());
                let mut raised_key_error = None;

                for (entry_pos, entry) in entries.iter().enumerate() {
                    let key = frame.regs.get(entry.key).ok_or(Error::MissingDictKey {
                        entry_pos,
                        register: entry.key,
                    })?;
                    let key = match Operations::as_dict_key(key) {
                        Ok(key) => key,
                        Err(error) => {
                            raised_key_error = Some(error);
                            break;
                        }
                    };

                    let value = frame.regs.get(entry.value).ok_or(Error::MissingDictValue {
                        entry_pos,
                        register: entry.value,
                    })?;

                    resolved_entries.push((key.to_owned(), Operations::capture_copy(value)));
                }

                if let Some(error) = raised_key_error {
                    frame.raise_typed_exception(error);
                    return Ok(ExecutionOutcome::Continue(frame));
                }

                match Operations::make_dict(resolved_entries) {
                    Ok(dict) => frame.regs.set(*dst, dict),
                    Err(error) => frame.raise_typed_exception(error),
                }
            }
            waymark_vm_instructions_pureset::PureSet::MakeException {
                dst,
                type_id,
                details,
            } => {
                let type_id_value = frame
                    .regs
                    .get(*type_id)
                    .ok_or(Error::MissingExceptionTypeId { register: *type_id })?;
                let type_id_value = Operations::as_exception_type_id(type_id_value)
                    .map_err(|source| Error::UnusableExceptionTypeId { source })?
                    .to_owned();
                let details_value = frame
                    .regs
                    .get(*details)
                    .ok_or(Error::MissingExceptionDetails { register: *details })?;
                let details_value = Operations::capture_copy(details_value);

                let exception = Operations::make_exception(type_id_value, details_value);
                frame.regs.set(*dst, exception);
            }
        }

        Ok(ExecutionOutcome::Continue(frame))
    }
}

impl<Spec, FunctionId, StateId, Operations, Value>
    PureSetInterpreter<Spec, FunctionId, StateId, Operations, Value>
where
    Operations: self::Operations<Value>,
    Operations: self::operations::Exceptions<Value>,
    Value: waymark_vm_runtime_value::RootValueAccess<RootValue = Value>,
    Value: self::operations::ExceptionValue<Operations>,
{
    fn execute_binary_operation(
        frame: &mut Frame<FunctionId, StateId, Value>,
        dst: waymark_vm_runtime_core::RegisterId,
        a: waymark_vm_runtime_core::RegisterId,
        b: waymark_vm_runtime_core::RegisterId,
        operation: BinaryOpKind,
    ) -> Result<(), Error> {
        let x = frame.regs.get(a).ok_or(Error::MissingBinaryOperand {
            operation,
            operand_pos: BinaryOperandPosition::First,
            register: a,
        })?;
        let x = match Operations::as_scalar_value(x) {
            Ok(scalar) => scalar,
            Err(error) => {
                frame.raise_typed_exception(error);
                return Ok(());
            }
        };

        let y = frame.regs.get(b).ok_or(Error::MissingBinaryOperand {
            operation,
            operand_pos: BinaryOperandPosition::Second,
            register: b,
        })?;
        let y = match Operations::as_scalar_value(y) {
            Ok(scalar) => scalar,
            Err(error) => {
                frame.raise_typed_exception(error);
                return Ok(());
            }
        };

        let operation_result = match operation {
            BinaryOpKind::Add => Operations::add(x, y),
            BinaryOpKind::Sub => Operations::sub(x, y),
            BinaryOpKind::Mul => Operations::mul(x, y),
            BinaryOpKind::Div => Operations::div(x, y),
            BinaryOpKind::FloorDiv => Operations::floor_div(x, y),
            BinaryOpKind::Mod => Operations::modulo(x, y),
            BinaryOpKind::Eq => Operations::eq(x, y),
            BinaryOpKind::Ne => Operations::ne(x, y),
            BinaryOpKind::Lt => Operations::lt(x, y),
            BinaryOpKind::Le => Operations::le(x, y),
            BinaryOpKind::Gt => Operations::gt(x, y),
            BinaryOpKind::Ge => Operations::ge(x, y),
            BinaryOpKind::In => Operations::contains(x, y),
            BinaryOpKind::NotIn => Operations::not_contains(x, y),
            BinaryOpKind::And => Operations::and(x, y),
            BinaryOpKind::Or => Operations::or(x, y),
        };

        match operation_result {
            Ok(value) => frame.regs.set(dst, Operations::from_scalar_value(value)),
            Err(error) => frame.raise_typed_exception(error),
        }
        Ok(())
    }

    fn execute_unary_operation(
        frame: &mut Frame<FunctionId, StateId, Value>,
        dst: waymark_vm_runtime_core::RegisterId,
        src: waymark_vm_runtime_core::RegisterId,
        operation: UnaryOpKind,
    ) -> Result<(), Error> {
        let value = frame.regs.get(src).ok_or(Error::MissingUnaryOperand {
            operation,
            register: src,
        })?;
        let value = match Operations::as_scalar_value(value) {
            Ok(scalar) => scalar,
            Err(error) => {
                frame.raise_typed_exception(error);
                return Ok(());
            }
        };

        let operation_result = match operation {
            UnaryOpKind::Neg => Operations::neg(value),
            UnaryOpKind::Not => Operations::not(value),
        };

        match operation_result {
            Ok(value) => frame.regs.set(dst, Operations::from_scalar_value(value)),
            Err(error) => frame.raise_typed_exception(error),
        }
        Ok(())
    }

    fn execute_length(
        frame: &mut Frame<FunctionId, StateId, Value>,
        dst: waymark_vm_runtime_core::RegisterId,
        src: waymark_vm_runtime_core::RegisterId,
    ) -> Result<(), Error> {
        let value = frame
            .regs
            .get(src)
            .ok_or(Error::MissingLengthValue { register: src })?;

        let length = match <Operations as self::operations::Length<Value>>::length(value) {
            Ok(length) => length,
            Err(error) => {
                frame.raise_typed_exception(error);
                return Ok(());
            }
        };
        match <Operations as self::operations::Length<Value>>::from_length(length) {
            Ok(value) => frame.regs.set(dst, value),
            Err(error) => frame.raise_typed_exception(error),
        }
        Ok(())
    }

    fn execute_index_operation(
        frame: &mut Frame<FunctionId, StateId, Value>,
        dst: waymark_vm_runtime_core::RegisterId,
        object: waymark_vm_runtime_core::RegisterId,
        index: waymark_vm_runtime_core::RegisterId,
    ) -> Result<(), Error> {
        let object_value = frame
            .regs
            .get(object)
            .ok_or(Error::MissingIndexObject { register: object })?;

        let index_value = frame
            .regs
            .get(index)
            .ok_or(Error::MissingIndexOperand { register: index })?;

        match Operations::index(object_value, index_value) {
            Ok(value) => frame.regs.set(dst, value),
            Err(error) => frame.raise_typed_exception(error),
        }
        Ok(())
    }

    fn execute_dot_operation(
        frame: &mut Frame<FunctionId, StateId, Value>,
        dst: waymark_vm_runtime_core::RegisterId,
        object: waymark_vm_runtime_core::RegisterId,
        attribute: &str,
    ) -> Result<(), Error> {
        let object_value = frame
            .regs
            .get(object)
            .ok_or_else(|| Error::MissingDotObject {
                attribute: attribute.to_owned(),
                register: object,
            })?;

        match Operations::dot(object_value, attribute) {
            Ok(value) => frame.regs.set(dst, value),
            Err(error) => frame.raise_typed_exception(error),
        }
        Ok(())
    }
}

impl<'s, 'r, Spec, Executable, FunctionId, StateId, Operations, Value>
    waymark_vm_interpreter::CaptureRuntimeView<
        's,
        waymark_vm_runtime_core::FullRuntimeView<'r, Executable, FunctionId, StateId, Value>,
    > for PureSetInterpreter<Spec, FunctionId, StateId, Operations, Value>
{
    type Captured = ();

    fn capture_runtime_view(
        _source: &'s mut waymark_vm_runtime_core::FullRuntimeView<
            'r,
            Executable,
            FunctionId,
            StateId,
            Value,
        >,
    ) -> Self::Captured {
    }
}
