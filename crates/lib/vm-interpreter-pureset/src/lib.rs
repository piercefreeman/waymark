//! The interpreter for the "pure" instructions set.

#![warn(missing_docs)]

mod error;
pub mod operations;
pub mod value;

use derive_where::derive_where;
use waymark_vm_interpreter::ExecutionOutcome;
use waymark_vm_interpreter_utils::register_values::with_register_values;
use waymark_vm_runtime_core::Frame;

pub use self::error::*;
pub use self::operations::Operations;
pub use self::value::Value;

use self::value::*;

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
    Operations: 'static,
    Value: self::operations::ExceptionValue<Operations>,
    Value: 'static,
    Value: value::Value,
    Value: for<'a> value::LoadConst<&'a Spec::ConstValue>,
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
                frame.regs.set(*dst, Value::load_const(value));
            }
            waymark_vm_instructions_pureset::PureSet::Copy { dst, src } => {
                let value = frame
                    .regs
                    .get(*src)
                    .ok_or(Error::MissingCopySource { register: *src })?;
                frame.regs.set(*dst, value.capture_copy());
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
                        Ok(value.capture_copy())
                    },
                    |items| Value::make_list(items.by_ref()),
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
                match Value::list_append(list_value, item_value.capture_copy()) {
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
                    let key = match key.as_dict_key() {
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

                    resolved_entries.push((key.to_owned(), value.capture_copy()));
                }

                if let Some(error) = raised_key_error {
                    frame.raise_typed_exception(error);
                    return Ok(ExecutionOutcome::Continue(frame));
                }

                match Value::make_dict(resolved_entries) {
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
                let type_id_value = type_id_value
                    .as_exception_type_id()
                    .map_err(|source| Error::UnusableExceptionTypeId { source })?
                    .to_owned();
                let details_value = frame
                    .regs
                    .get(*details)
                    .ok_or(Error::MissingExceptionDetails { register: *details })?
                    .capture_copy();

                let exception = Value::make_exception(type_id_value, details_value);
                frame.regs.set(*dst, exception);
            }
        }

        Ok(ExecutionOutcome::Continue(frame))
    }
}

impl<Spec, FunctionId, StateId, Operations, Value>
    PureSetInterpreter<Spec, FunctionId, StateId, Operations, Value>
where
    Value: value::Value,
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
        let x = match x.as_scalar() {
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
        let y = match y.as_scalar() {
            Ok(scalar) => scalar,
            Err(error) => {
                frame.raise_typed_exception(error);
                return Ok(());
            }
        };

        let operation_result = match operation {
            BinaryOpKind::Add => Value::Scalar::add(x, y),
            BinaryOpKind::Sub => Value::Scalar::sub(x, y),
            BinaryOpKind::Mul => Value::Scalar::mul(x, y),
            BinaryOpKind::Div => Value::Scalar::div(x, y),
            BinaryOpKind::FloorDiv => Value::Scalar::floor_div(x, y),
            BinaryOpKind::Mod => Value::Scalar::modulo(x, y),
            BinaryOpKind::Eq => Value::Scalar::eq(x, y),
            BinaryOpKind::Ne => Value::Scalar::ne(x, y),
            BinaryOpKind::Lt => Value::Scalar::lt(x, y),
            BinaryOpKind::Le => Value::Scalar::le(x, y),
            BinaryOpKind::Gt => Value::Scalar::gt(x, y),
            BinaryOpKind::Ge => Value::Scalar::ge(x, y),
            BinaryOpKind::In => Value::Scalar::contains(x, y),
            BinaryOpKind::NotIn => Value::Scalar::not_contains(x, y),
            BinaryOpKind::And => Value::Scalar::and(x, y),
            BinaryOpKind::Or => Value::Scalar::or(x, y),
        };

        match operation_result {
            Ok(value) => frame.regs.set(dst, Value::from_scalar(value)),
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
        let value = match value.as_scalar() {
            Ok(scalar) => scalar,
            Err(error) => {
                frame.raise_typed_exception(error);
                return Ok(());
            }
        };

        let operation_result = match operation {
            UnaryOpKind::Neg => Value::Scalar::neg(value),
            UnaryOpKind::Not => Value::Scalar::not(value),
        };

        match operation_result {
            Ok(value) => frame.regs.set(dst, Value::from_scalar(value)),
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

        let length = match <Value as value::Length>::length(value) {
            Ok(length) => length,
            Err(error) => {
                frame.raise_typed_exception(error);
                return Ok(());
            }
        };
        match <Value as value::Length>::from_length(length) {
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

        match Value::index(object_value, index_value) {
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

        match Value::dot(object_value, attribute) {
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
