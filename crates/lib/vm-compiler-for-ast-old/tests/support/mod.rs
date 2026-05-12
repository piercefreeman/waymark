use waymark_vm_compiler_for_ast_old_test_support::{
    TestConstValue, TestExecutable, TestLowering, TestSpec,
};

type TestInterpreter =
    waymark_vm_interpreter_fullset::FullSetInterpreter<TestSpec, TestExecutable, TestValue>;

pub type TestRuntime = waymark_vm_runtime::Runtime<TestExecutable, TestInterpreter, TestValue>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TestValue {
    Int(i64),
    Bool(bool),
    None,
    List(Vec<TestValue>),
}

fn is_truthy(value: &TestValue) -> bool {
    match value {
        TestValue::Int(value) => *value != 0,
        TestValue::Bool(value) => *value,
        TestValue::None => false,
        TestValue::List(items) => !items.is_empty(),
    }
}

impl waymark_vm_interpreter_coreset::value::ShouldJump for TestValue {
    fn should_jump(
        &self,
    ) -> Result<bool, waymark_vm_interpreter_coreset::value::NotAConditionalError> {
        Ok(is_truthy(self))
    }
}

impl waymark_vm_interpreter_pureset::value::BinaryOps for TestValue {
    fn add(
        a: &Self,
        b: &Self,
    ) -> Result<Self, waymark_vm_interpreter_pureset::value::BinaryOperationError> {
        match (a, b) {
            (Self::Int(a), Self::Int(b)) => a.checked_add(*b).map(Self::Int).ok_or(
                waymark_vm_interpreter_pureset::value::BinaryOperationError::ResultOutOfBounds {
                    operation: waymark_vm_interpreter_pureset::value::BinaryOperationKind::Add,
                },
            ),
            (Self::List(left), Self::List(right)) => {
                let mut items = left.clone();
                items.extend(right.clone());
                Ok(Self::List(items))
            }
            _ => Err(
                waymark_vm_interpreter_pureset::value::BinaryOperationError::UnsupportedOperation {
                    operation: waymark_vm_interpreter_pureset::value::BinaryOperationKind::Add,
                },
            ),
        }
    }

    fn sub(
        a: &Self,
        b: &Self,
    ) -> Result<Self, waymark_vm_interpreter_pureset::value::BinaryOperationError> {
        match (a, b) {
            (Self::Int(a), Self::Int(b)) => a.checked_sub(*b).map(Self::Int).ok_or(
                waymark_vm_interpreter_pureset::value::BinaryOperationError::ResultOutOfBounds {
                    operation: waymark_vm_interpreter_pureset::value::BinaryOperationKind::Sub,
                },
            ),
            _ => Err(
                waymark_vm_interpreter_pureset::value::BinaryOperationError::UnsupportedOperation {
                    operation: waymark_vm_interpreter_pureset::value::BinaryOperationKind::Sub,
                },
            ),
        }
    }

    fn mul(
        a: &Self,
        b: &Self,
    ) -> Result<Self, waymark_vm_interpreter_pureset::value::BinaryOperationError> {
        match (a, b) {
            (Self::Int(a), Self::Int(b)) => a.checked_mul(*b).map(Self::Int).ok_or(
                waymark_vm_interpreter_pureset::value::BinaryOperationError::ResultOutOfBounds {
                    operation: waymark_vm_interpreter_pureset::value::BinaryOperationKind::Mul,
                },
            ),
            _ => Err(
                waymark_vm_interpreter_pureset::value::BinaryOperationError::UnsupportedOperation {
                    operation: waymark_vm_interpreter_pureset::value::BinaryOperationKind::Mul,
                },
            ),
        }
    }

    fn div(
        a: &Self,
        b: &Self,
    ) -> Result<Self, waymark_vm_interpreter_pureset::value::BinaryOperationError> {
        match (a, b) {
            (Self::Int(_), Self::Int(0)) => Err(
                waymark_vm_interpreter_pureset::value::BinaryOperationError::DivisionByZero {
                    operation: waymark_vm_interpreter_pureset::value::BinaryOperationKind::Div,
                },
            ),
            (Self::Int(a), Self::Int(b)) => {
                if *a % *b == 0 {
                    Ok(Self::Int(*a / *b))
                } else {
                    Err(
                        waymark_vm_interpreter_pureset::value::BinaryOperationError::ResultOutOfBounds {
                            operation: waymark_vm_interpreter_pureset::value::BinaryOperationKind::Div,
                        },
                    )
                }
            }
            _ => Err(
                waymark_vm_interpreter_pureset::value::BinaryOperationError::UnsupportedOperation {
                    operation: waymark_vm_interpreter_pureset::value::BinaryOperationKind::Div,
                },
            ),
        }
    }

    fn floor_div(
        a: &Self,
        b: &Self,
    ) -> Result<Self, waymark_vm_interpreter_pureset::value::BinaryOperationError> {
        match (a, b) {
            (Self::Int(_), Self::Int(0)) => Err(
                waymark_vm_interpreter_pureset::value::BinaryOperationError::DivisionByZero {
                    operation: waymark_vm_interpreter_pureset::value::BinaryOperationKind::FloorDiv,
                },
            ),
            (Self::Int(a), Self::Int(b)) => {
                let value = ((*a as f64) / (*b as f64)).floor();
                Ok(Self::Int(value as i64))
            }
            _ => Err(
                waymark_vm_interpreter_pureset::value::BinaryOperationError::UnsupportedOperation {
                    operation: waymark_vm_interpreter_pureset::value::BinaryOperationKind::FloorDiv,
                },
            ),
        }
    }

    fn modulo(
        a: &Self,
        b: &Self,
    ) -> Result<Self, waymark_vm_interpreter_pureset::value::BinaryOperationError> {
        match (a, b) {
            (Self::Int(_), Self::Int(0)) => Err(
                waymark_vm_interpreter_pureset::value::BinaryOperationError::DivisionByZero {
                    operation: waymark_vm_interpreter_pureset::value::BinaryOperationKind::Mod,
                },
            ),
            (Self::Int(a), Self::Int(b)) => Ok(Self::Int(*a % *b)),
            _ => Err(
                waymark_vm_interpreter_pureset::value::BinaryOperationError::UnsupportedOperation {
                    operation: waymark_vm_interpreter_pureset::value::BinaryOperationKind::Mod,
                },
            ),
        }
    }

    fn eq(
        a: &Self,
        b: &Self,
    ) -> Result<Self, waymark_vm_interpreter_pureset::value::BinaryOperationError> {
        Ok(Self::Bool(a == b))
    }

    fn ne(
        a: &Self,
        b: &Self,
    ) -> Result<Self, waymark_vm_interpreter_pureset::value::BinaryOperationError> {
        Ok(Self::Bool(a != b))
    }

    fn lt(
        a: &Self,
        b: &Self,
    ) -> Result<Self, waymark_vm_interpreter_pureset::value::BinaryOperationError> {
        match (a, b) {
            (Self::Int(a), Self::Int(b)) => Ok(Self::Bool(a < b)),
            _ => Err(
                waymark_vm_interpreter_pureset::value::BinaryOperationError::UnsupportedOperation {
                    operation: waymark_vm_interpreter_pureset::value::BinaryOperationKind::Lt,
                },
            ),
        }
    }

    fn le(
        a: &Self,
        b: &Self,
    ) -> Result<Self, waymark_vm_interpreter_pureset::value::BinaryOperationError> {
        match (a, b) {
            (Self::Int(a), Self::Int(b)) => Ok(Self::Bool(a <= b)),
            _ => Err(
                waymark_vm_interpreter_pureset::value::BinaryOperationError::UnsupportedOperation {
                    operation: waymark_vm_interpreter_pureset::value::BinaryOperationKind::Le,
                },
            ),
        }
    }

    fn gt(
        a: &Self,
        b: &Self,
    ) -> Result<Self, waymark_vm_interpreter_pureset::value::BinaryOperationError> {
        match (a, b) {
            (Self::Int(a), Self::Int(b)) => Ok(Self::Bool(a > b)),
            _ => Err(
                waymark_vm_interpreter_pureset::value::BinaryOperationError::UnsupportedOperation {
                    operation: waymark_vm_interpreter_pureset::value::BinaryOperationKind::Gt,
                },
            ),
        }
    }

    fn ge(
        a: &Self,
        b: &Self,
    ) -> Result<Self, waymark_vm_interpreter_pureset::value::BinaryOperationError> {
        match (a, b) {
            (Self::Int(a), Self::Int(b)) => Ok(Self::Bool(a >= b)),
            _ => Err(
                waymark_vm_interpreter_pureset::value::BinaryOperationError::UnsupportedOperation {
                    operation: waymark_vm_interpreter_pureset::value::BinaryOperationKind::Ge,
                },
            ),
        }
    }

    fn contains(
        a: &Self,
        b: &Self,
    ) -> Result<Self, waymark_vm_interpreter_pureset::value::BinaryOperationError> {
        match b {
            Self::List(items) => Ok(Self::Bool(items.iter().any(|item| item == a))),
            _ => Err(
                waymark_vm_interpreter_pureset::value::BinaryOperationError::UnsupportedOperation {
                    operation: waymark_vm_interpreter_pureset::value::BinaryOperationKind::In,
                },
            ),
        }
    }

    fn not_contains(
        a: &Self,
        b: &Self,
    ) -> Result<Self, waymark_vm_interpreter_pureset::value::BinaryOperationError> {
        match Self::contains(a, b)? {
            Self::Bool(value) => Ok(Self::Bool(!value)),
            _ => unreachable!(),
        }
    }

    fn and(
        a: &Self,
        b: &Self,
    ) -> Result<Self, waymark_vm_interpreter_pureset::value::BinaryOperationError> {
        if is_truthy(a) {
            Ok(b.clone())
        } else {
            Ok(a.clone())
        }
    }

    fn or(
        a: &Self,
        b: &Self,
    ) -> Result<Self, waymark_vm_interpreter_pureset::value::BinaryOperationError> {
        if is_truthy(a) {
            Ok(a.clone())
        } else {
            Ok(b.clone())
        }
    }
}

impl waymark_vm_interpreter_pureset::value::UnaryOps for TestValue {
    fn neg(
        value: &Self,
    ) -> Result<Self, waymark_vm_interpreter_pureset::value::UnaryOperationError> {
        match value {
            Self::Int(value) => value.checked_neg().map(Self::Int).ok_or(
                waymark_vm_interpreter_pureset::value::UnaryOperationError::ResultOutOfBounds {
                    operation: waymark_vm_interpreter_pureset::value::UnaryOperationKind::Neg,
                },
            ),
            _ => Err(
                waymark_vm_interpreter_pureset::value::UnaryOperationError::UnsupportedOperation {
                    operation: waymark_vm_interpreter_pureset::value::UnaryOperationKind::Neg,
                },
            ),
        }
    }

    fn not(
        value: &Self,
    ) -> Result<Self, waymark_vm_interpreter_pureset::value::UnaryOperationError> {
        Ok(Self::Bool(!is_truthy(value)))
    }
}

impl waymark_vm_interpreter_pureset::value::MakeList for TestValue {
    fn make_list<I>(items: I) -> Result<Self, waymark_vm_interpreter_pureset::value::MakeListError>
    where
        I: IntoIterator<Item = Self>,
    {
        Ok(Self::List(items.into_iter().collect()))
    }
}

impl From<TestConstValue> for TestValue {
    fn from(value: TestConstValue) -> Self {
        match value {
            TestConstValue::Int(value) => Self::Int(value),
            TestConstValue::None => Self::None,
        }
    }
}

pub fn compile_program(program: &waymark_vm_ast_old::Program) -> TestExecutable {
    waymark_vm_compiler_for_ast_old::compile::<TestSpec, TestLowering>(program)
        .expect("program should compile")
}

pub fn runtime(executable: TestExecutable) -> TestRuntime {
    waymark_vm_runtime::Runtime::with_conventional_entrypoint(
        TestInterpreter::default(),
        executable,
    )
    .expect("compiled main function should exist")
}

pub fn runtime_with_args(executable: TestExecutable, args: Vec<TestValue>) -> TestRuntime {
    waymark_vm_runtime::Runtime::with_custom_entrypoint(
        TestInterpreter::default(),
        executable,
        waymark_vm_runtime::CallSpec {
            func: waymark_vm_bytecode_core::FunctionId::default(),
            args,
        },
    )
    .expect("compiled main function should exist")
}
