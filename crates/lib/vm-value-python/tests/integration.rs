use indexmap::IndexMap;
use typed_floats::NonNaNFinite;
use waymark_vm_instructions_pureset::BinaryOpKind;
use waymark_vm_interpreter_coreset::value::ShouldJump as _;
use waymark_vm_interpreter_extcallset::value::{
    CaptureActionCallArgument as _, SleepDuration as _,
};
use waymark_vm_interpreter_pureset::value::{
    AsDictKey as _, AsDictKeyError, BinaryOperationError, BinaryOps as _, DotOp as _,
    DotOperationError, FromLengthError, IndexOp as _, IndexOperationError, Length as _,
    LengthError, MakeDict as _, MakeList as _, UnaryOps as _,
};
use waymark_vm_runtime_exception::{
    Match as _, RaisedExceptionToValue as _, ValueToRaisedException as _,
};
use waymark_vm_value::extcallset;
use waymark_vm_value_python::exception::classes;
use waymark_vm_value_python::raised_exception::Pattern;
use waymark_vm_value_python::{Exception, MaterialValue, RaisedException, Value};

#[test]
fn values_follow_truthiness() {
    assert!(MaterialValue::String("x".to_owned()).should_jump().unwrap());
    assert!(!MaterialValue::String(String::new()).should_jump().unwrap());
    assert!(!MaterialValue::None.should_jump().unwrap());
    assert!(
        MaterialValue::Float(1.5.try_into().unwrap())
            .should_jump()
            .unwrap()
    );
    assert!(
        !MaterialValue::Float(0.0.try_into().unwrap())
            .should_jump()
            .unwrap()
    );
    assert!(
        MaterialValue::List(vec![Value::Ready(MaterialValue::Int(1))])
            .should_jump()
            .unwrap()
    );
    assert!(
        MaterialValue::Dict(IndexMap::from([(
            "key".to_owned(),
            Value::Ready(MaterialValue::Int(1))
        )]))
        .should_jump()
        .unwrap()
    );
}

#[test]
fn binary_and_unary_operations_cover_current_vm_value_cases() {
    assert_eq!(
        MaterialValue::add(&MaterialValue::Int(2), &MaterialValue::Int(3)).unwrap(),
        MaterialValue::Int(5)
    );
    assert_eq!(
        MaterialValue::add(
            &MaterialValue::String("hello ".to_owned()),
            &MaterialValue::String("world".to_owned())
        )
        .unwrap(),
        MaterialValue::String("hello world".to_owned())
    );
    assert_eq!(
        MaterialValue::add(
            &MaterialValue::List(vec![Value::Ready(MaterialValue::Int(1))]),
            &MaterialValue::List(vec![Value::Ready(MaterialValue::Int(2))])
        )
        .unwrap(),
        MaterialValue::List(vec![
            Value::Ready(MaterialValue::Int(1)),
            Value::Ready(MaterialValue::Int(2))
        ])
    );
    assert_eq!(
        MaterialValue::add(
            &MaterialValue::Float(1.25.try_into().unwrap()),
            &MaterialValue::Float(2.0.try_into().unwrap()),
        )
        .unwrap(),
        MaterialValue::Float(3.25.try_into().unwrap())
    );
    assert_eq!(
        MaterialValue::sub(
            &MaterialValue::Float(3.5.try_into().unwrap()),
            &MaterialValue::Float(1.25.try_into().unwrap()),
        )
        .unwrap(),
        MaterialValue::Float(2.25.try_into().unwrap())
    );
    assert_eq!(
        MaterialValue::mul(
            &MaterialValue::Float(3.0.try_into().unwrap()),
            &MaterialValue::Float(0.5.try_into().unwrap()),
        )
        .unwrap(),
        MaterialValue::Float(1.5.try_into().unwrap())
    );
    assert_eq!(
        MaterialValue::div(
            &MaterialValue::Float(3.0.try_into().unwrap()),
            &MaterialValue::Float(2.0.try_into().unwrap()),
        )
        .unwrap(),
        MaterialValue::Float(1.5.try_into().unwrap())
    );
    assert_eq!(
        MaterialValue::floor_div(&MaterialValue::Int(-3), &MaterialValue::Int(2)).unwrap(),
        MaterialValue::Int(-2)
    );
    assert_eq!(
        MaterialValue::floor_div(
            &MaterialValue::Float((-3.0).try_into().unwrap()),
            &MaterialValue::Float(2.0.try_into().unwrap()),
        )
        .unwrap(),
        MaterialValue::Float((-2.0).try_into().unwrap())
    );
    assert_eq!(
        MaterialValue::modulo(&MaterialValue::Int(3), &MaterialValue::Int(-2)).unwrap(),
        MaterialValue::Int(-1)
    );
    assert_eq!(
        MaterialValue::modulo(
            &MaterialValue::Float(3.5.try_into().unwrap()),
            &MaterialValue::Float(2.0.try_into().unwrap()),
        )
        .unwrap(),
        MaterialValue::Float(1.5.try_into().unwrap())
    );
    assert_eq!(
        MaterialValue::modulo(
            &MaterialValue::Float(3.5.try_into().unwrap()),
            &MaterialValue::Float((-2.0).try_into().unwrap()),
        )
        .unwrap(),
        MaterialValue::Float((-0.5).try_into().unwrap())
    );
    assert_eq!(
        MaterialValue::contains(
            &MaterialValue::String("ell".to_owned()),
            &MaterialValue::String("hello".to_owned())
        )
        .unwrap(),
        MaterialValue::Bool(true)
    );
    assert_eq!(
        MaterialValue::contains(
            &MaterialValue::Int(2),
            &MaterialValue::List(vec![
                Value::Ready(MaterialValue::Int(1)),
                Value::Ready(MaterialValue::Int(2))
            ])
        )
        .unwrap(),
        MaterialValue::Bool(true)
    );
    assert_eq!(
        MaterialValue::contains(
            &MaterialValue::String("key".to_owned()),
            &MaterialValue::Dict(IndexMap::from([(
                "key".to_owned(),
                Value::Ready(MaterialValue::String("x".to_owned())),
            )]))
        )
        .unwrap(),
        MaterialValue::Bool(true)
    );
    assert_eq!(
        MaterialValue::lt(
            &MaterialValue::Float(1.5.try_into().unwrap()),
            &MaterialValue::Float(2.0.try_into().unwrap()),
        )
        .unwrap(),
        MaterialValue::Bool(true)
    );
    assert_eq!(
        MaterialValue::neg(&MaterialValue::Int(7)).unwrap(),
        MaterialValue::Int(-7)
    );
    assert_eq!(
        MaterialValue::neg(&MaterialValue::Float(2.5.try_into().unwrap())).unwrap(),
        MaterialValue::Float((-2.5).try_into().unwrap())
    );
    assert_eq!(
        MaterialValue::not(&MaterialValue::None).unwrap(),
        MaterialValue::Bool(true)
    );
}

#[test]
fn mixed_numeric_operations_do_not_silently_promote_ints_to_floats() {
    assert!(matches!(
        MaterialValue::add(
            &MaterialValue::Float(1.25.try_into().unwrap()),
            &MaterialValue::Int(2)
        ),
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOpKind::Add,
        })
    ));
    assert!(matches!(
        MaterialValue::mul(
            &MaterialValue::Int(3),
            &MaterialValue::Float(0.5.try_into().unwrap())
        ),
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOpKind::Mul,
        })
    ));
    assert!(matches!(
        MaterialValue::div(&MaterialValue::Int(3), &MaterialValue::Int(2)),
        Err(BinaryOperationError::ResultOutOfBounds {
            operation: BinaryOpKind::Div,
        })
    ));
    assert!(matches!(
        MaterialValue::floor_div(
            &MaterialValue::Float((-3.0).try_into().unwrap()),
            &MaterialValue::Int(2)
        ),
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOpKind::FloorDiv,
        })
    ));
    assert_eq!(
        waymark_vm_interpreter_pureset::value::BinaryOps::eq(
            &MaterialValue::Int(1),
            &MaterialValue::Float(1.0.try_into().unwrap()),
        )
        .unwrap(),
        MaterialValue::Bool(false)
    );
    assert!(matches!(
        MaterialValue::lt(
            &MaterialValue::Float(1.5.try_into().unwrap()),
            &MaterialValue::Int(2)
        ),
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOpKind::Lt,
        })
    ));
    assert_eq!(
        MaterialValue::contains(
            &MaterialValue::Float(1.0.try_into().unwrap()),
            &MaterialValue::Dict(IndexMap::from([(
                "value".to_owned(),
                Value::Ready(MaterialValue::String("x".to_owned())),
            )]))
        )
        .unwrap(),
        MaterialValue::Bool(false)
    );
}

#[test]
fn index_and_dot_operations_follow_runtime_semantics() {
    assert_eq!(
        MaterialValue::index(
            &MaterialValue::List(vec![
                Value::Ready(MaterialValue::Int(1)),
                Value::Ready(MaterialValue::Int(2))
            ]),
            &MaterialValue::Int(-1)
        )
        .unwrap(),
        Value::Ready(MaterialValue::Int(2))
    );
    assert_eq!(
        MaterialValue::index(
            &MaterialValue::String("hello".to_owned()),
            &MaterialValue::Int(1)
        )
        .unwrap(),
        Value::Ready(MaterialValue::String("e".to_owned()))
    );
    assert_eq!(
        MaterialValue::index(
            &MaterialValue::Dict(IndexMap::from([(
                "field".to_owned(),
                Value::Ready(MaterialValue::Int(7))
            )])),
            &MaterialValue::String("field".to_owned())
        )
        .unwrap(),
        Value::Ready(MaterialValue::Int(7))
    );
    assert_eq!(
        MaterialValue::dot(
            &MaterialValue::Dict(IndexMap::from([(
                "field".to_owned(),
                Value::Ready(MaterialValue::Int(7))
            )])),
            "field"
        )
        .unwrap(),
        Value::Ready(MaterialValue::Int(7))
    );
}

#[test]
fn index_and_dot_operations_surface_expected_errors() {
    assert!(matches!(
        MaterialValue::index(
            &MaterialValue::List(vec![Value::Ready(MaterialValue::Int(1))]),
            &MaterialValue::Int(1)
        ),
        Err(IndexOperationError::IndexOutOfBounds)
    ));
    assert!(matches!(
        MaterialValue::index(
            &MaterialValue::Dict(IndexMap::from([(
                "field".to_owned(),
                Value::Ready(MaterialValue::Int(7))
            )])),
            &MaterialValue::String("missing".to_owned())
        ),
        Err(IndexOperationError::MissingKey)
    ));
    assert!(matches!(
        MaterialValue::dot(
            &MaterialValue::Dict(IndexMap::from([(
                "field".to_owned(),
                Value::Ready(MaterialValue::Int(7))
            )])),
            "missing"
        ),
        Err(DotOperationError::MissingAttribute)
    ));
    assert!(matches!(
        MaterialValue::dot(&MaterialValue::List(Vec::new()), "field"),
        Err(DotOperationError::UnsupportedOperation)
    ));
}

#[test]
fn dict_values_compare_equal_independent_of_insertion_order() {
    let left = MaterialValue::Dict(IndexMap::from([
        ("first".to_owned(), Value::Ready(MaterialValue::Int(1))),
        ("second".to_owned(), Value::Ready(MaterialValue::Int(2))),
    ]));
    let right = MaterialValue::Dict(IndexMap::from([
        ("second".to_owned(), Value::Ready(MaterialValue::Int(2))),
        ("first".to_owned(), Value::Ready(MaterialValue::Int(1))),
    ]));

    assert_eq!(left, right);
}

#[test]
fn logical_ops_lists_and_sleep_duration_use_runtime_semantics() {
    assert_eq!(
        MaterialValue::and(
            &MaterialValue::Int(1),
            &MaterialValue::String("x".to_owned())
        )
        .unwrap(),
        MaterialValue::String("x".to_owned())
    );
    assert_eq!(
        MaterialValue::or(
            &MaterialValue::None,
            &MaterialValue::String("fallback".to_owned())
        )
        .unwrap(),
        MaterialValue::String("fallback".to_owned())
    );
    assert_eq!(
        MaterialValue::make_list([
            Value::Ready(MaterialValue::Int(2)),
            Value::Ready(MaterialValue::Bool(false))
        ])
        .unwrap(),
        MaterialValue::List(vec![
            Value::Ready(MaterialValue::Int(2)),
            Value::Ready(MaterialValue::Bool(false))
        ])
    );
    assert_eq!(
        MaterialValue::make_dict([
            ("name".to_owned(), Value::Ready(MaterialValue::Int(2))),
            (
                "na\"me".to_owned(),
                Value::Ready(MaterialValue::Bool(false))
            ),
        ])
        .unwrap(),
        MaterialValue::Dict(IndexMap::from([
            ("name".to_owned(), Value::Ready(MaterialValue::Int(2))),
            (
                "na\"me".to_owned(),
                Value::Ready(MaterialValue::Bool(false))
            ),
        ]))
    );
    assert!(matches!(
        MaterialValue::Int(3).as_dict_key(),
        Err(AsDictKeyError::UnsupportedKeyType)
    ));
    assert!(matches!(
        MaterialValue::List(vec![Value::Ready(MaterialValue::String(
            "nested".to_owned()
        ))])
        .as_dict_key(),
        Err(AsDictKeyError::UnsupportedKeyType)
    ));
    assert_eq!(
        MaterialValue::Int(5).to_sleep_duration().unwrap().get(),
        std::time::Duration::from_secs(5)
    );
    assert_eq!(
        MaterialValue::Float(0.5.try_into().unwrap())
            .to_sleep_duration()
            .unwrap()
            .get(),
        std::time::Duration::from_millis(500)
    );
    assert!(matches!(
        MaterialValue::Int(0).to_sleep_duration().unwrap_err(),
        extcallset::SleepDurationError::Zero(_)
    ));
    assert!(matches!(
        MaterialValue::Int(-1).to_sleep_duration().unwrap_err(),
        extcallset::SleepDurationError::Negative
    ));
    assert!(matches!(
        MaterialValue::Float(0.0.try_into().unwrap())
            .to_sleep_duration()
            .unwrap_err(),
        extcallset::SleepDurationError::Zero(_)
    ));
    assert!(matches!(
        MaterialValue::Float((-0.25).try_into().unwrap())
            .to_sleep_duration()
            .unwrap_err(),
        extcallset::SleepDurationError::FloatConversion(_)
    ));
    let value: Result<NonNaNFinite, _> = f64::NAN.try_into();
    assert!(value.is_err());
    assert_eq!(
        MaterialValue::Bool(true).to_sleep_duration().unwrap_err(),
        extcallset::SleepDurationError::UnsupportedValue
    );
}

#[test]
fn length_operations_follow_runtime_semantics() {
    assert_eq!(
        MaterialValue::List(vec![
            Value::Ready(MaterialValue::Int(1)),
            Value::Ready(MaterialValue::Int(2))
        ])
        .length()
        .unwrap(),
        2
    );
    assert_eq!(
        MaterialValue::String("hello".to_owned()).length().unwrap(),
        5
    );
    assert_eq!(
        MaterialValue::Dict(IndexMap::from([(
            "key".to_owned(),
            Value::Ready(MaterialValue::Int(1))
        )]))
        .length()
        .unwrap(),
        1
    );
    assert_eq!(
        MaterialValue::from_length(3).unwrap(),
        MaterialValue::Int(3)
    );
    assert!(matches!(
        MaterialValue::Bool(false).length(),
        Err(LengthError::UnsupportedValue)
    ));

    let too_large = usize::try_from(i64::MAX as u128 + 1).ok();
    if let Some(too_large) = too_large {
        assert!(matches!(
            MaterialValue::from_length(too_large),
            Err(FromLengthError::ResultOutOfBounds)
        ));
    }
}

#[test]
fn operation_errors_raise_the_class_they_map_to() {
    let exception = RaisedException::from(BinaryOperationError::UnsupportedOperation {
        operation: BinaryOpKind::Add,
    });
    assert_eq!(exception.type_id, "TypeError");
    assert_eq!(exception.mro_type_ids, ["Exception", "BaseException"]);
    assert_eq!(
        exception.details,
        Value::Ready(MaterialValue::String(
            "+ is not supported for these operands".to_owned()
        ))
    );

    let exception = RaisedException::from(IndexOperationError::MissingKey);
    assert_eq!(exception.type_id, "KeyError");
    assert_eq!(
        exception.mro_type_ids,
        ["LookupError", "Exception", "BaseException"]
    );

    let exception = RaisedException::from(FromLengthError::ResultOutOfBounds);
    assert_eq!(exception.type_id, "OverflowError");
    assert_eq!(
        exception.mro_type_ids,
        ["ArithmeticError", "Exception", "BaseException"]
    );
}

/// The three runtime exceptions have Python proxies in
/// `python/src/waymark/vm_exceptions.py`, pinned to the same literals by
/// `python/tests/test_vm_exceptions.py`; this is the Rust side of that
/// pin, so a change to either table fails a test.
#[test]
fn runtime_exception_classes_match_their_python_proxies() {
    assert_eq!(classes::ACTION_TIMEOUT.type_id, "ActionTimeout");
    assert_eq!(classes::ACTION_TIMEOUT.mro_type_ids, ["BaseException"]);

    assert_eq!(
        classes::ACTION_EXECUTION_NOT_STARTED.type_id,
        "ActionExecutionNotStarted"
    );
    assert_eq!(
        classes::ACTION_EXECUTION_NOT_STARTED.mro_type_ids,
        ["Exception", "BaseException"]
    );

    assert_eq!(
        classes::ACTION_EXECUTION_LOST.type_id,
        "ActionExecutionLost"
    );
    assert_eq!(
        classes::ACTION_EXECUTION_LOST.mro_type_ids,
        ["BaseException"]
    );
}

#[test]
fn exceptions_match_handlers_by_class_hierarchy() {
    let key_error = classes::KEY_ERROR.exception(Value::Ready(MaterialValue::None));

    let class = |name: &str| Pattern::Classes(vec![name.to_owned()]);
    assert!(key_error.matches(&class("KeyError")));
    assert!(key_error.matches(&class("LookupError")));
    assert!(key_error.matches(&class("Exception")));
    assert!(key_error.matches(&class("BaseException")));
    assert!(!key_error.matches(&class("IndexError")));
    assert!(!key_error.matches(&class("ArithmeticError")));

    // Any class listed catches.
    let several = Pattern::Classes(vec!["ValueError".to_owned(), "LookupError".to_owned()]);
    assert!(key_error.matches(&several));

    // No classes listed catches nothing, as `except ():` does; the bare
    // `except:` is its own pattern.
    assert!(!key_error.matches(&Pattern::Classes(Vec::new())));
    assert!(key_error.matches(&Pattern::Any));

    // The action exceptions sit directly under `BaseException`: a bare
    // `except Exception` does not catch them.
    let timeout = classes::ACTION_TIMEOUT.exception(Value::Ready(MaterialValue::None));
    assert!(!timeout.matches(&class("Exception")));
    assert!(timeout.matches(&class("BaseException")));
    let lost = classes::ACTION_EXECUTION_LOST.exception(Value::Ready(MaterialValue::None));
    assert!(!lost.matches(&class("Exception")));
    assert!(lost.matches(&class("BaseException")));
    let not_started =
        classes::ACTION_EXECUTION_NOT_STARTED.exception(Value::Ready(MaterialValue::None));
    assert!(not_started.matches(&class("Exception")));

    // A class whose bases are not known matches by its own name alone.
    let bare = Exception {
        type_id: "RetryCounterError".to_owned(),
        mro_type_ids: Vec::new(),
        details: Value::Ready(MaterialValue::None),
    };
    assert!(bare.matches(&class("RetryCounterError")));
    assert!(!bare.matches(&class("Exception")));
    assert!(bare.matches(&Pattern::Any));
}

#[test]
fn float_operations_follow_non_nan_finite_semantics() {
    assert!(matches!(
        MaterialValue::div(
            &MaterialValue::Float(0.0.try_into().unwrap()),
            &MaterialValue::Float(0.0.try_into().unwrap()),
        ),
        Err(BinaryOperationError::ResultOutOfBounds {
            operation: BinaryOpKind::Div,
        })
    ));
    assert!(matches!(
        MaterialValue::modulo(
            &MaterialValue::Float(1.0.try_into().unwrap()),
            &MaterialValue::Float(0.0.try_into().unwrap()),
        ),
        Err(BinaryOperationError::ResultOutOfBounds {
            operation: BinaryOpKind::Mod,
        })
    ));
    assert!(matches!(
        MaterialValue::mul(
            &MaterialValue::Float(f64::MAX.try_into().unwrap()),
            &MaterialValue::Float(2.0.try_into().unwrap()),
        ),
        Err(BinaryOperationError::ResultOutOfBounds {
            operation: BinaryOpKind::Mul,
        })
    ));
    assert!(matches!(
        MaterialValue::div(
            &MaterialValue::Float(1.0.try_into().unwrap()),
            &MaterialValue::Float(0.0.try_into().unwrap()),
        ),
        Err(BinaryOperationError::ResultOutOfBounds {
            operation: BinaryOpKind::Div,
        })
    ));
    assert!(matches!(
        MaterialValue::floor_div(
            &MaterialValue::Float((-1.0).try_into().unwrap()),
            &MaterialValue::Float(0.0.try_into().unwrap()),
        ),
        Err(BinaryOperationError::ResultOutOfBounds {
            operation: BinaryOpKind::FloorDiv,
        })
    ));

    let infinity: Result<NonNaNFinite, _> = f64::INFINITY.try_into();
    let negative_infinity: Result<NonNaNFinite, _> = f64::NEG_INFINITY.try_into();

    assert!(infinity.is_err());
    assert!(negative_infinity.is_err());
}

#[test]
fn exception_values_cross_to_and_from_the_raised_domain() {
    let exception =
        classes::VALUE_ERROR.exception(Value::Ready(MaterialValue::String("boom".to_owned())));
    let value = MaterialValue::Exception(Box::new(exception.clone()));

    assert_eq!(value.capture_action_call_argument().unwrap(), value.clone());
    assert!(value.should_jump().unwrap());
    assert!(matches!(
        value.to_sleep_duration().unwrap_err(),
        extcallset::SleepDurationError::UnsupportedValue
    ));

    // The raised exception and the exception value are one type: raising
    // and capturing are the identity on it.
    let raised: RaisedException = value
        .clone()
        .into_raised()
        .expect("a ready exception value raises");
    assert_eq!(raised, exception);
    assert_eq!(
        MaterialValue::from_raised(raised.clone()).expect("a raised exception is captured"),
        value
    );

    let wrapped = Value::Ready(value.clone());
    let raised: RaisedException = wrapped
        .into_raised()
        .expect("a promise value forwards the raise to its ready value");
    assert_eq!(raised, exception);
    assert_eq!(
        Value::from_raised(raised).expect("a promise value captures as ready"),
        Value::Ready(value)
    );

    let not_an_exception: Result<RaisedException, _> = MaterialValue::Int(1).into_raised();
    assert!(not_an_exception.is_err());
    let not_an_exception: Result<RaisedException, _> =
        Value::Ready(MaterialValue::Int(1)).into_raised();
    assert!(not_an_exception.is_err());
    let pending: Result<RaisedException, _> =
        Value::Pending(waymark_vm_runtime_promise_core::PromiseStateId::from_raw(3)).into_raised();
    assert!(pending.is_err());
}
