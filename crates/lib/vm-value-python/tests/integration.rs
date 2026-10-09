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
use waymark_vm_value_python::{Exception, RaisedException, ReadyValue, Value};

#[test]
fn values_follow_truthiness() {
    assert!(ReadyValue::String("x".to_owned()).should_jump().unwrap());
    assert!(!ReadyValue::String(String::new()).should_jump().unwrap());
    assert!(!ReadyValue::None.should_jump().unwrap());
    assert!(
        ReadyValue::Float(1.5.try_into().unwrap())
            .should_jump()
            .unwrap()
    );
    assert!(
        !ReadyValue::Float(0.0.try_into().unwrap())
            .should_jump()
            .unwrap()
    );
    assert!(
        ReadyValue::List(vec![Value::Ready(ReadyValue::Int(1))])
            .should_jump()
            .unwrap()
    );
    assert!(
        ReadyValue::Dict(IndexMap::from([(
            "key".to_owned(),
            Value::Ready(ReadyValue::Int(1))
        )]))
        .should_jump()
        .unwrap()
    );
}

#[test]
fn binary_and_unary_operations_cover_current_vm_value_cases() {
    assert_eq!(
        ReadyValue::add(&ReadyValue::Int(2), &ReadyValue::Int(3)).unwrap(),
        ReadyValue::Int(5)
    );
    assert_eq!(
        ReadyValue::add(
            &ReadyValue::String("hello ".to_owned()),
            &ReadyValue::String("world".to_owned())
        )
        .unwrap(),
        ReadyValue::String("hello world".to_owned())
    );
    assert_eq!(
        ReadyValue::add(
            &ReadyValue::List(vec![Value::Ready(ReadyValue::Int(1))]),
            &ReadyValue::List(vec![Value::Ready(ReadyValue::Int(2))])
        )
        .unwrap(),
        ReadyValue::List(vec![
            Value::Ready(ReadyValue::Int(1)),
            Value::Ready(ReadyValue::Int(2))
        ])
    );
    assert_eq!(
        ReadyValue::add(
            &ReadyValue::Float(1.25.try_into().unwrap()),
            &ReadyValue::Float(2.0.try_into().unwrap()),
        )
        .unwrap(),
        ReadyValue::Float(3.25.try_into().unwrap())
    );
    assert_eq!(
        ReadyValue::sub(
            &ReadyValue::Float(3.5.try_into().unwrap()),
            &ReadyValue::Float(1.25.try_into().unwrap()),
        )
        .unwrap(),
        ReadyValue::Float(2.25.try_into().unwrap())
    );
    assert_eq!(
        ReadyValue::mul(
            &ReadyValue::Float(3.0.try_into().unwrap()),
            &ReadyValue::Float(0.5.try_into().unwrap()),
        )
        .unwrap(),
        ReadyValue::Float(1.5.try_into().unwrap())
    );
    assert_eq!(
        ReadyValue::div(
            &ReadyValue::Float(3.0.try_into().unwrap()),
            &ReadyValue::Float(2.0.try_into().unwrap()),
        )
        .unwrap(),
        ReadyValue::Float(1.5.try_into().unwrap())
    );
    assert_eq!(
        ReadyValue::floor_div(&ReadyValue::Int(-3), &ReadyValue::Int(2)).unwrap(),
        ReadyValue::Int(-2)
    );
    assert_eq!(
        ReadyValue::floor_div(
            &ReadyValue::Float((-3.0).try_into().unwrap()),
            &ReadyValue::Float(2.0.try_into().unwrap()),
        )
        .unwrap(),
        ReadyValue::Float((-2.0).try_into().unwrap())
    );
    assert_eq!(
        ReadyValue::modulo(&ReadyValue::Int(3), &ReadyValue::Int(-2)).unwrap(),
        ReadyValue::Int(-1)
    );
    assert_eq!(
        ReadyValue::modulo(
            &ReadyValue::Float(3.5.try_into().unwrap()),
            &ReadyValue::Float(2.0.try_into().unwrap()),
        )
        .unwrap(),
        ReadyValue::Float(1.5.try_into().unwrap())
    );
    assert_eq!(
        ReadyValue::modulo(
            &ReadyValue::Float(3.5.try_into().unwrap()),
            &ReadyValue::Float((-2.0).try_into().unwrap()),
        )
        .unwrap(),
        ReadyValue::Float((-0.5).try_into().unwrap())
    );
    assert_eq!(
        ReadyValue::contains(
            &ReadyValue::String("ell".to_owned()),
            &ReadyValue::String("hello".to_owned())
        )
        .unwrap(),
        ReadyValue::Bool(true)
    );
    assert_eq!(
        ReadyValue::contains(
            &ReadyValue::Int(2),
            &ReadyValue::List(vec![
                Value::Ready(ReadyValue::Int(1)),
                Value::Ready(ReadyValue::Int(2))
            ])
        )
        .unwrap(),
        ReadyValue::Bool(true)
    );
    assert_eq!(
        ReadyValue::contains(
            &ReadyValue::String("key".to_owned()),
            &ReadyValue::Dict(IndexMap::from([(
                "key".to_owned(),
                Value::Ready(ReadyValue::String("x".to_owned())),
            )]))
        )
        .unwrap(),
        ReadyValue::Bool(true)
    );
    assert_eq!(
        ReadyValue::lt(
            &ReadyValue::Float(1.5.try_into().unwrap()),
            &ReadyValue::Float(2.0.try_into().unwrap()),
        )
        .unwrap(),
        ReadyValue::Bool(true)
    );
    assert_eq!(
        ReadyValue::neg(&ReadyValue::Int(7)).unwrap(),
        ReadyValue::Int(-7)
    );
    assert_eq!(
        ReadyValue::neg(&ReadyValue::Float(2.5.try_into().unwrap())).unwrap(),
        ReadyValue::Float((-2.5).try_into().unwrap())
    );
    assert_eq!(
        ReadyValue::not(&ReadyValue::None).unwrap(),
        ReadyValue::Bool(true)
    );
}

#[test]
fn mixed_numeric_operations_do_not_silently_promote_ints_to_floats() {
    assert!(matches!(
        ReadyValue::add(
            &ReadyValue::Float(1.25.try_into().unwrap()),
            &ReadyValue::Int(2)
        ),
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOpKind::Add,
        })
    ));
    assert!(matches!(
        ReadyValue::mul(
            &ReadyValue::Int(3),
            &ReadyValue::Float(0.5.try_into().unwrap())
        ),
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOpKind::Mul,
        })
    ));
    assert!(matches!(
        ReadyValue::div(&ReadyValue::Int(3), &ReadyValue::Int(2)),
        Err(BinaryOperationError::ResultOutOfBounds {
            operation: BinaryOpKind::Div,
        })
    ));
    assert!(matches!(
        ReadyValue::floor_div(
            &ReadyValue::Float((-3.0).try_into().unwrap()),
            &ReadyValue::Int(2)
        ),
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOpKind::FloorDiv,
        })
    ));
    assert_eq!(
        waymark_vm_interpreter_pureset::value::BinaryOps::eq(
            &ReadyValue::Int(1),
            &ReadyValue::Float(1.0.try_into().unwrap()),
        )
        .unwrap(),
        ReadyValue::Bool(false)
    );
    assert!(matches!(
        ReadyValue::lt(
            &ReadyValue::Float(1.5.try_into().unwrap()),
            &ReadyValue::Int(2)
        ),
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOpKind::Lt,
        })
    ));
    assert_eq!(
        ReadyValue::contains(
            &ReadyValue::Float(1.0.try_into().unwrap()),
            &ReadyValue::Dict(IndexMap::from([(
                "value".to_owned(),
                Value::Ready(ReadyValue::String("x".to_owned())),
            )]))
        )
        .unwrap(),
        ReadyValue::Bool(false)
    );
}

#[test]
fn index_and_dot_operations_follow_runtime_semantics() {
    assert_eq!(
        ReadyValue::index(
            &ReadyValue::List(vec![
                Value::Ready(ReadyValue::Int(1)),
                Value::Ready(ReadyValue::Int(2))
            ]),
            &ReadyValue::Int(-1)
        )
        .unwrap(),
        Value::Ready(ReadyValue::Int(2))
    );
    assert_eq!(
        ReadyValue::index(&ReadyValue::String("hello".to_owned()), &ReadyValue::Int(1)).unwrap(),
        Value::Ready(ReadyValue::String("e".to_owned()))
    );
    assert_eq!(
        ReadyValue::index(
            &ReadyValue::Dict(IndexMap::from([(
                "field".to_owned(),
                Value::Ready(ReadyValue::Int(7))
            )])),
            &ReadyValue::String("field".to_owned())
        )
        .unwrap(),
        Value::Ready(ReadyValue::Int(7))
    );
    assert_eq!(
        ReadyValue::dot(
            &ReadyValue::Dict(IndexMap::from([(
                "field".to_owned(),
                Value::Ready(ReadyValue::Int(7))
            )])),
            "field"
        )
        .unwrap(),
        Value::Ready(ReadyValue::Int(7))
    );
}

#[test]
fn index_and_dot_operations_surface_expected_errors() {
    assert!(matches!(
        ReadyValue::index(
            &ReadyValue::List(vec![Value::Ready(ReadyValue::Int(1))]),
            &ReadyValue::Int(1)
        ),
        Err(IndexOperationError::IndexOutOfBounds)
    ));
    assert!(matches!(
        ReadyValue::index(
            &ReadyValue::Dict(IndexMap::from([(
                "field".to_owned(),
                Value::Ready(ReadyValue::Int(7))
            )])),
            &ReadyValue::String("missing".to_owned())
        ),
        Err(IndexOperationError::MissingKey)
    ));
    assert!(matches!(
        ReadyValue::dot(
            &ReadyValue::Dict(IndexMap::from([(
                "field".to_owned(),
                Value::Ready(ReadyValue::Int(7))
            )])),
            "missing"
        ),
        Err(DotOperationError::MissingAttribute)
    ));
    assert!(matches!(
        ReadyValue::dot(&ReadyValue::List(Vec::new()), "field"),
        Err(DotOperationError::UnsupportedOperation)
    ));
}

#[test]
fn dict_values_compare_equal_independent_of_insertion_order() {
    let left = ReadyValue::Dict(IndexMap::from([
        ("first".to_owned(), Value::Ready(ReadyValue::Int(1))),
        ("second".to_owned(), Value::Ready(ReadyValue::Int(2))),
    ]));
    let right = ReadyValue::Dict(IndexMap::from([
        ("second".to_owned(), Value::Ready(ReadyValue::Int(2))),
        ("first".to_owned(), Value::Ready(ReadyValue::Int(1))),
    ]));

    assert_eq!(left, right);
}

#[test]
fn logical_ops_lists_and_sleep_duration_use_runtime_semantics() {
    assert_eq!(
        ReadyValue::and(&ReadyValue::Int(1), &ReadyValue::String("x".to_owned())).unwrap(),
        ReadyValue::String("x".to_owned())
    );
    assert_eq!(
        ReadyValue::or(
            &ReadyValue::None,
            &ReadyValue::String("fallback".to_owned())
        )
        .unwrap(),
        ReadyValue::String("fallback".to_owned())
    );
    assert_eq!(
        ReadyValue::make_list([
            Value::Ready(ReadyValue::Int(2)),
            Value::Ready(ReadyValue::Bool(false))
        ])
        .unwrap(),
        ReadyValue::List(vec![
            Value::Ready(ReadyValue::Int(2)),
            Value::Ready(ReadyValue::Bool(false))
        ])
    );
    assert_eq!(
        ReadyValue::make_dict([
            ("name".to_owned(), Value::Ready(ReadyValue::Int(2))),
            ("na\"me".to_owned(), Value::Ready(ReadyValue::Bool(false))),
        ])
        .unwrap(),
        ReadyValue::Dict(IndexMap::from([
            ("name".to_owned(), Value::Ready(ReadyValue::Int(2))),
            ("na\"me".to_owned(), Value::Ready(ReadyValue::Bool(false))),
        ]))
    );
    assert!(matches!(
        ReadyValue::Int(3).as_dict_key(),
        Err(AsDictKeyError::UnsupportedKeyType)
    ));
    assert!(matches!(
        ReadyValue::List(vec![Value::Ready(ReadyValue::String("nested".to_owned()))]).as_dict_key(),
        Err(AsDictKeyError::UnsupportedKeyType)
    ));
    assert_eq!(
        ReadyValue::Int(5).to_sleep_duration().unwrap().get(),
        std::time::Duration::from_secs(5)
    );
    assert_eq!(
        ReadyValue::Float(0.5.try_into().unwrap())
            .to_sleep_duration()
            .unwrap()
            .get(),
        std::time::Duration::from_millis(500)
    );
    assert!(matches!(
        ReadyValue::Int(0).to_sleep_duration().unwrap_err(),
        extcallset::SleepDurationError::Zero(_)
    ));
    assert!(matches!(
        ReadyValue::Int(-1).to_sleep_duration().unwrap_err(),
        extcallset::SleepDurationError::Negative
    ));
    assert!(matches!(
        ReadyValue::Float(0.0.try_into().unwrap())
            .to_sleep_duration()
            .unwrap_err(),
        extcallset::SleepDurationError::Zero(_)
    ));
    assert!(matches!(
        ReadyValue::Float((-0.25).try_into().unwrap())
            .to_sleep_duration()
            .unwrap_err(),
        extcallset::SleepDurationError::FloatConversion(_)
    ));
    let value: Result<NonNaNFinite, _> = f64::NAN.try_into();
    assert!(value.is_err());
    assert_eq!(
        ReadyValue::Bool(true).to_sleep_duration().unwrap_err(),
        extcallset::SleepDurationError::UnsupportedValue
    );
}

#[test]
fn length_operations_follow_runtime_semantics() {
    assert_eq!(
        ReadyValue::List(vec![
            Value::Ready(ReadyValue::Int(1)),
            Value::Ready(ReadyValue::Int(2))
        ])
        .length()
        .unwrap(),
        2
    );
    assert_eq!(ReadyValue::String("hello".to_owned()).length().unwrap(), 5);
    assert_eq!(
        ReadyValue::Dict(IndexMap::from([(
            "key".to_owned(),
            Value::Ready(ReadyValue::Int(1))
        )]))
        .length()
        .unwrap(),
        1
    );
    assert_eq!(ReadyValue::from_length(3).unwrap(), ReadyValue::Int(3));
    assert!(matches!(
        ReadyValue::Bool(false).length(),
        Err(LengthError::UnsupportedValue)
    ));

    let too_large = usize::try_from(i64::MAX as u128 + 1).ok();
    if let Some(too_large) = too_large {
        assert!(matches!(
            ReadyValue::from_length(too_large),
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
        Value::Ready(ReadyValue::String(
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
    let key_error = classes::KEY_ERROR.exception(Value::Ready(ReadyValue::None));

    let class = |name: &str| Pattern::from(&vec![name.to_owned()]);
    assert!(key_error.matches(&class("KeyError")));
    assert!(key_error.matches(&class("LookupError")));
    assert!(key_error.matches(&class("Exception")));
    assert!(key_error.matches(&class("BaseException")));
    assert!(!key_error.matches(&class("IndexError")));
    assert!(!key_error.matches(&class("ArithmeticError")));

    // Any class listed catches; the list lowers as given.
    let several = Pattern::from(&vec!["ValueError".to_owned(), "LookupError".to_owned()]);
    assert_eq!(
        several,
        Pattern::Classes(nonempty_collections::nev![
            "ValueError".to_owned(),
            "LookupError".to_owned()
        ])
    );
    assert!(key_error.matches(&several));

    // No classes listed is the bare `except:`.
    assert_eq!(Pattern::from(&Vec::<String>::new()), Pattern::Any);
    assert!(key_error.matches(&Pattern::Any));

    // The action exceptions sit directly under `BaseException`: a bare
    // `except Exception` does not catch them.
    let timeout = classes::ACTION_TIMEOUT.exception(Value::Ready(ReadyValue::None));
    assert!(!timeout.matches(&class("Exception")));
    assert!(timeout.matches(&class("BaseException")));
    let lost = classes::ACTION_EXECUTION_LOST.exception(Value::Ready(ReadyValue::None));
    assert!(!lost.matches(&class("Exception")));
    assert!(lost.matches(&class("BaseException")));
    let not_started =
        classes::ACTION_EXECUTION_NOT_STARTED.exception(Value::Ready(ReadyValue::None));
    assert!(not_started.matches(&class("Exception")));

    // A class whose bases are not known matches by its own name alone.
    let bare = Exception {
        type_id: "RetryCounterError".to_owned(),
        mro_type_ids: Vec::new(),
        details: Value::Ready(ReadyValue::None),
    };
    assert!(bare.matches(&class("RetryCounterError")));
    assert!(!bare.matches(&class("Exception")));
    assert!(bare.matches(&Pattern::Any));
}

#[test]
fn float_operations_follow_non_nan_finite_semantics() {
    assert!(matches!(
        ReadyValue::div(
            &ReadyValue::Float(0.0.try_into().unwrap()),
            &ReadyValue::Float(0.0.try_into().unwrap()),
        ),
        Err(BinaryOperationError::ResultOutOfBounds {
            operation: BinaryOpKind::Div,
        })
    ));
    assert!(matches!(
        ReadyValue::modulo(
            &ReadyValue::Float(1.0.try_into().unwrap()),
            &ReadyValue::Float(0.0.try_into().unwrap()),
        ),
        Err(BinaryOperationError::ResultOutOfBounds {
            operation: BinaryOpKind::Mod,
        })
    ));
    assert!(matches!(
        ReadyValue::mul(
            &ReadyValue::Float(f64::MAX.try_into().unwrap()),
            &ReadyValue::Float(2.0.try_into().unwrap()),
        ),
        Err(BinaryOperationError::ResultOutOfBounds {
            operation: BinaryOpKind::Mul,
        })
    ));
    assert!(matches!(
        ReadyValue::div(
            &ReadyValue::Float(1.0.try_into().unwrap()),
            &ReadyValue::Float(0.0.try_into().unwrap()),
        ),
        Err(BinaryOperationError::ResultOutOfBounds {
            operation: BinaryOpKind::Div,
        })
    ));
    assert!(matches!(
        ReadyValue::floor_div(
            &ReadyValue::Float((-1.0).try_into().unwrap()),
            &ReadyValue::Float(0.0.try_into().unwrap()),
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
        classes::VALUE_ERROR.exception(Value::Ready(ReadyValue::String("boom".to_owned())));
    let value = ReadyValue::Exception(Box::new(exception.clone()));

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
        ReadyValue::from_raised(raised.clone()).expect("a raised exception is captured"),
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

    let not_an_exception: Result<RaisedException, _> = ReadyValue::Int(1).into_raised();
    assert!(not_an_exception.is_err());
    let not_an_exception: Result<RaisedException, _> =
        Value::Ready(ReadyValue::Int(1)).into_raised();
    assert!(not_an_exception.is_err());
    let pending: Result<RaisedException, _> =
        Value::Pending(waymark_vm_runtime_promise_core::PromiseStateId(3)).into_raised();
    assert!(pending.is_err());
}
