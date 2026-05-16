use indexmap::IndexMap;
use typed_floats::NonNaNFinite;
use waymark_vm_instructions_pureset::BinaryOpKind;
use waymark_vm_interpreter_coreset::value::ShouldJump as _;
use waymark_vm_interpreter_extcallset::value::SleepDuration as _;
use waymark_vm_interpreter_pureset::value::{
    BinaryOperationError, BinaryOps as _, FromLengthError, Length as _, LengthError, MakeDict as _,
    MakeDictError, MakeList as _, UnaryOps as _,
};
use waymark_vm_value::{Value, extcallset};

#[test]
fn values_follow_truthiness() {
    assert!(Value::String("x".to_owned()).should_jump().unwrap());
    assert!(!Value::String(String::new()).should_jump().unwrap());
    assert!(!Value::None.should_jump().unwrap());
    assert!(Value::Float(1.5.try_into().unwrap()).should_jump().unwrap());
    assert!(!Value::Float(0.0.try_into().unwrap()).should_jump().unwrap());
    assert!(Value::List(vec![Value::Int(1)]).should_jump().unwrap());
    assert!(
        Value::Dict(IndexMap::from([("key".to_owned(), Value::Int(1))]))
            .should_jump()
            .unwrap()
    );
}

#[test]
fn binary_and_unary_operations_cover_current_vm_value_cases() {
    assert_eq!(
        Value::add(&Value::Int(2), &Value::Int(3)).unwrap(),
        Value::Int(5)
    );
    assert_eq!(
        Value::add(
            &Value::String("hello ".to_owned()),
            &Value::String("world".to_owned())
        )
        .unwrap(),
        Value::String("hello world".to_owned())
    );
    assert_eq!(
        Value::add(
            &Value::List(vec![Value::Int(1)]),
            &Value::List(vec![Value::Int(2)])
        )
        .unwrap(),
        Value::List(vec![Value::Int(1), Value::Int(2)])
    );
    assert_eq!(
        Value::add(
            &Value::Float(1.25.try_into().unwrap()),
            &Value::Float(2.0.try_into().unwrap()),
        )
        .unwrap(),
        Value::Float(3.25.try_into().unwrap())
    );
    assert_eq!(
        Value::sub(
            &Value::Float(3.5.try_into().unwrap()),
            &Value::Float(1.25.try_into().unwrap()),
        )
        .unwrap(),
        Value::Float(2.25.try_into().unwrap())
    );
    assert_eq!(
        Value::mul(
            &Value::Float(3.0.try_into().unwrap()),
            &Value::Float(0.5.try_into().unwrap()),
        )
        .unwrap(),
        Value::Float(1.5.try_into().unwrap())
    );
    assert_eq!(
        Value::div(
            &Value::Float(3.0.try_into().unwrap()),
            &Value::Float(2.0.try_into().unwrap()),
        )
        .unwrap(),
        Value::Float(1.5.try_into().unwrap())
    );
    assert_eq!(
        Value::floor_div(&Value::Int(-3), &Value::Int(2)).unwrap(),
        Value::Int(-2)
    );
    assert_eq!(
        Value::floor_div(
            &Value::Float((-3.0).try_into().unwrap()),
            &Value::Float(2.0.try_into().unwrap()),
        )
        .unwrap(),
        Value::Float((-2.0).try_into().unwrap())
    );
    assert_eq!(
        Value::modulo(&Value::Int(3), &Value::Int(-2)).unwrap(),
        Value::Int(-1)
    );
    assert_eq!(
        Value::modulo(
            &Value::Float(3.5.try_into().unwrap()),
            &Value::Float(2.0.try_into().unwrap()),
        )
        .unwrap(),
        Value::Float(1.5.try_into().unwrap())
    );
    assert_eq!(
        Value::modulo(
            &Value::Float(3.5.try_into().unwrap()),
            &Value::Float((-2.0).try_into().unwrap()),
        )
        .unwrap(),
        Value::Float((-0.5).try_into().unwrap())
    );
    assert_eq!(
        Value::contains(
            &Value::String("ell".to_owned()),
            &Value::String("hello".to_owned())
        )
        .unwrap(),
        Value::Bool(true)
    );
    assert_eq!(
        Value::contains(
            &Value::Int(2),
            &Value::List(vec![Value::Int(1), Value::Int(2)])
        )
        .unwrap(),
        Value::Bool(true)
    );
    assert_eq!(
        Value::contains(
            &Value::String("key".to_owned()),
            &Value::Dict(IndexMap::from([(
                "key".to_owned(),
                Value::String("x".to_owned()),
            )]))
        )
        .unwrap(),
        Value::Bool(true)
    );
    assert_eq!(
        Value::lt(
            &Value::Float(1.5.try_into().unwrap()),
            &Value::Float(2.0.try_into().unwrap()),
        )
        .unwrap(),
        Value::Bool(true)
    );
    assert_eq!(Value::neg(&Value::Int(7)).unwrap(), Value::Int(-7));
    assert_eq!(
        Value::neg(&Value::Float(2.5.try_into().unwrap())).unwrap(),
        Value::Float((-2.5).try_into().unwrap())
    );
    assert_eq!(Value::not(&Value::None).unwrap(), Value::Bool(true));
}

#[test]
fn mixed_numeric_operations_do_not_silently_promote_ints_to_floats() {
    assert!(matches!(
        Value::add(&Value::Float(1.25.try_into().unwrap()), &Value::Int(2)),
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOpKind::Add,
        })
    ));
    assert!(matches!(
        Value::mul(&Value::Int(3), &Value::Float(0.5.try_into().unwrap())),
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOpKind::Mul,
        })
    ));
    assert!(matches!(
        Value::div(&Value::Int(3), &Value::Int(2)),
        Err(BinaryOperationError::ResultOutOfBounds {
            operation: BinaryOpKind::Div,
        })
    ));
    assert!(matches!(
        Value::floor_div(&Value::Float((-3.0).try_into().unwrap()), &Value::Int(2)),
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOpKind::FloorDiv,
        })
    ));
    assert_eq!(
        waymark_vm_interpreter_pureset::value::BinaryOps::eq(
            &Value::Int(1),
            &Value::Float(1.0.try_into().unwrap()),
        )
        .unwrap(),
        Value::Bool(false)
    );
    assert!(matches!(
        Value::lt(&Value::Float(1.5.try_into().unwrap()), &Value::Int(2)),
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOpKind::Lt,
        })
    ));
    assert_eq!(
        Value::contains(
            &Value::Float(1.0.try_into().unwrap()),
            &Value::Dict(IndexMap::from([(
                "value".to_owned(),
                Value::String("x".to_owned()),
            )]))
        )
        .unwrap(),
        Value::Bool(false)
    );
}

#[test]
fn dict_values_compare_equal_independent_of_insertion_order() {
    let left = Value::Dict(IndexMap::from([
        ("first".to_owned(), Value::Int(1)),
        ("second".to_owned(), Value::Int(2)),
    ]));
    let right = Value::Dict(IndexMap::from([
        ("second".to_owned(), Value::Int(2)),
        ("first".to_owned(), Value::Int(1)),
    ]));

    assert_eq!(left, right);
}

#[test]
fn logical_ops_lists_and_sleep_duration_use_runtime_semantics() {
    assert_eq!(
        Value::and(&Value::Int(1), &Value::String("x".to_owned())).unwrap(),
        Value::String("x".to_owned())
    );
    assert_eq!(
        Value::or(&Value::None, &Value::String("fallback".to_owned())).unwrap(),
        Value::String("fallback".to_owned())
    );
    assert_eq!(
        Value::make_list([Value::Int(2), Value::Bool(false)]).unwrap(),
        Value::List(vec![Value::Int(2), Value::Bool(false)])
    );
    assert_eq!(
        Value::make_dict([
            (Value::String("name".to_owned()), Value::Int(2)),
            (Value::String("na\"me".to_owned()), Value::Bool(false)),
        ])
        .unwrap(),
        Value::Dict(IndexMap::from([
            ("name".to_owned(), Value::Int(2)),
            ("na\"me".to_owned(), Value::Bool(false)),
        ]))
    );
    assert!(matches!(
        Value::make_dict([(Value::Int(3), Value::Bool(false))]),
        Err(MakeDictError::UnsupportedKeyType)
    ));
    assert!(matches!(
        Value::make_dict([(
            Value::List(vec![Value::String("nested".to_owned())]),
            Value::None,
        )]),
        Err(MakeDictError::UnsupportedKeyType)
    ));
    assert_eq!(
        Value::Int(5).to_sleep_duration().unwrap().get(),
        std::time::Duration::from_secs(5)
    );
    assert_eq!(
        Value::Float(0.5.try_into().unwrap())
            .to_sleep_duration()
            .unwrap()
            .get(),
        std::time::Duration::from_millis(500)
    );
    assert!(matches!(
        Value::Int(0).to_sleep_duration().unwrap_err(),
        extcallset::SleepDurationError::Zero(_)
    ));
    assert!(matches!(
        Value::Int(-1).to_sleep_duration().unwrap_err(),
        extcallset::SleepDurationError::Negative
    ));
    assert!(matches!(
        Value::Float(0.0.try_into().unwrap())
            .to_sleep_duration()
            .unwrap_err(),
        extcallset::SleepDurationError::Zero(_)
    ));
    assert!(matches!(
        Value::Float((-0.25).try_into().unwrap())
            .to_sleep_duration()
            .unwrap_err(),
        extcallset::SleepDurationError::FloatConversion(_)
    ));
    let value: Result<NonNaNFinite, _> = f64::NAN.try_into();
    assert!(value.is_err());
    assert_eq!(
        Value::Bool(true).to_sleep_duration().unwrap_err(),
        extcallset::SleepDurationError::UnsupportedValue
    );
}

#[test]
fn length_operations_follow_runtime_semantics() {
    assert_eq!(
        Value::List(vec![Value::Int(1), Value::Int(2)])
            .length()
            .unwrap(),
        2
    );
    assert_eq!(Value::String("hello".to_owned()).length().unwrap(), 5);
    assert_eq!(
        Value::Dict(IndexMap::from([("key".to_owned(), Value::Int(1))]))
            .length()
            .unwrap(),
        1
    );
    assert_eq!(Value::from_length(3).unwrap(), Value::Int(3));
    assert!(matches!(
        Value::Bool(false).length(),
        Err(LengthError::UnsupportedValue)
    ));

    let too_large = usize::try_from(i64::MAX as u128 + 1).ok();
    if let Some(too_large) = too_large {
        assert!(matches!(
            Value::from_length(too_large),
            Err(FromLengthError::ResultOutOfBounds)
        ));
    }
}

#[test]
fn float_operations_follow_non_nan_finite_semantics() {
    assert!(matches!(
        Value::div(
            &Value::Float(0.0.try_into().unwrap()),
            &Value::Float(0.0.try_into().unwrap()),
        ),
        Err(BinaryOperationError::ResultOutOfBounds {
            operation: BinaryOpKind::Div,
        })
    ));
    assert!(matches!(
        Value::modulo(
            &Value::Float(1.0.try_into().unwrap()),
            &Value::Float(0.0.try_into().unwrap()),
        ),
        Err(BinaryOperationError::ResultOutOfBounds {
            operation: BinaryOpKind::Mod,
        })
    ));
    assert!(matches!(
        Value::mul(
            &Value::Float(f64::MAX.try_into().unwrap()),
            &Value::Float(2.0.try_into().unwrap()),
        ),
        Err(BinaryOperationError::ResultOutOfBounds {
            operation: BinaryOpKind::Mul,
        })
    ));
    assert!(matches!(
        Value::div(
            &Value::Float(1.0.try_into().unwrap()),
            &Value::Float(0.0.try_into().unwrap()),
        ),
        Err(BinaryOperationError::ResultOutOfBounds {
            operation: BinaryOpKind::Div,
        })
    ));
    assert!(matches!(
        Value::floor_div(
            &Value::Float((-1.0).try_into().unwrap()),
            &Value::Float(0.0.try_into().unwrap()),
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
