use indexmap::IndexMap;
use typed_floats::NonNaNFinite;
use waymark_vm_instructions_pureset::BinaryOpKind;
use waymark_vm_interpreter_coreset::value::ShouldJump as _;
use waymark_vm_interpreter_pureset::value::{
    AsDictKey as _, AsDictKeyError, AsExceptionTypeId as _, AsExceptionTypeIdError,
    BinaryOperationError, BinaryOps as _, DotOp as _, DotOperationError, FromLengthError,
    IndexOp as _, IndexOperationError, Length as _, LengthError, MakeDict as _, MakeException as _,
    MakeList as _, UnaryOps as _,
};
use waymark_vm_runtime_exception::{AsException as _, Exception, IntoException as _};
use waymark_vm_value::{ReadyValue, Value};

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
fn logical_ops_and_lists_use_runtime_semantics() {
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
    let value: Result<NonNaNFinite, _> = f64::NAN.try_into();
    assert!(value.is_err());
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
fn exception_operations_follow_runtime_semantics() {
    assert_eq!(
        ReadyValue::make_exception("ValueError".to_owned(), Value::Ready(ReadyValue::Int(41))),
        ReadyValue::Exception(Box::new(Exception {
            type_id: "ValueError".to_owned(),
            details: Value::Ready(ReadyValue::Int(41)),
        }))
    );
    assert_eq!(
        ReadyValue::String("ValueError".to_owned())
            .as_exception_type_id()
            .unwrap(),
        "ValueError"
    );
    assert!(matches!(
        ReadyValue::Int(3).as_exception_type_id(),
        Err(AsExceptionTypeIdError::UnsupportedTypeIdType)
    ));
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
fn exception_values_round_trip_through_exception_traits() {
    let details = Value::Ready(ReadyValue::String("boom".to_owned()));
    let value = ReadyValue::Exception(Box::new(Exception {
        type_id: "ValueError".to_owned(),
        details: details.clone(),
    }));

    let exception = value
        .as_exception()
        .expect("ready value should be an exception");
    assert_eq!(exception.type_id, "ValueError");
    assert_eq!(exception.details, details);
    assert!(value.should_jump().unwrap());

    let owned = value
        .clone()
        .into_exception()
        .expect("owned ready exception");
    assert_eq!(owned.type_id, "ValueError");
    assert_eq!(owned.details, details);

    let wrapped = Value::Ready(value);
    let exception = wrapped
        .as_exception()
        .expect("promise value should forward exception refs");
    assert_eq!(exception.type_id, "ValueError");
    assert_eq!(exception.details, details);

    let owned = wrapped
        .into_exception()
        .expect("promise value should forward owned exceptions");
    assert_eq!(owned.type_id, "ValueError");
    assert_eq!(owned.details, details);

    assert!(ReadyValue::Int(1).as_exception().is_err());
    assert!(Value::Ready(ReadyValue::Int(1)).into_exception().is_err());
}
