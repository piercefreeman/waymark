//! Behavioral tests for the Python variation's pureset operations over
//! the shared value shape.

use indexmap::IndexMap;
use typed_floats::NonNaNFinite;
use waymark_vm_instructions_pureset::BinaryOpKind;
use waymark_vm_interpreter_operations::Operations;
use waymark_vm_interpreter_operations_python::PythonVariation;
use waymark_vm_interpreter_operations_python::pureset::error::{
    AsDictKeyError, BinaryOperationError, DotOperationError, FromLengthError, IndexOperationError,
    LengthError,
};
use waymark_vm_interpreter_pureset::operations::{
    AsDictKey as _, AsExceptionTypeId as _, AsExceptionTypeIdError, BinaryOps as _, DotOp as _,
    IndexOp as _, Length, MakeDict, MakeException, MakeList, UnaryOps as _,
};
use waymark_vm_runtime_exception::{AsException as _, Exception, IntoException as _};
use waymark_vm_value::{ReadyValue, Value};

type PythonOperations = Operations<PythonVariation>;

#[test]
fn binary_and_unary_operations_cover_current_vm_value_cases() {
    assert_eq!(
        PythonOperations::add(&ReadyValue::Int(2), &ReadyValue::Int(3)).unwrap(),
        ReadyValue::Int(5)
    );
    assert_eq!(
        PythonOperations::add(
            &ReadyValue::String("hello ".to_owned()),
            &ReadyValue::String("world".to_owned())
        )
        .unwrap(),
        ReadyValue::String("hello world".to_owned())
    );
    assert_eq!(
        PythonOperations::add(
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
        PythonOperations::add(
            &ReadyValue::Float(1.25.try_into().unwrap()),
            &ReadyValue::Float(2.0.try_into().unwrap()),
        )
        .unwrap(),
        ReadyValue::Float(3.25.try_into().unwrap())
    );
    assert_eq!(
        PythonOperations::sub(
            &ReadyValue::Float(3.5.try_into().unwrap()),
            &ReadyValue::Float(1.25.try_into().unwrap()),
        )
        .unwrap(),
        ReadyValue::Float(2.25.try_into().unwrap())
    );
    assert_eq!(
        PythonOperations::mul(
            &ReadyValue::Float(3.0.try_into().unwrap()),
            &ReadyValue::Float(0.5.try_into().unwrap()),
        )
        .unwrap(),
        ReadyValue::Float(1.5.try_into().unwrap())
    );
    assert_eq!(
        PythonOperations::div(
            &ReadyValue::Float(3.0.try_into().unwrap()),
            &ReadyValue::Float(2.0.try_into().unwrap()),
        )
        .unwrap(),
        ReadyValue::Float(1.5.try_into().unwrap())
    );
    assert_eq!(
        PythonOperations::floor_div(&ReadyValue::Int(-3), &ReadyValue::Int(2)).unwrap(),
        ReadyValue::Int(-2)
    );
    assert_eq!(
        PythonOperations::floor_div(
            &ReadyValue::Float((-3.0).try_into().unwrap()),
            &ReadyValue::Float(2.0.try_into().unwrap()),
        )
        .unwrap(),
        ReadyValue::Float((-2.0).try_into().unwrap())
    );
    assert_eq!(
        PythonOperations::modulo(&ReadyValue::Int(3), &ReadyValue::Int(-2)).unwrap(),
        ReadyValue::Int(-1)
    );
    assert_eq!(
        PythonOperations::modulo(
            &ReadyValue::Float(3.5.try_into().unwrap()),
            &ReadyValue::Float(2.0.try_into().unwrap()),
        )
        .unwrap(),
        ReadyValue::Float(1.5.try_into().unwrap())
    );
    assert_eq!(
        PythonOperations::modulo(
            &ReadyValue::Float(3.5.try_into().unwrap()),
            &ReadyValue::Float((-2.0).try_into().unwrap()),
        )
        .unwrap(),
        ReadyValue::Float((-0.5).try_into().unwrap())
    );
    assert_eq!(
        PythonOperations::contains(
            &ReadyValue::String("ell".to_owned()),
            &ReadyValue::String("hello".to_owned())
        )
        .unwrap(),
        ReadyValue::Bool(true)
    );
    assert_eq!(
        PythonOperations::contains(
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
        PythonOperations::contains(
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
        PythonOperations::lt(
            &ReadyValue::Float(1.5.try_into().unwrap()),
            &ReadyValue::Float(2.0.try_into().unwrap()),
        )
        .unwrap(),
        ReadyValue::Bool(true)
    );
    assert_eq!(
        PythonOperations::neg(&ReadyValue::Int(7)).unwrap(),
        ReadyValue::Int(-7)
    );
    assert_eq!(
        PythonOperations::neg(&ReadyValue::Float(2.5.try_into().unwrap())).unwrap(),
        ReadyValue::Float((-2.5).try_into().unwrap())
    );
    assert_eq!(
        PythonOperations::not(&ReadyValue::None).unwrap(),
        ReadyValue::Bool(true)
    );
}

#[test]
fn mixed_numeric_operations_do_not_silently_promote_ints_to_floats() {
    assert!(matches!(
        PythonOperations::add(
            &ReadyValue::Float(1.25.try_into().unwrap()),
            &ReadyValue::Int(2)
        ),
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOpKind::Add,
        })
    ));
    assert!(matches!(
        PythonOperations::mul(
            &ReadyValue::Int(3),
            &ReadyValue::Float(0.5.try_into().unwrap())
        ),
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOpKind::Mul,
        })
    ));
    assert!(matches!(
        PythonOperations::div(&ReadyValue::Int(3), &ReadyValue::Int(2)),
        Err(BinaryOperationError::ResultOutOfBounds {
            operation: BinaryOpKind::Div,
        })
    ));
    assert!(matches!(
        PythonOperations::floor_div(
            &ReadyValue::Float((-3.0).try_into().unwrap()),
            &ReadyValue::Int(2)
        ),
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOpKind::FloorDiv,
        })
    ));
    assert_eq!(
        PythonOperations::eq(
            &ReadyValue::Int(1),
            &ReadyValue::Float(1.0.try_into().unwrap()),
        )
        .unwrap(),
        ReadyValue::Bool(false)
    );
    assert!(matches!(
        PythonOperations::lt(
            &ReadyValue::Float(1.5.try_into().unwrap()),
            &ReadyValue::Int(2)
        ),
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOpKind::Lt,
        })
    ));
    assert_eq!(
        PythonOperations::contains(
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
        PythonOperations::index(
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
        PythonOperations::index(&ReadyValue::String("hello".to_owned()), &ReadyValue::Int(1))
            .unwrap(),
        Value::Ready(ReadyValue::String("e".to_owned()))
    );
    assert_eq!(
        PythonOperations::index(
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
        PythonOperations::dot(
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
        PythonOperations::index(
            &ReadyValue::List(vec![Value::Ready(ReadyValue::Int(1))]),
            &ReadyValue::Int(1)
        ),
        Err(IndexOperationError::IndexOutOfBounds)
    ));
    assert!(matches!(
        PythonOperations::index(
            &ReadyValue::Dict(IndexMap::from([(
                "field".to_owned(),
                Value::Ready(ReadyValue::Int(7))
            )])),
            &ReadyValue::String("missing".to_owned())
        ),
        Err(IndexOperationError::MissingKey)
    ));
    assert!(matches!(
        PythonOperations::dot(
            &ReadyValue::Dict(IndexMap::from([(
                "field".to_owned(),
                Value::Ready(ReadyValue::Int(7))
            )])),
            "missing"
        ),
        Err(DotOperationError::MissingAttribute)
    ));
    assert!(matches!(
        PythonOperations::dot(&ReadyValue::List(Vec::new()), "field"),
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
        PythonOperations::and(&ReadyValue::Int(1), &ReadyValue::String("x".to_owned())).unwrap(),
        ReadyValue::String("x".to_owned())
    );
    assert_eq!(
        PythonOperations::or(
            &ReadyValue::None,
            &ReadyValue::String("fallback".to_owned())
        )
        .unwrap(),
        ReadyValue::String("fallback".to_owned())
    );
    assert_eq!(
        <PythonOperations as MakeList<ReadyValue>>::make_list([
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
        <PythonOperations as MakeDict<ReadyValue>>::make_dict([
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
        PythonOperations::as_dict_key(&ReadyValue::Int(3)),
        Err(AsDictKeyError::UnsupportedKeyType)
    ));
    assert!(matches!(
        PythonOperations::as_dict_key(&ReadyValue::List(vec![Value::Ready(ReadyValue::String(
            "nested".to_owned()
        ))])),
        Err(AsDictKeyError::UnsupportedKeyType)
    ));
    let value: Result<NonNaNFinite, _> = f64::NAN.try_into();
    assert!(value.is_err());
}

#[test]
fn length_operations_follow_runtime_semantics() {
    assert_eq!(
        PythonOperations::length(&ReadyValue::List(vec![
            Value::Ready(ReadyValue::Int(1)),
            Value::Ready(ReadyValue::Int(2))
        ]))
        .unwrap(),
        2
    );
    assert_eq!(
        PythonOperations::length(&ReadyValue::String("hello".to_owned())).unwrap(),
        5
    );
    assert_eq!(
        PythonOperations::length(&ReadyValue::Dict(IndexMap::from([(
            "key".to_owned(),
            Value::Ready(ReadyValue::Int(1))
        )])))
        .unwrap(),
        1
    );
    assert_eq!(
        <PythonOperations as Length<ReadyValue>>::from_length(3).unwrap(),
        ReadyValue::Int(3)
    );
    assert!(matches!(
        PythonOperations::length(&ReadyValue::Bool(false)),
        Err(LengthError::UnsupportedValue)
    ));

    let too_large = usize::try_from(i64::MAX as u128 + 1).ok();
    if let Some(too_large) = too_large {
        assert!(matches!(
            <PythonOperations as Length<ReadyValue>>::from_length(too_large),
            Err(FromLengthError::ResultOutOfBounds)
        ));
    }
}

#[test]
fn exception_operations_follow_runtime_semantics() {
    assert_eq!(
        <PythonOperations as MakeException<ReadyValue>>::make_exception(
            "ValueError".to_owned(),
            Value::Ready(ReadyValue::Int(41))
        ),
        ReadyValue::Exception(Box::new(Exception {
            type_id: "ValueError".to_owned(),
            details: Value::Ready(ReadyValue::Int(41)),
        }))
    );
    assert_eq!(
        PythonOperations::as_exception_type_id(&ReadyValue::String("ValueError".to_owned()))
            .unwrap(),
        "ValueError"
    );
    assert!(matches!(
        PythonOperations::as_exception_type_id(&ReadyValue::Int(3)),
        Err(AsExceptionTypeIdError::UnsupportedTypeIdType)
    ));
}

#[test]
fn float_operations_follow_non_nan_finite_semantics() {
    assert!(matches!(
        PythonOperations::div(
            &ReadyValue::Float(0.0.try_into().unwrap()),
            &ReadyValue::Float(0.0.try_into().unwrap()),
        ),
        Err(BinaryOperationError::ResultOutOfBounds {
            operation: BinaryOpKind::Div,
        })
    ));
    assert!(matches!(
        PythonOperations::modulo(
            &ReadyValue::Float(1.0.try_into().unwrap()),
            &ReadyValue::Float(0.0.try_into().unwrap()),
        ),
        Err(BinaryOperationError::ResultOutOfBounds {
            operation: BinaryOpKind::Mod,
        })
    ));
    assert!(matches!(
        PythonOperations::mul(
            &ReadyValue::Float(f64::MAX.try_into().unwrap()),
            &ReadyValue::Float(2.0.try_into().unwrap()),
        ),
        Err(BinaryOperationError::ResultOutOfBounds {
            operation: BinaryOpKind::Mul,
        })
    ));
    assert!(matches!(
        PythonOperations::div(
            &ReadyValue::Float(1.0.try_into().unwrap()),
            &ReadyValue::Float(0.0.try_into().unwrap()),
        ),
        Err(BinaryOperationError::ResultOutOfBounds {
            operation: BinaryOpKind::Div,
        })
    ));
    assert!(matches!(
        PythonOperations::floor_div(
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
