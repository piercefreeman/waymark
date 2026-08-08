/// View a value as a scalar that supports unary and binary operations.
pub trait AsScalarValue<Value> {
    /// The scalar value type used for arithmetic, comparison, and logical operators.
    type ScalarValue;

    /// The implementation-specific error returned when the value is not
    /// a scalar.
    type Error: core::fmt::Debug;

    /// Borrow the scalar view of this value.
    fn as_scalar_value(value: &Value) -> Result<&Self::ScalarValue, Self::Error>;

    /// Rewrap a scalar result back into the enclosing value type.
    fn from_scalar_value(scalar: Self::ScalarValue) -> Value;
}

/// Apply binary operations to scalar values.
///
/// Every operation is required: an implementation states what each
/// operator means for its scalars — including that an operator is
/// unsupported.
pub trait BinaryOps<ScalarValue> {
    /// The implementation-specific error returned when an operation fails.
    type Error: core::fmt::Debug;

    /// Add two values together.
    fn add(a: &ScalarValue, b: &ScalarValue) -> Result<ScalarValue, Self::Error>;

    /// Subtract the right value from the left value.
    fn sub(a: &ScalarValue, b: &ScalarValue) -> Result<ScalarValue, Self::Error>;

    /// Multiply two values together.
    fn mul(a: &ScalarValue, b: &ScalarValue) -> Result<ScalarValue, Self::Error>;

    /// Divide the left value by the right value.
    fn div(a: &ScalarValue, b: &ScalarValue) -> Result<ScalarValue, Self::Error>;

    /// Floor-divide the left value by the right value.
    fn floor_div(a: &ScalarValue, b: &ScalarValue) -> Result<ScalarValue, Self::Error>;

    /// Compute the left value modulo the right value.
    fn modulo(a: &ScalarValue, b: &ScalarValue) -> Result<ScalarValue, Self::Error>;

    /// Compare two values for equality.
    fn eq(a: &ScalarValue, b: &ScalarValue) -> Result<ScalarValue, Self::Error>;

    /// Compare two values for inequality.
    fn ne(a: &ScalarValue, b: &ScalarValue) -> Result<ScalarValue, Self::Error>;

    /// Compare whether the left value is less than the right value.
    fn lt(a: &ScalarValue, b: &ScalarValue) -> Result<ScalarValue, Self::Error>;

    /// Compare whether the left value is less than or equal to the right value.
    fn le(a: &ScalarValue, b: &ScalarValue) -> Result<ScalarValue, Self::Error>;

    /// Compare whether the left value is greater than the right value.
    fn gt(a: &ScalarValue, b: &ScalarValue) -> Result<ScalarValue, Self::Error>;

    /// Compare whether the left value is greater than or equal to the right value.
    fn ge(a: &ScalarValue, b: &ScalarValue) -> Result<ScalarValue, Self::Error>;

    /// Test whether the left value is contained in the right value.
    fn contains(a: &ScalarValue, b: &ScalarValue) -> Result<ScalarValue, Self::Error>;

    /// Test whether the left value is not contained in the right value.
    fn not_contains(a: &ScalarValue, b: &ScalarValue) -> Result<ScalarValue, Self::Error>;

    /// Apply logical `and` to two values.
    fn and(a: &ScalarValue, b: &ScalarValue) -> Result<ScalarValue, Self::Error>;

    /// Apply logical `or` to two values.
    fn or(a: &ScalarValue, b: &ScalarValue) -> Result<ScalarValue, Self::Error>;
}

/// Apply unary operations to scalar values.
///
/// Every operation is required: see [`BinaryOps`].
pub trait UnaryOps<ScalarValue> {
    /// The implementation-specific error returned when an operation fails.
    type Error: core::fmt::Debug;

    /// Negate the operand value.
    fn neg(value: &ScalarValue) -> Result<ScalarValue, Self::Error>;

    /// Apply logical `not` to the operand value.
    fn not(value: &ScalarValue) -> Result<ScalarValue, Self::Error>;
}

/// The scalar value type [`AsScalarValue`] views `Value` as.
pub type ScalarValueFor<Operations, Value> = <Operations as AsScalarValue<Value>>::ScalarValue;

/// The error [`AsScalarValue`] returns for `Value`.
pub type AsScalarValueErrorFor<Operations, Value> = <Operations as AsScalarValue<Value>>::Error;

/// The error [`BinaryOps`] returns for the scalars of `Value`.
pub type BinaryOpsErrorFor<Operations, Value> =
    <Operations as BinaryOps<ScalarValueFor<Operations, Value>>>::Error;

/// The error [`UnaryOps`] returns for the scalars of `Value`.
pub type UnaryOpsErrorFor<Operations, Value> =
    <Operations as UnaryOps<ScalarValueFor<Operations, Value>>>::Error;
