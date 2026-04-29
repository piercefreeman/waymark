use waymark_proto::ast;
use waymark_vm_ast_old::{self as vm_ast, Spanned};

pub type Result<T> = std::result::Result<T, ConvertError>;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ConvertError {
    #[error("waymark_proto::ast missing required field `{field}`")]
    MissingField { field: &'static str },
    #[error("waymark_proto::ast contains invalid value `{value}` for enum `{enum_name}`")]
    InvalidEnumValue { enum_name: &'static str, value: i32 },
    #[error("waymark_proto::ast contains unspecified value for required enum `{enum_name}`")]
    UnspecifiedEnumValue { enum_name: &'static str },
}

pub trait Convert<From> {
    type To;

    fn convert(from: From) -> Result<Self::To>;
}

pub enum Converter {}

pub trait IntoOwned<T> {
    fn into_owned(self) -> T;
}

impl<T> IntoOwned<T> for T {
    fn into_owned(self) -> T {
        self
    }
}

impl<T> IntoOwned<T> for Box<T> {
    fn into_owned(self) -> T {
        *self
    }
}

pub fn convert<T>(from: T) -> Result<<Converter as Convert<T>>::To>
where
    Converter: Convert<T>,
{
    <Converter as Convert<T>>::convert(from)
}

pub fn required<T>(value: Option<T>, context: &'static str) -> Result<T> {
    value.ok_or(ConvertError::MissingField { field: context })
}

pub fn required_owned<T, U>(value: Option<U>, context: &'static str) -> Result<T>
where
    U: IntoOwned<T>,
{
    Ok(required(value, context)?.into_owned())
}

pub fn convert_required<T>(
    value: Option<T>,
    context: &'static str,
) -> Result<<Converter as Convert<T>>::To>
where
    Converter: Convert<T>,
{
    convert(required(value, context)?)
}

pub fn convert_required_owned<T, U>(
    value: Option<U>,
    context: &'static str,
) -> Result<<Converter as Convert<T>>::To>
where
    U: IntoOwned<T>,
    Converter: Convert<T>,
{
    convert(required_owned(value, context)?)
}

pub fn convert_optional_owned<T, U>(
    value: Option<U>,
) -> Result<Option<<Converter as Convert<T>>::To>>
where
    U: IntoOwned<T>,
    Converter: Convert<T>,
{
    value.map(|value| convert(value.into_owned())).transpose()
}

pub fn default_span() -> vm_ast::Span {
    vm_ast::Span {
        start_line: 0,
        start_col: 0,
        end_line: 0,
        end_col: 0,
    }
}

pub fn spanned<T>(value: T, span: Option<ast::Span>) -> Result<Spanned<T>> {
    let span = span.map(convert).transpose()?.unwrap_or_else(default_span);
    Ok(Spanned { value, span })
}

pub fn parse_enum<E>(value: i32, context: &'static str) -> Result<E>
where
    E: TryFrom<i32>,
{
    E::try_from(value).map_err(|_| ConvertError::InvalidEnumValue {
        enum_name: context,
        value,
    })
}

pub fn convert_binary_operator(value: i32) -> Result<vm_ast::BinaryOperator> {
    convert(parse_enum::<ast::BinaryOperator>(value, "BinaryOperator")?)
}

pub fn convert_unary_operator(value: i32) -> Result<vm_ast::UnaryOperator> {
    convert(parse_enum::<ast::UnaryOperator>(value, "UnaryOperator")?)
}

pub fn convert_global_function(value: i32) -> Result<Option<vm_ast::GlobalFunction>> {
    match parse_enum::<ast::GlobalFunction>(value, "GlobalFunction")? {
        ast::GlobalFunction::Unspecified => Ok(None),
        global_function => Ok(Some(convert(global_function)?)),
    }
}
