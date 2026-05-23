//! Pretty-printer for `vm-ast-old` AST structures.

mod display;
mod r#impl;

pub struct Fmt<'a, T: ?Sized>(pub &'a T);

pub fn display<'a, T: ?Sized>(value: &'a T) -> Fmt<'a, T>
where
    Fmt<'a, T>: core::fmt::Display,
{
    Fmt(value)
}
