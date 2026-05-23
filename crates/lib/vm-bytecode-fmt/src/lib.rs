mod display;
mod r#impl;

pub struct Fmt<'a, T>(pub &'a T);

pub fn display<'a, T>(value: &'a T) -> Fmt<'a, T>
where
    Fmt<'a, T>: core::fmt::Display,
{
    Fmt(value)
}
