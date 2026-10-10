/// Turn a handler pattern as the bytecode lists it into the runtime one.
///
/// A pushed handler block carries the compiler's const patterns; the
/// frame holds the runtime patterns the raised exception matches against,
/// which this loads.
pub trait LoadConstExceptionPattern<ConstExceptionPattern>: Sized {
    /// Convert an instruction-set constant into the runtime pattern type.
    fn load_const_exception_pattern(const_pattern: ConstExceptionPattern) -> Self;
}

impl<T, ConstExceptionPattern> LoadConstExceptionPattern<ConstExceptionPattern> for T
where
    T: From<ConstExceptionPattern>,
{
    fn load_const_exception_pattern(const_pattern: ConstExceptionPattern) -> Self {
        const_pattern.into()
    }
}
