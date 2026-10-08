/// Turn a bytecode-embedded exception into the raised one.
///
/// The counterpart of loading a const value into a runtime value: the
/// instruction set carries the compiler's const form, the frame raises
/// the runtime form this loads.
pub trait LoadConstException<ConstException>: Sized {
    /// Convert an instruction-set constant into the raised exception type.
    fn load_const_exception(const_exception: ConstException) -> Self;
}

impl<T, ConstException> LoadConstException<ConstException> for T
where
    T: From<ConstException>,
{
    fn load_const_exception(const_exception: ConstException) -> Self {
        const_exception.into()
    }
}
