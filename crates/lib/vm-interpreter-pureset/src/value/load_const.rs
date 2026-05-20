/// Load a const value into the interpreter.
pub trait LoadConst<ConstValue>: Sized {
    /// Convert an instruction-set constant into the runtime value type.
    fn load_const(const_value: ConstValue) -> Self;
}
