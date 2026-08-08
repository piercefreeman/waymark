/// Load a const value into the interpreter.
pub trait LoadConst<Value, ConstValue> {
    /// Convert an instruction-set constant into the runtime value type.
    fn load_const(const_value: ConstValue) -> Value;
}
