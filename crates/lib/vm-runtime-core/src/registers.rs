use index_type::{IndexTooBigError, IndexType, typed_vec::TypedVec};

/// Error returned when a raw index cannot be represented as a [`RegisterId`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, IndexTooBigError)]
#[index_too_big_error(msg = "register id")]
pub struct RegisterIdTooBigError;

/// Index of a register in the [`Registers`] type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, IndexType)]
#[index_type(error = RegisterIdTooBigError)]
pub struct RegisterId(pub usize);

/// A list of registers.
///
/// Used to store values per frame.
#[derive(Debug)]
pub struct Registers<Value>(TypedVec<RegisterId, Option<Value>>);

impl<Value> std::ops::Index<RegisterId> for Registers<Value> {
    type Output = Value;

    fn index(&self, index: RegisterId) -> &Self::Output {
        self.0[index].as_ref().unwrap()
    }
}

impl<Value> Registers<Value> {
    /// New [`Registers`] for an function call.
    ///
    /// Takes the `size` as the amount of the registers to allocate, and
    /// the `args` to fill in the values into the corresponding registers.
    pub fn new_for_fn_call(size: usize, args: impl IntoIterator<Item = Value>) -> Self {
        let mut regs = TypedVec::<RegisterId, _>::with_capacity(size);

        regs.extend(args.into_iter().map(Some));

        while regs.len().to_scalar() < size {
            regs.push(None);
        }

        Self(regs)
    }

    /// New [`Registers`].
    ///
    /// Takes the `size` as the amount of the registers to allocate.
    pub fn new(size: usize) -> Self {
        let mut regs = TypedVec::<RegisterId, _>::with_capacity(size);

        while regs.len().to_scalar() < size {
            regs.push(None);
        }

        Self(regs)
    }

    /// Set the given register to the specified value.
    ///
    /// Panics if the register is out of bounds.
    pub fn set(&mut self, index: RegisterId, value: Value) {
        self.0[index] = Some(value);
    }

    /// Get a value at the given register.
    ///
    /// Returns `None` if the register is empty.
    ///
    /// Panics if the register is out of bounds.
    pub fn get(&self, index: RegisterId) -> Option<&Value> {
        self.0[index].as_ref()
    }

    /// Take a value at the given register and unset it.
    ///
    /// Returns `None` if the register is empty.
    ///
    /// Panics if the register is out of bounds.
    pub fn take(&mut self, index: RegisterId) -> Option<Value> {
        self.0[index].take()
    }
}

#[cfg(test)]
mod tests {
    use super::{RegisterId, Registers};

    #[test]
    fn new_for_fn_call_populates_args_and_pads_remaining_registers() {
        let regs = Registers::new_for_fn_call(3, [10, 20]);

        assert_eq!(regs.get(RegisterId(0)), Some(&10));
        assert_eq!(regs.get(RegisterId(1)), Some(&20));
        assert_eq!(regs.get(RegisterId(2)), None);
    }

    #[test]
    fn set_and_take_manage_register_contents() {
        let mut regs = Registers::new(2);

        assert_eq!(regs.get(RegisterId(1)), None);

        regs.set(RegisterId(1), 99);

        assert_eq!(regs.get(RegisterId(1)), Some(&99));
        assert_eq!(*regs.get(RegisterId(1)).expect("register is set"), 99);

        assert_eq!(regs.take(RegisterId(1)), Some(99));
        assert_eq!(regs.get(RegisterId(1)), None);
    }
}
