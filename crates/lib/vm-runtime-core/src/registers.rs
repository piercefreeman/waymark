use index_type::{IndexType, typed_vec::TypedVec};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, IndexType)]
pub struct RegisterId(pub usize);

pub struct Registers<Value>(TypedVec<RegisterId, Option<Value>>);

impl<Value> std::ops::Index<RegisterId> for Registers<Value> {
    type Output = Value;

    fn index(&self, index: RegisterId) -> &Self::Output {
        self.0[index].as_ref().unwrap()
    }
}

impl<Value> std::ops::IndexMut<RegisterId> for Registers<Value> {
    fn index_mut(&mut self, index: RegisterId) -> &mut Self::Output {
        self.0[index].as_mut().unwrap()
    }
}

impl<Value> Registers<Value> {
    pub fn new_for_fn_call(size: usize, args: impl IntoIterator<Item = Value>) -> Self {
        let mut regs = TypedVec::<RegisterId, _>::with_capacity(size);

        regs.extend(args.into_iter().map(Some));

        while regs.len().to_scalar() < size {
            regs.push(None);
        }

        Self(regs)
    }
}
