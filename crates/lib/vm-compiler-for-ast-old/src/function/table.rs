//! Function table construction used by the compiler.

use std::collections::HashMap;

use waymark_vm_ast_old::Program;
use waymark_vm_bytecode_core::FunctionId;

/// Errors produced while building the program-wide function table.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Two functions shared the same name.
    #[error("function `{name}` is defined more than once")]
    DuplicateFunction {
        /// The duplicate function name.
        name: String,
    },
}

#[derive(Clone, Copy)]
pub(crate) struct KnownFunction {
    pub id: FunctionId,
    pub arity: usize,
}

pub(crate) struct FunctionTable {
    by_name: HashMap<String, KnownFunction>,
}

impl FunctionTable {
    pub fn build(program: &Program) -> Result<Self, Error> {
        let mut by_name = HashMap::with_capacity(program.functions.len());

        for (index, function) in program.functions.iter().enumerate() {
            let name = function.value.name.clone();
            let known = KnownFunction {
                id: FunctionId(index),
                arity: function.value.io.value.inputs.len(),
            };

            if by_name.insert(name.clone(), known).is_some() {
                return Err(Error::DuplicateFunction { name });
            }
        }

        Ok(Self { by_name })
    }

    pub fn get(&self, name: &str) -> Option<KnownFunction> {
        self.by_name.get(name).copied()
    }
}
