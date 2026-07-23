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

/// Resolved metadata for a known function.
#[derive(Clone)]
pub(crate) struct KnownFunction {
    /// Function id assigned from source order.
    pub id: FunctionId,

    /// Declared input names in positional order.
    pub inputs: Vec<String>,
}

/// Lookup table from function names to their resolved metadata.
pub(crate) struct FunctionTable {
    /// Function metadata keyed by source name.
    by_name: HashMap<String, KnownFunction>,
}

impl FunctionTable {
    /// Builds the function table for a whole program.
    pub fn build(program: &Program) -> Result<Self, Error> {
        let mut by_name = HashMap::with_capacity(program.functions.len());

        for (index, function) in program.functions.iter().enumerate() {
            let name = function.value.name.clone();
            let known = KnownFunction {
                id: FunctionId(index),
                inputs: function.value.io.value.inputs.clone(),
            };

            if by_name.insert(name.clone(), known).is_some() {
                return Err(Error::DuplicateFunction { name });
            }
        }

        Ok(Self { by_name })
    }

    /// Looks up a function by name.
    pub fn get(&self, name: &str) -> Option<&KnownFunction> {
        self.by_name.get(name)
    }
}

#[cfg(test)]
mod tests {
    use super::{Error, FunctionTable};
    use waymark_vm_ast_old_helpers::{function, program};
    use waymark_vm_bytecode_core::FunctionId;

    #[test]
    fn builds_function_ids_and_arities_from_source_order() {
        let program = program(vec![
            function("main", &[], vec![]),
            function("child", &["left", "right"], vec![]),
        ]);

        let table = FunctionTable::build(&program).expect("function table should build");

        let main = table.get("main").expect("main function should exist");
        let child = table.get("child").expect("child function should exist");

        assert_eq!(main.id, FunctionId(0));
        assert_eq!(main.inputs, Vec::<String>::new());
        assert_eq!(child.id, FunctionId(1));
        assert_eq!(child.inputs, vec!["left".to_owned(), "right".to_owned()]);
        assert!(table.get("missing").is_none());
    }

    #[test]
    fn rejects_duplicate_function_names() {
        let program = program(vec![
            function("main", &[], vec![]),
            function("main", &["value"], vec![]),
        ]);

        let error = match FunctionTable::build(&program) {
            Ok(_) => panic!("duplicate function names should be rejected"),
            Err(error) => error,
        };

        assert!(matches!(error, Error::DuplicateFunction { name } if name == "main"));
    }
}
