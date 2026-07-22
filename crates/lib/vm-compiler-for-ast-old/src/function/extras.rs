//! Program-wide collection of extra functions.

use waymark_vm_bytecode_core::FunctionId;
use waymark_vm_compiler_for_ast_old_core::InstructionFor;

/// The extra functions introduced during a program compilation.
///
/// Extra functions have no source-level definition of their own - the
/// compiler introduces them while lowering (e.g. the per-call-site policy
/// wrappers). Their ids continue right after the source function ids, so
/// they must be appended to the executable in generation order directly
/// after the source functions.
pub struct ExtraFunctions<Spec>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
{
    /// The function id to assign to the next extra function.
    next_function_id: usize,

    /// The compiled extra functions in generation order.
    functions: Vec<waymark_vm_bytecode::Function<InstructionFor<Spec>>>,
}

impl<Spec> ExtraFunctions<Spec>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
{
    /// Creates an empty collection for a program with the given number of
    /// source functions.
    pub fn new(source_function_count: usize) -> Self {
        Self {
            next_function_id: source_function_count,
            functions: Vec::new(),
        }
    }

    /// Adds one compiled extra function and returns its assigned id.
    pub fn insert(
        &mut self,
        function: waymark_vm_bytecode::Function<InstructionFor<Spec>>,
    ) -> FunctionId {
        let function_id = FunctionId(self.next_function_id);
        self.next_function_id += 1;
        self.functions.push(function);
        function_id
    }

    /// Consumes the collection, returning the extra functions to append to
    /// the executable after the source functions.
    pub fn finish(self) -> Vec<waymark_vm_bytecode::Function<InstructionFor<Spec>>> {
        self.functions
    }
}
