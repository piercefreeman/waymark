//! Lowering helper: converts an AST [`waymark_vm_ast_old::ActionCall`] into
//! an [`waymark_action_core::ActionRef`].
//!
//! Policy brackets are not part of the action reference: they lower into
//! bytecode at the call site instead.

#![warn(missing_docs)]

use waymark_action_core::ActionRef;
use waymark_vm_ast_old::ActionCall;

/// Lower an AST action call into an [`ActionRef`].
pub fn lower_action_ref(call: &ActionCall) -> ActionRef {
    ActionRef {
        action_name: call.action_name.clone(),
        module_name: call.module_name.clone(),
        call_args: call.kwargs.iter().map(|k| k.name.clone()).collect(),
    }
}
