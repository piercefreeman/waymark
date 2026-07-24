//! Lowering helper: converts an AST [`waymark_vm_ast_old::ActionCall`] into
//! an [`waymark_action_core::ActionRef`], extracting policy-derived fields.

#![warn(missing_docs)]

use waymark_action_core::ActionRef;
use waymark_vm_ast_old::ActionCall;

/// Lower an AST action call into an [`ActionRef`], computing timeout, retry
/// limits, and exception-type filters from the action's policies.
pub fn lower_action_ref(call: &ActionCall) -> ActionRef {
    let timeout_seconds = call
        .policies
        .iter()
        .filter_map(|p| match p {
            waymark_vm_ast_old::PolicyBracket::Timeout(t) => {
                let s = t.timeout.seconds;
                if s > 0 { Some(s) } else { None }
            }
            _ => None,
        })
        .min()
        .map(|s| s.min(u64::from(u32::MAX)) as u32)
        .unwrap_or(300);

    let max_retries = call
        .policies
        .iter()
        .filter_map(|p| match p {
            waymark_vm_ast_old::PolicyBracket::Retry(r) => Some(r.max_retries),
            _ => None,
        })
        .max()
        .unwrap_or(0);

    let mut exception_types: Vec<String> = Vec::new();
    for p in &call.policies {
        if let waymark_vm_ast_old::PolicyBracket::Retry(r) = p {
            for ty in &r.exception_types {
                if !exception_types.contains(ty) {
                    exception_types.push(ty.clone());
                }
            }
        }
    }

    ActionRef {
        runtime: call.runtime,
        action_name: call.action_name.clone(),
        module_name: call.module_name.clone(),
        call_args: call.kwargs.iter().map(|k| k.name.clone()).collect(),
        timeout_seconds,
        max_retries,
        exception_types,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_javascript_runtime() {
        let call = ActionCall {
            runtime: waymark_action_core::ActionRuntime::JavaScript,
            action_name: "send_email".to_owned(),
            kwargs: Vec::new(),
            policies: Vec::new(),
            module_name: Some("src/actions/email.ts".to_owned()),
        };

        let action_ref = lower_action_ref(&call);

        assert_eq!(
            action_ref.runtime,
            waymark_action_core::ActionRuntime::JavaScript
        );
    }
}
