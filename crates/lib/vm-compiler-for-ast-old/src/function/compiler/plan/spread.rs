//! Spread planning.

use waymark_vm_ast_old::{ActionCall, Expr, Spanned};

/// A validated spread over a collection of action calls.
#[derive(Debug, Clone, Copy)]
pub struct SpreadPlan<'a> {
    /// Collection expression that drives the spread.
    collection: &'a Spanned<Expr>,

    /// Source loop variable name referenced by the action kwargs.
    loop_var: &'a str,

    /// Action call to execute for each collection item.
    action: &'a ActionCall,
}

impl<'a> SpreadPlan<'a> {
    /// Builds a spread plan from the parsed AST fields.
    pub fn build(collection: &'a Spanned<Expr>, loop_var: &'a str, action: &'a ActionCall) -> Self {
        Self {
            collection,
            loop_var,
            action,
        }
    }

    /// Returns the stored spread parts.
    pub fn into_parts(self) -> (&'a Spanned<Expr>, &'a str, &'a ActionCall) {
        (self.collection, self.loop_var, self.action)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use waymark_vm_ast_old_helpers::{action_call, variable};

    #[test]
    fn spread_plan_preserves_collection_loop_var_and_action() {
        let collection = variable("items");
        let action = action_call("notify", vec![("value", variable("item"))]);

        let spread = SpreadPlan::build(&collection, "item", &action);
        let (planned_collection, loop_var, planned_action) = spread.into_parts();

        assert!(
            matches!(planned_collection.value, waymark_vm_ast_old::Expr::Variable { ref name } if name == "items")
        );
        assert_eq!(loop_var, "item");
        assert_eq!(planned_action.action_name, "notify");
    }
}
