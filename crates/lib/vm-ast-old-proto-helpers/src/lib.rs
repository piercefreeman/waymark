//! Traversal and runtime helpers for protobuf workflow ASTs.

#![warn(missing_docs)]

use waymark_proto::ast;

/// Error returned when an action call does not declare a supported runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ActionRuntimeError {
    /// The protobuf enum value is not recognized.
    #[error("invalid action runtime value {0}")]
    Invalid(i32),

    /// The action call omitted its runtime.
    #[error("action runtime is required")]
    Unspecified,
}

/// Return every action call in a program, including nested expressions.
pub fn action_calls(program: &ast::Program) -> Vec<&ast::ActionCall> {
    let mut actions = Vec::new();
    for function in &program.functions {
        if let Some(body) = &function.body {
            collect_block_actions(body, &mut actions);
        }
    }
    actions
}

/// Return the distinct runtimes required by a program, in encounter order.
pub fn required_action_runtimes(
    program: &ast::Program,
) -> Result<Vec<waymark_action_core::ActionRuntime>, ActionRuntimeError> {
    let mut runtimes = Vec::new();
    for action in action_calls(program) {
        let runtime = decode_runtime(action.runtime)?;
        if !runtimes.contains(&runtime) {
            runtimes.push(runtime);
        }
    }
    Ok(runtimes)
}

/// Set the runtime for every action call in a program.
pub fn set_action_runtime(program: &mut ast::Program, runtime: waymark_action_core::ActionRuntime) {
    for function in &mut program.functions {
        if let Some(body) = &mut function.body {
            set_block_action_runtime(body, runtime);
        }
    }
}

/// Set the runtime for every action call in an expression.
pub fn set_expression_action_runtime(
    expression: &mut ast::Expr,
    runtime: waymark_action_core::ActionRuntime,
) {
    set_expr_action_runtime(expression, runtime);
}

fn decode_runtime(value: i32) -> Result<waymark_action_core::ActionRuntime, ActionRuntimeError> {
    match waymark_proto::action::ActionRuntime::try_from(value)
        .map_err(|_| ActionRuntimeError::Invalid(value))?
    {
        waymark_proto::action::ActionRuntime::Python => {
            Ok(waymark_action_core::ActionRuntime::Python)
        }
        waymark_proto::action::ActionRuntime::Javascript => {
            Ok(waymark_action_core::ActionRuntime::JavaScript)
        }
        waymark_proto::action::ActionRuntime::Unspecified => Err(ActionRuntimeError::Unspecified),
    }
}

fn encoded_runtime(runtime: waymark_action_core::ActionRuntime) -> i32 {
    match runtime {
        waymark_action_core::ActionRuntime::Python => {
            waymark_proto::action::ActionRuntime::Python as i32
        }
        waymark_action_core::ActionRuntime::JavaScript => {
            waymark_proto::action::ActionRuntime::Javascript as i32
        }
    }
}

fn collect_block_actions<'a>(block: &'a ast::Block, actions: &mut Vec<&'a ast::ActionCall>) {
    for statement in &block.statements {
        let Some(kind) = &statement.kind else {
            continue;
        };
        match kind {
            ast::statement::Kind::Assignment(assignment) => {
                if let Some(value) = &assignment.value {
                    collect_expr_actions(value, actions);
                }
            }
            ast::statement::Kind::ActionCall(action) => {
                collect_action_actions(action, actions);
            }
            ast::statement::Kind::SpreadAction(spread) => {
                if let Some(collection) = &spread.collection {
                    collect_expr_actions(collection, actions);
                }
                if let Some(action) = &spread.action {
                    collect_action_actions(action, actions);
                }
            }
            ast::statement::Kind::ParallelBlock(parallel) => {
                for call in &parallel.calls {
                    collect_call_actions(call, actions);
                }
            }
            ast::statement::Kind::ForLoop(for_loop) => {
                if let Some(iterable) = &for_loop.iterable {
                    collect_expr_actions(iterable, actions);
                }
                if let Some(body) = &for_loop.block_body {
                    collect_block_actions(body, actions);
                }
            }
            ast::statement::Kind::Conditional(conditional) => {
                if let Some(branch) = &conditional.if_branch {
                    collect_optional_expr_actions(&branch.condition, actions);
                    collect_optional_block_actions(&branch.block_body, actions);
                }
                for branch in &conditional.elif_branches {
                    collect_optional_expr_actions(&branch.condition, actions);
                    collect_optional_block_actions(&branch.block_body, actions);
                }
                if let Some(branch) = &conditional.else_branch {
                    collect_optional_block_actions(&branch.block_body, actions);
                }
            }
            ast::statement::Kind::TryExcept(try_except) => {
                collect_optional_block_actions(&try_except.try_block, actions);
                for handler in &try_except.handlers {
                    collect_optional_block_actions(&handler.block_body, actions);
                }
            }
            ast::statement::Kind::ReturnStmt(return_statement) => {
                collect_optional_expr_actions(&return_statement.value, actions);
            }
            ast::statement::Kind::ExprStmt(expression_statement) => {
                collect_optional_expr_actions(&expression_statement.expr, actions);
            }
            ast::statement::Kind::WhileLoop(while_loop) => {
                collect_optional_expr_actions(&while_loop.condition, actions);
                collect_optional_block_actions(&while_loop.block_body, actions);
            }
            ast::statement::Kind::SleepStmt(sleep) => {
                collect_optional_expr_actions(&sleep.duration, actions);
            }
            ast::statement::Kind::BreakStmt(_) | ast::statement::Kind::ContinueStmt(_) => {}
        }
    }
}

fn collect_optional_block_actions<'a>(
    block: &'a Option<ast::Block>,
    actions: &mut Vec<&'a ast::ActionCall>,
) {
    if let Some(block) = block {
        collect_block_actions(block, actions);
    }
}

fn collect_optional_expr_actions<'a>(
    expression: &'a Option<ast::Expr>,
    actions: &mut Vec<&'a ast::ActionCall>,
) {
    if let Some(expression) = expression {
        collect_expr_actions(expression, actions);
    }
}

fn collect_action_actions<'a>(action: &'a ast::ActionCall, actions: &mut Vec<&'a ast::ActionCall>) {
    actions.push(action);
    for kwarg in &action.kwargs {
        collect_optional_expr_actions(&kwarg.value, actions);
    }
}

fn collect_call_actions<'a>(call: &'a ast::Call, actions: &mut Vec<&'a ast::ActionCall>) {
    match &call.kind {
        Some(ast::call::Kind::Action(action)) => collect_action_actions(action, actions),
        Some(ast::call::Kind::Function(function)) => {
            for argument in &function.args {
                collect_expr_actions(argument, actions);
            }
            for kwarg in &function.kwargs {
                collect_optional_expr_actions(&kwarg.value, actions);
            }
        }
        None => {}
    }
}

fn collect_expr_actions<'a>(expression: &'a ast::Expr, actions: &mut Vec<&'a ast::ActionCall>) {
    let Some(kind) = &expression.kind else {
        return;
    };
    match kind {
        ast::expr::Kind::BinaryOp(binary) => {
            if let Some(left) = &binary.left {
                collect_expr_actions(left, actions);
            }
            if let Some(right) = &binary.right {
                collect_expr_actions(right, actions);
            }
        }
        ast::expr::Kind::UnaryOp(unary) => {
            if let Some(operand) = &unary.operand {
                collect_expr_actions(operand, actions);
            }
        }
        ast::expr::Kind::List(list) => {
            for element in &list.elements {
                collect_expr_actions(element, actions);
            }
        }
        ast::expr::Kind::Dict(dict) => {
            for entry in &dict.entries {
                collect_optional_expr_actions(&entry.key, actions);
                collect_optional_expr_actions(&entry.value, actions);
            }
        }
        ast::expr::Kind::Index(index) => {
            if let Some(object) = &index.object {
                collect_expr_actions(object, actions);
            }
            if let Some(index) = &index.index {
                collect_expr_actions(index, actions);
            }
        }
        ast::expr::Kind::Dot(dot) => {
            if let Some(object) = &dot.object {
                collect_expr_actions(object, actions);
            }
        }
        ast::expr::Kind::FunctionCall(function) => {
            for argument in &function.args {
                collect_expr_actions(argument, actions);
            }
            for kwarg in &function.kwargs {
                collect_optional_expr_actions(&kwarg.value, actions);
            }
        }
        ast::expr::Kind::ActionCall(action) => collect_action_actions(action, actions),
        ast::expr::Kind::ParallelExpr(parallel) => {
            for call in &parallel.calls {
                collect_call_actions(call, actions);
            }
        }
        ast::expr::Kind::SpreadExpr(spread) => {
            if let Some(collection) = &spread.collection {
                collect_expr_actions(collection, actions);
            }
            if let Some(action) = &spread.action {
                collect_action_actions(action, actions);
            }
        }
        ast::expr::Kind::Literal(_) | ast::expr::Kind::Variable(_) => {}
    }
}

fn set_block_action_runtime(block: &mut ast::Block, runtime: waymark_action_core::ActionRuntime) {
    for statement in &mut block.statements {
        let Some(kind) = &mut statement.kind else {
            continue;
        };
        match kind {
            ast::statement::Kind::Assignment(assignment) => {
                if let Some(value) = &mut assignment.value {
                    set_expr_action_runtime(value, runtime);
                }
            }
            ast::statement::Kind::ActionCall(action) => {
                set_action_call_runtime(action, runtime);
            }
            ast::statement::Kind::SpreadAction(spread) => {
                if let Some(collection) = &mut spread.collection {
                    set_expr_action_runtime(collection, runtime);
                }
                if let Some(action) = &mut spread.action {
                    set_action_call_runtime(action, runtime);
                }
            }
            ast::statement::Kind::ParallelBlock(parallel) => {
                for call in &mut parallel.calls {
                    set_call_action_runtime(call, runtime);
                }
            }
            ast::statement::Kind::ForLoop(for_loop) => {
                if let Some(iterable) = &mut for_loop.iterable {
                    set_expr_action_runtime(iterable, runtime);
                }
                if let Some(body) = &mut for_loop.block_body {
                    set_block_action_runtime(body, runtime);
                }
            }
            ast::statement::Kind::Conditional(conditional) => {
                if let Some(branch) = &mut conditional.if_branch {
                    set_optional_expr_action_runtime(&mut branch.condition, runtime);
                    set_optional_block_action_runtime(&mut branch.block_body, runtime);
                }
                for branch in &mut conditional.elif_branches {
                    set_optional_expr_action_runtime(&mut branch.condition, runtime);
                    set_optional_block_action_runtime(&mut branch.block_body, runtime);
                }
                if let Some(branch) = &mut conditional.else_branch {
                    set_optional_block_action_runtime(&mut branch.block_body, runtime);
                }
            }
            ast::statement::Kind::TryExcept(try_except) => {
                set_optional_block_action_runtime(&mut try_except.try_block, runtime);
                for handler in &mut try_except.handlers {
                    set_optional_block_action_runtime(&mut handler.block_body, runtime);
                }
            }
            ast::statement::Kind::ReturnStmt(return_statement) => {
                set_optional_expr_action_runtime(&mut return_statement.value, runtime);
            }
            ast::statement::Kind::ExprStmt(expression_statement) => {
                set_optional_expr_action_runtime(&mut expression_statement.expr, runtime);
            }
            ast::statement::Kind::WhileLoop(while_loop) => {
                set_optional_expr_action_runtime(&mut while_loop.condition, runtime);
                set_optional_block_action_runtime(&mut while_loop.block_body, runtime);
            }
            ast::statement::Kind::SleepStmt(sleep) => {
                set_optional_expr_action_runtime(&mut sleep.duration, runtime);
            }
            ast::statement::Kind::BreakStmt(_) | ast::statement::Kind::ContinueStmt(_) => {}
        }
    }
}

fn set_optional_block_action_runtime(
    block: &mut Option<ast::Block>,
    runtime: waymark_action_core::ActionRuntime,
) {
    if let Some(block) = block {
        set_block_action_runtime(block, runtime);
    }
}

fn set_optional_expr_action_runtime(
    expression: &mut Option<ast::Expr>,
    runtime: waymark_action_core::ActionRuntime,
) {
    if let Some(expression) = expression {
        set_expr_action_runtime(expression, runtime);
    }
}

fn set_action_call_runtime(
    action: &mut ast::ActionCall,
    runtime: waymark_action_core::ActionRuntime,
) {
    action.runtime = encoded_runtime(runtime);
    for kwarg in &mut action.kwargs {
        set_optional_expr_action_runtime(&mut kwarg.value, runtime);
    }
}

fn set_call_action_runtime(call: &mut ast::Call, runtime: waymark_action_core::ActionRuntime) {
    match &mut call.kind {
        Some(ast::call::Kind::Action(action)) => set_action_call_runtime(action, runtime),
        Some(ast::call::Kind::Function(function)) => {
            for argument in &mut function.args {
                set_expr_action_runtime(argument, runtime);
            }
            for kwarg in &mut function.kwargs {
                set_optional_expr_action_runtime(&mut kwarg.value, runtime);
            }
        }
        None => {}
    }
}

fn set_expr_action_runtime(
    expression: &mut ast::Expr,
    runtime: waymark_action_core::ActionRuntime,
) {
    let Some(kind) = &mut expression.kind else {
        return;
    };
    match kind {
        ast::expr::Kind::BinaryOp(binary) => {
            if let Some(left) = &mut binary.left {
                set_expr_action_runtime(left, runtime);
            }
            if let Some(right) = &mut binary.right {
                set_expr_action_runtime(right, runtime);
            }
        }
        ast::expr::Kind::UnaryOp(unary) => {
            if let Some(operand) = &mut unary.operand {
                set_expr_action_runtime(operand, runtime);
            }
        }
        ast::expr::Kind::List(list) => {
            for element in &mut list.elements {
                set_expr_action_runtime(element, runtime);
            }
        }
        ast::expr::Kind::Dict(dict) => {
            for entry in &mut dict.entries {
                set_optional_expr_action_runtime(&mut entry.key, runtime);
                set_optional_expr_action_runtime(&mut entry.value, runtime);
            }
        }
        ast::expr::Kind::Index(index) => {
            if let Some(object) = &mut index.object {
                set_expr_action_runtime(object, runtime);
            }
            if let Some(index) = &mut index.index {
                set_expr_action_runtime(index, runtime);
            }
        }
        ast::expr::Kind::Dot(dot) => {
            if let Some(object) = &mut dot.object {
                set_expr_action_runtime(object, runtime);
            }
        }
        ast::expr::Kind::FunctionCall(function) => {
            for argument in &mut function.args {
                set_expr_action_runtime(argument, runtime);
            }
            for kwarg in &mut function.kwargs {
                set_optional_expr_action_runtime(&mut kwarg.value, runtime);
            }
        }
        ast::expr::Kind::ActionCall(action) => set_action_call_runtime(action, runtime),
        ast::expr::Kind::ParallelExpr(parallel) => {
            for call in &mut parallel.calls {
                set_call_action_runtime(call, runtime);
            }
        }
        ast::expr::Kind::SpreadExpr(spread) => {
            if let Some(collection) = &mut spread.collection {
                set_expr_action_runtime(collection, runtime);
            }
            if let Some(action) = &mut spread.action {
                set_action_call_runtime(action, runtime);
            }
        }
        ast::expr::Kind::Literal(_) | ast::expr::Kind::Variable(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn action(runtime: waymark_proto::action::ActionRuntime, name: &str) -> ast::ActionCall {
        ast::ActionCall {
            action_name: name.to_owned(),
            kwargs: Vec::new(),
            policies: Vec::new(),
            module_name: None,
            runtime: runtime as i32,
        }
    }

    #[test]
    fn finds_and_updates_nested_action_runtimes() {
        let mut program = ast::Program {
            functions: vec![ast::FunctionDef {
                name: "main".to_owned(),
                io: None,
                body: Some(ast::Block {
                    statements: vec![
                        ast::Statement {
                            kind: Some(ast::statement::Kind::ActionCall(action(
                                waymark_proto::action::ActionRuntime::Python,
                                "first",
                            ))),
                            span: None,
                        },
                        ast::Statement {
                            kind: Some(ast::statement::Kind::ReturnStmt(ast::ReturnStmt {
                                value: Some(ast::Expr {
                                    kind: Some(ast::expr::Kind::ActionCall(action(
                                        waymark_proto::action::ActionRuntime::Javascript,
                                        "second",
                                    ))),
                                    span: None,
                                }),
                            })),
                            span: None,
                        },
                    ],
                    span: None,
                }),
                span: None,
            }],
        };

        assert_eq!(
            required_action_runtimes(&program),
            Ok(vec![
                waymark_action_core::ActionRuntime::Python,
                waymark_action_core::ActionRuntime::JavaScript,
            ])
        );

        set_action_runtime(&mut program, waymark_action_core::ActionRuntime::JavaScript);

        assert_eq!(
            required_action_runtimes(&program),
            Ok(vec![waymark_action_core::ActionRuntime::JavaScript])
        );
    }
}
