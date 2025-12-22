use std::collections::HashSet;

use crate::messages::ast as ir_ast;

pub fn validate_program(program: &ir_ast::Program) -> Result<(), String> {
    for fn_def in &program.functions {
        validate_function(fn_def)?;
    }
    Ok(())
}

fn validate_function(fn_def: &ir_ast::FunctionDef) -> Result<(), String> {
    let mut scope: HashSet<String> = HashSet::new();
    if let Some(io) = fn_def.io.as_ref() {
        for input in &io.inputs {
            scope.insert(input.clone());
        }
    }
    scope.insert("self".to_string());

    if let Some(body) = fn_def.body.as_ref() {
        validate_block(body, &scope, &fn_def.name)?;
    }
    Ok(())
}

fn validate_block(
    block: &ir_ast::Block,
    scope: &HashSet<String>,
    fn_name: &str,
) -> Result<HashSet<String>, String> {
    let mut current = scope.clone();
    for stmt in &block.statements {
        current = validate_statement(stmt, &current, fn_name)?;
    }
    Ok(current)
}

fn validate_statement(
    stmt: &ir_ast::Statement,
    scope: &HashSet<String>,
    fn_name: &str,
) -> Result<HashSet<String>, String> {
    use ir_ast::statement::Kind;

    let mut current = scope.clone();
    let Some(kind) = &stmt.kind else {
        return Ok(current);
    };

    match kind {
        Kind::Assignment(assign) => {
            if let Some(value) = assign.value.as_ref() {
                validate_expr(value, &current, fn_name)?;
                for target in &assign.targets {
                    if let Some(base) = base_target_name(target)
                        && !current.contains(base)
                    {
                        return Err(undefined_variable_message(
                            base,
                            fn_name,
                            value.span.as_ref(),
                        ));
                    }
                    if is_identifier(target) {
                        current.insert(target.clone());
                    }
                }
            }
        }
        Kind::ExprStmt(expr_stmt) => {
            if let Some(expr) = expr_stmt.expr.as_ref() {
                validate_expr(expr, &current, fn_name)?;
            }
        }
        Kind::ActionCall(call) => {
            validate_action_call(call, &current, fn_name)?;
        }
        Kind::SpreadAction(spread) => {
            if let Some(collection) = spread.collection.as_ref() {
                validate_expr(collection, &current, fn_name)?;
            }
            let mut spread_scope = current.clone();
            if !spread.loop_var.is_empty() {
                spread_scope.insert(spread.loop_var.clone());
            }
            if let Some(action) = spread.action.as_ref() {
                validate_action_call(action, &spread_scope, fn_name)?;
            }
        }
        Kind::ParallelBlock(block) => {
            for call in &block.calls {
                match &call.kind {
                    Some(ir_ast::call::Kind::Action(action)) => {
                        validate_action_call(action, &current, fn_name)?;
                    }
                    Some(ir_ast::call::Kind::Function(function)) => {
                        validate_function_call(function, &current, fn_name)?;
                    }
                    None => {}
                }
            }
        }
        Kind::ForLoop(for_loop) => {
            if let Some(iterable) = for_loop.iterable.as_ref() {
                validate_expr(iterable, &current, fn_name)?;
            }
            let mut loop_scope = current.clone();
            for var in &for_loop.loop_vars {
                loop_scope.insert(var.clone());
            }
            if let Some(body) = for_loop.block_body.as_ref() {
                let loop_result = validate_block(body, &loop_scope, fn_name)?;
                current.extend(loop_result);
            }
        }
        Kind::Conditional(cond) => {
            if let Some(if_branch) = cond.if_branch.as_ref()
                && let Some(condition) = if_branch.condition.as_ref()
            {
                validate_expr(condition, &current, fn_name)?;
            }
            let mut branch_scopes: Vec<HashSet<String>> = Vec::new();
            if let Some(if_branch) = cond.if_branch.as_ref()
                && let Some(body) = if_branch.block_body.as_ref()
            {
                branch_scopes.push(validate_block(body, &current, fn_name)?);
            }
            for branch in &cond.elif_branches {
                if let Some(condition) = branch.condition.as_ref() {
                    validate_expr(condition, &current, fn_name)?;
                }
                if let Some(body) = branch.block_body.as_ref() {
                    branch_scopes.push(validate_block(body, &current, fn_name)?);
                }
            }
            if let Some(else_branch) = cond.else_branch.as_ref()
                && let Some(body) = else_branch.block_body.as_ref()
            {
                branch_scopes.push(validate_block(body, &current, fn_name)?);
            }
            for branch_scope in branch_scopes {
                current.extend(branch_scope);
            }
        }
        Kind::TryExcept(try_except) => {
            let mut branch_scopes: Vec<HashSet<String>> = Vec::new();
            if let Some(body) = try_except.try_block.as_ref() {
                branch_scopes.push(validate_block(body, &current, fn_name)?);
            }
            for handler in &try_except.handlers {
                if let Some(body) = handler.block_body.as_ref() {
                    branch_scopes.push(validate_block(body, &current, fn_name)?);
                }
            }
            for branch_scope in branch_scopes {
                current.extend(branch_scope);
            }
        }
        Kind::ReturnStmt(ret) => {
            if let Some(value) = ret.value.as_ref() {
                validate_expr(value, &current, fn_name)?;
            }
        }
    }

    Ok(current)
}

fn validate_expr(
    expr: &ir_ast::Expr,
    scope: &HashSet<String>,
    fn_name: &str,
) -> Result<(), String> {
    use ir_ast::expr::Kind;

    let Some(kind) = &expr.kind else {
        return Ok(());
    };

    match kind {
        Kind::Variable(var) => {
            if !scope.contains(&var.name) {
                return Err(undefined_variable_message(
                    &var.name,
                    fn_name,
                    expr.span.as_ref(),
                ));
            }
        }
        Kind::BinaryOp(bin) => {
            if let Some(left) = bin.left.as_ref() {
                validate_expr(left, scope, fn_name)?;
            }
            if let Some(right) = bin.right.as_ref() {
                validate_expr(right, scope, fn_name)?;
            }
        }
        Kind::UnaryOp(unary) => {
            if let Some(operand) = unary.operand.as_ref() {
                validate_expr(operand, scope, fn_name)?;
            }
        }
        Kind::List(list) => {
            for elem in &list.elements {
                validate_expr(elem, scope, fn_name)?;
            }
        }
        Kind::Dict(dict) => {
            for entry in &dict.entries {
                if let Some(key) = entry.key.as_ref() {
                    validate_expr(key, scope, fn_name)?;
                }
                if let Some(value) = entry.value.as_ref() {
                    validate_expr(value, scope, fn_name)?;
                }
            }
        }
        Kind::Index(index) => {
            if let Some(object) = index.object.as_ref() {
                validate_expr(object, scope, fn_name)?;
            }
            if let Some(idx) = index.index.as_ref() {
                validate_expr(idx, scope, fn_name)?;
            }
        }
        Kind::Dot(dot) => {
            if let Some(object) = dot.object.as_ref() {
                validate_expr(object, scope, fn_name)?;
            }
        }
        Kind::FunctionCall(call) => {
            validate_function_call(call, scope, fn_name)?;
        }
        Kind::ActionCall(call) => {
            validate_action_call(call, scope, fn_name)?;
        }
        Kind::ParallelExpr(parallel) => {
            for call in &parallel.calls {
                match &call.kind {
                    Some(ir_ast::call::Kind::Action(action)) => {
                        validate_action_call(action, scope, fn_name)?;
                    }
                    Some(ir_ast::call::Kind::Function(function)) => {
                        validate_function_call(function, scope, fn_name)?;
                    }
                    None => {}
                }
            }
        }
        Kind::SpreadExpr(spread) => {
            if let Some(collection) = spread.collection.as_ref() {
                validate_expr(collection, scope, fn_name)?;
            }
            let mut spread_scope = scope.clone();
            if !spread.loop_var.is_empty() {
                spread_scope.insert(spread.loop_var.clone());
            }
            if let Some(action) = spread.action.as_ref() {
                validate_action_call(action, &spread_scope, fn_name)?;
            }
        }
        Kind::Literal(_) => {}
    }

    Ok(())
}

fn validate_action_call(
    call: &ir_ast::ActionCall,
    scope: &HashSet<String>,
    fn_name: &str,
) -> Result<(), String> {
    for kwarg in &call.kwargs {
        if let Some(value) = kwarg.value.as_ref() {
            validate_expr(value, scope, fn_name)?;
        }
    }
    Ok(())
}

fn validate_function_call(
    call: &ir_ast::FunctionCall,
    scope: &HashSet<String>,
    fn_name: &str,
) -> Result<(), String> {
    for arg in &call.args {
        validate_expr(arg, scope, fn_name)?;
    }
    for kwarg in &call.kwargs {
        if let Some(value) = kwarg.value.as_ref() {
            validate_expr(value, scope, fn_name)?;
        }
    }
    Ok(())
}

fn base_target_name(target: &str) -> Option<&str> {
    target
        .split_once('[')
        .map(|(base, _)| base)
        .or_else(|| target.split_once('.').map(|(base, _)| base))
}

fn is_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return false;
    }
    chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

fn undefined_variable_message(name: &str, fn_name: &str, span: Option<&ir_ast::Span>) -> String {
    let location = span
        .and_then(|span| {
            if span.start_line > 0 {
                Some((span.start_line, span.start_col))
            } else {
                None
            }
        })
        .map(|(line, col)| format!(" (line {line}, col {col})"))
        .unwrap_or_default();
    format!("Variable '{name}' referenced before assignment in function '{fn_name}'{location}")
}

#[cfg(test)]
mod tests {
    use super::validate_program;
    use crate::messages::ast as ir_ast;

    fn expr_var(name: &str) -> ir_ast::Expr {
        ir_ast::Expr {
            kind: Some(ir_ast::expr::Kind::Variable(ir_ast::Variable {
                name: name.to_string(),
            })),
            span: None,
        }
    }

    fn action_call(name: &str, kwargs: Vec<(&str, ir_ast::Expr)>) -> ir_ast::ActionCall {
        ir_ast::ActionCall {
            action_name: name.to_string(),
            kwargs: kwargs
                .into_iter()
                .map(|(key, value)| ir_ast::Kwarg {
                    name: key.to_string(),
                    value: Some(value),
                })
                .collect(),
            policies: Vec::new(),
            module_name: None,
        }
    }

    fn program_with_statements(
        inputs: Vec<&str>,
        statements: Vec<ir_ast::Statement>,
    ) -> ir_ast::Program {
        ir_ast::Program {
            functions: vec![ir_ast::FunctionDef {
                name: "main".to_string(),
                io: Some(ir_ast::IoDecl {
                    inputs: inputs.into_iter().map(|s| s.to_string()).collect(),
                    outputs: Vec::new(),
                    span: None,
                }),
                body: Some(ir_ast::Block {
                    statements,
                    span: None,
                }),
                span: None,
            }],
        }
    }

    #[test]
    fn validate_program_accepts_defined_variable() {
        let action = action_call("echo", vec![("value", expr_var("value"))]);
        let stmt = ir_ast::Statement {
            kind: Some(ir_ast::statement::Kind::ActionCall(action)),
            span: None,
        };
        let program = program_with_statements(vec!["value"], vec![stmt]);
        assert!(validate_program(&program).is_ok());
    }

    #[test]
    fn validate_program_rejects_undefined_variable() {
        let action = action_call("echo", vec![("value", expr_var("missing"))]);
        let stmt = ir_ast::Statement {
            kind: Some(ir_ast::statement::Kind::ActionCall(action)),
            span: None,
        };
        let program = program_with_statements(vec![], vec![stmt]);
        let err = validate_program(&program).expect_err("expected undefined variable error");
        assert!(err.contains("missing"));
    }

    #[test]
    fn validate_program_allows_spread_loop_var() {
        let spread_action = ir_ast::SpreadAction {
            collection: Some(expr_var("items")),
            loop_var: "item".to_string(),
            action: Some(action_call("echo", vec![("value", expr_var("item"))])),
        };
        let stmt = ir_ast::Statement {
            kind: Some(ir_ast::statement::Kind::SpreadAction(spread_action)),
            span: None,
        };
        let program = program_with_statements(vec!["items"], vec![stmt]);
        assert!(validate_program(&program).is_ok());
    }
}
