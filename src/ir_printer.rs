//! IR Pretty Printer - Converts protobuf IR AST to formatted source code.
//!
//! This module provides functionality to convert a Rappel IR Program (from protobuf)
//! into human-readable source code. Used for logging registered workflows.

use crate::messages::ast;

/// Pretty printer for Rappel IR AST.
pub struct IrPrinter {
    indent_level: usize,
    indent_str: String,
}

impl IrPrinter {
    /// Create a new IR printer with default settings (4-space indent).
    pub fn new() -> Self {
        Self {
            indent_level: 0,
            indent_str: "    ".to_string(),
        }
    }

    /// Print a program as formatted source code.
    pub fn print_program(&mut self, program: &ast::Program) -> String {
        let mut parts = Vec::new();
        for func in &program.functions {
            parts.push(self.print_function_def(func));
        }
        parts.join("\n\n")
    }

    /// Print a function definition.
    pub fn print_function_def(&mut self, func: &ast::FunctionDef) -> String {
        let io = func
            .io
            .as_ref()
            .map(|io| {
                let inputs = io.inputs.join(", ");
                let outputs = io.outputs.join(", ");
                format!("input: [{}], output: [{}]", inputs, outputs)
            })
            .unwrap_or_default();

        let body = func
            .body
            .as_ref()
            .map(|b| self.print_block(b))
            .unwrap_or_default();

        format!("fn {}({}):\n{}", func.name, io, body)
    }

    /// Print a block of statements with proper indentation.
    fn print_block(&mut self, block: &ast::Block) -> String {
        self.indent_level += 1;
        let mut lines = Vec::new();
        for stmt in &block.statements {
            let stmt_str = self.print_statement(stmt);
            for line in stmt_str.lines() {
                lines.push(format!("{}{}", self.current_indent(), line));
            }
        }
        self.indent_level -= 1;
        lines.join("\n")
    }

    /// Get the current indentation string.
    fn current_indent(&self) -> String {
        self.indent_str.repeat(self.indent_level)
    }

    /// Print a statement.
    pub fn print_statement(&mut self, stmt: &ast::Statement) -> String {
        match &stmt.kind {
            Some(ast::statement::Kind::Assignment(assign)) => self.print_assignment(assign),
            Some(ast::statement::Kind::ActionCall(action)) => self.print_action_call(action),
            Some(ast::statement::Kind::SpreadAction(spread)) => self.print_spread_action(spread),
            Some(ast::statement::Kind::ParallelBlock(parallel)) => {
                self.print_parallel_block(parallel)
            }
            Some(ast::statement::Kind::ForLoop(for_loop)) => self.print_for_loop(for_loop),
            Some(ast::statement::Kind::Conditional(cond)) => self.print_conditional(cond),
            Some(ast::statement::Kind::TryExcept(try_except)) => self.print_try_except(try_except),
            Some(ast::statement::Kind::ReturnStmt(ret)) => self.print_return(ret),
            Some(ast::statement::Kind::ExprStmt(expr_stmt)) => self.print_expr_statement(expr_stmt),
            None => String::new(),
        }
    }

    /// Print an assignment statement.
    fn print_assignment(&mut self, assign: &ast::Assignment) -> String {
        let targets = assign.targets.join(", ");
        let value = assign
            .value
            .as_ref()
            .map(|v| self.print_expr(v))
            .unwrap_or_default();
        format!("{} = {}", targets, value)
    }

    /// Print an action call (statement form - no target).
    fn print_action_call(&mut self, action: &ast::ActionCall) -> String {
        let kwargs = self.print_kwargs(&action.kwargs);
        let policies = self.print_policies(&action.policies);
        format!("@{}({}){}", action.action_name, kwargs, policies)
    }

    /// Print a spread action (statement form - no target).
    fn print_spread_action(&mut self, spread: &ast::SpreadAction) -> String {
        let collection = spread
            .collection
            .as_ref()
            .map(|c| self.print_expr(c))
            .unwrap_or_default();
        let action = spread
            .action
            .as_ref()
            .map(|a| {
                let kwargs = self.print_kwargs(&a.kwargs);
                let policies = self.print_policies(&a.policies);
                format!("@{}({}){}", a.action_name, kwargs, policies)
            })
            .unwrap_or_default();

        format!("spread {}:{} -> {}", collection, spread.loop_var, action)
    }

    /// Print a parallel block (statement form - no target).
    fn print_parallel_block(&mut self, parallel: &ast::ParallelBlock) -> String {
        let header = "parallel:".to_string();

        self.indent_level += 1;
        let mut call_lines = Vec::new();
        for call in &parallel.calls {
            let call_str = match &call.kind {
                Some(ast::call::Kind::Action(action)) => {
                    let kwargs = self.print_kwargs(&action.kwargs);
                    format!(
                        "{}@{}({})",
                        self.current_indent(),
                        action.action_name,
                        kwargs
                    )
                }
                Some(ast::call::Kind::Function(func)) => {
                    let args = self.print_call_args(&func.args, &func.kwargs);
                    format!("{}{}({})", self.current_indent(), func.name, args)
                }
                None => String::new(),
            };
            call_lines.push(call_str);
        }
        self.indent_level -= 1;

        format!("{}\n{}", header, call_lines.join("\n"))
    }

    /// Print a for loop.
    fn print_for_loop(&mut self, for_loop: &ast::ForLoop) -> String {
        let loop_vars = for_loop.loop_vars.join(", ");
        let iterable = for_loop
            .iterable
            .as_ref()
            .map(|i| self.print_expr(i))
            .unwrap_or_default();
        let body = for_loop
            .block_body
            .as_ref()
            .map(|b| self.print_block(b))
            .unwrap_or_default();

        format!("for {} in {}:\n{}", loop_vars, iterable, body)
    }

    /// Print a conditional (if/elif/else).
    fn print_conditional(&mut self, cond: &ast::Conditional) -> String {
        let mut result = String::new();

        // Print if branch
        if let Some(ref if_branch) = cond.if_branch {
            let condition = if_branch
                .condition
                .as_ref()
                .map(|c| self.print_expr(c))
                .unwrap_or_default();
            let body = if_branch
                .block_body
                .as_ref()
                .map(|b| self.print_block(b))
                .unwrap_or_default();
            result.push_str(&format!("if {}:\n{}", condition, body));
        }

        // Print elif branches
        for elif in &cond.elif_branches {
            let condition = elif
                .condition
                .as_ref()
                .map(|c| self.print_expr(c))
                .unwrap_or_default();
            let body = elif
                .block_body
                .as_ref()
                .map(|b| self.print_block(b))
                .unwrap_or_default();
            result.push_str(&format!("\nelif {}:\n{}", condition, body));
        }

        // Print else branch
        if let Some(ref else_branch) = cond.else_branch {
            let body = else_branch
                .block_body
                .as_ref()
                .map(|b| self.print_block(b))
                .unwrap_or_default();
            result.push_str(&format!("\nelse:\n{}", body));
        }

        result
    }

    /// Print a try/except block.
    fn print_try_except(&mut self, try_except: &ast::TryExcept) -> String {
        let mut result = String::new();

        // Print try block
        let try_body = try_except
            .try_block
            .as_ref()
            .map(|b| self.print_block(b))
            .unwrap_or_default();
        result.push_str(&format!("try:\n{}", try_body));

        // Print except handlers
        for handler in &try_except.handlers {
            let exc_types = if handler.exception_types.is_empty() {
                String::new()
            } else {
                format!(" {}", handler.exception_types.join(", "))
            };
            let body = handler
                .block_body
                .as_ref()
                .map(|b| self.print_block(b))
                .unwrap_or_default();
            result.push_str(&format!(
                "\n{}except{}:\n{}",
                self.current_indent(),
                exc_types,
                body
            ));
        }

        result
    }

    /// Print a return statement.
    fn print_return(&mut self, ret: &ast::ReturnStmt) -> String {
        if let Some(ref value) = ret.value {
            format!("return {}", self.print_expr(value))
        } else {
            "return".to_string()
        }
    }

    /// Print an expression statement.
    fn print_expr_statement(&mut self, expr_stmt: &ast::ExprStmt) -> String {
        expr_stmt
            .expr
            .as_ref()
            .map(|e| self.print_expr(e))
            .unwrap_or_default()
    }

    /// Print an expression.
    pub fn print_expr(&mut self, expr: &ast::Expr) -> String {
        match &expr.kind {
            Some(ast::expr::Kind::Literal(lit)) => self.print_literal(lit),
            Some(ast::expr::Kind::Variable(var)) => var.name.clone(),
            Some(ast::expr::Kind::BinaryOp(binop)) => self.print_binary_op(binop),
            Some(ast::expr::Kind::UnaryOp(unop)) => self.print_unary_op(unop),
            Some(ast::expr::Kind::List(list)) => self.print_list(list),
            Some(ast::expr::Kind::Dict(dict)) => self.print_dict(dict),
            Some(ast::expr::Kind::Index(index)) => self.print_index(index),
            Some(ast::expr::Kind::Dot(dot)) => self.print_dot(dot),
            Some(ast::expr::Kind::FunctionCall(call)) => self.print_function_call(call),
            Some(ast::expr::Kind::ActionCall(action)) => {
                let kwargs = self.print_kwargs(&action.kwargs);
                let policies = self.print_policies(&action.policies);
                format!("@{}({}){}", action.action_name, kwargs, policies)
            }
            Some(ast::expr::Kind::ParallelExpr(parallel)) => self.print_parallel_expr(parallel),
            Some(ast::expr::Kind::SpreadExpr(spread)) => self.print_spread_expr(spread),
            None => String::new(),
        }
    }

    /// Print a parallel expression.
    fn print_parallel_expr(&mut self, parallel: &ast::ParallelExpr) -> String {
        let mut calls_str = Vec::new();
        for call in &parallel.calls {
            if let Some(kind) = &call.kind {
                match kind {
                    ast::call::Kind::Action(action) => {
                        let kwargs = self.print_kwargs(&action.kwargs);
                        let policies = self.print_policies(&action.policies);
                        calls_str.push(format!("@{}({}){}", action.action_name, kwargs, policies));
                    }
                    ast::call::Kind::Function(func) => {
                        let args = func
                            .args
                            .iter()
                            .map(|a| self.print_expr(a))
                            .collect::<Vec<_>>()
                            .join(", ");
                        calls_str.push(format!("{}({})", func.name, args));
                    }
                }
            }
        }
        format!("parallel:\n    {}", calls_str.join("\n    "))
    }

    /// Print a spread expression.
    fn print_spread_expr(&mut self, spread: &ast::SpreadExpr) -> String {
        let collection = spread
            .collection
            .as_ref()
            .map(|c| self.print_expr(c))
            .unwrap_or_default();
        let action = spread
            .action
            .as_ref()
            .map(|a| {
                let kwargs = self.print_kwargs(&a.kwargs);
                let policies = self.print_policies(&a.policies);
                format!("@{}({}){}", a.action_name, kwargs, policies)
            })
            .unwrap_or_default();
        format!("spread {}:{} -> {}", collection, spread.loop_var, action)
    }

    /// Print a literal value.
    fn print_literal(&self, lit: &ast::Literal) -> String {
        match &lit.value {
            Some(ast::literal::Value::IntValue(n)) => n.to_string(),
            Some(ast::literal::Value::FloatValue(f)) => {
                if f.fract() == 0.0 {
                    format!("{}.0", f)
                } else {
                    f.to_string()
                }
            }
            Some(ast::literal::Value::StringValue(s)) => format!("\"{}\"", s),
            Some(ast::literal::Value::BoolValue(b)) => {
                if *b {
                    "True".to_string()
                } else {
                    "False".to_string()
                }
            }
            Some(ast::literal::Value::IsNone(true)) => "None".to_string(),
            Some(ast::literal::Value::IsNone(false)) => "None".to_string(),
            None => String::new(),
        }
    }

    /// Print a binary operation.
    fn print_binary_op(&mut self, binop: &ast::BinaryOp) -> String {
        let left = binop
            .left
            .as_ref()
            .map(|l| self.print_expr(l))
            .unwrap_or_default();
        let right = binop
            .right
            .as_ref()
            .map(|r| self.print_expr(r))
            .unwrap_or_default();
        let op = self.binary_op_to_str(binop.op);

        format!("({} {} {})", left, op, right)
    }

    /// Convert binary operator enum to string.
    fn binary_op_to_str(&self, op: i32) -> &'static str {
        match op {
            x if x == ast::BinaryOperator::BinaryOpAdd as i32 => "+",
            x if x == ast::BinaryOperator::BinaryOpSub as i32 => "-",
            x if x == ast::BinaryOperator::BinaryOpMul as i32 => "*",
            x if x == ast::BinaryOperator::BinaryOpDiv as i32 => "/",
            x if x == ast::BinaryOperator::BinaryOpFloorDiv as i32 => "//",
            x if x == ast::BinaryOperator::BinaryOpMod as i32 => "%",
            x if x == ast::BinaryOperator::BinaryOpEq as i32 => "==",
            x if x == ast::BinaryOperator::BinaryOpNe as i32 => "!=",
            x if x == ast::BinaryOperator::BinaryOpLt as i32 => "<",
            x if x == ast::BinaryOperator::BinaryOpLe as i32 => "<=",
            x if x == ast::BinaryOperator::BinaryOpGt as i32 => ">",
            x if x == ast::BinaryOperator::BinaryOpGe as i32 => ">=",
            x if x == ast::BinaryOperator::BinaryOpIn as i32 => "in",
            x if x == ast::BinaryOperator::BinaryOpNotIn as i32 => "not in",
            x if x == ast::BinaryOperator::BinaryOpAnd as i32 => "and",
            x if x == ast::BinaryOperator::BinaryOpOr as i32 => "or",
            _ => "?",
        }
    }

    /// Print a unary operation.
    fn print_unary_op(&mut self, unop: &ast::UnaryOp) -> String {
        let operand = unop
            .operand
            .as_ref()
            .map(|o| self.print_expr(o))
            .unwrap_or_default();
        let op = self.unary_op_to_str(unop.op);

        if op == "not" {
            format!("(not {})", operand)
        } else {
            format!("({}{})", op, operand)
        }
    }

    /// Convert unary operator enum to string.
    fn unary_op_to_str(&self, op: i32) -> &'static str {
        match op {
            x if x == ast::UnaryOperator::UnaryOpNeg as i32 => "-",
            x if x == ast::UnaryOperator::UnaryOpNot as i32 => "not",
            _ => "?",
        }
    }

    /// Print a list expression.
    fn print_list(&mut self, list: &ast::ListExpr) -> String {
        let elements: Vec<String> = list.elements.iter().map(|e| self.print_expr(e)).collect();
        format!("[{}]", elements.join(", "))
    }

    /// Print a dict expression.
    fn print_dict(&mut self, dict: &ast::DictExpr) -> String {
        let entries: Vec<String> = dict
            .entries
            .iter()
            .map(|entry| {
                let key = entry
                    .key
                    .as_ref()
                    .map(|k| self.print_expr(k))
                    .unwrap_or_default();
                let value = entry
                    .value
                    .as_ref()
                    .map(|v| self.print_expr(v))
                    .unwrap_or_default();
                format!("{}: {}", key, value)
            })
            .collect();
        format!("{{{}}}", entries.join(", "))
    }

    /// Print an index access.
    fn print_index(&mut self, index: &ast::IndexAccess) -> String {
        let object = index
            .object
            .as_ref()
            .map(|o| self.print_expr(o))
            .unwrap_or_default();
        let idx = index
            .index
            .as_ref()
            .map(|i| self.print_expr(i))
            .unwrap_or_default();
        format!("{}[{}]", object, idx)
    }

    /// Print a dot access.
    fn print_dot(&mut self, dot: &ast::DotAccess) -> String {
        let object = dot
            .object
            .as_ref()
            .map(|o| self.print_expr(o))
            .unwrap_or_default();
        format!("{}.{}", object, dot.attribute)
    }

    /// Print a function call.
    fn print_function_call(&mut self, call: &ast::FunctionCall) -> String {
        let args = self.print_call_args(&call.args, &call.kwargs);
        format!("{}({})", call.name, args)
    }

    /// Print function call arguments (positional + keyword).
    fn print_call_args(&mut self, args: &[ast::Expr], kwargs: &[ast::Kwarg]) -> String {
        let mut parts = Vec::new();

        for arg in args {
            parts.push(self.print_expr(arg));
        }

        for kwarg in kwargs {
            let value = kwarg
                .value
                .as_ref()
                .map(|v| self.print_expr(v))
                .unwrap_or_default();
            parts.push(format!("{}={}", kwarg.name, value));
        }

        parts.join(", ")
    }

    /// Print keyword arguments.
    fn print_kwargs(&mut self, kwargs: &[ast::Kwarg]) -> String {
        let parts: Vec<String> = kwargs
            .iter()
            .map(|kwarg| {
                let value = kwarg
                    .value
                    .as_ref()
                    .map(|v| self.print_expr(v))
                    .unwrap_or_default();
                format!("{}={}", kwarg.name, value)
            })
            .collect();
        parts.join(", ")
    }

    /// Print policy brackets.
    fn print_policies(&self, policies: &[ast::PolicyBracket]) -> String {
        let mut result = String::new();
        for policy in policies {
            match &policy.kind {
                Some(ast::policy_bracket::Kind::Retry(retry)) => {
                    let mut parts = Vec::new();

                    let exc_prefix = if !retry.exception_types.is_empty() {
                        format!("{} -> ", retry.exception_types.join(", "))
                    } else {
                        String::new()
                    };

                    parts.push(format!("retry: {}", retry.max_retries));

                    if let Some(ref backoff) = retry.backoff {
                        parts.push(format!("backoff: {}", self.format_duration(backoff)));
                    }

                    result.push_str(&format!(" [{}{}]", exc_prefix, parts.join(", ")));
                }
                Some(ast::policy_bracket::Kind::Timeout(timeout)) => {
                    if let Some(ref duration) = timeout.timeout {
                        result.push_str(&format!(" [timeout: {}]", self.format_duration(duration)));
                    }
                }
                None => {}
            }
        }
        result
    }

    /// Format a duration value.
    fn format_duration(&self, duration: &ast::Duration) -> String {
        let seconds = duration.seconds;
        if seconds >= 3600 && seconds.is_multiple_of(3600) {
            format!("{}h", seconds / 3600)
        } else if seconds >= 60 && seconds.is_multiple_of(60) {
            format!("{}m", seconds / 60)
        } else {
            format!("{}s", seconds)
        }
    }
}

impl Default for IrPrinter {
    fn default() -> Self {
        Self::new()
    }
}

/// Convenience function to print a program.
pub fn print_program(program: &ast::Program) -> String {
    let mut printer = IrPrinter::new();
    printer.print_program(program)
}

/// Convenience function to print a single statement.
pub fn print_statement(stmt: &ast::Statement) -> String {
    let mut printer = IrPrinter::new();
    printer.print_statement(stmt)
}

/// Convenience function to print a single expression.
pub fn print_expr(expr: &ast::Expr) -> String {
    let mut printer = IrPrinter::new();
    printer.print_expr(expr)
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    #[test]
    fn test_print_benchmark_workflow_ir() {
        // Read the IR from the temp file (generated by Python test)
        let ir_path = "/tmp/test_workflow_ir.bin";
        if !std::path::Path::new(ir_path).exists() {
            println!("Skipping test: {} not found", ir_path);
            println!("Run the Python test first to generate this file");
            return;
        }

        let bytes = std::fs::read(ir_path).expect("Failed to read IR file");
        let program = ast::Program::decode(&bytes[..]).expect("Failed to decode IR");

        let output = print_program(&program);
        println!("=== Pretty-printed IR ===\n{}", output);

        // Basic assertions
        assert!(output.contains("fn run"), "Should contain run function");
        assert!(
            output.contains("fn __for_body_1__"),
            "Should contain implicit for body function"
        );
        assert!(output.contains("spread"), "Should contain spread action");
        assert!(output.contains("if"), "Should contain conditional");
        assert!(
            output.contains("@analyze_hash"),
            "Should contain analyze_hash action"
        );
        assert!(
            output.contains("@process_special_hash"),
            "Should contain process_special_hash action"
        );
        assert!(
            output.contains("@process_normal_hash"),
            "Should contain process_normal_hash action"
        );
    }
}
