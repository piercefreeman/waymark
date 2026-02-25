//! Pretty-printer for IR AST structures.

use crate::messages::ast as ir;
const DEFAULT_INDENT: &str = "    ";

/// Render IR AST nodes into a source-like representation.
pub struct IRFormatter {
    indent: String,
}

impl IRFormatter {
    pub fn new(indent: &str) -> Self {
        Self {
            indent: indent.to_string(),
        }
    }

    pub fn format_program(&self, program: &ir::Program) -> String {
        let mut lines: Vec<String> = Vec::new();
        for (idx, fn_def) in program.functions.iter().enumerate() {
            if idx > 0 {
                lines.push("".to_string());
            }
            lines.extend(self.format_function(fn_def));
        }
        lines.join("\n")
    }

    fn format_function(&self, fn_def: &ir::FunctionDef) -> Vec<String> {
        let io_decl = fn_def.io.as_ref();
        let inputs = io_decl.map(|io| io.inputs.join(", ")).unwrap_or_default();
        let outputs = io_decl.map(|io| io.outputs.join(", ")).unwrap_or_default();
        let header = format!(
            "fn {}(input: [{}], output: [{}]):",
            fn_def.name, inputs, outputs
        );
        let mut lines = vec![header];
        if let Some(body) = &fn_def.body {
            lines.extend(self.format_block(body, 1));
        } else {
            lines.push(self.indent_line(1, "pass"));
        }
        lines
    }

    fn format_block(&self, block: &ir::Block, level: usize) -> Vec<String> {
        if block.statements.is_empty() {
            return vec![self.indent_line(level, "pass")];
        }
        let mut lines = Vec::new();
        for stmt in &block.statements {
            lines.extend(self.format_statement(stmt, level));
        }
        lines
    }

    fn format_statement(&self, stmt: &ir::Statement, level: usize) -> Vec<String> {
        let indent = self.indent_line(level, "");
        match stmt.kind.as_ref() {
            Some(ir::statement::Kind::Assignment(assign)) => {
                let targets = if assign.targets.is_empty() {
                    "_".to_string()
                } else {
                    assign.targets.join(", ")
                };
                let value = assign.value.as_ref();
                if let Some(expr) = value {
                    match expr.kind.as_ref() {
                        Some(ir::expr::Kind::ParallelExpr(parallel)) => {
                            return self.format_parallel_block(parallel, level, Some(&targets));
                        }
                        Some(ir::expr::Kind::SpreadExpr(spread)) => {
                            let expr_str = self.format_spread_expr(spread);
                            return vec![format!("{indent}{targets} = {expr_str}")];
                        }
                        _ => {
                            let expr_str = self.format_expr(expr, 0);
                            return vec![format!("{indent}{targets} = {expr_str}")];
                        }
                    }
                }
                vec![format!("{indent}pass")]
            }
            Some(ir::statement::Kind::ActionCall(action)) => {
                vec![format!("{indent}{}", self.format_action_call(action))]
            }
            Some(ir::statement::Kind::SpreadAction(spread)) => {
                vec![format!("{indent}{}", self.format_spread_action(spread))]
            }
            Some(ir::statement::Kind::ParallelBlock(parallel)) => {
                self.format_parallel_block_stmt(parallel, level, None)
            }
            Some(ir::statement::Kind::ForLoop(loop_stmt)) => {
                let loop_vars = if loop_stmt.loop_vars.is_empty() {
                    "_".to_string()
                } else {
                    loop_stmt.loop_vars.join(", ")
                };
                let iterable = loop_stmt
                    .iterable
                    .as_ref()
                    .map(|expr| self.format_expr(expr, 0))
                    .unwrap_or_else(|| "None".to_string());
                let mut lines = vec![format!("{indent}for {loop_vars} in {iterable}:")];
                if let Some(block) = &loop_stmt.block_body {
                    lines.extend(self.format_block(block, level + 1));
                } else {
                    lines.push(self.indent_line(level + 1, "pass"));
                }
                lines
            }
            Some(ir::statement::Kind::WhileLoop(loop_stmt)) => {
                let condition = loop_stmt
                    .condition
                    .as_ref()
                    .map(|expr| self.format_expr(expr, 0))
                    .unwrap_or_else(|| "None".to_string());
                let mut lines = vec![format!("{indent}while {condition}:")];
                if let Some(block) = &loop_stmt.block_body {
                    lines.extend(self.format_block(block, level + 1));
                } else {
                    lines.push(self.indent_line(level + 1, "pass"));
                }
                lines
            }
            Some(ir::statement::Kind::Conditional(cond)) => self.format_conditional(cond, level),
            Some(ir::statement::Kind::TryExcept(try_except)) => {
                self.format_try_except(try_except, level)
            }
            Some(ir::statement::Kind::ReturnStmt(ret)) => {
                if let Some(value) = &ret.value {
                    let expr_str = self.format_expr(value, 0);
                    vec![format!("{indent}return {expr_str}")]
                } else {
                    vec![format!("{indent}return")]
                }
            }
            Some(ir::statement::Kind::BreakStmt(_)) => vec![format!("{indent}break")],
            Some(ir::statement::Kind::ContinueStmt(_)) => vec![format!("{indent}continue")],
            Some(ir::statement::Kind::SleepStmt(sleep_stmt)) => {
                if let Some(duration) = &sleep_stmt.duration {
                    let duration_str = self.format_expr(duration, 0);
                    vec![format!("{indent}sleep {duration_str}")]
                } else {
                    vec![format!("{indent}sleep")]
                }
            }
            Some(ir::statement::Kind::ExprStmt(expr_stmt)) => {
                if let Some(expr) = &expr_stmt.expr {
                    match expr.kind.as_ref() {
                        Some(ir::expr::Kind::ParallelExpr(parallel)) => {
                            return self.format_parallel_block(parallel, level, None);
                        }
                        Some(ir::expr::Kind::SpreadExpr(spread)) => {
                            let expr_str = self.format_spread_expr(spread);
                            return vec![format!("{indent}{expr_str}")];
                        }
                        _ => {
                            let expr_str = self.format_expr(expr, 0);
                            return vec![format!("{indent}{expr_str}")];
                        }
                    }
                }
                vec![format!("{indent}pass")]
            }
            None => vec![format!("{indent}pass")],
        }
    }

    fn format_conditional(&self, conditional: &ir::Conditional, level: usize) -> Vec<String> {
        let indent = self.indent_line(level, "");
        let mut lines: Vec<String> = Vec::new();

        if let Some(if_branch) = &conditional.if_branch {
            let condition = if_branch
                .condition
                .as_ref()
                .map(|expr| self.format_expr(expr, 0))
                .unwrap_or_else(|| "None".to_string());
            lines.push(format!("{indent}if {condition}:"));
            if let Some(block) = &if_branch.block_body {
                lines.extend(self.format_block(block, level + 1));
            } else {
                lines.push(self.indent_line(level + 1, "pass"));
            }
        }

        for branch in &conditional.elif_branches {
            let condition = branch
                .condition
                .as_ref()
                .map(|expr| self.format_expr(expr, 0))
                .unwrap_or_else(|| "None".to_string());
            lines.push(format!("{indent}elif {condition}:"));
            if let Some(block) = &branch.block_body {
                lines.extend(self.format_block(block, level + 1));
            } else {
                lines.push(self.indent_line(level + 1, "pass"));
            }
        }

        if let Some(else_branch) = &conditional.else_branch {
            lines.push(format!("{indent}else:"));
            if let Some(block) = &else_branch.block_body {
                lines.extend(self.format_block(block, level + 1));
            } else {
                lines.push(self.indent_line(level + 1, "pass"));
            }
        }

        lines
    }

    fn format_try_except(&self, try_except: &ir::TryExcept, level: usize) -> Vec<String> {
        let indent = self.indent_line(level, "");
        let mut lines = vec![format!("{indent}try:")];
        if let Some(block) = &try_except.try_block {
            lines.extend(self.format_block(block, level + 1));
        } else {
            lines.push(self.indent_line(level + 1, "pass"));
        }

        for handler in &try_except.handlers {
            let exc_types = handler.exception_types.join(", ");
            let mut header = if exc_types.is_empty() {
                "except".to_string()
            } else {
                format!("except {exc_types}")
            };
            if let Some(exception_var) = handler.exception_var.as_deref()
                && !exception_var.is_empty()
            {
                header = format!("{header} as {exception_var}");
            }
            lines.push(format!("{indent}{header}:"));
            if let Some(block) = &handler.block_body {
                lines.extend(self.format_block(block, level + 1));
            } else {
                lines.push(self.indent_line(level + 1, "pass"));
            }
        }
        lines
    }

    fn format_parallel_block(
        &self,
        parallel: &ir::ParallelExpr,
        level: usize,
        targets: Option<&str>,
    ) -> Vec<String> {
        let indent = self.indent_line(level, "");
        let mut header = "parallel:".to_string();
        if let Some(targets) = targets {
            header = format!("{targets} = {header}");
        }
        let mut lines = vec![format!("{indent}{header}")];
        if parallel.calls.is_empty() {
            lines.push(self.indent_line(level + 1, "pass"));
            return lines;
        }
        for call in &parallel.calls {
            let rendered = self.format_call(call);
            lines.push(self.indent_line(level + 1, &rendered));
        }
        lines
    }

    fn format_parallel_block_stmt(
        &self,
        parallel: &ir::ParallelBlock,
        level: usize,
        targets: Option<&str>,
    ) -> Vec<String> {
        let expr = ir::ParallelExpr {
            calls: parallel.calls.clone(),
        };
        self.format_parallel_block(&expr, level, targets)
    }

    fn format_spread_action(&self, spread: &ir::SpreadAction) -> String {
        let collection = spread
            .collection
            .as_ref()
            .map(|expr| self.format_expr(expr, 0))
            .unwrap_or_else(|| "None".to_string());
        let action = spread
            .action
            .as_ref()
            .map(|action| self.format_action_call(action))
            .unwrap_or_else(|| "pass".to_string());
        format!("spread {collection}:{} -> {action}", spread.loop_var)
    }

    fn format_spread_expr(&self, spread: &ir::SpreadExpr) -> String {
        let collection = spread
            .collection
            .as_ref()
            .map(|expr| self.format_expr(expr, 0))
            .unwrap_or_else(|| "None".to_string());
        let action = spread
            .action
            .as_ref()
            .map(|action| self.format_action_call(action))
            .unwrap_or_else(|| "pass".to_string());
        format!("spread {collection}:{} -> {action}", spread.loop_var)
    }

    fn format_call(&self, call: &ir::Call) -> String {
        match call.kind.as_ref() {
            Some(ir::call::Kind::Action(action)) => self.format_action_call(action),
            Some(ir::call::Kind::Function(function)) => self.format_function_call(function),
            None => "pass".to_string(),
        }
    }

    fn format_action_call(&self, action: &ir::ActionCall) -> String {
        let mut prefix = action.action_name.clone();
        if let Some(module) = &action.module_name {
            prefix = format!("{module}.{prefix}");
        }
        let mut args: Vec<String> = Vec::new();
        for kw in &action.kwargs {
            if let Some(value) = &kw.value {
                args.push(format!("{}={}", kw.name, self.format_expr(value, 0)));
            }
        }
        let mut rendered = format!("@{}({})", prefix, args.join(", "));
        for policy in &action.policies {
            rendered = format!("{} {}", rendered, self.format_policy(policy));
        }
        rendered
    }

    fn format_function_call(&self, call: &ir::FunctionCall) -> String {
        let name = self.resolve_function_name(call);
        let mut args: Vec<String> = call
            .args
            .iter()
            .map(|arg| self.format_expr(arg, 0))
            .collect();
        for kw in &call.kwargs {
            if let Some(value) = &kw.value {
                args.push(format!("{}={}", kw.name, self.format_expr(value, 0)));
            }
        }
        format!("{}({})", name, args.join(", "))
    }

    fn resolve_function_name(&self, call: &ir::FunctionCall) -> String {
        if !call.name.is_empty() {
            return call.name.clone();
        }
        match ir::GlobalFunction::try_from(call.global_function).ok() {
            Some(ir::GlobalFunction::Range) => "range".to_string(),
            Some(ir::GlobalFunction::Len) => "len".to_string(),
            Some(ir::GlobalFunction::Enumerate) => "enumerate".to_string(),
            Some(ir::GlobalFunction::Isexception) => "isexception".to_string(),
            Some(ir::GlobalFunction::Unspecified) | None => "fn".to_string(),
        }
    }

    fn format_policy(&self, policy: &ir::PolicyBracket) -> String {
        match policy.kind.as_ref() {
            Some(ir::policy_bracket::Kind::Retry(retry)) => {
                let exc_types = retry.exception_types.join(", ");
                let header = if exc_types.is_empty() {
                    "".to_string()
                } else {
                    format!("{exc_types} -> ")
                };
                let mut parts = vec![format!("retry: {}", retry.max_retries)];
                if let Some(backoff) = &retry.backoff {
                    parts.push(format!("backoff: {}", self.format_duration(backoff)));
                }
                format!("[{header}{}]", parts.join(", "))
            }
            Some(ir::policy_bracket::Kind::Timeout(timeout)) => {
                let fallback = ir::Duration { seconds: 0 };
                let duration = timeout.timeout.as_ref().unwrap_or(&fallback);
                format!("[timeout: {}]", self.format_duration(duration))
            }
            None => "[]".to_string(),
        }
    }

    fn format_duration(&self, duration: &ir::Duration) -> String {
        let seconds = duration.seconds;
        if seconds != 0 && seconds.is_multiple_of(3600) {
            return format!("{}h", seconds / 3600);
        }
        if seconds != 0 && seconds.is_multiple_of(60) {
            return format!("{}m", seconds / 60);
        }
        format!("{}s", seconds)
    }

    fn format_expr(&self, expr: &ir::Expr, parent_prec: i32) -> String {
        match expr.kind.as_ref() {
            Some(ir::expr::Kind::Literal(lit)) => self.format_literal(lit),
            Some(ir::expr::Kind::Variable(var)) => var.name.clone(),
            Some(ir::expr::Kind::BinaryOp(op)) => self.format_binary_op(op, parent_prec),
            Some(ir::expr::Kind::UnaryOp(op)) => self.format_unary_op(op, parent_prec),
            Some(ir::expr::Kind::List(list)) => {
                let items: Vec<String> = list
                    .elements
                    .iter()
                    .map(|item| self.format_expr(item, 0))
                    .collect();
                format!("[{}]", items.join(", "))
            }
            Some(ir::expr::Kind::Dict(dict_expr)) => {
                let entries: Vec<String> = dict_expr
                    .entries
                    .iter()
                    .map(|entry| {
                        let key = entry
                            .key
                            .as_ref()
                            .map(|expr| self.format_expr(expr, 0))
                            .unwrap_or_default();
                        let value = entry
                            .value
                            .as_ref()
                            .map(|expr| self.format_expr(expr, 0))
                            .unwrap_or_default();
                        format!("{key}: {value}")
                    })
                    .collect();
                format!("{{{}}}", entries.join(", "))
            }
            Some(ir::expr::Kind::Index(index)) => {
                let obj = index
                    .object
                    .as_ref()
                    .map(|expr| self.format_expr(expr, self.precedence("index")))
                    .unwrap_or_else(|| "None".to_string());
                let idx = index
                    .index
                    .as_ref()
                    .map(|expr| self.format_expr(expr, 0))
                    .unwrap_or_else(|| "None".to_string());
                format!("{obj}[{idx}]")
            }
            Some(ir::expr::Kind::Dot(dot)) => {
                let obj = dot
                    .object
                    .as_ref()
                    .map(|expr| self.format_expr(expr, self.precedence("dot")))
                    .unwrap_or_else(|| "None".to_string());
                format!("{obj}.{}", dot.attribute)
            }
            Some(ir::expr::Kind::FunctionCall(call)) => self.format_function_call(call),
            Some(ir::expr::Kind::ActionCall(call)) => self.format_action_call(call),
            Some(ir::expr::Kind::ParallelExpr(parallel)) => {
                let calls = parallel
                    .calls
                    .iter()
                    .map(|call| self.format_call(call))
                    .collect::<Vec<String>>()
                    .join(", ");
                format!("parallel({calls})")
            }
            Some(ir::expr::Kind::SpreadExpr(spread)) => self.format_spread_expr(spread),
            None => "None".to_string(),
        }
    }

    fn format_binary_op(&self, op: &ir::BinaryOp, parent_prec: i32) -> String {
        let (op_str, prec) = self.binary_operator(op.op);
        let left = op
            .left
            .as_ref()
            .map(|expr| self.format_expr(expr, prec))
            .unwrap_or_else(|| "None".to_string());
        let right = op
            .right
            .as_ref()
            .map(|expr| self.format_expr(expr, prec + 1))
            .unwrap_or_else(|| "None".to_string());
        let rendered = format!("{left} {op_str} {right}");
        if prec < parent_prec {
            format!("({rendered})")
        } else {
            rendered
        }
    }

    fn format_unary_op(&self, op: &ir::UnaryOp, parent_prec: i32) -> String {
        let (op_str, prec) = self.unary_operator(op.op);
        let operand = op
            .operand
            .as_ref()
            .map(|expr| self.format_expr(expr, prec))
            .unwrap_or_else(|| "None".to_string());
        let rendered = format!("{op_str}{operand}");
        if prec < parent_prec {
            format!("({rendered})")
        } else {
            rendered
        }
    }

    fn binary_operator(&self, op: i32) -> (String, i32) {
        match ir::BinaryOperator::try_from(op).ok() {
            Some(ir::BinaryOperator::BinaryOpOr) => ("or".to_string(), 10),
            Some(ir::BinaryOperator::BinaryOpAnd) => ("and".to_string(), 20),
            Some(ir::BinaryOperator::BinaryOpEq) => ("==".to_string(), 30),
            Some(ir::BinaryOperator::BinaryOpNe) => ("!=".to_string(), 30),
            Some(ir::BinaryOperator::BinaryOpLt) => ("<".to_string(), 30),
            Some(ir::BinaryOperator::BinaryOpLe) => ("<=".to_string(), 30),
            Some(ir::BinaryOperator::BinaryOpGt) => (">".to_string(), 30),
            Some(ir::BinaryOperator::BinaryOpGe) => (">=".to_string(), 30),
            Some(ir::BinaryOperator::BinaryOpIn) => ("in".to_string(), 30),
            Some(ir::BinaryOperator::BinaryOpNotIn) => ("not in".to_string(), 30),
            Some(ir::BinaryOperator::BinaryOpAdd) => ("+".to_string(), 40),
            Some(ir::BinaryOperator::BinaryOpSub) => ("-".to_string(), 40),
            Some(ir::BinaryOperator::BinaryOpMul) => ("*".to_string(), 50),
            Some(ir::BinaryOperator::BinaryOpDiv) => ("/".to_string(), 50),
            Some(ir::BinaryOperator::BinaryOpFloorDiv) => ("//".to_string(), 50),
            Some(ir::BinaryOperator::BinaryOpMod) => ("%".to_string(), 50),
            Some(ir::BinaryOperator::BinaryOpUnspecified) | None => ("?".to_string(), 0),
        }
    }

    fn unary_operator(&self, op: i32) -> (String, i32) {
        match ir::UnaryOperator::try_from(op).ok() {
            Some(ir::UnaryOperator::UnaryOpNeg) => ("-".to_string(), 60),
            Some(ir::UnaryOperator::UnaryOpNot) => ("not ".to_string(), 60),
            Some(ir::UnaryOperator::UnaryOpUnspecified) | None => ("?".to_string(), 0),
        }
    }

    fn precedence(&self, kind: &str) -> i32 {
        match kind {
            "index" | "dot" => 80,
            _ => 0,
        }
    }

    fn format_literal(&self, lit: &ir::Literal) -> String {
        match lit.value.as_ref() {
            Some(ir::literal::Value::IntValue(value)) => value.to_string(),
            Some(ir::literal::Value::FloatValue(value)) => format!("{value:?}"),
            Some(ir::literal::Value::StringValue(value)) => {
                serde_json::to_string(value).unwrap_or_else(|_| "\"\"".to_string())
            }
            Some(ir::literal::Value::BoolValue(value)) => {
                if *value {
                    "True".to_string()
                } else {
                    "False".to_string()
                }
            }
            Some(ir::literal::Value::IsNone(_)) => "None".to_string(),
            None => "None".to_string(),
        }
    }

    fn indent_line(&self, level: usize, text: &str) -> String {
        format!("{}{}", self.indent.repeat(level), text)
    }
}

/// Convenience wrapper to format a program.
pub fn format_program(program: &ir::Program) -> String {
    IRFormatter::new(DEFAULT_INDENT).format_program(program)
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_INDENT, format_program};
    use waymark_ir_parser::IRParser;

    #[test]
    fn test_format_program_happy_path() {
        let source = [
            "fn main(input: [x], output: [y]):",
            "    y = x + 1",
            "    return y",
        ]
        .join("\n");

        let mut parser = IRParser::new(DEFAULT_INDENT);
        let program = parser.parse_program(&source).expect("parse program");
        let formatted = format_program(&program);

        assert_eq!(formatted, source);
    }
}
