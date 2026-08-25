use core::fmt;

use waymark_vm_ast_old as ast;

use crate::Fmt;

const DEFAULT_INDENT: &str = "    ";

impl<'a> Fmt<'a, ast::Program> {
    pub(crate) fn padded_fmt(&self, f: &mut fmt::Formatter<'_>, padding: usize) -> fmt::Result {
        for (index, function) in self.0.functions.iter().enumerate() {
            if index > 0 {
                f.write_str("\n\n")?;
            }

            Fmt(function).padded_fmt(f, padding)?;
        }

        Ok(())
    }
}

impl<'a> Fmt<'a, ast::Spanned<ast::FunctionDef>> {
    pub(crate) fn padded_fmt(&self, f: &mut fmt::Formatter<'_>, padding: usize) -> fmt::Result {
        write_indent(f, padding)?;
        write!(f, "fn {}(input: [", self.0.value.name)?;
        write_joined_strings(f, &self.0.value.io.value.inputs)?;
        f.write_str("], output: [")?;
        write_joined_strings(f, &self.0.value.io.value.outputs)?;
        f.write_str("]):\n")?;
        Fmt(&self.0.value.body).padded_fmt(f, padding + 1)
    }
}

impl<'a> Fmt<'a, ast::Spanned<ast::Block>> {
    pub(crate) fn padded_fmt(&self, f: &mut fmt::Formatter<'_>, padding: usize) -> fmt::Result {
        if self.0.value.statements.is_empty() {
            write_indent(f, padding)?;
            return f.write_str("pass");
        }

        for (index, statement) in self.0.value.statements.iter().enumerate() {
            if index > 0 {
                f.write_str("\n")?;
            }

            Fmt(statement).padded_fmt(f, padding)?;
        }

        Ok(())
    }
}

impl<'a> Fmt<'a, ast::Spanned<ast::Statement>> {
    pub(crate) fn padded_fmt(&self, f: &mut fmt::Formatter<'_>, padding: usize) -> fmt::Result {
        match &self.0.value {
            ast::Statement::Assignment { targets, value } => match &value.value {
                ast::Expr::ParallelExpr { calls } => {
                    Fmt(calls.as_slice()).parallel_block_fmt(f, padding, Some(targets))
                }
                ast::Expr::SpreadExpr {
                    collection,
                    loop_var,
                    call,
                } => {
                    write_indent(f, padding)?;
                    write_names_or_placeholder(f, targets)?;
                    f.write_str(" = ")?;
                    write_spread(f, collection, loop_var, Fmt(call))
                }
                _ => {
                    write_indent(f, padding)?;
                    write_names_or_placeholder(f, targets)?;
                    f.write_str(" = ")?;
                    Fmt(value).fmt_with_prec(f, 0)
                }
            },
            ast::Statement::ActionCall { call } => {
                write_indent(f, padding)?;
                write!(f, "{}", Fmt(call))
            }
            ast::Statement::SpreadAction {
                collection,
                loop_var,
                action,
            } => {
                write_indent(f, padding)?;
                write_spread(f, collection, loop_var, Fmt(action))
            }
            ast::Statement::ParallelBlock { calls } => {
                Fmt(calls.as_slice()).parallel_block_fmt(f, padding, None)
            }
            ast::Statement::ForLoop {
                loop_vars,
                iterable,
                body,
            } => {
                write_indent(f, padding)?;
                f.write_str("for ")?;
                write_names_or_placeholder(f, loop_vars)?;
                f.write_str(" in ")?;
                Fmt(iterable).fmt_with_prec(f, 0)?;
                f.write_str(":\n")?;
                Fmt(body).padded_fmt(f, padding + 1)
            }
            ast::Statement::WhileLoop { condition, body } => {
                write_indent(f, padding)?;
                f.write_str("while ")?;
                Fmt(condition).fmt_with_prec(f, 0)?;
                f.write_str(":\n")?;
                Fmt(body).padded_fmt(f, padding + 1)
            }
            ast::Statement::Conditional {
                if_branch,
                elif_branches,
                else_branch,
            } => {
                write_indent(f, padding)?;
                f.write_str("if ")?;
                Fmt(&if_branch.value.condition).fmt_with_prec(f, 0)?;
                f.write_str(":\n")?;
                Fmt(&if_branch.value.body).padded_fmt(f, padding + 1)?;

                for branch in elif_branches {
                    f.write_str("\n")?;
                    write_indent(f, padding)?;
                    f.write_str("elif ")?;
                    Fmt(&branch.value.condition).fmt_with_prec(f, 0)?;
                    f.write_str(":\n")?;
                    Fmt(&branch.value.body).padded_fmt(f, padding + 1)?;
                }

                if let Some(branch) = else_branch {
                    f.write_str("\n")?;
                    write_indent(f, padding)?;
                    f.write_str("else:\n")?;
                    Fmt(&branch.value.body).padded_fmt(f, padding + 1)?;
                }

                Ok(())
            }
            ast::Statement::TryExcept {
                handlers,
                try_block,
            } => {
                write_indent(f, padding)?;
                f.write_str("try:\n")?;
                Fmt(try_block).padded_fmt(f, padding + 1)?;

                for handler in handlers {
                    f.write_str("\n")?;
                    write_indent(f, padding)?;
                    f.write_str("except")?;

                    if !handler.value.exception_types.is_empty() {
                        f.write_str(" ")?;
                        write_joined_strings(f, &handler.value.exception_types)?;
                    }

                    if let Some(exception_var) = handler.value.exception_var.as_deref()
                        && !exception_var.is_empty()
                    {
                        write!(f, " as {exception_var}")?;
                    }

                    f.write_str(":\n")?;
                    Fmt(&handler.value.body).padded_fmt(f, padding + 1)?;
                }

                Ok(())
            }
            ast::Statement::Return { value } => {
                write_indent(f, padding)?;
                f.write_str("return")?;

                if let Some(expr) = value {
                    f.write_str(" ")?;
                    Fmt(expr).fmt_with_prec(f, 0)?;
                }

                Ok(())
            }
            ast::Statement::ExprStmt { expr } => match &expr.value {
                ast::Expr::ParallelExpr { calls } => {
                    Fmt(calls.as_slice()).parallel_block_fmt(f, padding, None)
                }
                ast::Expr::SpreadExpr {
                    collection,
                    loop_var,
                    call,
                } => {
                    write_indent(f, padding)?;
                    write_spread(f, collection, loop_var, Fmt(call))
                }
                _ => {
                    write_indent(f, padding)?;
                    Fmt(expr).fmt_with_prec(f, 0)
                }
            },
            ast::Statement::Break => {
                write_indent(f, padding)?;
                f.write_str("break")
            }
            ast::Statement::Continue => {
                write_indent(f, padding)?;
                f.write_str("continue")
            }
            ast::Statement::Sleep { duration } => {
                write_indent(f, padding)?;
                f.write_str("sleep ")?;
                Fmt(duration).fmt_with_prec(f, 0)
            }
        }
    }
}

impl<'a> Fmt<'a, [ast::Call]> {
    fn parallel_block_fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
        padding: usize,
        targets: Option<&[String]>,
    ) -> fmt::Result {
        write_indent(f, padding)?;

        if let Some(targets) = targets {
            write_names_or_placeholder(f, targets)?;
            f.write_str(" = parallel:")?;
        } else {
            f.write_str("parallel:")?;
        }

        if self.0.is_empty() {
            f.write_str("\n")?;
            write_indent(f, padding + 1)?;
            return f.write_str("pass");
        }

        for call in self.0 {
            f.write_str("\n")?;
            write_indent(f, padding + 1)?;
            write!(f, "{}", Fmt(call))?;
        }

        Ok(())
    }
}

impl<'a> Fmt<'a, ast::FunctionCall> {
    pub(crate) fn name(&self) -> &str {
        if !self.0.name.is_empty() {
            &self.0.name
        } else {
            match self.0.global_function.as_ref() {
                Some(ast::GlobalFunction::Range) => "range",
                Some(ast::GlobalFunction::Len) => "len",
                Some(ast::GlobalFunction::Enumerate) => "enumerate",
                Some(ast::GlobalFunction::IsException) => "isexception",
                None => "fn",
            }
        }
    }
}

impl<'a> Fmt<'a, ast::Spanned<ast::Expr>> {
    pub(crate) fn fmt_with_prec(
        &self,
        f: &mut fmt::Formatter<'_>,
        parent_prec: i32,
    ) -> fmt::Result {
        match &self.0.value {
            ast::Expr::Literal { value } => write!(f, "{}", Fmt(value)),
            ast::Expr::Variable { name } => f.write_str(name),
            ast::Expr::BinaryOp { left, op, right } => {
                let (op_str, prec) = binary_operator(op);
                write_maybe_parenthesized(f, prec, parent_prec, |f| {
                    Fmt(left.as_ref()).fmt_with_prec(f, prec)?;
                    write!(f, " {op_str} ")?;
                    Fmt(right.as_ref()).fmt_with_prec(f, prec + 1)
                })
            }
            ast::Expr::UnaryOp { op, operand } => {
                let (op_str, prec) = unary_operator(op);
                write_maybe_parenthesized(f, prec, parent_prec, |f| {
                    f.write_str(op_str)?;
                    Fmt(operand.as_ref()).fmt_with_prec(f, prec)
                })
            }
            ast::Expr::List { elements } => {
                f.write_str("[")?;

                for (index, element) in elements.iter().enumerate() {
                    if index > 0 {
                        f.write_str(", ")?;
                    }

                    Fmt(element).fmt_with_prec(f, 0)?;
                }

                f.write_str("]")
            }
            ast::Expr::Dict { entries } => {
                f.write_str("{")?;

                for (index, entry) in entries.iter().enumerate() {
                    if index > 0 {
                        f.write_str(", ")?;
                    }

                    Fmt(&entry.key).fmt_with_prec(f, 0)?;
                    f.write_str(": ")?;
                    Fmt(&entry.value).fmt_with_prec(f, 0)?;
                }

                f.write_str("}")
            }
            ast::Expr::Index { object, index } => {
                let prec = precedence("index");
                write_maybe_parenthesized(f, prec, parent_prec, |f| {
                    Fmt(object.as_ref()).fmt_with_prec(f, prec)?;
                    f.write_str("[")?;
                    Fmt(index.as_ref()).fmt_with_prec(f, 0)?;
                    f.write_str("]")
                })
            }
            ast::Expr::Dot { object, attribute } => {
                let prec = precedence("dot");
                write_maybe_parenthesized(f, prec, parent_prec, |f| {
                    Fmt(object.as_ref()).fmt_with_prec(f, prec)?;
                    write!(f, ".{attribute}")
                })
            }
            ast::Expr::FunctionCall { call } => write!(f, "{}", Fmt(call)),
            ast::Expr::ActionCall { call } => write!(f, "{}", Fmt(call)),
            ast::Expr::ParallelExpr { calls } => {
                f.write_str("parallel(")?;

                for (index, call) in calls.iter().enumerate() {
                    if index > 0 {
                        f.write_str(", ")?;
                    }

                    write!(f, "{}", Fmt(call))?;
                }

                f.write_str(")")
            }
            ast::Expr::SpreadExpr {
                collection,
                loop_var,
                call,
            } => write_spread(f, collection, loop_var, Fmt(call)),
        }
    }
}

fn write_names_or_placeholder(f: &mut fmt::Formatter<'_>, names: &[String]) -> fmt::Result {
    if names.is_empty() {
        f.write_str("_")
    } else {
        write_joined_strings(f, names)
    }
}

fn write_joined_strings(f: &mut fmt::Formatter<'_>, values: &[String]) -> fmt::Result {
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            f.write_str(", ")?;
        }

        f.write_str(value)?;
    }

    Ok(())
}

fn write_spread(
    f: &mut fmt::Formatter<'_>,
    collection: &ast::Spanned<ast::Expr>,
    loop_var: &str,
    call: impl fmt::Display,
) -> fmt::Result {
    f.write_str("spread ")?;
    Fmt(collection).fmt_with_prec(f, 0)?;
    write!(f, ":{loop_var} -> ")?;
    write!(f, "{call}")
}

fn write_maybe_parenthesized(
    f: &mut fmt::Formatter<'_>,
    prec: i32,
    parent_prec: i32,
    inner: impl FnOnce(&mut fmt::Formatter<'_>) -> fmt::Result,
) -> fmt::Result {
    if prec < parent_prec {
        f.write_str("(")?;
        inner(f)?;
        f.write_str(")")
    } else {
        inner(f)
    }
}

fn write_indent(f: &mut fmt::Formatter<'_>, level: usize) -> fmt::Result {
    for _ in 0..level {
        f.write_str(DEFAULT_INDENT)?;
    }

    Ok(())
}

fn binary_operator(op: &ast::BinaryOperator) -> (&'static str, i32) {
    match op {
        ast::BinaryOperator::Or => ("or", 10),
        ast::BinaryOperator::And => ("and", 20),
        ast::BinaryOperator::Eq => ("==", 30),
        ast::BinaryOperator::Ne => ("!=", 30),
        ast::BinaryOperator::Lt => ("<", 30),
        ast::BinaryOperator::Le => ("<=", 30),
        ast::BinaryOperator::Gt => (">", 30),
        ast::BinaryOperator::Ge => (">=", 30),
        ast::BinaryOperator::In => ("in", 30),
        ast::BinaryOperator::NotIn => ("not in", 30),
        ast::BinaryOperator::Add => ("+", 40),
        ast::BinaryOperator::Sub => ("-", 40),
        ast::BinaryOperator::Mul => ("*", 50),
        ast::BinaryOperator::Div => ("/", 50),
        ast::BinaryOperator::FloorDiv => ("//", 50),
        ast::BinaryOperator::Mod => ("%", 50),
    }
}

fn unary_operator(op: &ast::UnaryOperator) -> (&'static str, i32) {
    match op {
        ast::UnaryOperator::Neg => ("-", 60),
        ast::UnaryOperator::Not => ("not ", 60),
    }
}

fn precedence(kind: &str) -> i32 {
    match kind {
        "index" | "dot" => 80,
        _ => 0,
    }
}
