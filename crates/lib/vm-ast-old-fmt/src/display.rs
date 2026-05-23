use core::fmt;

use waymark_vm_ast_old as ast;

use crate::Fmt;

impl<'a> fmt::Display for Fmt<'a, ast::Program> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.padded_fmt(f, 0)
    }
}

impl<'a> fmt::Display for Fmt<'a, ast::Spanned<ast::FunctionDef>> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.padded_fmt(f, 0)
    }
}

impl<'a> fmt::Display for Fmt<'a, ast::Spanned<ast::Block>> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.padded_fmt(f, 0)
    }
}

impl<'a> fmt::Display for Fmt<'a, ast::Spanned<ast::Statement>> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.padded_fmt(f, 0)
    }
}

impl<'a> fmt::Display for Fmt<'a, ast::Spanned<ast::Expr>> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_with_prec(f, 0)
    }
}

impl<'a> fmt::Display for Fmt<'a, ast::Call> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            ast::Call::Action(action) => write!(f, "{}", Fmt(action)),
            ast::Call::Function(function) => write!(f, "{}", Fmt(function)),
        }
    }
}

impl<'a> fmt::Display for Fmt<'a, ast::ActionCall> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("@")?;

        if let Some(module_name) = &self.0.module_name {
            f.write_str(module_name)?;
            f.write_str(".")?;
        }

        f.write_str(&self.0.action_name)?;
        f.write_str("(")?;

        for (index, kwarg) in self.0.kwargs.iter().enumerate() {
            if index > 0 {
                f.write_str(", ")?;
            }

            write!(f, "{}={}", kwarg.name, Fmt(&kwarg.value))?;
        }

        f.write_str(")")?;

        for policy in &self.0.policies {
            write!(f, " {}", Fmt(policy))?;
        }

        Ok(())
    }
}

impl<'a> fmt::Display for Fmt<'a, ast::FunctionCall> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())?;
        f.write_str("(")?;

        let mut needs_separator = false;

        for arg in &self.0.args {
            if needs_separator {
                f.write_str(", ")?;
            }

            write!(f, "{}", Fmt(arg))?;
            needs_separator = true;
        }

        for kwarg in &self.0.kwargs {
            if needs_separator {
                f.write_str(", ")?;
            }

            write!(f, "{}={}", kwarg.name, Fmt(&kwarg.value))?;
            needs_separator = true;
        }

        f.write_str(")")
    }
}

impl<'a> fmt::Display for Fmt<'a, ast::PolicyBracket> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            ast::PolicyBracket::Retry(retry) => {
                f.write_str("[")?;

                if !retry.exception_types.is_empty() {
                    for (index, exception_type) in retry.exception_types.iter().enumerate() {
                        if index > 0 {
                            f.write_str(", ")?;
                        }

                        f.write_str(exception_type)?;
                    }

                    f.write_str(" -> ")?;
                }

                write!(f, "retry: {}", retry.max_retries)?;

                if let Some(backoff) = &retry.backoff {
                    write!(f, ", backoff: {}", Fmt(backoff))?;
                }

                f.write_str("]")
            }
            ast::PolicyBracket::Timeout(timeout) => {
                write!(f, "[timeout: {}]", Fmt(&timeout.timeout))
            }
        }
    }
}

impl<'a> fmt::Display for Fmt<'a, ast::DurationLiteral> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let seconds = self.0.seconds;

        if seconds != 0 && seconds.is_multiple_of(3600) {
            return write!(f, "{}h", seconds / 3600);
        }

        if seconds != 0 && seconds.is_multiple_of(60) {
            return write!(f, "{}m", seconds / 60);
        }

        write!(f, "{seconds}s")
    }
}

impl<'a> fmt::Display for Fmt<'a, ast::Literal> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            ast::Literal::Int(value) => write!(f, "{value}"),
            ast::Literal::Float(value) => write!(f, "{value:?}"),
            ast::Literal::String(value) => {
                let rendered =
                    serde_json::to_string(value).unwrap_or_else(|_| format!("\"{value}\""));
                f.write_str(&rendered)
            }
            ast::Literal::Bool(value) => {
                if *value {
                    f.write_str("True")
                } else {
                    f.write_str("False")
                }
            }
            ast::Literal::None => f.write_str("None"),
        }
    }
}
