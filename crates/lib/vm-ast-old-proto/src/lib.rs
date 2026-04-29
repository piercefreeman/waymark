//! The conversions between the native old AST types and protobuf codegen.

mod machinery;

#[cfg(test)]
mod tests;

use waymark_proto::ast;
use waymark_vm_ast_old::{self as vm_ast, Spanned};

use machinery::*;

pub use machinery::{ConvertError, convert};

impl Convert<ast::Program> for Converter {
    type To = vm_ast::Program;

    fn convert(from: ast::Program) -> Result<Self::To> {
        let functions = from
            .functions
            .into_iter()
            .map(convert)
            .collect::<Result<Vec<_>>>()?;
        Ok(vm_ast::Program { functions })
    }
}

impl Convert<ast::FunctionDef> for Converter {
    type To = Spanned<vm_ast::FunctionDef>;

    fn convert(from: ast::FunctionDef) -> Result<Self::To> {
        let value = vm_ast::FunctionDef {
            name: from.name,
            io: convert_required(from.io, "FunctionDef.io")?,
            body: convert_required(from.body, "FunctionDef.body")?,
        };
        spanned(value, from.span)
    }
}

impl Convert<ast::IoDecl> for Converter {
    type To = Spanned<vm_ast::IoDecl>;

    fn convert(from: ast::IoDecl) -> Result<Self::To> {
        let value = vm_ast::IoDecl {
            inputs: from.inputs,
            outputs: from.outputs,
        };
        spanned(value, from.span)
    }
}

impl Convert<ast::Block> for Converter {
    type To = Spanned<vm_ast::Block>;

    fn convert(from: ast::Block) -> Result<Self::To> {
        let statements = from
            .statements
            .into_iter()
            .map(convert)
            .collect::<Result<Vec<_>>>()?;
        let value = vm_ast::Block { statements };
        spanned(value, from.span)
    }
}

impl Convert<ast::Statement> for Converter {
    type To = Spanned<vm_ast::Statement>;

    fn convert(from: ast::Statement) -> Result<Self::To> {
        let value = match required(from.kind, "Statement.kind")? {
            ast::statement::Kind::Assignment(assign) => vm_ast::Statement::Assignment {
                targets: assign.targets,
                value: convert_required(assign.value, "Assignment.value")?,
            },
            ast::statement::Kind::ActionCall(call) => vm_ast::Statement::ActionCall {
                call: convert(call)?,
            },
            ast::statement::Kind::SpreadAction(spread) => {
                let spread: ast::SpreadAction = spread.into_owned();
                vm_ast::Statement::SpreadAction {
                    collection: convert_required(spread.collection, "SpreadAction.collection")?,
                    loop_var: spread.loop_var,
                    action: convert_required(spread.action, "SpreadAction.action")?,
                }
            }
            ast::statement::Kind::ParallelBlock(parallel) => {
                let parallel: ast::ParallelBlock = parallel.into_owned();
                let calls = parallel
                    .calls
                    .into_iter()
                    .map(convert)
                    .collect::<Result<Vec<_>>>()?;
                vm_ast::Statement::ParallelBlock { calls }
            }
            ast::statement::Kind::ForLoop(loop_stmt) => {
                let loop_stmt: ast::ForLoop = loop_stmt.into_owned();
                vm_ast::Statement::ForLoop {
                    loop_vars: loop_stmt.loop_vars,
                    iterable: convert_required(loop_stmt.iterable, "ForLoop.iterable")?,
                    body: convert_required(loop_stmt.block_body, "ForLoop.block_body")?,
                }
            }
            ast::statement::Kind::Conditional(conditional) => {
                let conditional: ast::Conditional = conditional.into_owned();
                let elif_branches = conditional
                    .elif_branches
                    .into_iter()
                    .map(convert)
                    .collect::<Result<Vec<_>>>()?;
                vm_ast::Statement::Conditional {
                    if_branch: convert_required(conditional.if_branch, "Conditional.if_branch")?,
                    elif_branches,
                    else_branch: convert_optional_owned::<ast::ElseBranch, _>(
                        conditional.else_branch,
                    )?,
                }
            }
            ast::statement::Kind::TryExcept(try_except) => {
                let try_except: ast::TryExcept = try_except.into_owned();
                let handlers = try_except
                    .handlers
                    .into_iter()
                    .map(convert)
                    .collect::<Result<Vec<_>>>()?;
                vm_ast::Statement::TryExcept {
                    handlers,
                    try_block: convert_required(try_except.try_block, "TryExcept.try_block")?,
                }
            }
            ast::statement::Kind::ReturnStmt(ret) => vm_ast::Statement::Return {
                value: convert_optional_owned(ret.value)?,
            },
            ast::statement::Kind::ExprStmt(expr_stmt) => vm_ast::Statement::ExprStmt {
                expr: convert_required(expr_stmt.expr, "ExprStmt.expr")?,
            },
            ast::statement::Kind::BreakStmt(_) => vm_ast::Statement::Break,
            ast::statement::Kind::ContinueStmt(_) => vm_ast::Statement::Continue,
            ast::statement::Kind::WhileLoop(loop_stmt) => {
                let loop_stmt: ast::WhileLoop = loop_stmt.into_owned();
                vm_ast::Statement::WhileLoop {
                    condition: convert_required(loop_stmt.condition, "WhileLoop.condition")?,
                    body: convert_required(loop_stmt.block_body, "WhileLoop.block_body")?,
                }
            }
            ast::statement::Kind::SleepStmt(sleep_stmt) => vm_ast::Statement::Sleep {
                duration: convert_optional_owned(sleep_stmt.duration)?,
            },
        };

        spanned(value, from.span)
    }
}

impl Convert<ast::Expr> for Converter {
    type To = Spanned<vm_ast::Expr>;

    fn convert(from: ast::Expr) -> Result<Self::To> {
        let value = match required(from.kind, "Expr.kind")? {
            ast::expr::Kind::Literal(literal) => vm_ast::Expr::Literal {
                value: convert(literal)?,
            },
            ast::expr::Kind::Variable(variable) => vm_ast::Expr::Variable {
                name: variable.name,
            },
            ast::expr::Kind::BinaryOp(binary) => {
                let binary: ast::BinaryOp = binary.into_owned();
                vm_ast::Expr::BinaryOp {
                    left: Box::new(convert_required_owned::<ast::Expr, _>(
                        binary.left,
                        "BinaryOp.left",
                    )?),
                    op: convert_binary_operator(binary.op)?,
                    right: Box::new(convert_required_owned::<ast::Expr, _>(
                        binary.right,
                        "BinaryOp.right",
                    )?),
                }
            }
            ast::expr::Kind::UnaryOp(unary) => {
                let unary: ast::UnaryOp = unary.into_owned();
                vm_ast::Expr::UnaryOp {
                    op: convert_unary_operator(unary.op)?,
                    operand: Box::new(convert_required_owned::<ast::Expr, _>(
                        unary.operand,
                        "UnaryOp.operand",
                    )?),
                }
            }
            ast::expr::Kind::List(list) => vm_ast::Expr::List {
                elements: list
                    .elements
                    .into_iter()
                    .map(convert)
                    .collect::<Result<Vec<_>>>()?,
            },
            ast::expr::Kind::Dict(dict) => vm_ast::Expr::Dict {
                entries: dict
                    .entries
                    .into_iter()
                    .map(convert)
                    .collect::<Result<Vec<_>>>()?,
            },
            ast::expr::Kind::Index(index) => {
                let index: ast::IndexAccess = index.into_owned();
                vm_ast::Expr::Index {
                    object: Box::new(convert_required_owned::<ast::Expr, _>(
                        index.object,
                        "IndexAccess.object",
                    )?),
                    index: Box::new(convert_required_owned::<ast::Expr, _>(
                        index.index,
                        "IndexAccess.index",
                    )?),
                }
            }
            ast::expr::Kind::Dot(dot) => {
                let dot: ast::DotAccess = dot.into_owned();
                vm_ast::Expr::Dot {
                    object: Box::new(convert_required_owned::<ast::Expr, _>(
                        dot.object,
                        "DotAccess.object",
                    )?),
                    attribute: dot.attribute,
                }
            }
            ast::expr::Kind::FunctionCall(call) => vm_ast::Expr::FunctionCall {
                call: convert(call)?,
            },
            ast::expr::Kind::ActionCall(call) => vm_ast::Expr::ActionCall {
                call: convert(call)?,
            },
            ast::expr::Kind::ParallelExpr(parallel) => {
                let parallel: ast::ParallelExpr = parallel.into_owned();
                vm_ast::Expr::ParallelExpr {
                    calls: parallel
                        .calls
                        .into_iter()
                        .map(convert)
                        .collect::<Result<Vec<_>>>()?,
                }
            }
            ast::expr::Kind::SpreadExpr(spread) => {
                let spread: ast::SpreadExpr = spread.into_owned();
                vm_ast::Expr::SpreadExpr {
                    collection: Box::new(convert_required_owned::<ast::Expr, _>(
                        spread.collection,
                        "SpreadExpr.collection",
                    )?),
                    loop_var: spread.loop_var,
                    action: convert_required(spread.action, "SpreadExpr.action")?,
                }
            }
        };

        spanned(value, from.span)
    }
}

impl Convert<ast::Literal> for Converter {
    type To = vm_ast::Literal;

    fn convert(from: ast::Literal) -> Result<Self::To> {
        let literal = match required(from.value, "Literal.value")? {
            ast::literal::Value::IntValue(value) => vm_ast::Literal::Int(value),
            ast::literal::Value::FloatValue(value) => vm_ast::Literal::Float(value),
            ast::literal::Value::StringValue(value) => vm_ast::Literal::String(value),
            ast::literal::Value::BoolValue(value) => vm_ast::Literal::Bool(value),
            ast::literal::Value::IsNone(_) => vm_ast::Literal::None,
        };
        Ok(literal)
    }
}

impl Convert<ast::DictEntry> for Converter {
    type To = vm_ast::DictEntry;

    fn convert(from: ast::DictEntry) -> Result<Self::To> {
        Ok(vm_ast::DictEntry {
            key: convert_required(from.key, "DictEntry.key")?,
            value: convert_required(from.value, "DictEntry.value")?,
        })
    }
}

impl Convert<ast::ActionCall> for Converter {
    type To = vm_ast::ActionCall;

    fn convert(from: ast::ActionCall) -> Result<Self::To> {
        let kwargs = from
            .kwargs
            .into_iter()
            .map(convert)
            .collect::<Result<Vec<_>>>()?;
        let policies = from
            .policies
            .into_iter()
            .map(convert)
            .collect::<Result<Vec<_>>>()?;

        Ok(vm_ast::ActionCall {
            action_name: from.action_name,
            kwargs,
            policies,
            module_name: from.module_name,
        })
    }
}

impl Convert<ast::Call> for Converter {
    type To = vm_ast::Call;

    fn convert(from: ast::Call) -> Result<Self::To> {
        let call = match required(from.kind, "Call.kind")? {
            ast::call::Kind::Action(action) => vm_ast::Call::Action(convert(action)?),
            ast::call::Kind::Function(function) => vm_ast::Call::Function(convert(function)?),
        };
        Ok(call)
    }
}

impl Convert<ast::FunctionCall> for Converter {
    type To = vm_ast::FunctionCall;

    fn convert(from: ast::FunctionCall) -> Result<Self::To> {
        let args = from
            .args
            .into_iter()
            .map(convert)
            .collect::<Result<Vec<_>>>()?;
        let kwargs = from
            .kwargs
            .into_iter()
            .map(convert)
            .collect::<Result<Vec<_>>>()?;

        Ok(vm_ast::FunctionCall {
            name: from.name,
            args,
            kwargs,
            global_function: convert_global_function(from.global_function)?,
        })
    }
}

impl Convert<ast::Kwarg> for Converter {
    type To = vm_ast::Kwarg;

    fn convert(from: ast::Kwarg) -> Result<Self::To> {
        Ok(vm_ast::Kwarg {
            name: from.name,
            value: convert_required(from.value, "Kwarg.value")?,
        })
    }
}

impl Convert<ast::IfBranch> for Converter {
    type To = Spanned<vm_ast::IfBranch>;

    fn convert(from: ast::IfBranch) -> Result<Self::To> {
        let value = vm_ast::IfBranch {
            condition: convert_required(from.condition, "IfBranch.condition")?,
            body: convert_required(from.block_body, "IfBranch.block_body")?,
        };
        spanned(value, from.span)
    }
}

impl Convert<ast::ElifBranch> for Converter {
    type To = Spanned<vm_ast::ElifBranch>;

    fn convert(from: ast::ElifBranch) -> Result<Self::To> {
        let value = vm_ast::ElifBranch {
            condition: convert_required(from.condition, "ElifBranch.condition")?,
            body: convert_required(from.block_body, "ElifBranch.block_body")?,
        };
        spanned(value, from.span)
    }
}

impl Convert<ast::ElseBranch> for Converter {
    type To = Spanned<vm_ast::ElseBranch>;

    fn convert(from: ast::ElseBranch) -> Result<Self::To> {
        let value = vm_ast::ElseBranch {
            body: convert_required(from.block_body, "ElseBranch.block_body")?,
        };
        spanned(value, from.span)
    }
}

impl Convert<ast::ExceptHandler> for Converter {
    type To = Spanned<vm_ast::ExceptHandler>;

    fn convert(from: ast::ExceptHandler) -> Result<Self::To> {
        let value = vm_ast::ExceptHandler {
            exception_types: from.exception_types,
            exception_var: from.exception_var,
            body: convert_required(from.block_body, "ExceptHandler.block_body")?,
        };
        spanned(value, from.span)
    }
}

impl Convert<ast::PolicyBracket> for Converter {
    type To = vm_ast::PolicyBracket;

    fn convert(from: ast::PolicyBracket) -> Result<Self::To> {
        let policy = match required(from.kind, "PolicyBracket.kind")? {
            ast::policy_bracket::Kind::Retry(retry) => {
                vm_ast::PolicyBracket::Retry(convert(retry)?)
            }
            ast::policy_bracket::Kind::Timeout(timeout) => {
                vm_ast::PolicyBracket::Timeout(convert(timeout)?)
            }
        };
        Ok(policy)
    }
}

impl Convert<ast::RetryPolicy> for Converter {
    type To = vm_ast::RetryPolicy;

    fn convert(from: ast::RetryPolicy) -> Result<Self::To> {
        Ok(vm_ast::RetryPolicy {
            exception_types: from.exception_types,
            max_retries: from.max_retries,
            backoff: from.backoff.map(convert).transpose()?,
        })
    }
}

impl Convert<ast::TimeoutPolicy> for Converter {
    type To = vm_ast::TimeoutPolicy;

    fn convert(from: ast::TimeoutPolicy) -> Result<Self::To> {
        Ok(vm_ast::TimeoutPolicy {
            timeout: convert_required(from.timeout, "TimeoutPolicy.timeout")?,
        })
    }
}

impl Convert<ast::Duration> for Converter {
    type To = vm_ast::DurationLiteral;

    fn convert(from: ast::Duration) -> Result<Self::To> {
        Ok(vm_ast::DurationLiteral {
            seconds: from.seconds,
        })
    }
}

impl Convert<ast::Span> for Converter {
    type To = vm_ast::Span;

    fn convert(from: ast::Span) -> Result<Self::To> {
        Ok(vm_ast::Span {
            start_line: from.start_line,
            start_col: from.start_col,
            end_line: from.end_line,
            end_col: from.end_col,
        })
    }
}

impl Convert<ast::BinaryOperator> for Converter {
    type To = vm_ast::BinaryOperator;

    fn convert(from: ast::BinaryOperator) -> Result<Self::To> {
        let operator = match from {
            ast::BinaryOperator::BinaryOpUnspecified => {
                return Err(ConvertError::UnspecifiedEnumValue {
                    enum_name: "BinaryOperator",
                });
            }
            ast::BinaryOperator::BinaryOpAdd => vm_ast::BinaryOperator::Add,
            ast::BinaryOperator::BinaryOpSub => vm_ast::BinaryOperator::Sub,
            ast::BinaryOperator::BinaryOpMul => vm_ast::BinaryOperator::Mul,
            ast::BinaryOperator::BinaryOpDiv => vm_ast::BinaryOperator::Div,
            ast::BinaryOperator::BinaryOpFloorDiv => vm_ast::BinaryOperator::FloorDiv,
            ast::BinaryOperator::BinaryOpMod => vm_ast::BinaryOperator::Mod,
            ast::BinaryOperator::BinaryOpEq => vm_ast::BinaryOperator::Eq,
            ast::BinaryOperator::BinaryOpNe => vm_ast::BinaryOperator::Ne,
            ast::BinaryOperator::BinaryOpLt => vm_ast::BinaryOperator::Lt,
            ast::BinaryOperator::BinaryOpLe => vm_ast::BinaryOperator::Le,
            ast::BinaryOperator::BinaryOpGt => vm_ast::BinaryOperator::Gt,
            ast::BinaryOperator::BinaryOpGe => vm_ast::BinaryOperator::Ge,
            ast::BinaryOperator::BinaryOpIn => vm_ast::BinaryOperator::In,
            ast::BinaryOperator::BinaryOpNotIn => vm_ast::BinaryOperator::NotIn,
            ast::BinaryOperator::BinaryOpAnd => vm_ast::BinaryOperator::And,
            ast::BinaryOperator::BinaryOpOr => vm_ast::BinaryOperator::Or,
        };
        Ok(operator)
    }
}

impl Convert<ast::UnaryOperator> for Converter {
    type To = vm_ast::UnaryOperator;

    fn convert(from: ast::UnaryOperator) -> Result<Self::To> {
        let operator = match from {
            ast::UnaryOperator::UnaryOpUnspecified => {
                return Err(ConvertError::UnspecifiedEnumValue {
                    enum_name: "UnaryOperator",
                });
            }
            ast::UnaryOperator::UnaryOpNeg => vm_ast::UnaryOperator::Neg,
            ast::UnaryOperator::UnaryOpNot => vm_ast::UnaryOperator::Not,
        };
        Ok(operator)
    }
}

impl Convert<ast::GlobalFunction> for Converter {
    type To = vm_ast::GlobalFunction;

    fn convert(from: ast::GlobalFunction) -> Result<Self::To> {
        let function = match from {
            ast::GlobalFunction::Unspecified => {
                return Err(ConvertError::UnspecifiedEnumValue {
                    enum_name: "GlobalFunction",
                });
            }
            ast::GlobalFunction::Range => vm_ast::GlobalFunction::Range,
            ast::GlobalFunction::Len => vm_ast::GlobalFunction::Len,
            ast::GlobalFunction::Enumerate => vm_ast::GlobalFunction::Enumerate,
            ast::GlobalFunction::Isexception => vm_ast::GlobalFunction::IsException,
        };
        Ok(function)
    }
}
