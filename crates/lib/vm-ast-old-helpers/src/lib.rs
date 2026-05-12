use waymark_vm_ast_old::{
    ActionCall, BinaryOperator, Block, Call, ElifBranch, ElseBranch, Expr, FunctionCall,
    FunctionDef, IfBranch, IoDecl, Kwarg, Literal, Program, Span, Spanned, Statement,
};

pub fn span() -> Span {
    Span {
        start_line: 0,
        start_col: 0,
        end_line: 0,
        end_col: 0,
    }
}

pub fn spanned<T>(value: T) -> Spanned<T> {
    Spanned {
        value,
        span: span(),
    }
}

pub fn program(functions: Vec<Spanned<FunctionDef>>) -> Program {
    Program { functions }
}

pub fn block(statements: Vec<Spanned<Statement>>) -> Spanned<Block> {
    spanned(Block { statements })
}

pub fn int(value: i64) -> Spanned<Expr> {
    spanned(Expr::Literal {
        value: Literal::Int(value),
    })
}

pub fn float(value: f64) -> Spanned<Expr> {
    spanned(Expr::Literal {
        value: Literal::Float(value),
    })
}

pub fn variable(name: &str) -> Spanned<Expr> {
    spanned(Expr::Variable {
        name: name.to_owned(),
    })
}

pub fn assignment(target: &str, value: Spanned<Expr>) -> Spanned<Statement> {
    assignment_targets(&[target], value)
}

pub fn assignment_targets(targets: &[&str], value: Spanned<Expr>) -> Spanned<Statement> {
    spanned(Statement::Assignment {
        targets: targets.iter().map(|target| (*target).to_owned()).collect(),
        value,
    })
}

pub fn return_stmt(value: Option<Spanned<Expr>>) -> Spanned<Statement> {
    spanned(Statement::Return { value })
}

pub fn action_stmt(name: &str) -> Spanned<Statement> {
    spanned(Statement::ActionCall {
        call: ActionCall {
            action_name: name.to_owned(),
            kwargs: Vec::new(),
            policies: Vec::new(),
            module_name: None,
        },
    })
}

pub fn parallel_stmt(calls: Vec<Call>) -> Spanned<Statement> {
    spanned(Statement::ParallelBlock { calls })
}

pub fn parallel_expr(calls: Vec<Call>) -> Spanned<Expr> {
    spanned(Expr::ParallelExpr { calls })
}

pub fn conditional_stmt(
    if_condition: Spanned<Expr>,
    if_body: Vec<Spanned<Statement>>,
    elif_branches: Vec<(Spanned<Expr>, Vec<Spanned<Statement>>)>,
    else_body: Option<Vec<Spanned<Statement>>>,
) -> Spanned<Statement> {
    spanned(Statement::Conditional {
        if_branch: spanned(IfBranch {
            condition: if_condition,
            body: block(if_body),
        }),
        elif_branches: elif_branches
            .into_iter()
            .map(|(condition, statements)| {
                spanned(ElifBranch {
                    condition,
                    body: block(statements),
                })
            })
            .collect(),
        else_branch: else_body.map(|statements| {
            spanned(ElseBranch {
                body: block(statements),
            })
        }),
    })
}

pub fn while_stmt(condition: Spanned<Expr>, body: Vec<Spanned<Statement>>) -> Spanned<Statement> {
    spanned(Statement::WhileLoop {
        condition,
        body: block(body),
    })
}

pub fn break_stmt() -> Spanned<Statement> {
    spanned(Statement::Break)
}

pub fn continue_stmt() -> Spanned<Statement> {
    spanned(Statement::Continue)
}

pub fn for_stmt(
    loop_vars: &[&str],
    iterable: Spanned<Expr>,
    body: Vec<Spanned<Statement>>,
) -> Spanned<Statement> {
    spanned(Statement::ForLoop {
        loop_vars: loop_vars
            .iter()
            .map(|loop_var| (*loop_var).to_owned())
            .collect(),
        iterable,
        body: block(body),
    })
}

pub fn function(
    name: &str,
    inputs: &[&str],
    statements: Vec<Spanned<Statement>>,
) -> Spanned<FunctionDef> {
    spanned(FunctionDef {
        name: name.to_owned(),
        io: spanned(IoDecl {
            inputs: inputs.iter().map(|input| (*input).to_owned()).collect(),
            outputs: Vec::new(),
        }),
        body: spanned(Block { statements }),
    })
}

pub fn binary_expr(left: Spanned<Expr>, op: BinaryOperator, right: Spanned<Expr>) -> Spanned<Expr> {
    spanned(Expr::BinaryOp {
        left: Box::new(left),
        op,
        right: Box::new(right),
    })
}

#[deprecated]
pub fn add(left: Spanned<Expr>, right: Spanned<Expr>) -> Spanned<Expr> {
    binary_expr(left, BinaryOperator::Add, right)
}

pub fn unary_expr(op: waymark_vm_ast_old::UnaryOperator, operand: Spanned<Expr>) -> Spanned<Expr> {
    spanned(Expr::UnaryOp {
        op,
        operand: Box::new(operand),
    })
}

pub fn function_expr(name: &str, args: Vec<Spanned<Expr>>) -> Spanned<Expr> {
    spanned(Expr::FunctionCall {
        call: function_call(name, args),
    })
}

pub fn function_call(name: &str, args: Vec<Spanned<Expr>>) -> FunctionCall {
    FunctionCall {
        name: name.to_owned(),
        args,
        kwargs: Vec::new(),
        global_function: None,
    }
}

pub fn action_call(name: &str, kwargs: Vec<(&str, Spanned<Expr>)>) -> ActionCall {
    ActionCall {
        action_name: name.to_owned(),
        kwargs: kwargs
            .into_iter()
            .map(|(name, value)| Kwarg {
                name: name.to_owned(),
                value,
            })
            .collect(),
        policies: Vec::new(),
        module_name: None,
    }
}

pub fn action_expr(name: &str, kwargs: Vec<(&str, Spanned<Expr>)>) -> Spanned<Expr> {
    spanned(Expr::ActionCall {
        call: action_call(name, kwargs),
    })
}
