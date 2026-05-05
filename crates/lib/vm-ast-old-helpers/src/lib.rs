use waymark_vm_ast_old::{
    ActionCall, BinaryOperator, Block, Expr, FunctionCall, FunctionDef, IoDecl, Kwarg, Literal,
    Program, Span, Spanned, Statement,
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
    spanned(Statement::Assignment {
        targets: vec![target.to_owned()],
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

pub fn while_stmt() -> Spanned<Statement> {
    spanned(Statement::WhileLoop {
        condition: int(1),
        body: spanned(Block {
            statements: Vec::new(),
        }),
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

pub fn add(left: Spanned<Expr>, right: Spanned<Expr>) -> Spanned<Expr> {
    spanned(Expr::BinaryOp {
        left: Box::new(left),
        op: BinaryOperator::Add,
        right: Box::new(right),
    })
}

pub fn function_expr(name: &str, args: Vec<Spanned<Expr>>) -> Spanned<Expr> {
    spanned(Expr::FunctionCall {
        call: FunctionCall {
            name: name.to_owned(),
            args,
            kwargs: Vec::new(),
            global_function: None,
        },
    })
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
