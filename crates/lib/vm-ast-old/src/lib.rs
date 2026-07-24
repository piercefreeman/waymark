//! The old ast from the DAG-based runner.

#[derive(Debug, Clone, PartialEq)]
pub struct Program {
    pub functions: Vec<Spanned<FunctionDef>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FunctionDef {
    pub name: String,
    pub io: Spanned<IoDecl>,
    pub body: Spanned<Block>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IoDecl {
    pub inputs: Vec<String>,
    pub outputs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Block {
    pub statements: Vec<Spanned<Statement>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Assignment {
        targets: Vec<String>,
        value: Spanned<Expr>,
    },
    ActionCall {
        call: ActionCall,
    },
    SpreadAction {
        collection: Spanned<Expr>,
        loop_var: String,
        action: ActionCall,
    },
    ParallelBlock {
        calls: Vec<Call>,
    },
    ForLoop {
        loop_vars: Vec<String>,
        iterable: Spanned<Expr>,
        body: Spanned<Block>,
    },
    WhileLoop {
        condition: Spanned<Expr>,
        body: Spanned<Block>,
    },
    Conditional {
        if_branch: Spanned<IfBranch>,
        elif_branches: Vec<Spanned<ElifBranch>>,
        else_branch: Option<Spanned<ElseBranch>>,
    },
    TryExcept {
        handlers: Vec<Spanned<ExceptHandler>>,
        try_block: Spanned<Block>,
    },
    Return {
        value: Option<Spanned<Expr>>,
    },
    ExprStmt {
        expr: Spanned<Expr>,
    },
    Break,
    Continue,
    Sleep {
        duration: Spanned<Expr>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Literal {
        value: Literal,
    },
    Variable {
        name: String,
    },
    BinaryOp {
        left: Box<Spanned<Expr>>,
        op: BinaryOperator,
        right: Box<Spanned<Expr>>,
    },
    UnaryOp {
        op: UnaryOperator,
        operand: Box<Spanned<Expr>>,
    },
    List {
        elements: Vec<Spanned<Expr>>,
    },
    Dict {
        entries: Vec<DictEntry>,
    },
    Index {
        object: Box<Spanned<Expr>>,
        index: Box<Spanned<Expr>>,
    },
    Dot {
        object: Box<Spanned<Expr>>,
        attribute: String,
    },
    FunctionCall {
        call: FunctionCall,
    },
    ActionCall {
        call: ActionCall,
    },
    ParallelExpr {
        calls: Vec<Call>,
    },
    SpreadExpr {
        collection: Box<Spanned<Expr>>,
        loop_var: String,
        action: ActionCall,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
    None,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DictEntry {
    pub key: Spanned<Expr>,
    pub value: Spanned<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ActionCall {
    pub runtime: waymark_action_core::ActionRuntime,
    pub action_name: String,
    pub kwargs: Vec<Kwarg>,
    pub policies: Vec<PolicyBracket>,
    pub module_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Call {
    Action(ActionCall),
    Function(FunctionCall),
}

#[derive(Debug, Clone, PartialEq)]
pub struct FunctionCall {
    pub name: String,
    pub args: Vec<Spanned<Expr>>,
    pub kwargs: Vec<Kwarg>,
    pub global_function: Option<GlobalFunction>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Kwarg {
    pub name: String,
    pub value: Spanned<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IfBranch {
    pub condition: Spanned<Expr>,
    pub body: Spanned<Block>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ElifBranch {
    pub condition: Spanned<Expr>,
    pub body: Spanned<Block>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ElseBranch {
    pub body: Spanned<Block>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExceptHandler {
    pub exception_types: Vec<String>,
    pub exception_var: Option<String>,
    pub body: Spanned<Block>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BinaryOperator {
    Add,
    Sub,
    Mul,
    Div,
    FloorDiv,
    Mod,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    In,
    NotIn,
    And,
    Or,
}

#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOperator {
    Neg,
    Not,
}

#[derive(Debug, Clone, PartialEq)]
pub enum GlobalFunction {
    Range,
    Len,
    Enumerate,
    IsException,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PolicyBracket {
    Retry(RetryPolicy),
    Timeout(TimeoutPolicy),
}

#[derive(Debug, Clone, PartialEq)]
pub struct RetryPolicy {
    pub exception_types: Vec<String>,
    pub max_retries: u32,
    pub backoff: Option<DurationLiteral>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TimeoutPolicy {
    pub timeout: DurationLiteral,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DurationLiteral {
    pub seconds: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub start_line: u32,
    pub start_col: u32,
    pub end_line: u32,
    pub end_col: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Spanned<T> {
    pub value: T,
    pub span: Span,
}
