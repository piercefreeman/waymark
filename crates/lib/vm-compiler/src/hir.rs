//! High-level IR tree.

pub enum BinOp {
    Add,
}

pub enum Expr<Reg, Value> {
    Const(Value),
    Reg(Reg),
    Binary(Reg, BinOp, Reg),
    Call(String, Vec<Reg>),
    ExtCall(String, Vec<Reg>),
    Await(Reg),
}

pub enum Stmt<Reg, Value> {
    Assign(Reg, Expr<Reg, Value>),
    Return(Reg),
}

pub struct Function<Reg, Value> {
    pub body: Vec<Stmt<Reg, Value>>,
    pub locals: u32,
}
