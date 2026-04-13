use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ActionCallSpec<ActionResult> {
    pub action_name: String,
    pub module_name: Option<String>,
    pub kwargs: HashMap<String, ValueExpr<ActionResult>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LiteralValue {
    pub value: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VariableValue {
    pub name: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BinaryOpValue<ActionResult> {
    pub left: Box<ValueExpr<ActionResult>>,
    pub op: i32,
    pub right: Box<ValueExpr<ActionResult>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UnaryOpValue<ActionResult> {
    pub op: i32,
    pub operand: Box<ValueExpr<ActionResult>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ListValue<ActionResult> {
    pub elements: Vec<ValueExpr<ActionResult>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DictEntryValue<ActionResult> {
    pub key: ValueExpr<ActionResult>,
    pub value: ValueExpr<ActionResult>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DictValue<ActionResult> {
    pub entries: Vec<DictEntryValue<ActionResult>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IndexValue<ActionResult> {
    pub object: Box<ValueExpr<ActionResult>>,
    pub index: Box<ValueExpr<ActionResult>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DotValue<ActionResult> {
    pub object: Box<ValueExpr<ActionResult>>,
    pub attribute: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FunctionCallValue<ActionResult> {
    pub name: String,
    pub args: Vec<ValueExpr<ActionResult>>,
    pub kwargs: HashMap<String, ValueExpr<ActionResult>>,
    pub global_function: Option<i32>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SpreadValue<ActionResult> {
    pub collection: Box<ValueExpr<ActionResult>>,
    pub loop_var: String,
    pub action: ActionCallSpec<ActionResult>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum ValueExpr<ActionResult> {
    Literal(LiteralValue),
    Variable(VariableValue),
    ActionResult(ActionResult),
    BinaryOp(BinaryOpValue<ActionResult>),
    UnaryOp(UnaryOpValue<ActionResult>),
    List(ListValue<ActionResult>),
    Dict(DictValue<ActionResult>),
    Index(IndexValue<ActionResult>),
    Dot(DotValue<ActionResult>),
    FunctionCall(FunctionCallValue<ActionResult>),
    Spread(SpreadValue<ActionResult>),
}
