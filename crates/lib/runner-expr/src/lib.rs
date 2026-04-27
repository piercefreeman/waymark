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
pub struct BinaryOpValue<NodeId> {
    pub left: Box<ValueExpr<NodeId>>,
    pub op: i32,
    pub right: Box<ValueExpr<NodeId>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UnaryOpValue<NodeId> {
    pub op: i32,
    pub operand: Box<ValueExpr<NodeId>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ListValue<NodeId> {
    pub elements: Vec<ValueExpr<NodeId>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DictEntryValue<NodeId> {
    pub key: ValueExpr<NodeId>,
    pub value: ValueExpr<NodeId>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DictValue<NodeId> {
    pub entries: Vec<DictEntryValue<NodeId>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IndexValue<NodeId> {
    pub object: Box<ValueExpr<NodeId>>,
    pub index: Box<ValueExpr<NodeId>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DotValue<NodeId> {
    pub object: Box<ValueExpr<NodeId>>,
    pub attribute: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FunctionCallValue<NodeId> {
    pub name: String,
    pub args: Vec<ValueExpr<NodeId>>,
    pub kwargs: HashMap<String, ValueExpr<NodeId>>,
    pub global_function: Option<i32>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SpreadValue<NodeId> {
    pub collection: Box<ValueExpr<NodeId>>,
    pub loop_var: String,
    pub action: ActionCallSpec<NodeId>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ActionResultValue<NodeId> {
    pub node_id: NodeId,
    pub action_name: String,
    pub iteration_index: Option<i32>,
    pub result_index: Option<i32>,
}

impl<NodeId> ActionResultValue<NodeId> {
    pub fn label(&self) -> String {
        let mut label = self.action_name.clone();
        if let Some(idx) = self.iteration_index {
            label = format!("{label}[{idx}]");
        }
        if let Some(idx) = self.result_index {
            label = format!("{label}[{idx}]");
        }
        label
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum ValueExpr<NodeId> {
    Literal(LiteralValue),
    Variable(VariableValue),
    ActionResult(ActionResultValue<NodeId>),
    BinaryOp(BinaryOpValue<NodeId>),
    UnaryOp(UnaryOpValue<NodeId>),
    List(ListValue<NodeId>),
    Dict(DictValue<NodeId>),
    Index(IndexValue<NodeId>),
    Dot(DotValue<NodeId>),
    FunctionCall(FunctionCallValue<NodeId>),
    Spread(SpreadValue<NodeId>),
}
