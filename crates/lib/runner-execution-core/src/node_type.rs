#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionNodeType {
    Input,
    Output,
    Assignment,
    ActionCall,
    FnCall,
    Parallel,
    Aggregator,
    Branch,
    Join,
    Return,
    Break,
    Continue,
    Sleep,
    Expression,
}

#[derive(Debug, thiserror::Error)]
#[error("unknown execution node type: {node_type}")]
pub struct UnknownExecutionNodeTypeError {
    pub node_type: String,
}

impl ExecutionNodeType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ExecutionNodeType::Input => "input",
            ExecutionNodeType::Output => "output",
            ExecutionNodeType::Assignment => "assignment",
            ExecutionNodeType::ActionCall => "action_call",
            ExecutionNodeType::FnCall => "fn_call",
            ExecutionNodeType::Parallel => "parallel",
            ExecutionNodeType::Aggregator => "aggregator",
            ExecutionNodeType::Branch => "branch",
            ExecutionNodeType::Join => "join",
            ExecutionNodeType::Return => "return",
            ExecutionNodeType::Break => "break",
            ExecutionNodeType::Continue => "continue",
            ExecutionNodeType::Sleep => "sleep",
            ExecutionNodeType::Expression => "expression",
        }
    }
}

impl TryFrom<&str> for ExecutionNodeType {
    type Error = UnknownExecutionNodeTypeError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "input" => Ok(ExecutionNodeType::Input),
            "output" => Ok(ExecutionNodeType::Output),
            "assignment" => Ok(ExecutionNodeType::Assignment),
            "action_call" => Ok(ExecutionNodeType::ActionCall),
            "fn_call" => Ok(ExecutionNodeType::FnCall),
            "parallel" => Ok(ExecutionNodeType::Parallel),
            "aggregator" => Ok(ExecutionNodeType::Aggregator),
            "branch" => Ok(ExecutionNodeType::Branch),
            "join" => Ok(ExecutionNodeType::Join),
            "return" => Ok(ExecutionNodeType::Return),
            "break" => Ok(ExecutionNodeType::Break),
            "continue" => Ok(ExecutionNodeType::Continue),
            "sleep" => Ok(ExecutionNodeType::Sleep),
            "expression" => Ok(ExecutionNodeType::Expression),
            _ => Err(UnknownExecutionNodeTypeError {
                node_type: value.into(),
            }),
        }
    }
}
