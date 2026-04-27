use waymark_ids::ExecutionId;

pub type ValueExpr = waymark_runner_expr::ValueExpr<ExecutionId>;
pub type LiteralValue = waymark_runner_expr::LiteralValue;
pub type VariableValue = waymark_runner_expr::VariableValue;
pub type BinaryOpValue = waymark_runner_expr::BinaryOpValue<ExecutionId>;
pub type UnaryOpValue = waymark_runner_expr::UnaryOpValue<ExecutionId>;
pub type ListValue = waymark_runner_expr::ListValue<ExecutionId>;
pub type DictValue = waymark_runner_expr::DictValue<ExecutionId>;
pub type DictEntryValue = waymark_runner_expr::DictEntryValue<ExecutionId>;
pub type IndexValue = waymark_runner_expr::IndexValue<ExecutionId>;
pub type DotValue = waymark_runner_expr::DotValue<ExecutionId>;
pub type FunctionCallValue = waymark_runner_expr::FunctionCallValue<ExecutionId>;
pub type SpreadValue = waymark_runner_expr::SpreadValue<ExecutionId>;
pub type ActionCallSpec = waymark_runner_expr::ActionCallSpec<ExecutionId>;
pub type ActionResultValue = waymark_runner_expr::ActionResultValue<ExecutionId>;
