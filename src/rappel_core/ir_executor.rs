//! Async IR statement executor for Rappel workflows.

use std::collections::HashMap;
use std::sync::Arc;

use futures::future::BoxFuture;
use serde_json::Value;

use crate::messages::ast as ir;
use crate::rappel_core::dag::EXCEPTION_SCOPE_VAR;

pub type ActionHandler = Arc<
    dyn Fn(
            ir::ActionCall,
            HashMap<String, Value>,
        ) -> BoxFuture<'static, Result<Value, ExecutionError>>
        + Send
        + Sync,
>;

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct ExecutionError {
    pub kind: String,
    pub message: String,
    pub errors: Option<Vec<ExecutionError>>,
}

impl ExecutionError {
    pub fn new(kind: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            message: message.into(),
            errors: None,
        }
    }

    pub fn parallel(errors: Vec<ExecutionError>) -> Self {
        Self {
            kind: "ParallelExecutionError".to_string(),
            message: "parallel execution failed".to_string(),
            errors: Some(errors),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct FunctionNotFoundError(pub String);

impl From<FunctionNotFoundError> for ExecutionError {
    fn from(err: FunctionNotFoundError) -> Self {
        ExecutionError::new("FunctionNotFoundError", err.0)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct VariableNotFoundError(pub String);

impl From<VariableNotFoundError> for ExecutionError {
    fn from(err: VariableNotFoundError) -> Self {
        ExecutionError::new("VariableNotFoundError", err.0)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("parallel execution failed")]
pub struct ParallelExecutionError {
    pub errors: Vec<ExecutionError>,
}

impl From<ParallelExecutionError> for ExecutionError {
    fn from(err: ParallelExecutionError) -> Self {
        ExecutionError::parallel(err.errors)
    }
}

#[derive(Clone, Debug)]
pub struct ExecutionLimits {
    pub max_call_depth: usize,
    pub max_loop_iterations: Option<usize>,
}

impl Default for ExecutionLimits {
    fn default() -> Self {
        Self {
            max_call_depth: 64,
            max_loop_iterations: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ControlFlow {
    None,
    Return,
    Break,
    Continue,
}

#[derive(Clone, Debug)]
pub struct StatementResult {
    pub control: ControlFlow,
    pub value: Option<Value>,
}

impl StatementResult {
    pub fn none() -> Self {
        Self {
            control: ControlFlow::None,
            value: None,
        }
    }

    pub fn returned(value: Value) -> Self {
        Self {
            control: ControlFlow::Return,
            value: Some(value),
        }
    }

    pub fn broke() -> Self {
        Self {
            control: ControlFlow::Break,
            value: None,
        }
    }

    pub fn continued() -> Self {
        Self {
            control: ControlFlow::Continue,
            value: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ExecutionFrame {
    variables: HashMap<String, Value>,
}

impl ExecutionFrame {
    pub fn new() -> Self {
        Self {
            variables: HashMap::new(),
        }
    }

    pub fn get(&self, name: &str) -> Result<Value, ExecutionError> {
        self.variables
            .get(name)
            .cloned()
            .ok_or_else(|| VariableNotFoundError(format!("variable not found: {name}")).into())
    }

    pub fn set(&mut self, name: &str, value: Value) {
        self.variables.insert(name.to_string(), value);
    }

    pub fn snapshot(&self) -> Self {
        Self {
            variables: self.variables.clone(),
        }
    }
}

impl Default for ExecutionFrame {
    fn default() -> Self {
        Self::new()
    }
}

pub struct StatementExecutor {
    action_handler: ActionHandler,
    limits: ExecutionLimits,
    functions: HashMap<String, ir::FunctionDef>,
}

impl StatementExecutor {
    pub fn new(
        program: ir::Program,
        action_handler: ActionHandler,
        limits: Option<ExecutionLimits>,
    ) -> Self {
        let functions = program
            .functions
            .iter()
            .map(|func| (func.name.clone(), func.clone()))
            .collect();
        Self {
            action_handler,
            limits: limits.unwrap_or_default(),
            functions,
        }
    }

    pub async fn execute_program(
        &self,
        entry: Option<&str>,
        inputs: Option<HashMap<String, Value>>,
    ) -> Result<Value, ExecutionError> {
        let entry = entry.unwrap_or("main");
        self.execute_function(entry, inputs, None, 0).await
    }

    pub async fn execute_function(
        &self,
        name: &str,
        inputs: Option<HashMap<String, Value>>,
        args: Option<Vec<Value>>,
        depth: usize,
    ) -> Result<Value, ExecutionError> {
        if depth >= self.limits.max_call_depth {
            return Err(ExecutionError::new(
                "ExecutionError",
                "maximum call depth exceeded",
            ));
        }
        let fn_def = self
            .functions
            .get(name)
            .cloned()
            .ok_or_else(|| FunctionNotFoundError(format!("function not found: {name}")))?;

        let mut frame = ExecutionFrame::new();
        let bound_inputs = self.bind_inputs(
            &fn_def,
            args.unwrap_or_default(),
            inputs.unwrap_or_default(),
        )?;
        frame.variables.extend(bound_inputs);

        let result = if let Some(body) = &fn_def.body {
            self.execute_block(body, &mut frame, depth).await?
        } else {
            StatementResult::none()
        };

        if result.control == ControlFlow::Return {
            return Ok(result.value.unwrap_or(Value::Null));
        }

        if let Some(io) = &fn_def.io
            && !io.outputs.is_empty()
        {
            return Ok(self.collect_output_values(io, &frame));
        }
        Ok(Value::Null)
    }

    fn execute_block<'a>(
        &'a self,
        block: &'a ir::Block,
        frame: &'a mut ExecutionFrame,
        depth: usize,
    ) -> BoxFuture<'a, Result<StatementResult, ExecutionError>> {
        Box::pin(async move {
            for stmt in &block.statements {
                let result = self.execute_statement(stmt, frame, depth).await?;
                if result.control != ControlFlow::None {
                    return Ok(result);
                }
            }
            Ok(StatementResult::none())
        })
    }

    fn execute_statement<'a>(
        &'a self,
        stmt: &'a ir::Statement,
        frame: &'a mut ExecutionFrame,
        depth: usize,
    ) -> BoxFuture<'a, Result<StatementResult, ExecutionError>> {
        Box::pin(async move {
            match stmt.kind.as_ref() {
                Some(ir::statement::Kind::Assignment(assign)) => {
                    let value = self.eval_expr(assign.value.as_ref(), frame, depth).await?;
                    self.assign_targets(&assign.targets, value, frame)?;
                    Ok(StatementResult::none())
                }
                Some(ir::statement::Kind::ActionCall(action)) => {
                    self.execute_action(action, frame, depth).await?;
                    Ok(StatementResult::none())
                }
                Some(ir::statement::Kind::SpreadAction(spread)) => {
                    self.execute_spread_action(spread, frame, depth).await?;
                    Ok(StatementResult::none())
                }
                Some(ir::statement::Kind::ParallelBlock(parallel)) => {
                    self.execute_parallel_block(parallel, frame, depth).await?;
                    Ok(StatementResult::none())
                }
                Some(ir::statement::Kind::ForLoop(for_loop)) => {
                    self.execute_for_loop(for_loop, frame, depth).await
                }
                Some(ir::statement::Kind::WhileLoop(while_loop)) => {
                    self.execute_while_loop(while_loop, frame, depth).await
                }
                Some(ir::statement::Kind::Conditional(cond)) => {
                    self.execute_conditional(cond, frame, depth).await
                }
                Some(ir::statement::Kind::TryExcept(try_except)) => {
                    self.execute_try_except(try_except, frame, depth).await
                }
                Some(ir::statement::Kind::ReturnStmt(ret)) => {
                    let value = if let Some(expr) = &ret.value {
                        self.eval_expr(Some(expr), frame, depth).await?
                    } else {
                        Value::Null
                    };
                    Ok(StatementResult::returned(value))
                }
                Some(ir::statement::Kind::BreakStmt(_)) => Ok(StatementResult::broke()),
                Some(ir::statement::Kind::ContinueStmt(_)) => Ok(StatementResult::continued()),
                Some(ir::statement::Kind::ExprStmt(expr_stmt)) => {
                    if let Some(expr) = &expr_stmt.expr {
                        self.eval_expr(Some(expr), frame, depth).await?;
                    }
                    Ok(StatementResult::none())
                }
                None => Ok(StatementResult::none()),
            }
        })
    }

    fn bind_inputs(
        &self,
        fn_def: &ir::FunctionDef,
        args: Vec<Value>,
        kwargs: HashMap<String, Value>,
    ) -> Result<HashMap<String, Value>, ExecutionError> {
        let mut bound: HashMap<String, Value> = HashMap::new();
        let inputs = fn_def
            .io
            .as_ref()
            .map(|io| io.inputs.clone())
            .unwrap_or_default();

        for (idx, arg) in args.into_iter().enumerate() {
            if idx >= inputs.len() {
                return Err(ExecutionError::new(
                    "ExecutionError",
                    "too many positional arguments",
                ));
            }
            bound.insert(inputs[idx].clone(), arg);
        }

        for (key, value) in kwargs {
            if !inputs.contains(&key) {
                return Err(ExecutionError::new(
                    "ExecutionError",
                    format!("unknown argument: {key}"),
                ));
            }
            bound.insert(key, value);
        }

        let missing: Vec<String> = inputs
            .iter()
            .filter(|name| !bound.contains_key(*name))
            .cloned()
            .collect();
        if !missing.is_empty() {
            return Err(ExecutionError::new(
                "ExecutionError",
                format!("missing arguments: {}", missing.join(", ")),
            ));
        }
        Ok(bound)
    }

    fn collect_output_values(&self, io: &ir::IoDecl, frame: &ExecutionFrame) -> Value {
        let outputs = io.outputs.clone();
        let mut values: Vec<Value> = outputs
            .iter()
            .map(|name| frame.variables.get(name).cloned().unwrap_or(Value::Null))
            .collect();
        if values.len() == 1 {
            values.pop().unwrap_or(Value::Null)
        } else {
            Value::Array(values)
        }
    }

    fn assign_targets(
        &self,
        targets: &[String],
        value: Value,
        frame: &mut ExecutionFrame,
    ) -> Result<(), ExecutionError> {
        if targets.is_empty() {
            return Ok(());
        }
        if targets.len() == 1 {
            frame.set(&targets[0], value);
            return Ok(());
        }
        if let Value::Array(items) = &value {
            if items.len() != targets.len() {
                return Err(ExecutionError::new(
                    "ExecutionError",
                    "tuple unpacking mismatch",
                ));
            }
            for (target, item) in targets.iter().zip(items.iter()) {
                frame.set(target, item.clone());
            }
            return Ok(());
        }
        for target in targets {
            frame.set(target, value.clone());
        }
        Ok(())
    }

    fn eval_expr<'a>(
        &'a self,
        expr: Option<&'a ir::Expr>,
        frame: &'a mut ExecutionFrame,
        depth: usize,
    ) -> BoxFuture<'a, Result<Value, ExecutionError>> {
        Box::pin(async move {
            let expr = match expr {
                Some(expr) => expr,
                None => return Ok(Value::Null),
            };

            match expr.kind.as_ref() {
                Some(ir::expr::Kind::Literal(lit)) => Ok(eval_literal(lit)),
                Some(ir::expr::Kind::Variable(var)) => frame.get(&var.name),
                Some(ir::expr::Kind::BinaryOp(op)) => self.eval_binary_op(op, frame, depth).await,
                Some(ir::expr::Kind::UnaryOp(op)) => self.eval_unary_op(op, frame, depth).await,
                Some(ir::expr::Kind::List(list)) => {
                    let mut items = Vec::new();
                    for item in &list.elements {
                        items.push(self.eval_expr(Some(item), frame, depth).await?);
                    }
                    Ok(Value::Array(items))
                }
                Some(ir::expr::Kind::Dict(dict_expr)) => {
                    self.eval_dict(dict_expr, frame, depth).await
                }
                Some(ir::expr::Kind::Index(index)) => self.eval_index(index, frame, depth).await,
                Some(ir::expr::Kind::Dot(dot)) => self.eval_dot(dot, frame, depth).await,
                Some(ir::expr::Kind::FunctionCall(call)) => {
                    self.eval_function_call(call, frame, depth).await
                }
                Some(ir::expr::Kind::ActionCall(action)) => {
                    self.execute_action(action, frame, depth).await
                }
                Some(ir::expr::Kind::ParallelExpr(parallel)) => {
                    self.eval_parallel_expr(parallel, frame, depth).await
                }
                Some(ir::expr::Kind::SpreadExpr(spread)) => {
                    self.eval_spread_expr(spread, frame, depth).await
                }
                None => Ok(Value::Null),
            }
        })
    }

    async fn eval_binary_op(
        &self,
        op: &ir::BinaryOp,
        frame: &mut ExecutionFrame,
        depth: usize,
    ) -> Result<Value, ExecutionError> {
        let left = op.left.as_ref().ok_or_else(|| {
            ExecutionError::new("ExecutionError", "binary operator missing operands")
        })?;
        let right = op.right.as_ref().ok_or_else(|| {
            ExecutionError::new("ExecutionError", "binary operator missing operands")
        })?;

        if op.op == ir::BinaryOperator::BinaryOpAnd as i32 {
            let left_value = self.eval_expr(Some(left), frame, depth).await?;
            if !is_truthy(&left_value) {
                return Ok(left_value);
            }
            return self.eval_expr(Some(right), frame, depth).await;
        }
        if op.op == ir::BinaryOperator::BinaryOpOr as i32 {
            let left_value = self.eval_expr(Some(left), frame, depth).await?;
            if is_truthy(&left_value) {
                return Ok(left_value);
            }
            return self.eval_expr(Some(right), frame, depth).await;
        }

        let left_value = self.eval_expr(Some(left), frame, depth).await?;
        let right_value = self.eval_expr(Some(right), frame, depth).await?;
        apply_binary(op.op, left_value, right_value)
    }

    async fn eval_unary_op(
        &self,
        op: &ir::UnaryOp,
        frame: &mut ExecutionFrame,
        depth: usize,
    ) -> Result<Value, ExecutionError> {
        let operand = op.operand.as_ref().ok_or_else(|| {
            ExecutionError::new("ExecutionError", "unary operator missing operand")
        })?;
        let value = self.eval_expr(Some(operand), frame, depth).await?;
        apply_unary(op.op, value)
    }

    async fn eval_dict(
        &self,
        dict_expr: &ir::DictExpr,
        frame: &mut ExecutionFrame,
        depth: usize,
    ) -> Result<Value, ExecutionError> {
        let mut map = serde_json::Map::new();
        for entry in &dict_expr.entries {
            let key_expr = entry.key.as_ref().ok_or_else(|| {
                ExecutionError::new("ExecutionError", "dict entry missing key or value")
            })?;
            let value_expr = entry.value.as_ref().ok_or_else(|| {
                ExecutionError::new("ExecutionError", "dict entry missing key or value")
            })?;
            let key_val = self.eval_expr(Some(key_expr), frame, depth).await?;
            let key = key_val
                .as_str()
                .map(|value| value.to_string())
                .unwrap_or_else(|| key_val.to_string());
            let value = self.eval_expr(Some(value_expr), frame, depth).await?;
            map.insert(key, value);
        }
        Ok(Value::Object(map))
    }

    async fn eval_index(
        &self,
        index: &ir::IndexAccess,
        frame: &mut ExecutionFrame,
        depth: usize,
    ) -> Result<Value, ExecutionError> {
        let object = index.object.as_ref().ok_or_else(|| {
            ExecutionError::new("ExecutionError", "index access missing object or index")
        })?;
        let index_expr = index.index.as_ref().ok_or_else(|| {
            ExecutionError::new("ExecutionError", "index access missing object or index")
        })?;
        let obj_value = self.eval_expr(Some(object), frame, depth).await?;
        let idx_value = self.eval_expr(Some(index_expr), frame, depth).await?;
        match (obj_value, idx_value) {
            (Value::Array(items), Value::Number(idx)) => {
                let idx = idx.as_i64().unwrap_or(-1);
                if idx < 0 || idx as usize >= items.len() {
                    return Err(ExecutionError::new("IndexError", "index out of range"));
                }
                Ok(items[idx as usize].clone())
            }
            (Value::Object(map), Value::String(key)) => map
                .get(&key)
                .cloned()
                .ok_or_else(|| ExecutionError::new("ExecutionError", "dict has no key")),
            _ => Err(ExecutionError::new(
                "ExecutionError",
                "unsupported index operation",
            )),
        }
    }

    async fn eval_dot(
        &self,
        dot: &ir::DotAccess,
        frame: &mut ExecutionFrame,
        depth: usize,
    ) -> Result<Value, ExecutionError> {
        let object = dot
            .object
            .as_ref()
            .ok_or_else(|| ExecutionError::new("ExecutionError", "dot access missing object"))?;
        let obj_value = self.eval_expr(Some(object), frame, depth).await?;
        if let Value::Object(map) = obj_value {
            return map
                .get(&dot.attribute)
                .cloned()
                .ok_or_else(|| ExecutionError::new("ExecutionError", "dict has no key"));
        }
        Err(ExecutionError::new("ExecutionError", "attribute not found"))
    }

    async fn eval_function_call(
        &self,
        call: &ir::FunctionCall,
        frame: &mut ExecutionFrame,
        depth: usize,
    ) -> Result<Value, ExecutionError> {
        if call.global_function != ir::GlobalFunction::Unspecified as i32 {
            return self.eval_global_function(call, frame, depth).await;
        }

        let mut args = Vec::new();
        for arg in &call.args {
            args.push(self.eval_expr(Some(arg), frame, depth).await?);
        }
        let mut kwargs = HashMap::new();
        for kw in &call.kwargs {
            if let Some(value) = &kw.value {
                kwargs.insert(
                    kw.name.clone(),
                    self.eval_expr(Some(value), frame, depth).await?,
                );
            }
        }
        self.execute_function(&call.name, Some(kwargs), Some(args), depth + 1)
            .await
    }

    async fn eval_global_function(
        &self,
        call: &ir::FunctionCall,
        frame: &mut ExecutionFrame,
        depth: usize,
    ) -> Result<Value, ExecutionError> {
        let mut args = Vec::new();
        for arg in &call.args {
            args.push(self.eval_expr(Some(arg), frame, depth).await?);
        }
        let mut kwargs = HashMap::new();
        for kw in &call.kwargs {
            if let Some(value) = &kw.value {
                kwargs.insert(
                    kw.name.clone(),
                    self.eval_expr(Some(value), frame, depth).await?,
                );
            }
        }

        match ir::GlobalFunction::try_from(call.global_function).ok() {
            Some(ir::GlobalFunction::Range) => Ok(Value::Array(range_from_args(&args)?)),
            Some(ir::GlobalFunction::Len) => {
                if let Some(first) = args.first() {
                    return Ok(Value::Number(len_of_value(first)?));
                }
                if let Some(items) = kwargs.get("items") {
                    return Ok(Value::Number(len_of_value(items)?));
                }
                Err(ExecutionError::new(
                    "ExecutionError",
                    "len() missing argument",
                ))
            }
            Some(ir::GlobalFunction::Enumerate) => {
                let items = if let Some(first) = args.first() {
                    first.clone()
                } else if let Some(items) = kwargs.get("items") {
                    items.clone()
                } else {
                    return Err(ExecutionError::new(
                        "ExecutionError",
                        "enumerate() missing argument",
                    ));
                };
                let list = match items {
                    Value::Array(items) => items,
                    _ => {
                        return Err(ExecutionError::new(
                            "ExecutionError",
                            "enumerate() expects list",
                        ));
                    }
                };
                let pairs: Vec<Value> = list
                    .into_iter()
                    .enumerate()
                    .map(|(idx, item)| Value::Array(vec![Value::Number((idx as i64).into()), item]))
                    .collect();
                Ok(Value::Array(pairs))
            }
            Some(ir::GlobalFunction::Isexception) => {
                if let Some(first) = args.first() {
                    return Ok(Value::Bool(is_exception_value(first)));
                }
                if let Some(value) = kwargs.get("value") {
                    return Ok(Value::Bool(is_exception_value(value)));
                }
                Err(ExecutionError::new(
                    "ExecutionError",
                    "isexception() missing argument",
                ))
            }
            Some(ir::GlobalFunction::Unspecified) | None => Err(ExecutionError::new(
                "ExecutionError",
                "global function unspecified",
            )),
        }
    }

    async fn execute_action(
        &self,
        action: &ir::ActionCall,
        frame: &mut ExecutionFrame,
        depth: usize,
    ) -> Result<Value, ExecutionError> {
        let mut kwargs = HashMap::new();
        for kw in &action.kwargs {
            if let Some(value) = &kw.value {
                kwargs.insert(
                    kw.name.clone(),
                    self.eval_expr(Some(value), frame, depth).await?,
                );
            }
        }
        (self.action_handler)(action.clone(), kwargs).await
    }

    async fn execute_spread_action(
        &self,
        spread: &ir::SpreadAction,
        frame: &mut ExecutionFrame,
        depth: usize,
    ) -> Result<(), ExecutionError> {
        let results = self.execute_spread(spread, frame, depth).await?;
        let mut errors = Vec::new();
        for result in results {
            if let Err(err) = result {
                errors.push(err);
            }
        }
        if !errors.is_empty() {
            return Err(ParallelExecutionError { errors }.into());
        }
        Ok(())
    }

    async fn eval_spread_expr(
        &self,
        spread: &ir::SpreadExpr,
        frame: &mut ExecutionFrame,
        depth: usize,
    ) -> Result<Value, ExecutionError> {
        let results = self.execute_spread(spread, frame, depth).await?;
        let mut values = Vec::new();
        for result in results {
            match result {
                Ok(value) => values.push(value),
                Err(err) => values.push(error_to_value(&err)),
            }
        }
        Ok(Value::Array(values))
    }

    async fn execute_spread<T>(
        &self,
        spread: &T,
        frame: &mut ExecutionFrame,
        depth: usize,
    ) -> Result<Vec<Result<Value, ExecutionError>>, ExecutionError>
    where
        T: SpreadLike,
    {
        let collection = self
            .eval_expr(Some(spread.collection()), frame, depth)
            .await?;
        let items = match collection {
            Value::Array(items) => items,
            _ => {
                return Err(ExecutionError::new(
                    "ExecutionError",
                    "spread collection is not iterable",
                ));
            }
        };

        let mut tasks = Vec::new();
        for item in items {
            let mut child_frame = frame.snapshot();
            child_frame.set(spread.loop_var(), item);
            let action = spread.action().clone();
            let executor = self;
            tasks.push(async move {
                executor
                    .execute_action(&action, &mut child_frame, depth)
                    .await
            });
        }

        let results = futures::future::join_all(tasks).await;
        Ok(results)
    }

    async fn execute_parallel_block(
        &self,
        parallel: &ir::ParallelBlock,
        frame: &mut ExecutionFrame,
        depth: usize,
    ) -> Result<(), ExecutionError> {
        let results = self
            .execute_parallel_calls(&parallel.calls, frame, depth)
            .await?;
        let mut errors = Vec::new();
        for result in results {
            if let Err(err) = result {
                errors.push(err);
            }
        }
        if !errors.is_empty() {
            return Err(ParallelExecutionError { errors }.into());
        }
        Ok(())
    }

    async fn eval_parallel_expr(
        &self,
        parallel: &ir::ParallelExpr,
        frame: &mut ExecutionFrame,
        depth: usize,
    ) -> Result<Value, ExecutionError> {
        let results = self
            .execute_parallel_calls(&parallel.calls, frame, depth)
            .await?;
        let mut values = Vec::new();
        for result in results {
            match result {
                Ok(value) => values.push(value),
                Err(err) => values.push(error_to_value(&err)),
            }
        }
        Ok(Value::Array(values))
    }

    async fn execute_parallel_calls(
        &self,
        calls: &[ir::Call],
        frame: &mut ExecutionFrame,
        depth: usize,
    ) -> Result<Vec<Result<Value, ExecutionError>>, ExecutionError> {
        let mut tasks = Vec::new();
        for call in calls {
            let mut child_frame = frame.snapshot();
            let executor = self;
            let call_ref = call;
            tasks.push(async move {
                executor
                    .execute_call(call_ref, &mut child_frame, depth)
                    .await
            });
        }
        Ok(futures::future::join_all(tasks).await)
    }

    async fn execute_call(
        &self,
        call: &ir::Call,
        frame: &mut ExecutionFrame,
        depth: usize,
    ) -> Result<Value, ExecutionError> {
        match call.kind.as_ref() {
            Some(ir::call::Kind::Action(action)) => self.execute_action(action, frame, depth).await,
            Some(ir::call::Kind::Function(function)) => {
                self.eval_function_call(function, frame, depth).await
            }
            None => Ok(Value::Null),
        }
    }

    async fn execute_for_loop(
        &self,
        for_loop: &ir::ForLoop,
        frame: &mut ExecutionFrame,
        depth: usize,
    ) -> Result<StatementResult, ExecutionError> {
        let iterable = for_loop
            .iterable
            .as_ref()
            .ok_or_else(|| ExecutionError::new("ExecutionError", "for loop missing iterable"))?;
        let collection = self.eval_expr(Some(iterable), frame, depth).await?;
        let items = match collection {
            Value::Array(items) => items,
            _ => {
                return Err(ExecutionError::new(
                    "ExecutionError",
                    "for loop iterable is not list",
                ));
            }
        };

        let mut iteration_count = 0usize;
        for item in items {
            iteration_count += 1;
            if let Some(limit) = self.limits.max_loop_iterations
                && iteration_count > limit
            {
                return Err(ExecutionError::new(
                    "ExecutionError",
                    "loop iteration limit exceeded",
                ));
            }
            self.assign_loop_vars(&for_loop.loop_vars, item, frame)?;

            if let Some(block) = &for_loop.block_body {
                let result = self.execute_block(block, frame, depth).await?;
                match result.control {
                    ControlFlow::Break => return Ok(StatementResult::none()),
                    ControlFlow::Continue => continue,
                    ControlFlow::Return => return Ok(result),
                    ControlFlow::None => {}
                }
            }
        }
        Ok(StatementResult::none())
    }

    async fn execute_while_loop(
        &self,
        while_loop: &ir::WhileLoop,
        frame: &mut ExecutionFrame,
        depth: usize,
    ) -> Result<StatementResult, ExecutionError> {
        let condition = while_loop
            .condition
            .as_ref()
            .ok_or_else(|| ExecutionError::new("ExecutionError", "while loop missing condition"))?;
        let mut iteration_count = 0usize;

        loop {
            let cond = self.eval_expr(Some(condition), frame, depth).await?;
            if !is_truthy(&cond) {
                return Ok(StatementResult::none());
            }

            iteration_count += 1;
            if let Some(limit) = self.limits.max_loop_iterations
                && iteration_count > limit
            {
                return Err(ExecutionError::new(
                    "ExecutionError",
                    "loop iteration limit exceeded",
                ));
            }

            if let Some(block) = &while_loop.block_body {
                let result = self.execute_block(block, frame, depth).await?;
                match result.control {
                    ControlFlow::Break => return Ok(StatementResult::none()),
                    ControlFlow::Continue => continue,
                    ControlFlow::Return => return Ok(result),
                    ControlFlow::None => {}
                }
            }
        }
    }

    async fn execute_conditional(
        &self,
        cond: &ir::Conditional,
        frame: &mut ExecutionFrame,
        depth: usize,
    ) -> Result<StatementResult, ExecutionError> {
        let if_branch = cond.if_branch.as_ref().ok_or_else(|| {
            ExecutionError::new("ExecutionError", "conditional missing if branch")
        })?;
        let condition = if_branch
            .condition
            .as_ref()
            .ok_or_else(|| ExecutionError::new("ExecutionError", "if branch missing condition"))?;
        let if_condition = self.eval_expr(Some(condition), frame, depth).await?;
        if is_truthy(&if_condition) {
            if let Some(block) = &if_branch.block_body {
                return self.execute_block(block, frame, depth).await;
            }
            return Ok(StatementResult::none());
        }

        for elif_branch in &cond.elif_branches {
            let condition = elif_branch.condition.as_ref().ok_or_else(|| {
                ExecutionError::new("ExecutionError", "elif branch missing condition")
            })?;
            let elif_condition = self.eval_expr(Some(condition), frame, depth).await?;
            if is_truthy(&elif_condition) {
                if let Some(block) = &elif_branch.block_body {
                    return self.execute_block(block, frame, depth).await;
                }
                return Ok(StatementResult::none());
            }
        }

        if let Some(else_branch) = &cond.else_branch
            && let Some(block) = &else_branch.block_body
        {
            return self.execute_block(block, frame, depth).await;
        }
        Ok(StatementResult::none())
    }

    async fn execute_try_except(
        &self,
        try_except: &ir::TryExcept,
        frame: &mut ExecutionFrame,
        depth: usize,
    ) -> Result<StatementResult, ExecutionError> {
        let try_block = match &try_except.try_block {
            Some(block) => block,
            None => return Ok(StatementResult::none()),
        };

        match self.execute_block(try_block, frame, depth).await {
            Ok(result) => {
                if result.control != ControlFlow::None {
                    Ok(result)
                } else {
                    Ok(StatementResult::none())
                }
            }
            Err(err) => {
                let exception_value = error_to_value(&err);
                for handler in &try_except.handlers {
                    if exception_matches(&handler.exception_types, &err) {
                        if let Some(var) = &handler.exception_var {
                            if !var.is_empty() {
                                frame.set(var, exception_value.clone());
                            } else {
                                frame.set(EXCEPTION_SCOPE_VAR, exception_value.clone());
                            }
                        }
                        if let Some(block) = &handler.block_body {
                            return self.execute_block(block, frame, depth).await;
                        }
                        return Ok(StatementResult::none());
                    }
                }
                Err(err)
            }
        }
    }

    fn assign_loop_vars(
        &self,
        loop_vars: &[String],
        item: Value,
        frame: &mut ExecutionFrame,
    ) -> Result<(), ExecutionError> {
        if loop_vars.is_empty() {
            return Ok(());
        }
        if loop_vars.len() == 1 {
            frame.set(&loop_vars[0], item);
            return Ok(());
        }
        if let Value::Array(items) = &item {
            if items.len() != loop_vars.len() {
                return Err(ExecutionError::new(
                    "ExecutionError",
                    "loop unpacking mismatch",
                ));
            }
            for (target, value) in loop_vars.iter().zip(items.iter()) {
                frame.set(target, value.clone());
            }
            return Ok(());
        }
        Err(ExecutionError::new(
            "ExecutionError",
            "loop unpacking requires tuple/list value",
        ))
    }
}

trait SpreadLike {
    fn collection(&self) -> &ir::Expr;
    fn loop_var(&self) -> &str;
    fn action(&self) -> &ir::ActionCall;
}

impl SpreadLike for ir::SpreadExpr {
    fn collection(&self) -> &ir::Expr {
        self.collection
            .as_ref()
            .expect("spread expr missing collection")
    }

    fn loop_var(&self) -> &str {
        &self.loop_var
    }

    fn action(&self) -> &ir::ActionCall {
        self.action.as_ref().expect("spread expr missing action")
    }
}

impl SpreadLike for ir::SpreadAction {
    fn collection(&self) -> &ir::Expr {
        self.collection
            .as_ref()
            .expect("spread action missing collection")
    }

    fn loop_var(&self) -> &str {
        &self.loop_var
    }

    fn action(&self) -> &ir::ActionCall {
        self.action.as_ref().expect("spread action missing action")
    }
}

fn eval_literal(lit: &ir::Literal) -> Value {
    match lit.value.as_ref() {
        Some(ir::literal::Value::IntValue(value)) => Value::Number((*value).into()),
        Some(ir::literal::Value::FloatValue(value)) => serde_json::Number::from_f64(*value)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        Some(ir::literal::Value::StringValue(value)) => Value::String(value.clone()),
        Some(ir::literal::Value::BoolValue(value)) => Value::Bool(*value),
        Some(ir::literal::Value::IsNone(_)) => Value::Null,
        None => Value::Null,
    }
}

fn apply_binary(op: i32, left: Value, right: Value) -> Result<Value, ExecutionError> {
    match ir::BinaryOperator::try_from(op).ok() {
        Some(ir::BinaryOperator::BinaryOpEq) => Ok(Value::Bool(left == right)),
        Some(ir::BinaryOperator::BinaryOpNe) => Ok(Value::Bool(left != right)),
        Some(ir::BinaryOperator::BinaryOpLt) => compare_values(left, right, |a, b| a < b),
        Some(ir::BinaryOperator::BinaryOpLe) => compare_values(left, right, |a, b| a <= b),
        Some(ir::BinaryOperator::BinaryOpGt) => compare_values(left, right, |a, b| a > b),
        Some(ir::BinaryOperator::BinaryOpGe) => compare_values(left, right, |a, b| a >= b),
        Some(ir::BinaryOperator::BinaryOpIn) => Ok(Value::Bool(value_in(&left, &right))),
        Some(ir::BinaryOperator::BinaryOpNotIn) => Ok(Value::Bool(!value_in(&left, &right))),
        Some(ir::BinaryOperator::BinaryOpAdd) => add_values(left, right),
        Some(ir::BinaryOperator::BinaryOpSub) => numeric_op(left, right, |a, b| a - b, true),
        Some(ir::BinaryOperator::BinaryOpMul) => numeric_op(left, right, |a, b| a * b, true),
        Some(ir::BinaryOperator::BinaryOpDiv) => {
            if right.as_f64().unwrap_or(0.0) == 0.0 {
                return Err(ExecutionError::new("ZeroDivisionError", "division by zero"));
            }
            numeric_op(left, right, |a, b| a / b, false)
        }
        Some(ir::BinaryOperator::BinaryOpFloorDiv) => {
            if right.as_f64().unwrap_or(0.0) == 0.0 {
                return Err(ExecutionError::new("ZeroDivisionError", "division by zero"));
            }
            numeric_op(left, right, |a, b| (a / b).floor(), true)
        }
        Some(ir::BinaryOperator::BinaryOpMod) => numeric_op(left, right, |a, b| a % b, true),
        Some(ir::BinaryOperator::BinaryOpUnspecified) | None => Err(ExecutionError::new(
            "ExecutionError",
            "unknown binary operator",
        )),
        Some(ir::BinaryOperator::BinaryOpAnd | ir::BinaryOperator::BinaryOpOr) => Err(
            ExecutionError::new("ExecutionError", "unexpected short-circuit operator"),
        ),
    }
}

fn apply_unary(op: i32, operand: Value) -> Result<Value, ExecutionError> {
    match ir::UnaryOperator::try_from(op).ok() {
        Some(ir::UnaryOperator::UnaryOpNeg) => {
            if let Some(value) = int_value(&operand) {
                return Ok(Value::Number((-value).into()));
            }
            match operand.as_f64() {
                Some(value) => Ok(Value::Number(
                    serde_json::Number::from_f64(-value)
                        .unwrap_or_else(|| serde_json::Number::from(0)),
                )),
                None => Err(ExecutionError::new(
                    "ExecutionError",
                    "unary neg expects number",
                )),
            }
        }
        Some(ir::UnaryOperator::UnaryOpNot) => Ok(Value::Bool(!is_truthy(&operand))),
        Some(ir::UnaryOperator::UnaryOpUnspecified) | None => Err(ExecutionError::new(
            "ExecutionError",
            "unknown unary operator",
        )),
    }
}

fn int_value(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
}

fn numeric_op(
    left: Value,
    right: Value,
    op: impl Fn(f64, f64) -> f64,
    prefer_int: bool,
) -> Result<Value, ExecutionError> {
    let left_num = left
        .as_f64()
        .ok_or_else(|| ExecutionError::new("ExecutionError", "numeric operation expects number"))?;
    let right_num = right
        .as_f64()
        .ok_or_else(|| ExecutionError::new("ExecutionError", "numeric operation expects number"))?;
    let result = op(left_num, right_num);
    if prefer_int && int_value(&left).is_some() && int_value(&right).is_some() && result.is_finite()
    {
        let rounded = result.round();
        if (result - rounded).abs() < 1e-9
            && rounded >= (i64::MIN as f64)
            && rounded <= (i64::MAX as f64)
        {
            return Ok(Value::Number((rounded as i64).into()));
        }
    }
    Ok(Value::Number(
        serde_json::Number::from_f64(result).unwrap_or_else(|| serde_json::Number::from(0)),
    ))
}

fn add_values(left: Value, right: Value) -> Result<Value, ExecutionError> {
    if let (Value::Array(mut left), Value::Array(right)) = (left.clone(), right.clone()) {
        left.extend(right);
        return Ok(Value::Array(left));
    }
    if let (Some(left), Some(right)) = (left.as_str(), right.as_str()) {
        return Ok(Value::String(format!("{left}{right}")));
    }
    numeric_op(left, right, |a, b| a + b, true)
}

fn compare_values(
    left: Value,
    right: Value,
    op: impl Fn(f64, f64) -> bool,
) -> Result<Value, ExecutionError> {
    let left = left
        .as_f64()
        .ok_or_else(|| ExecutionError::new("ExecutionError", "comparison expects number"))?;
    let right = right
        .as_f64()
        .ok_or_else(|| ExecutionError::new("ExecutionError", "comparison expects number"))?;
    Ok(Value::Bool(op(left, right)))
}

fn value_in(value: &Value, container: &Value) -> bool {
    match container {
        Value::Array(items) => items.iter().any(|item| item == value),
        Value::Object(map) => value
            .as_str()
            .map(|key| map.contains_key(key))
            .unwrap_or(false),
        Value::String(text) => value
            .as_str()
            .map(|needle| text.contains(needle))
            .unwrap_or(false),
        _ => false,
    }
}

fn is_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(number) => number.as_f64().map(|value| value != 0.0).unwrap_or(false),
        Value::String(value) => !value.is_empty(),
        Value::Array(values) => !values.is_empty(),
        Value::Object(map) => !map.is_empty(),
    }
}

fn is_exception_value(value: &Value) -> bool {
    if let Value::Object(map) = value {
        return map.contains_key("type") && map.contains_key("message");
    }
    false
}

fn error_to_value(err: &ExecutionError) -> Value {
    let mut map = serde_json::Map::new();
    map.insert("type".to_string(), Value::String(err.kind.clone()));
    map.insert("message".to_string(), Value::String(err.message.clone()));
    if let Some(errors) = &err.errors {
        let values = errors.iter().map(error_to_value).collect();
        map.insert("errors".to_string(), Value::Array(values));
    }
    Value::Object(map)
}

fn exception_matches(exception_types: &[String], err: &ExecutionError) -> bool {
    if exception_types.is_empty() {
        return true;
    }
    if exception_types.len() == 1 && exception_types[0] == "Exception" {
        return true;
    }
    exception_types.iter().any(|name| name == &err.kind)
}

fn len_of_value(value: &Value) -> Result<serde_json::Number, ExecutionError> {
    let len = match value {
        Value::Array(items) => items.len() as i64,
        Value::String(text) => text.len() as i64,
        Value::Object(map) => map.len() as i64,
        _ => {
            return Err(ExecutionError::new(
                "ExecutionError",
                "len() expects list, string, or dict",
            ));
        }
    };
    Ok(len.into())
}

fn range_from_args(args: &[Value]) -> Result<Vec<Value>, ExecutionError> {
    let mut start = 0i64;
    let mut end = 0i64;
    let mut step = 1i64;
    if args.len() == 1 {
        end = args[0].as_i64().unwrap_or(0);
    } else if args.len() >= 2 {
        start = args[0].as_i64().unwrap_or(0);
        end = args[1].as_i64().unwrap_or(0);
        if args.len() >= 3 {
            step = args[2].as_i64().unwrap_or(1);
        }
    }
    if step == 0 {
        return Err(ExecutionError::new(
            "ExecutionError",
            "range() step cannot be 0",
        ));
    }
    let mut values = Vec::new();
    if step > 0 {
        let mut current = start;
        while current < end {
            values.push(Value::Number(current.into()));
            current += step;
        }
    } else {
        let mut current = start;
        while current > end {
            values.push(Value::Number(current.into()));
            current += step;
        }
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use crate::messages::ast as ir;

    #[tokio::test]
    async fn test_statement_executor_runs_basic_program() {
        let action_handler: ActionHandler = Arc::new(|_, _| {
            Box::pin(async {
                Err(ExecutionError::new(
                    "ExecutionError",
                    "action handler should not be called",
                ))
            })
        });

        let program = ir::Program {
            functions: vec![ir::FunctionDef {
                name: "main".to_string(),
                io: Some(ir::IoDecl {
                    inputs: vec!["x".to_string()],
                    outputs: vec!["result".to_string()],
                    span: None,
                }),
                body: Some(ir::Block {
                    statements: vec![
                        ir::Statement {
                            kind: Some(ir::statement::Kind::Assignment(ir::Assignment {
                                targets: vec!["y".to_string()],
                                value: Some(ir::Expr {
                                    kind: Some(ir::expr::Kind::BinaryOp(Box::new(ir::BinaryOp {
                                        left: Some(Box::new(ir::Expr {
                                            kind: Some(ir::expr::Kind::Variable(ir::Variable {
                                                name: "x".to_string(),
                                            })),
                                            span: None,
                                        })),
                                        op: ir::BinaryOperator::BinaryOpAdd as i32,
                                        right: Some(Box::new(ir::Expr {
                                            kind: Some(ir::expr::Kind::Literal(ir::Literal {
                                                value: Some(ir::literal::Value::IntValue(1)),
                                            })),
                                            span: None,
                                        })),
                                    }))),
                                    span: None,
                                }),
                            })),
                            span: None,
                        },
                        ir::Statement {
                            kind: Some(ir::statement::Kind::ReturnStmt(ir::ReturnStmt {
                                value: Some(ir::Expr {
                                    kind: Some(ir::expr::Kind::Variable(ir::Variable {
                                        name: "y".to_string(),
                                    })),
                                    span: None,
                                }),
                            })),
                            span: None,
                        },
                    ],
                    span: None,
                }),
                span: None,
            }],
        };

        let executor = StatementExecutor::new(program, action_handler, None);
        let result = executor
            .execute_program(
                None,
                Some(HashMap::from([("x".to_string(), Value::Number(2.into()))])),
            )
            .await
            .expect("execution");

        assert_eq!(result, Value::Number(3.into()));
    }
}
