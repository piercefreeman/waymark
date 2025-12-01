use std::{collections::HashMap, convert::TryFrom};

use anyhow::{Context, Result, anyhow};
use serde_json::{Number, Value};

use crate::messages::proto;

pub type EvalContext = HashMap<String, Value>;

pub fn eval_expr(expr: &proto::Expr, ctx: &EvalContext) -> Result<Value> {
    match expr.kind.as_ref().context("expr missing kind")? {
        proto::expr::Kind::Name(name) => ctx
            .get(&name.id)
            .cloned()
            .with_context(|| format!("name '{}' not found", name.id)),
        proto::expr::Kind::Constant(c) => Ok(constant_to_value(c)),
        proto::expr::Kind::Attribute(attr) => {
            let base = eval_expr(attr.value.as_ref().context("attr missing value")?, ctx)?;
            attr_lookup(&base, &attr.attr)
        }
        proto::expr::Kind::Subscript(sub) => {
            let base = eval_expr(sub.value.as_ref().context("subscript missing value")?, ctx)?;
            let idx = eval_expr(sub.slice.as_ref().context("subscript missing slice")?, ctx)?;
            subscript_lookup(&base, &idx)
        }
        proto::expr::Kind::BinOp(bin) => {
            let left = eval_expr(bin.left.as_ref().context("binop missing left")?, ctx)?;
            let right = eval_expr(bin.right.as_ref().context("binop missing right")?, ctx)?;
            eval_bin_op(&left, &right, bin.op())
        }
        proto::expr::Kind::BoolOp(bop) => {
            let op = bop.op();
            let mut last = Value::Bool(false);
            match op {
                proto::BoolOpKind::And => {
                    for v in &bop.values {
                        last = eval_expr(v, ctx)?;
                        if !is_truthy(&last) {
                            return Ok(last);
                        }
                    }
                    Ok(last)
                }
                proto::BoolOpKind::Or => {
                    for v in &bop.values {
                        last = eval_expr(v, ctx)?;
                        if is_truthy(&last) {
                            return Ok(last);
                        }
                    }
                    Ok(last)
                }
                _ => Err(anyhow!("unsupported bool op")),
            }
        }
        proto::expr::Kind::Compare(cmp) => eval_compare(cmp, ctx),
        proto::expr::Kind::Call(call) => eval_call(call, ctx),
        proto::expr::Kind::List(list) => {
            let mut items = Vec::with_capacity(list.elts.len());
            for elt in &list.elts {
                items.push(eval_expr(elt, ctx)?);
            }
            Ok(Value::Array(items))
        }
        proto::expr::Kind::Tuple(tuple) => {
            let mut items = Vec::with_capacity(tuple.elts.len());
            for elt in &tuple.elts {
                items.push(eval_expr(elt, ctx)?);
            }
            Ok(Value::Array(items))
        }
        proto::expr::Kind::Dict(dict) => {
            let mut map = serde_json::Map::new();
            for (k, v) in dict.keys.iter().zip(dict.values.iter()) {
                let key_val = eval_expr(k, ctx)?;
                let Value::String(key) = key_val else {
                    return Err(anyhow!("dict key must be string"));
                };
                let value = eval_expr(v, ctx)?;
                map.insert(key, value);
            }
            Ok(Value::Object(map))
        }
        proto::expr::Kind::UnaryOp(op) => {
            let operand = eval_expr(op.operand.as_ref().context("unary missing operand")?, ctx)?;
            eval_unary_op(&operand, op.op())
        }
    }
}

pub fn eval_stmt(stmt: &proto::Stmt, ctx: &mut EvalContext) -> Result<()> {
    match stmt.kind.as_ref().context("stmt missing kind")? {
        proto::stmt::Kind::Assign(assign) => {
            let value = eval_expr(assign.value.as_ref().context("assign missing value")?, ctx)?;
            for target in &assign.targets {
                assign_target(target, value.clone(), ctx)?;
            }
            Ok(())
        }
        proto::stmt::Kind::Expr(expr) => {
            // Handle list.append() specially since it mutates the list
            if let Some(proto::expr::Kind::Call(call)) = &expr.kind
                && let Some(func) = &call.func
                && let Some(proto::expr::Kind::Attribute(attr)) = &func.kind
                && attr.attr == "append"
            {
                return eval_list_append(attr, &call.args, ctx);
            }
            let _ = eval_expr(expr, ctx)?;
            Ok(())
        }
        proto::stmt::Kind::ForStmt(for_stmt) => {
            let iter_value = eval_expr(for_stmt.iter.as_ref().context("for missing iter")?, ctx)?;
            let Value::Array(items) = iter_value else {
                return Err(anyhow!("for loop requires iterable"));
            };
            let target = for_stmt.target.as_ref().context("for missing target")?;
            for item in items {
                assign_target(target, item, ctx)?;
                for body_stmt in &for_stmt.body {
                    eval_stmt(body_stmt, ctx)?;
                }
            }
            Ok(())
        }
        proto::stmt::Kind::AugAssign(aug) => {
            let target = aug.target.as_ref().context("aug_assign missing target")?;
            let current = eval_expr(target, ctx)?;
            let operand = eval_expr(aug.value.as_ref().context("aug_assign missing value")?, ctx)?;
            let op = proto::BinOpKind::try_from(aug.op).unwrap_or(proto::BinOpKind::Unspecified);
            let result = eval_bin_op(&current, &operand, op)?;
            assign_target(target, result, ctx)?;
            Ok(())
        }
    }
}

fn assign_target(target: &proto::Expr, value: Value, ctx: &mut EvalContext) -> Result<()> {
    match target.kind.as_ref().context("assign target missing kind")? {
        proto::expr::Kind::Name(name) => {
            ctx.insert(name.id.clone(), value);
            Ok(())
        }
        proto::expr::Kind::Subscript(sub) => {
            let base = eval_expr(sub.value.as_ref().context("subscript missing value")?, ctx)?;
            let idx = eval_expr(sub.slice.as_ref().context("subscript missing slice")?, ctx)?;
            set_subscript(base, idx, value, ctx, sub.value.as_ref().unwrap())
        }
        proto::expr::Kind::Attribute(attr) => {
            let base = eval_expr(attr.value.as_ref().context("attr missing value")?, ctx)?;
            set_attribute(base, &attr.attr, value, ctx, attr.value.as_ref().unwrap())
        }
        _ => Err(anyhow!("unsupported assignment target")),
    }
}

fn set_subscript(
    base: Value,
    idx: Value,
    value: Value,
    ctx: &mut EvalContext,
    base_expr: &proto::Expr,
) -> Result<()> {
    match (base, idx) {
        (Value::Array(mut arr), Value::Number(n)) => {
            let i = n
                .as_i64()
                .ok_or_else(|| anyhow!("subscript index must be int"))?;
            if i < 0 {
                return Err(anyhow!("subscript index out of range"));
            }
            let idx_usize = i as usize;
            if idx_usize >= arr.len() {
                return Err(anyhow!("subscript index out of range"));
            }
            arr[idx_usize] = value;
            let name = base_name(base_expr)?;
            ctx.insert(name, Value::Array(arr));
            Ok(())
        }
        (Value::Object(mut map), Value::String(key)) => {
            map.insert(key.clone(), value);
            let name = base_name(base_expr)?;
            ctx.insert(name, Value::Object(map));
            Ok(())
        }
        _ => Err(anyhow!("unsupported subscript assignment target")),
    }
}

fn set_attribute(
    base: Value,
    attr: &str,
    value: Value,
    ctx: &mut EvalContext,
    base_expr: &proto::Expr,
) -> Result<()> {
    if let Value::Object(mut map) = base {
        map.insert(attr.to_string(), value);
        let name = base_name(base_expr)?;
        ctx.insert(name, Value::Object(map));
        Ok(())
    } else {
        Err(anyhow!("unsupported attribute assignment target"))
    }
}

fn base_name(expr: &proto::Expr) -> Result<String> {
    match expr.kind.as_ref().context("base expr missing kind")? {
        proto::expr::Kind::Name(name) => Ok(name.id.clone()),
        _ => Err(anyhow!("nested assignment targets not supported")),
    }
}

fn eval_list_append(
    attr: &proto::Attribute,
    args: &[proto::Expr],
    ctx: &mut EvalContext,
) -> Result<()> {
    if args.len() != 1 {
        return Err(anyhow!("list.append expects 1 argument"));
    }
    let base_expr = attr.value.as_ref().context("append missing base")?;
    let var_name = base_name(base_expr)?;

    // Get the list from context
    let base = ctx
        .get(&var_name)
        .cloned()
        .with_context(|| format!("name '{}' not found", var_name))?;
    let Value::Array(mut arr) = base else {
        return Err(anyhow!("append requires list"));
    };

    // Evaluate the argument
    let value = eval_expr(&args[0], ctx)?;

    // Append and update context
    arr.push(value);
    ctx.insert(var_name, Value::Array(arr));
    Ok(())
}

fn constant_to_value(c: &proto::Constant) -> Value {
    match c.value.as_ref() {
        Some(proto::constant::Value::StringValue(s)) => Value::String(s.clone()),
        Some(proto::constant::Value::FloatValue(f)) => {
            Value::Number(Number::from_f64(*f).unwrap_or(Number::from(0)))
        }
        Some(proto::constant::Value::IntValue(i)) => Value::Number(Number::from(*i)),
        Some(proto::constant::Value::BoolValue(b)) => Value::Bool(*b),
        Some(proto::constant::Value::IsNone(_)) => Value::Null,
        None => Value::Null,
    }
}

fn attr_lookup(base: &Value, attr: &str) -> Result<Value> {
    match base {
        Value::Object(map) => map
            .get(attr)
            .cloned()
            .with_context(|| format!("attribute '{}' not found", attr)),
        _ => Err(anyhow!("attribute access requires object")),
    }
}

fn subscript_lookup(base: &Value, idx: &Value) -> Result<Value> {
    match (base, idx) {
        (Value::Array(arr), Value::Number(n)) => {
            let i = n
                .as_i64()
                .ok_or_else(|| anyhow!("subscript index must be int"))?;
            if i < 0 {
                return Err(anyhow!("subscript index out of range"));
            }
            let idx_usize = i as usize;
            arr.get(idx_usize)
                .cloned()
                .with_context(|| format!("index {} out of range", idx_usize))
        }
        (Value::Object(map), Value::String(key)) => map
            .get(key)
            .cloned()
            .with_context(|| format!("key '{}' not found", key)),
        (Value::String(s), Value::Number(n)) => {
            let i = n
                .as_i64()
                .ok_or_else(|| anyhow!("subscript index must be int"))?;
            let idx_usize = i
                .try_into()
                .map_err(|_| anyhow!("subscript index out of range"))?;
            s.chars()
                .nth(idx_usize)
                .map(|c| Value::String(c.to_string()))
                .with_context(|| format!("index {} out of range", idx_usize))
        }
        _ => Err(anyhow!("unsupported subscript lookup")),
    }
}

fn eval_bin_op(left: &Value, right: &Value, op: proto::BinOpKind) -> Result<Value> {
    match op {
        proto::BinOpKind::Add => add_values(left, right),
        proto::BinOpKind::Sub => numeric_op(left, right, |a, b| a - b),
        proto::BinOpKind::Mult => numeric_op(left, right, |a, b| a * b),
        proto::BinOpKind::Div => numeric_op(left, right, |a, b| a / b),
        proto::BinOpKind::Mod => numeric_op(left, right, |a, b| a % b),
        proto::BinOpKind::Floordiv => numeric_op(left, right, |a, b| (a / b).floor()),
        proto::BinOpKind::Pow => numeric_op(left, right, |a, b| a.powf(b)),
        _ => Err(anyhow!("unsupported bin op")),
    }
}

fn add_values(left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::Number(a), Value::Number(b)) => {
            if let (Some(ai), Some(bi)) = (a.as_i64(), b.as_i64()) {
                return Ok(Value::Number(Number::from(ai + bi)));
            }
            if let (Some(au), Some(bu)) = (a.as_u64(), b.as_u64()) {
                return Ok(Value::Number(Number::from(au + bu)));
            }
            let res = to_f64(&Value::Number(a.clone()))? + to_f64(&Value::Number(b.clone()))?;
            Ok(Value::Number(
                Number::from_f64(res).unwrap_or(Number::from(0)),
            ))
        }
        (Value::String(a), Value::String(b)) => Ok(Value::String(format!("{a}{b}"))),
        (Value::Array(a), Value::Array(b)) => {
            let mut merged = a.clone();
            merged.extend(b.clone());
            Ok(Value::Array(merged))
        }
        _ => Err(anyhow!("unsupported add operands")),
    }
}

fn numeric_op<F>(left: &Value, right: &Value, f: F) -> Result<Value>
where
    F: Fn(f64, f64) -> f64,
{
    // Try integer arithmetic first for better precision and compatibility with subscript indexing
    if let (Value::Number(ln), Value::Number(rn)) = (left, right)
        && let (Some(li), Some(ri)) = (ln.as_i64(), rn.as_i64())
    {
        let res = f(li as f64, ri as f64);
        // If result is a whole number and fits in i64, return as integer
        if res.fract() == 0.0 && res >= i64::MIN as f64 && res <= i64::MAX as f64 {
            return Ok(Value::Number(Number::from(res as i64)));
        }
    }
    let a = to_f64(left)?;
    let b = to_f64(right)?;
    let res = f(a, b);
    Ok(Value::Number(
        Number::from_f64(res).unwrap_or(Number::from(0)),
    ))
}

fn to_f64(v: &Value) -> Result<f64> {
    match v {
        Value::Number(n) => n
            .as_f64()
            .or_else(|| n.as_i64().map(|i| i as f64))
            .context("number expected"),
        _ => Err(anyhow!("number expected")),
    }
}

fn to_i64(v: &Value) -> Result<i64> {
    match v {
        Value::Number(n) => n
            .as_i64()
            .or_else(|| n.as_f64().map(|f| f as i64))
            .context("integer expected"),
        _ => Err(anyhow!("integer expected")),
    }
}

fn eval_unary_op(operand: &Value, op: proto::UnaryOpKind) -> Result<Value> {
    match op {
        proto::UnaryOpKind::Usub => {
            let val = -to_f64(operand)?;
            Ok(Value::Number(
                Number::from_f64(val).unwrap_or(Number::from(0)),
            ))
        }
        proto::UnaryOpKind::Uadd => {
            let val = to_f64(operand)?;
            Ok(Value::Number(
                Number::from_f64(val).unwrap_or(Number::from(0)),
            ))
        }
        proto::UnaryOpKind::Not => Ok(Value::Bool(!is_truthy(operand))),
        _ => Err(anyhow!("unsupported unary op")),
    }
}

fn eval_compare(cmp: &proto::Compare, ctx: &EvalContext) -> Result<Value> {
    let mut left = eval_expr(cmp.left.as_ref().context("compare missing left")?, ctx)?;
    for (op, comp) in cmp.ops.iter().zip(cmp.comparators.iter()) {
        let right = eval_expr(comp, ctx)?;
        let op_kind = proto::CmpOpKind::try_from(*op).unwrap_or(proto::CmpOpKind::Unspecified);
        let ok = match op_kind {
            proto::CmpOpKind::Eq => left == right,
            proto::CmpOpKind::NotEq => left != right,
            proto::CmpOpKind::Lt => to_f64(&left)? < to_f64(&right)?,
            proto::CmpOpKind::LtE => to_f64(&left)? <= to_f64(&right)?,
            proto::CmpOpKind::Gt => to_f64(&left)? > to_f64(&right)?,
            proto::CmpOpKind::GtE => to_f64(&left)? >= to_f64(&right)?,
            proto::CmpOpKind::In => contains(&right, &left)?,
            proto::CmpOpKind::NotIn => !contains(&right, &left)?,
            proto::CmpOpKind::Is => is_identical(&left, &right),
            proto::CmpOpKind::IsNot => !is_identical(&left, &right),
            _ => return Err(anyhow!("unsupported compare op")),
        };
        if !ok {
            return Ok(Value::Bool(false));
        }
        left = right;
    }
    Ok(Value::Bool(true))
}

fn contains(container: &Value, item: &Value) -> Result<bool> {
    match container {
        Value::Array(arr) => Ok(arr.contains(item)),
        Value::String(s) => {
            let Value::String(needle) = item else {
                return Ok(false);
            };
            Ok(s.contains(needle.as_str()))
        }
        Value::Object(map) => {
            let Value::String(key) = item else {
                return Ok(false);
            };
            Ok(map.contains_key(key))
        }
        _ => Ok(false),
    }
}

fn is_identical(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Null, Value::Null) => true,
        _ => a == b,
    }
}

fn eval_call(call: &proto::Call, ctx: &EvalContext) -> Result<Value> {
    let func_expr = call.func.as_ref().context("call missing func")?;
    let mut args = Vec::with_capacity(call.args.len());
    for arg in &call.args {
        args.push(eval_expr(arg, ctx)?);
    }
    match func_expr.kind.as_ref().context("call func missing kind")? {
        proto::expr::Kind::Name(name) => match name.id.as_str() {
            "len" => {
                if args.len() != 1 {
                    return Err(anyhow!("len expects 1 argument"));
                }
                let len = match &args[0] {
                    Value::Array(a) => a.len(),
                    Value::Object(o) => o.len(),
                    Value::String(s) => s.chars().count(),
                    _ => return Err(anyhow!("len unsupported type")),
                };
                Ok(Value::Number(Number::from(len as u64)))
            }
            "str" => {
                if args.len() != 1 {
                    return Err(anyhow!("str expects 1 argument"));
                }
                let text = match &args[0] {
                    Value::String(s) => s.clone(),
                    Value::Number(n) => n.to_string(),
                    Value::Bool(b) => b.to_string(),
                    Value::Null => "None".to_string(),
                    other => serde_json::to_string(other).unwrap_or_default(),
                };
                Ok(Value::String(text))
            }
            "range" => {
                // range(stop) or range(start, stop) or range(start, stop, step)
                let (start, stop, step) = match args.len() {
                    1 => (0i64, to_i64(&args[0])?, 1i64),
                    2 => (to_i64(&args[0])?, to_i64(&args[1])?, 1i64),
                    3 => (to_i64(&args[0])?, to_i64(&args[1])?, to_i64(&args[2])?),
                    _ => return Err(anyhow!("range expects 1, 2, or 3 arguments")),
                };
                if step == 0 {
                    return Err(anyhow!("range step cannot be zero"));
                }
                let mut result = Vec::new();
                let mut i = start;
                if step > 0 {
                    while i < stop {
                        result.push(Value::Number(Number::from(i)));
                        i += step;
                    }
                } else {
                    while i > stop {
                        result.push(Value::Number(Number::from(i)));
                        i += step;
                    }
                }
                Ok(Value::Array(result))
            }
            "enumerate" => {
                // enumerate(iterable) or enumerate(iterable, start)
                if args.is_empty() || args.len() > 2 {
                    return Err(anyhow!("enumerate expects 1 or 2 arguments"));
                }
                let start = if args.len() == 2 {
                    to_i64(&args[1])?
                } else {
                    0
                };
                let Value::Array(items) = &args[0] else {
                    return Err(anyhow!("enumerate requires an iterable"));
                };
                let result: Vec<Value> = items
                    .iter()
                    .enumerate()
                    .map(|(i, v)| {
                        Value::Array(vec![
                            Value::Number(Number::from(start + i as i64)),
                            v.clone(),
                        ])
                    })
                    .collect();
                Ok(Value::Array(result))
            }
            "list" => {
                // list(iterable) - convert to list (mostly a no-op for arrays)
                if args.len() != 1 {
                    return Err(anyhow!("list expects 1 argument"));
                }
                match &args[0] {
                    Value::Array(a) => Ok(Value::Array(a.clone())),
                    Value::String(s) => {
                        // Convert string to list of characters
                        let chars: Vec<Value> =
                            s.chars().map(|c| Value::String(c.to_string())).collect();
                        Ok(Value::Array(chars))
                    }
                    _ => Err(anyhow!("list requires an iterable")),
                }
            }
            "int" => {
                if args.len() != 1 {
                    return Err(anyhow!("int expects 1 argument"));
                }
                let val = match &args[0] {
                    Value::Number(n) => n.as_i64().unwrap_or(0),
                    Value::String(s) => s.parse::<i64>().unwrap_or(0),
                    Value::Bool(b) => {
                        if *b {
                            1
                        } else {
                            0
                        }
                    }
                    _ => return Err(anyhow!("int unsupported type")),
                };
                Ok(Value::Number(Number::from(val)))
            }
            "float" => {
                if args.len() != 1 {
                    return Err(anyhow!("float expects 1 argument"));
                }
                let val = match &args[0] {
                    Value::Number(n) => n.as_f64().unwrap_or(0.0),
                    Value::String(s) => s.parse::<f64>().unwrap_or(0.0),
                    Value::Bool(b) => {
                        if *b {
                            1.0
                        } else {
                            0.0
                        }
                    }
                    _ => return Err(anyhow!("float unsupported type")),
                };
                Ok(Value::Number(
                    Number::from_f64(val).unwrap_or(Number::from(0)),
                ))
            }
            "bool" => {
                if args.len() != 1 {
                    return Err(anyhow!("bool expects 1 argument"));
                }
                Ok(Value::Bool(is_truthy(&args[0])))
            }
            "min" => {
                if args.is_empty() {
                    return Err(anyhow!("min expects at least 1 argument"));
                }
                // Handle both min(a, b, c) and min([a, b, c])
                let items = if args.len() == 1 {
                    match &args[0] {
                        Value::Array(arr) => arr.clone(),
                        _ => args.clone(),
                    }
                } else {
                    args.clone()
                };
                if items.is_empty() {
                    return Err(anyhow!("min arg is an empty sequence"));
                }
                let mut min_val = to_f64(&items[0])?;
                let mut min_item = items[0].clone();
                for item in items.iter().skip(1) {
                    let val = to_f64(item)?;
                    if val < min_val {
                        min_val = val;
                        min_item = item.clone();
                    }
                }
                Ok(min_item)
            }
            "max" => {
                if args.is_empty() {
                    return Err(anyhow!("max expects at least 1 argument"));
                }
                // Handle both max(a, b, c) and max([a, b, c])
                let items = if args.len() == 1 {
                    match &args[0] {
                        Value::Array(arr) => arr.clone(),
                        _ => args.clone(),
                    }
                } else {
                    args.clone()
                };
                if items.is_empty() {
                    return Err(anyhow!("max arg is an empty sequence"));
                }
                let mut max_val = to_f64(&items[0])?;
                let mut max_item = items[0].clone();
                for item in items.iter().skip(1) {
                    let val = to_f64(item)?;
                    if val > max_val {
                        max_val = val;
                        max_item = item.clone();
                    }
                }
                Ok(max_item)
            }
            "sum" => {
                if args.len() != 1 {
                    return Err(anyhow!("sum expects 1 argument"));
                }
                let Value::Array(items) = &args[0] else {
                    return Err(anyhow!("sum requires an iterable"));
                };
                let mut total = 0.0;
                for item in items {
                    total += to_f64(item)?;
                }
                // Return int if result is whole number
                if total.fract() == 0.0 && total.abs() < i64::MAX as f64 {
                    Ok(Value::Number(Number::from(total as i64)))
                } else {
                    Ok(Value::Number(
                        Number::from_f64(total).unwrap_or(Number::from(0)),
                    ))
                }
            }
            "abs" => {
                if args.len() != 1 {
                    return Err(anyhow!("abs expects 1 argument"));
                }
                let val = to_f64(&args[0])?;
                if val.fract() == 0.0 {
                    Ok(Value::Number(Number::from(val.abs() as i64)))
                } else {
                    Ok(Value::Number(
                        Number::from_f64(val.abs()).unwrap_or(Number::from(0)),
                    ))
                }
            }
            other => Err(anyhow!("unsupported call '{}'", other)),
        },
        proto::expr::Kind::Attribute(attr) => {
            let base = eval_expr(
                attr.value.as_ref().context("attribute call missing base")?,
                ctx,
            )?;
            match attr.attr.as_str() {
                "get" => {
                    if args.is_empty() || args.len() > 2 {
                        return Err(anyhow!("dict.get expects 1 or 2 arguments"));
                    }
                    let default = args.get(1).cloned().unwrap_or(Value::Null);
                    let key = match &args[0] {
                        Value::String(s) => s.clone(),
                        _ => return Ok(default),
                    };
                    match base {
                        Value::Object(map) => Ok(map.get(&key).cloned().unwrap_or(default)),
                        _ => Err(anyhow!("get requires object base")),
                    }
                }
                other => Err(anyhow!("unsupported attribute call '{}'", other)),
            }
        }
        _ => Err(anyhow!("only simple function names supported")),
    }
}

fn is_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                i != 0
            } else if let Some(f) = n.as_f64() {
                f != 0.0
            } else {
                true
            }
        }
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name(id: &str) -> proto::Expr {
        proto::Expr {
            kind: Some(proto::expr::Kind::Name(proto::Name { id: id.to_string() })),
        }
    }

    fn int(i: i64) -> proto::Expr {
        proto::Expr {
            kind: Some(proto::expr::Kind::Constant(proto::Constant {
                value: Some(proto::constant::Value::IntValue(i)),
            })),
        }
    }

    #[test]
    fn evals_binop_add_numbers() {
        let expr = proto::Expr {
            kind: Some(proto::expr::Kind::BinOp(Box::new(proto::BinOp {
                left: Some(Box::new(int(2))),
                right: Some(Box::new(int(3))),
                op: proto::BinOpKind::Add as i32,
            }))),
        };
        let ctx = EvalContext::new();
        let result = eval_expr(&expr, &ctx).unwrap();
        assert_eq!(result, Value::Number(Number::from(5)));
    }

    #[test]
    fn evals_compare_chain() {
        let expr = proto::Expr {
            kind: Some(proto::expr::Kind::Compare(Box::new(proto::Compare {
                left: Some(Box::new(int(1))),
                ops: vec![proto::CmpOpKind::Lt as i32, proto::CmpOpKind::Lt as i32],
                comparators: vec![int(2), int(3)],
            }))),
        };
        let ctx = EvalContext::new();
        let result = eval_expr(&expr, &ctx).unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn supports_len_call() {
        let expr = proto::Expr {
            kind: Some(proto::expr::Kind::Call(Box::new(proto::Call {
                func: Some(Box::new(name("len"))),
                args: vec![proto::Expr {
                    kind: Some(proto::expr::Kind::List(proto::List {
                        elts: vec![int(1), int(2)],
                    })),
                }],
                keywords: Vec::new(),
            }))),
        };
        let ctx = EvalContext::new();
        let result = eval_expr(&expr, &ctx).unwrap();
        assert_eq!(result, Value::Number(Number::from(2)));
    }

    #[test]
    fn assigns_to_context() {
        let mut ctx = EvalContext::new();
        let stmt = proto::Stmt {
            kind: Some(proto::stmt::Kind::Assign(proto::Assign {
                targets: vec![name("x")],
                value: Some(int(10)),
            })),
        };
        eval_stmt(&stmt, &mut ctx).unwrap();
        assert_eq!(ctx.get("x"), Some(&Value::Number(Number::from(10))));
    }

    fn call(func_name: &str, args: Vec<proto::Expr>) -> proto::Expr {
        proto::Expr {
            kind: Some(proto::expr::Kind::Call(Box::new(proto::Call {
                func: Some(Box::new(name(func_name))),
                args,
                keywords: Vec::new(),
            }))),
        }
    }

    #[test]
    fn supports_range_single_arg() {
        let expr = call("range", vec![int(5)]);
        let ctx = EvalContext::new();
        let result = eval_expr(&expr, &ctx).unwrap();
        assert_eq!(
            result,
            Value::Array(vec![
                Value::Number(Number::from(0)),
                Value::Number(Number::from(1)),
                Value::Number(Number::from(2)),
                Value::Number(Number::from(3)),
                Value::Number(Number::from(4)),
            ])
        );
    }

    #[test]
    fn supports_range_two_args() {
        let expr = call("range", vec![int(2), int(5)]);
        let ctx = EvalContext::new();
        let result = eval_expr(&expr, &ctx).unwrap();
        assert_eq!(
            result,
            Value::Array(vec![
                Value::Number(Number::from(2)),
                Value::Number(Number::from(3)),
                Value::Number(Number::from(4)),
            ])
        );
    }

    #[test]
    fn supports_range_three_args() {
        let expr = call("range", vec![int(0), int(10), int(2)]);
        let ctx = EvalContext::new();
        let result = eval_expr(&expr, &ctx).unwrap();
        assert_eq!(
            result,
            Value::Array(vec![
                Value::Number(Number::from(0)),
                Value::Number(Number::from(2)),
                Value::Number(Number::from(4)),
                Value::Number(Number::from(6)),
                Value::Number(Number::from(8)),
            ])
        );
    }

    #[test]
    fn supports_range_negative_step() {
        let expr = call("range", vec![int(5), int(0), int(-1)]);
        let ctx = EvalContext::new();
        let result = eval_expr(&expr, &ctx).unwrap();
        assert_eq!(
            result,
            Value::Array(vec![
                Value::Number(Number::from(5)),
                Value::Number(Number::from(4)),
                Value::Number(Number::from(3)),
                Value::Number(Number::from(2)),
                Value::Number(Number::from(1)),
            ])
        );
    }

    #[test]
    fn supports_enumerate() {
        let list_expr = proto::Expr {
            kind: Some(proto::expr::Kind::List(proto::List {
                elts: vec![int(10), int(20), int(30)],
            })),
        };
        let expr = call("enumerate", vec![list_expr]);
        let ctx = EvalContext::new();
        let result = eval_expr(&expr, &ctx).unwrap();
        assert_eq!(
            result,
            Value::Array(vec![
                Value::Array(vec![
                    Value::Number(Number::from(0)),
                    Value::Number(Number::from(10))
                ]),
                Value::Array(vec![
                    Value::Number(Number::from(1)),
                    Value::Number(Number::from(20))
                ]),
                Value::Array(vec![
                    Value::Number(Number::from(2)),
                    Value::Number(Number::from(30))
                ]),
            ])
        );
    }

    #[test]
    fn supports_enumerate_with_start() {
        let list_expr = proto::Expr {
            kind: Some(proto::expr::Kind::List(proto::List {
                elts: vec![int(10), int(20)],
            })),
        };
        let expr = call("enumerate", vec![list_expr, int(5)]);
        let ctx = EvalContext::new();
        let result = eval_expr(&expr, &ctx).unwrap();
        assert_eq!(
            result,
            Value::Array(vec![
                Value::Array(vec![
                    Value::Number(Number::from(5)),
                    Value::Number(Number::from(10))
                ]),
                Value::Array(vec![
                    Value::Number(Number::from(6)),
                    Value::Number(Number::from(20))
                ]),
            ])
        );
    }

    #[test]
    fn supports_min_max_sum() {
        let list_expr = proto::Expr {
            kind: Some(proto::expr::Kind::List(proto::List {
                elts: vec![int(3), int(1), int(4), int(1), int(5)],
            })),
        };
        let ctx = EvalContext::new();

        let min_result = eval_expr(&call("min", vec![list_expr.clone()]), &ctx).unwrap();
        assert_eq!(min_result, Value::Number(Number::from(1)));

        let max_result = eval_expr(&call("max", vec![list_expr.clone()]), &ctx).unwrap();
        assert_eq!(max_result, Value::Number(Number::from(5)));

        let sum_result = eval_expr(&call("sum", vec![list_expr]), &ctx).unwrap();
        assert_eq!(sum_result, Value::Number(Number::from(14)));
    }

    #[test]
    fn supports_abs() {
        let ctx = EvalContext::new();
        let result = eval_expr(&call("abs", vec![int(-42)]), &ctx).unwrap();
        assert_eq!(result, Value::Number(Number::from(42)));
    }

    #[test]
    fn supports_list_append() {
        let mut ctx = EvalContext::new();
        ctx.insert(
            "items".to_string(),
            Value::Array(vec![Value::Number(Number::from(1))]),
        );

        // Create: items.append(2)
        let append_call = proto::Expr {
            kind: Some(proto::expr::Kind::Call(Box::new(proto::Call {
                func: Some(Box::new(proto::Expr {
                    kind: Some(proto::expr::Kind::Attribute(Box::new(proto::Attribute {
                        value: Some(Box::new(name("items"))),
                        attr: "append".to_string(),
                    }))),
                })),
                args: vec![int(2)],
                keywords: Vec::new(),
            }))),
        };
        let stmt = proto::Stmt {
            kind: Some(proto::stmt::Kind::Expr(append_call)),
        };
        eval_stmt(&stmt, &mut ctx).unwrap();
        assert_eq!(
            ctx.get("items"),
            Some(&Value::Array(vec![
                Value::Number(Number::from(1)),
                Value::Number(Number::from(2)),
            ]))
        );
    }

    #[test]
    fn supports_modulo_preserves_int() {
        // Test that (a + b) % c returns an integer when inputs are integers
        let mut ctx = EvalContext::new();
        ctx.insert("a".to_string(), Value::Number(Number::from(5)));
        ctx.insert("b".to_string(), Value::Number(Number::from(3)));
        ctx.insert("c".to_string(), Value::Number(Number::from(4)));

        // (a + b) % c = (5 + 3) % 4 = 8 % 4 = 0
        let expr = proto::Expr {
            kind: Some(proto::expr::Kind::BinOp(Box::new(proto::BinOp {
                left: Some(Box::new(proto::Expr {
                    kind: Some(proto::expr::Kind::BinOp(Box::new(proto::BinOp {
                        left: Some(Box::new(name("a"))),
                        right: Some(Box::new(name("b"))),
                        op: proto::BinOpKind::Add as i32,
                    }))),
                })),
                right: Some(Box::new(name("c"))),
                op: proto::BinOpKind::Mod as i32,
            }))),
        };
        let result = eval_expr(&expr, &ctx).unwrap();
        // Should be an integer 0, not a float 0.0
        assert_eq!(result, Value::Number(Number::from(0i64)));
        // Verify it's usable as an array index
        assert!(result.as_i64().is_some());
    }
}
