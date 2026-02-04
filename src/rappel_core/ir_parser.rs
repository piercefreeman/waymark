//! Parser for the IR source-like format.

use std::fmt;

use crate::messages::ast as ir;

/// Raised when parsing the IR source representation fails.
#[derive(Debug, Clone)]
pub struct IRParseError(pub String);

impl fmt::Display for IRParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for IRParseError {}

#[derive(Debug, Clone)]
struct Token {
    kind: String,
    value: String,
    position: usize,
}

struct Tokenizer {
    text: String,
    pos: usize,
    length: usize,
}

impl Tokenizer {
    fn new(text: &str) -> Self {
        Self {
            text: text.to_string(),
            pos: 0,
            length: text.len(),
        }
    }

    fn next_token(&mut self) -> Result<Token, IRParseError> {
        self.skip_whitespace();
        if self.pos >= self.length {
            return Ok(Token {
                kind: "EOF".to_string(),
                value: "".to_string(),
                position: self.pos,
            });
        }

        let ch = self.peek_char();
        if ch.is_ascii_alphabetic() || ch == '_' {
            let start = self.pos;
            self.pos += 1;
            while self.pos < self.length {
                let ch = self.peek_char();
                if ch.is_ascii_alphanumeric() || ch == '_' {
                    self.pos += 1;
                } else {
                    break;
                }
            }
            let value = self.text[start..self.pos].to_string();
            return Ok(Token {
                kind: "NAME".to_string(),
                value,
                position: start,
            });
        }

        if ch.is_ascii_digit() {
            let start = self.pos;
            self.pos += 1;
            while self.pos < self.length && self.peek_char().is_ascii_digit() {
                self.pos += 1;
            }
            if self.pos < self.length && self.peek_char() == '.' {
                self.pos += 1;
                while self.pos < self.length && self.peek_char().is_ascii_digit() {
                    self.pos += 1;
                }
            }
            if self.pos < self.length && matches!(self.peek_char(), 'e' | 'E') {
                self.pos += 1;
                if self.pos < self.length && matches!(self.peek_char(), '+' | '-') {
                    self.pos += 1;
                }
                while self.pos < self.length && self.peek_char().is_ascii_digit() {
                    self.pos += 1;
                }
            }
            let value = self.text[start..self.pos].to_string();
            return Ok(Token {
                kind: "NUMBER".to_string(),
                value,
                position: start,
            });
        }

        if ch == '\'' || ch == '"' {
            let start = self.pos;
            let quote = ch;
            self.pos += 1;
            let mut escaped = false;
            while self.pos < self.length {
                let current = self.peek_char();
                if escaped {
                    escaped = false;
                } else if current == '\\' {
                    escaped = true;
                } else if current == quote {
                    self.pos += 1;
                    break;
                }
                self.pos += 1;
            }
            let value = self.text[start..self.pos].to_string();
            return Ok(Token {
                kind: "STRING".to_string(),
                value,
                position: start,
            });
        }

        let two_char = ["==", "!=", ">=", "<=", "//", "->"];
        if self.pos + 1 < self.length {
            let pair = &self.text[self.pos..self.pos + 2];
            if two_char.contains(&pair) {
                self.pos += 2;
                return Ok(Token {
                    kind: pair.to_string(),
                    value: pair.to_string(),
                    position: self.pos - 2,
                });
            }
        }

        if "()[]{}.,:+-*/%<>=@".contains(ch) {
            self.pos += 1;
            return Ok(Token {
                kind: ch.to_string(),
                value: ch.to_string(),
                position: self.pos - 1,
            });
        }

        Err(IRParseError(format!("Unexpected character '{ch}'")))
    }

    fn skip_whitespace(&mut self) {
        while self.pos < self.length && self.peek_char().is_whitespace() {
            self.pos += 1;
        }
    }

    fn peek_char(&self) -> char {
        self.text[self.pos..].chars().next().unwrap_or('\0')
    }
}

struct TokenStream {
    tokens: Vec<Token>,
    index: usize,
}

impl TokenStream {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, index: 0 }
    }

    fn peek(&self) -> &Token {
        &self.tokens[self.index]
    }

    fn peek_next(&self) -> Token {
        if self.index + 1 < self.tokens.len() {
            self.tokens[self.index + 1].clone()
        } else {
            Token {
                kind: "EOF".to_string(),
                value: "".to_string(),
                position: self.tokens.last().map(|tok| tok.position).unwrap_or(0),
            }
        }
    }

    fn advance(&mut self) -> Token {
        let token = self.tokens[self.index].clone();
        if self.index + 1 < self.tokens.len() {
            self.index += 1;
        }
        token
    }

    fn expect(&mut self, kind: &str, value: Option<&str>) -> Result<Token, IRParseError> {
        let token = self.peek().clone();
        if token.kind != kind {
            return Err(IRParseError(format!(
                "Expected {kind}, found {}",
                token.kind
            )));
        }
        if let Some(value) = value
            && token.value != value
        {
            return Err(IRParseError(format!(
                "Expected {value}, found {}",
                token.value
            )));
        }
        Ok(self.advance())
    }

    fn r#match(&mut self, kind: &str, value: Option<&str>) -> bool {
        let token = self.peek();
        if token.kind != kind {
            return false;
        }
        if let Some(value) = value
            && token.value != value
        {
            return false;
        }
        self.advance();
        true
    }
}

struct ExprParser {
    stream: TokenStream,
}

impl ExprParser {
    fn new(text: &str) -> Result<Self, IRParseError> {
        let mut tokenizer = Tokenizer::new(text);
        let mut tokens = Vec::new();
        loop {
            let token = tokenizer.next_token()?;
            let done = token.kind == "EOF";
            tokens.push(token);
            if done {
                break;
            }
        }
        Ok(Self {
            stream: TokenStream::new(tokens),
        })
    }

    fn parse(&mut self) -> Result<ir::Expr, IRParseError> {
        let expr = self.parse_expr(0)?;
        if self.stream.peek().kind != "EOF" {
            return Err(IRParseError(
                "Unexpected tokens at end of expression".to_string(),
            ));
        }
        Ok(expr)
    }

    fn parse_expr(&mut self, min_prec: i32) -> Result<ir::Expr, IRParseError> {
        let mut left = self.parse_unary()?;

        loop {
            let op_info = self.peek_binary_op();
            let (op_str, prec) = match op_info {
                Some(info) => info,
                None => break,
            };
            if prec < min_prec {
                break;
            }
            if op_str == "not in" {
                self.stream.expect("NAME", Some("not"))?;
                self.stream.expect("NAME", Some("in"))?;
            } else {
                let token = self.stream.advance();
                let _ = token.value;
            }

            let right = self.parse_expr(prec + 1)?;
            let op = Self::binary_operator(op_str.as_str());
            left = ir::Expr {
                kind: Some(ir::expr::Kind::BinaryOp(Box::new(ir::BinaryOp {
                    left: Some(Box::new(left)),
                    op,
                    right: Some(Box::new(right)),
                }))),
                span: None,
            };
        }

        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<ir::Expr, IRParseError> {
        let token = self.stream.peek().clone();
        if token.kind == "NAME" && token.value == "not" {
            self.stream.advance();
            let operand = self.parse_expr(60)?;
            return Ok(ir::Expr {
                kind: Some(ir::expr::Kind::UnaryOp(Box::new(ir::UnaryOp {
                    op: ir::UnaryOperator::UnaryOpNot as i32,
                    operand: Some(Box::new(operand)),
                }))),
                span: None,
            });
        }
        if token.kind == "-" {
            self.stream.advance();
            let operand = self.parse_expr(60)?;
            return Ok(ir::Expr {
                kind: Some(ir::expr::Kind::UnaryOp(Box::new(ir::UnaryOp {
                    op: ir::UnaryOperator::UnaryOpNeg as i32,
                    operand: Some(Box::new(operand)),
                }))),
                span: None,
            });
        }
        self.parse_postfix()
    }

    fn parse_postfix(&mut self) -> Result<ir::Expr, IRParseError> {
        let mut expr = self.parse_primary()?;
        loop {
            if self.stream.r#match(".", None) {
                let attr = self.stream.expect("NAME", None)?;
                expr = ir::Expr {
                    kind: Some(ir::expr::Kind::Dot(Box::new(ir::DotAccess {
                        object: Some(Box::new(expr)),
                        attribute: attr.value,
                    }))),
                    span: None,
                };
                continue;
            }
            if self.stream.r#match("[", None) {
                let index_expr = self.parse_expr(0)?;
                self.stream.expect("]", None)?;
                expr = ir::Expr {
                    kind: Some(ir::expr::Kind::Index(Box::new(ir::IndexAccess {
                        object: Some(Box::new(expr)),
                        index: Some(Box::new(index_expr)),
                    }))),
                    span: None,
                };
                continue;
            }
            break;
        }
        Ok(expr)
    }

    fn parse_primary(&mut self) -> Result<ir::Expr, IRParseError> {
        let token = self.stream.peek().clone();
        if token.kind == "NUMBER" {
            self.stream.advance();
            if token.value.contains('.') || token.value.contains('e') || token.value.contains('E') {
                let value: f64 = token
                    .value
                    .parse()
                    .map_err(|_| IRParseError("Invalid float".to_string()))?;
                return Ok(ir::Expr {
                    kind: Some(ir::expr::Kind::Literal(ir::Literal {
                        value: Some(ir::literal::Value::FloatValue(value)),
                    })),
                    span: None,
                });
            }
            let value: i64 = token
                .value
                .parse()
                .map_err(|_| IRParseError("Invalid int".to_string()))?;
            return Ok(ir::Expr {
                kind: Some(ir::expr::Kind::Literal(ir::Literal {
                    value: Some(ir::literal::Value::IntValue(value)),
                })),
                span: None,
            });
        }

        if token.kind == "STRING" {
            self.stream.advance();
            let value = decode_string(&token.value)?;
            return Ok(ir::Expr {
                kind: Some(ir::expr::Kind::Literal(ir::Literal {
                    value: Some(ir::literal::Value::StringValue(value)),
                })),
                span: None,
            });
        }

        if token.kind == "NAME" {
            if token.value == "True" {
                self.stream.advance();
                return Ok(ir::Expr {
                    kind: Some(ir::expr::Kind::Literal(ir::Literal {
                        value: Some(ir::literal::Value::BoolValue(true)),
                    })),
                    span: None,
                });
            }
            if token.value == "False" {
                self.stream.advance();
                return Ok(ir::Expr {
                    kind: Some(ir::expr::Kind::Literal(ir::Literal {
                        value: Some(ir::literal::Value::BoolValue(false)),
                    })),
                    span: None,
                });
            }
            if token.value == "None" {
                self.stream.advance();
                return Ok(ir::Expr {
                    kind: Some(ir::expr::Kind::Literal(ir::Literal {
                        value: Some(ir::literal::Value::IsNone(true)),
                    })),
                    span: None,
                });
            }
            if token.value == "spread" {
                return self.parse_spread_expr();
            }
            if token.value == "parallel" && self.stream.peek_next().kind == "(" {
                return self.parse_parallel_expr();
            }
            let name = self.stream.advance().value;
            if self.stream.peek().kind == "(" {
                return Ok(ir::Expr {
                    kind: Some(ir::expr::Kind::FunctionCall(
                        self.parse_function_call(&name)?,
                    )),
                    span: None,
                });
            }
            return Ok(ir::Expr {
                kind: Some(ir::expr::Kind::Variable(ir::Variable { name })),
                span: None,
            });
        }

        if token.kind == "@" {
            let action = self.parse_action_call()?;
            return Ok(ir::Expr {
                kind: Some(ir::expr::Kind::ActionCall(action)),
                span: None,
            });
        }

        if token.kind == "(" {
            self.stream.advance();
            let expr = self.parse_expr(0)?;
            self.stream.expect(")", None)?;
            return Ok(expr);
        }

        if token.kind == "[" {
            return self.parse_list();
        }

        if token.kind == "{" {
            return self.parse_dict();
        }

        Err(IRParseError(format!("Unexpected token {}", token.kind)))
    }

    fn parse_list(&mut self) -> Result<ir::Expr, IRParseError> {
        self.stream.expect("[", None)?;
        let mut elements: Vec<ir::Expr> = Vec::new();
        if self.stream.r#match("]", None) {
            return Ok(ir::Expr {
                kind: Some(ir::expr::Kind::List(ir::ListExpr { elements })),
                span: None,
            });
        }
        loop {
            elements.push(self.parse_expr(0)?);
            if self.stream.r#match(",", None) {
                if self.stream.peek().kind == "]" {
                    break;
                }
                continue;
            }
            break;
        }
        self.stream.expect("]", None)?;
        Ok(ir::Expr {
            kind: Some(ir::expr::Kind::List(ir::ListExpr { elements })),
            span: None,
        })
    }

    fn parse_dict(&mut self) -> Result<ir::Expr, IRParseError> {
        self.stream.expect("{", None)?;
        let mut entries: Vec<ir::DictEntry> = Vec::new();
        if self.stream.r#match("}", None) {
            return Ok(ir::Expr {
                kind: Some(ir::expr::Kind::Dict(ir::DictExpr { entries })),
                span: None,
            });
        }
        loop {
            let key = self.parse_expr(0)?;
            self.stream.expect(":", None)?;
            let value = self.parse_expr(0)?;
            entries.push(ir::DictEntry {
                key: Some(key),
                value: Some(value),
            });
            if self.stream.r#match(",", None) {
                if self.stream.peek().kind == "}" {
                    break;
                }
                continue;
            }
            break;
        }
        self.stream.expect("}", None)?;
        Ok(ir::Expr {
            kind: Some(ir::expr::Kind::Dict(ir::DictExpr { entries })),
            span: None,
        })
    }

    fn parse_function_call(&mut self, name: &str) -> Result<ir::FunctionCall, IRParseError> {
        self.stream.expect("(", None)?;
        let mut args: Vec<ir::Expr> = Vec::new();
        let mut kwargs: Vec<ir::Kwarg> = Vec::new();
        if self.stream.r#match(")", None) {
            return Ok(self.build_function_call(name, args, kwargs));
        }
        loop {
            let token = self.stream.peek().clone();
            if token.kind == "NAME" && self.stream.peek_next().kind == "=" {
                let key = self.stream.advance().value;
                self.stream.expect("=", None)?;
                let value = self.parse_expr(0)?;
                kwargs.push(ir::Kwarg {
                    name: key,
                    value: Some(value),
                });
            } else {
                args.push(self.parse_expr(0)?);
            }
            if self.stream.r#match(",", None) {
                if self.stream.peek().kind == ")" {
                    break;
                }
                continue;
            }
            break;
        }
        self.stream.expect(")", None)?;
        Ok(self.build_function_call(name, args, kwargs))
    }

    fn build_function_call(
        &self,
        name: &str,
        args: Vec<ir::Expr>,
        kwargs: Vec<ir::Kwarg>,
    ) -> ir::FunctionCall {
        let global_function = match name {
            "range" => ir::GlobalFunction::Range as i32,
            "len" => ir::GlobalFunction::Len as i32,
            "enumerate" => ir::GlobalFunction::Enumerate as i32,
            "isexception" => ir::GlobalFunction::Isexception as i32,
            _ => ir::GlobalFunction::Unspecified as i32,
        };
        if global_function != ir::GlobalFunction::Unspecified as i32 {
            return ir::FunctionCall {
                name: "".to_string(),
                args,
                kwargs,
                global_function,
            };
        }
        ir::FunctionCall {
            name: name.to_string(),
            args,
            kwargs,
            global_function,
        }
    }

    fn parse_parallel_expr(&mut self) -> Result<ir::Expr, IRParseError> {
        self.stream.expect("NAME", Some("parallel"))?;
        self.stream.expect("(", None)?;
        let mut calls: Vec<ir::Call> = Vec::new();
        if self.stream.r#match(")", None) {
            return Ok(ir::Expr {
                kind: Some(ir::expr::Kind::ParallelExpr(ir::ParallelExpr { calls })),
                span: None,
            });
        }
        loop {
            calls.push(self.parse_call()?);
            if self.stream.r#match(",", None) {
                if self.stream.peek().kind == ")" {
                    break;
                }
                continue;
            }
            break;
        }
        self.stream.expect(")", None)?;
        Ok(ir::Expr {
            kind: Some(ir::expr::Kind::ParallelExpr(ir::ParallelExpr { calls })),
            span: None,
        })
    }

    fn parse_call(&mut self) -> Result<ir::Call, IRParseError> {
        let token = self.stream.peek().clone();
        if token.kind == "@" {
            let action = self.parse_action_call()?;
            return Ok(ir::Call {
                kind: Some(ir::call::Kind::Action(action)),
            });
        }
        if token.kind == "NAME" && self.stream.peek_next().kind == "(" {
            let name = self.stream.advance().value;
            let func = self.parse_function_call(&name)?;
            return Ok(ir::Call {
                kind: Some(ir::call::Kind::Function(func)),
            });
        }
        Err(IRParseError("Expected action or function call".to_string()))
    }

    fn parse_action_call(&mut self) -> Result<ir::ActionCall, IRParseError> {
        self.stream.expect("@", None)?;
        let mut name_parts: Vec<String> = Vec::new();
        name_parts.push(self.stream.expect("NAME", None)?.value);
        while self.stream.r#match(".", None) {
            name_parts.push(self.stream.expect("NAME", None)?.value);
        }
        if name_parts.is_empty() {
            return Err(IRParseError("Action call missing name".to_string()));
        }
        let (module_name, action_name) = if name_parts.len() == 1 {
            (None, name_parts[0].clone())
        } else {
            (
                Some(name_parts[..name_parts.len() - 1].join(".")),
                name_parts.last().cloned().unwrap(),
            )
        };

        self.stream.expect("(", None)?;
        let mut kwargs: Vec<ir::Kwarg> = Vec::new();
        if !self.stream.r#match(")", None) {
            loop {
                let token = self.stream.peek().clone();
                if token.kind != "NAME" || self.stream.peek_next().kind != "=" {
                    return Err(IRParseError(
                        "Action calls require keyword arguments".to_string(),
                    ));
                }
                let key = self.stream.advance().value;
                self.stream.expect("=", None)?;
                let value = self.parse_expr(0)?;
                kwargs.push(ir::Kwarg {
                    name: key,
                    value: Some(value),
                });
                if self.stream.r#match(",", None) {
                    if self.stream.peek().kind == ")" {
                        break;
                    }
                    continue;
                }
                break;
            }
            self.stream.expect(")", None)?;
        }

        let mut policies: Vec<ir::PolicyBracket> = Vec::new();
        while self.stream.peek().kind == "[" {
            policies.push(self.parse_policy()?);
        }

        let mut action = ir::ActionCall {
            action_name,
            kwargs,
            policies,
            module_name: None,
        };
        if let Some(module) = module_name {
            action.module_name = Some(module);
        }
        Ok(action)
    }

    fn parse_policy(&mut self) -> Result<ir::PolicyBracket, IRParseError> {
        self.stream.expect("[", None)?;
        let mut exception_types: Vec<String> = Vec::new();
        if self.has_exception_header() {
            loop {
                exception_types.push(self.stream.expect("NAME", None)?.value);
                if self.stream.r#match(",", None) {
                    continue;
                }
                break;
            }
            self.stream.expect("->", None)?;
        }

        let kind_token = self.stream.expect("NAME", None)?;
        let kind = kind_token.value;
        self.stream.expect(":", None)?;
        if kind == "retry" {
            let max_retries = self.parse_int_value()?;
            let mut backoff: Option<ir::Duration> = None;
            if self.stream.r#match(",", None) {
                let key = self.stream.expect("NAME", None)?.value;
                self.stream.expect(":", None)?;
                if key != "backoff" {
                    return Err(IRParseError(format!(
                        "Unsupported retry policy field: {key}"
                    )));
                }
                backoff = Some(self.parse_duration()?);
            }
            self.stream.expect("]", None)?;
            let mut policy = ir::RetryPolicy {
                exception_types,
                max_retries,
                backoff,
            };
            if policy.backoff.is_none() {
                policy.backoff = None;
            }
            return Ok(ir::PolicyBracket {
                kind: Some(ir::policy_bracket::Kind::Retry(policy)),
            });
        }

        if kind == "timeout" {
            if !exception_types.is_empty() {
                return Err(IRParseError(
                    "Timeout policy cannot specify exception types".to_string(),
                ));
            }
            let duration = self.parse_duration()?;
            self.stream.expect("]", None)?;
            return Ok(ir::PolicyBracket {
                kind: Some(ir::policy_bracket::Kind::Timeout(ir::TimeoutPolicy {
                    timeout: Some(duration),
                })),
            });
        }

        Err(IRParseError(format!("Unsupported policy kind: {kind}")))
    }

    fn parse_int_value(&mut self) -> Result<u32, IRParseError> {
        let token = self.stream.expect("NUMBER", None)?;
        if token.value.contains('.') || token.value.contains('e') || token.value.contains('E') {
            return Err(IRParseError("Expected integer value".to_string()));
        }
        token
            .value
            .parse::<u32>()
            .map_err(|_| IRParseError("Expected integer value".to_string()))
    }

    fn parse_duration(&mut self) -> Result<ir::Duration, IRParseError> {
        let value = self.parse_int_value()? as u64;
        let mut unit = "s".to_string();
        if self.stream.peek().kind == "NAME" {
            let token = self.stream.peek().clone();
            if ["s", "m", "h"].contains(&token.value.as_str()) {
                unit = self.stream.advance().value;
            }
        }
        let mut seconds = value;
        if unit == "m" {
            seconds *= 60;
        } else if unit == "h" {
            seconds *= 3600;
        }
        Ok(ir::Duration { seconds })
    }

    fn has_exception_header(&self) -> bool {
        let mut idx = self.stream.index;
        while idx < self.stream.tokens.len() {
            let token = &self.stream.tokens[idx];
            if token.kind == "]" || token.kind == "EOF" {
                return false;
            }
            if token.kind == ":" {
                return false;
            }
            if token.kind == "->" {
                return true;
            }
            idx += 1;
        }
        false
    }

    fn parse_spread_expr(&mut self) -> Result<ir::Expr, IRParseError> {
        self.stream.expect("NAME", Some("spread"))?;
        let collection = self.parse_expr(0)?;
        self.stream.expect(":", None)?;
        let loop_var = self.stream.expect("NAME", None)?.value;
        self.stream.expect("->", None)?;
        let action = self.parse_action_call()?;
        Ok(ir::Expr {
            kind: Some(ir::expr::Kind::SpreadExpr(Box::new(ir::SpreadExpr {
                collection: Some(Box::new(collection)),
                loop_var,
                action: Some(action),
            }))),
            span: None,
        })
    }

    fn peek_binary_op(&self) -> Option<(String, i32)> {
        let token = self.stream.peek();
        if token.kind == "NAME" {
            if token.value == "or" {
                return Some(("or".to_string(), 10));
            }
            if token.value == "and" {
                return Some(("and".to_string(), 20));
            }
            if token.value == "in" {
                return Some(("in".to_string(), 30));
            }
            if token.value == "not" && self.stream.peek_next().value == "in" {
                return Some(("not in".to_string(), 30));
            }
        }
        if [
            "==", "!=", "<", "<=", ">", ">=", "+", "-", "*", "/", "//", "%",
        ]
        .contains(&token.kind.as_str())
        {
            return Some((token.value.clone(), self.binary_precedence(&token.value)));
        }
        None
    }

    fn binary_precedence(&self, op: &str) -> i32 {
        match op {
            "or" => 10,
            "and" => 20,
            "==" | "!=" | "<" | "<=" | ">" | ">=" | "in" | "not in" => 30,
            "+" | "-" => 40,
            "*" | "/" | "//" | "%" => 50,
            _ => 0,
        }
    }

    fn binary_operator(op: &str) -> i32 {
        match op {
            "or" => ir::BinaryOperator::BinaryOpOr as i32,
            "and" => ir::BinaryOperator::BinaryOpAnd as i32,
            "==" => ir::BinaryOperator::BinaryOpEq as i32,
            "!=" => ir::BinaryOperator::BinaryOpNe as i32,
            "<" => ir::BinaryOperator::BinaryOpLt as i32,
            "<=" => ir::BinaryOperator::BinaryOpLe as i32,
            ">" => ir::BinaryOperator::BinaryOpGt as i32,
            ">=" => ir::BinaryOperator::BinaryOpGe as i32,
            "in" => ir::BinaryOperator::BinaryOpIn as i32,
            "not in" => ir::BinaryOperator::BinaryOpNotIn as i32,
            "+" => ir::BinaryOperator::BinaryOpAdd as i32,
            "-" => ir::BinaryOperator::BinaryOpSub as i32,
            "*" => ir::BinaryOperator::BinaryOpMul as i32,
            "/" => ir::BinaryOperator::BinaryOpDiv as i32,
            "//" => ir::BinaryOperator::BinaryOpFloorDiv as i32,
            "%" => ir::BinaryOperator::BinaryOpMod as i32,
            _ => ir::BinaryOperator::BinaryOpUnspecified as i32,
        }
    }
}

/// Parse IR source strings into protobuf AST structures.
pub struct IRParser {
    indent: String,
    lines: Vec<String>,
    index: usize,
}

impl IRParser {
    pub fn new(indent: &str) -> Self {
        Self {
            indent: indent.to_string(),
            lines: Vec::new(),
            index: 0,
        }
    }

    pub fn parse_program(&mut self, source: &str) -> Result<ir::Program, IRParseError> {
        self.lines = source.lines().map(|line| line.to_string()).collect();
        self.index = 0;
        let mut functions: Vec<ir::FunctionDef> = Vec::new();

        while self.index < self.lines.len() {
            let line = self.current_line().trim().to_string();
            if line.is_empty() {
                self.index += 1;
                continue;
            }
            if !line.starts_with("fn ") {
                return Err(IRParseError(format!(
                    "Expected function definition, found: {line}"
                )));
            }
            functions.push(self.parse_function()?);
        }

        Ok(ir::Program { functions })
    }

    pub fn parse_expr(&mut self, source: &str) -> Result<ir::Expr, IRParseError> {
        ExprParser::new(source)?.parse()
    }

    fn parse_function(&mut self) -> Result<ir::FunctionDef, IRParseError> {
        let line = self.current_line();
        let (name, inputs, outputs) = parse_function_header(&line)?;
        self.index += 1;

        let body = self.parse_block(self.indent_level(&line)? + 1)?;
        Ok(ir::FunctionDef {
            name,
            io: Some(ir::IoDecl {
                inputs,
                outputs,
                span: None,
            }),
            body: Some(body),
            span: None,
        })
    }

    fn parse_block(&mut self, indent_level: usize) -> Result<ir::Block, IRParseError> {
        let mut statements: Vec<ir::Statement> = Vec::new();
        let mut saw_pass = false;

        while self.index < self.lines.len() {
            let line = self.current_line();
            if line.trim().is_empty() {
                self.index += 1;
                continue;
            }
            let level = self.indent_level(&line)?;
            if level < indent_level {
                break;
            }
            if level > indent_level {
                return Err(IRParseError("Unexpected indentation".to_string()));
            }
            let content = line.trim();
            if content == "pass" {
                saw_pass = true;
                self.index += 1;
                continue;
            }
            if let Some(stmt) = self.parse_statement(indent_level)? {
                statements.push(stmt);
            }
        }

        if statements.is_empty() && saw_pass {
            return Ok(ir::Block {
                statements,
                span: None,
            });
        }
        Ok(ir::Block {
            statements,
            span: None,
        })
    }

    fn parse_statement(
        &mut self,
        indent_level: usize,
    ) -> Result<Option<ir::Statement>, IRParseError> {
        let line = self.current_line().trim().to_string();
        if line.starts_with("return") {
            self.index += 1;
            let parts: Vec<&str> = line.splitn(2, ' ').collect();
            if parts.len() == 1 {
                return Ok(Some(ir::Statement {
                    kind: Some(ir::statement::Kind::ReturnStmt(ir::ReturnStmt {
                        value: None,
                    })),
                    span: None,
                }));
            }
            let expr = self.parse_expr(parts[1])?;
            return Ok(Some(ir::Statement {
                kind: Some(ir::statement::Kind::ReturnStmt(ir::ReturnStmt {
                    value: Some(expr),
                })),
                span: None,
            }));
        }

        if line == "break" {
            self.index += 1;
            return Ok(Some(ir::Statement {
                kind: Some(ir::statement::Kind::BreakStmt(ir::BreakStmt {})),
                span: None,
            }));
        }

        if line == "continue" {
            self.index += 1;
            return Ok(Some(ir::Statement {
                kind: Some(ir::statement::Kind::ContinueStmt(ir::ContinueStmt {})),
                span: None,
            }));
        }

        if line.starts_with("for ") {
            return self.parse_for_loop(indent_level).map(Some);
        }

        if line.starts_with("while ") {
            return self.parse_while_loop(indent_level).map(Some);
        }

        if line.starts_with("if ") {
            return self.parse_conditional(indent_level).map(Some);
        }

        if line.starts_with("try:") {
            return self.parse_try_except(indent_level).map(Some);
        }

        if line.starts_with("parallel:") && line.trim_end() == "parallel:" {
            return self.parse_parallel_block(indent_level, None);
        }

        if line.starts_with("spread ") {
            self.index += 1;
            let spread = parse_spread(&line)?;
            return Ok(Some(ir::Statement {
                kind: Some(ir::statement::Kind::SpreadAction(spread)),
                span: None,
            }));
        }

        if line.starts_with('@') {
            self.index += 1;
            let expr = ExprParser::new(&line)?.parse()?;
            if let Some(ir::expr::Kind::ActionCall(action)) = expr.kind {
                return Ok(Some(ir::Statement {
                    kind: Some(ir::statement::Kind::ActionCall(action)),
                    span: None,
                }));
            }
            return Err(IRParseError("Expected action call statement".to_string()));
        }

        if let Some((targets_str, rhs)) = split_assignment(&line) {
            let targets = parse_targets(&targets_str);
            if rhs == "parallel:" {
                return self.parse_parallel_block(indent_level, Some(targets));
            }
            self.index += 1;
            let expr = self.parse_expr(&rhs)?;
            return Ok(Some(ir::Statement {
                kind: Some(ir::statement::Kind::Assignment(ir::Assignment {
                    targets,
                    value: Some(expr),
                })),
                span: None,
            }));
        }

        self.index += 1;
        let expr = self.parse_expr(&line)?;
        Ok(Some(ir::Statement {
            kind: Some(ir::statement::Kind::ExprStmt(ir::ExprStmt {
                expr: Some(expr),
            })),
            span: None,
        }))
    }

    fn parse_for_loop(&mut self, indent_level: usize) -> Result<ir::Statement, IRParseError> {
        let line = self.current_line().trim().to_string();
        let pattern = regex::Regex::new(r"^for\s+(.+)\s+in\s+(.+):$").unwrap();
        let captures = pattern
            .captures(&line)
            .ok_or_else(|| IRParseError("Invalid for-loop header".to_string()))?;
        let vars_str = captures.get(1).unwrap().as_str();
        let iterable_str = captures.get(2).unwrap().as_str();
        let loop_vars = parse_targets(vars_str);
        let iterable = self.parse_expr(iterable_str)?;
        self.index += 1;
        let block = self.parse_block(indent_level + 1)?;
        Ok(ir::Statement {
            kind: Some(ir::statement::Kind::ForLoop(ir::ForLoop {
                loop_vars,
                iterable: Some(iterable),
                block_body: Some(block),
            })),
            span: None,
        })
    }

    fn parse_while_loop(&mut self, indent_level: usize) -> Result<ir::Statement, IRParseError> {
        let line = self.current_line().trim().to_string();
        let pattern = regex::Regex::new(r"^while\s+(.+):$").unwrap();
        let captures = pattern
            .captures(&line)
            .ok_or_else(|| IRParseError("Invalid while-loop header".to_string()))?;
        let condition = self.parse_expr(captures.get(1).unwrap().as_str())?;
        self.index += 1;
        let block = self.parse_block(indent_level + 1)?;
        Ok(ir::Statement {
            kind: Some(ir::statement::Kind::WhileLoop(ir::WhileLoop {
                condition: Some(condition),
                block_body: Some(block),
            })),
            span: None,
        })
    }

    fn parse_conditional(&mut self, indent_level: usize) -> Result<ir::Statement, IRParseError> {
        let line = self.current_line().trim().to_string();
        let pattern = regex::Regex::new(r"^if\s+(.+):$").unwrap();
        let captures = pattern
            .captures(&line)
            .ok_or_else(|| IRParseError("Invalid if header".to_string()))?;
        let if_cond = self.parse_expr(captures.get(1).unwrap().as_str())?;
        self.index += 1;
        let if_block = self.parse_block(indent_level + 1)?;
        let mut conditional = ir::Conditional {
            if_branch: Some(ir::IfBranch {
                condition: Some(if_cond),
                span: None,
                block_body: Some(if_block),
            }),
            elif_branches: Vec::new(),
            else_branch: None,
        };

        let elif_pattern = regex::Regex::new(r"^elif\s+(.+):$").unwrap();

        while self.index < self.lines.len() {
            let line = self.current_line();
            if line.trim().is_empty() {
                self.index += 1;
                continue;
            }
            let level = self.indent_level(&line)?;
            if level != indent_level {
                break;
            }
            let stripped = line.trim();
            if stripped.starts_with("elif ") {
                let captures = elif_pattern
                    .captures(stripped)
                    .ok_or_else(|| IRParseError("Invalid elif header".to_string()))?;
                let cond = self.parse_expr(captures.get(1).unwrap().as_str())?;
                self.index += 1;
                let block = self.parse_block(indent_level + 1)?;
                conditional.elif_branches.push(ir::ElifBranch {
                    condition: Some(cond),
                    span: None,
                    block_body: Some(block),
                });
                continue;
            }
            if stripped == "else:" {
                self.index += 1;
                let block = self.parse_block(indent_level + 1)?;
                conditional.else_branch = Some(ir::ElseBranch {
                    span: None,
                    block_body: Some(block),
                });
                break;
            }
            break;
        }

        Ok(ir::Statement {
            kind: Some(ir::statement::Kind::Conditional(conditional)),
            span: None,
        })
    }

    fn parse_try_except(&mut self, indent_level: usize) -> Result<ir::Statement, IRParseError> {
        self.index += 1;
        let try_block = self.parse_block(indent_level + 1)?;
        let mut handlers: Vec<ir::ExceptHandler> = Vec::new();

        while self.index < self.lines.len() {
            let line = self.current_line();
            if line.trim().is_empty() {
                self.index += 1;
                continue;
            }
            let level = self.indent_level(&line)?;
            if level != indent_level {
                break;
            }
            let stripped = line.trim();
            if !stripped.starts_with("except") {
                break;
            }
            let mut header = stripped["except".len()..].trim().to_string();
            if !header.ends_with(":") {
                return Err(IRParseError("Invalid except header".to_string()));
            }
            header = header[..header.len() - 1].trim().to_string();
            let mut exc_types: Vec<String> = Vec::new();
            let mut exc_var = "".to_string();
            if !header.is_empty() {
                if let Some((left, right)) = header.split_once(" as ") {
                    let new_header = left.trim().to_string();
                    let new_exc_var = right.trim().to_string();
                    header = new_header;
                    exc_var = new_exc_var;
                }
                if !header.is_empty() {
                    exc_types = header
                        .split(',')
                        .map(|part| part.trim())
                        .filter(|part| !part.is_empty())
                        .map(|part| part.to_string())
                        .collect();
                }
            }
            self.index += 1;
            let block = self.parse_block(indent_level + 1)?;
            let mut handler = ir::ExceptHandler {
                exception_types: exc_types,
                span: None,
                block_body: Some(block),
                exception_var: None,
            };
            if !exc_var.is_empty() {
                handler.exception_var = Some(exc_var);
            }
            handlers.push(handler);
        }

        if handlers.is_empty() {
            return Err(IRParseError(
                "try block missing except handlers".to_string(),
            ));
        }

        Ok(ir::Statement {
            kind: Some(ir::statement::Kind::TryExcept(ir::TryExcept {
                handlers,
                try_block: Some(try_block),
            })),
            span: None,
        })
    }

    fn parse_parallel_block(
        &mut self,
        indent_level: usize,
        targets: Option<Vec<String>>,
    ) -> Result<Option<ir::Statement>, IRParseError> {
        self.index += 1;
        let mut calls: Vec<ir::Call> = Vec::new();
        while self.index < self.lines.len() {
            let line = self.current_line();
            if line.trim().is_empty() {
                self.index += 1;
                continue;
            }
            let level = self.indent_level(&line)?;
            if level < indent_level + 1 {
                break;
            }
            if level > indent_level + 1 {
                return Err(IRParseError(
                    "Unexpected indentation in parallel block".to_string(),
                ));
            }
            let content = line.trim();
            if content == "pass" {
                self.index += 1;
                continue;
            }
            let expr = ExprParser::new(content)?.parse()?;
            match expr.kind {
                Some(ir::expr::Kind::ActionCall(action)) => {
                    calls.push(ir::Call {
                        kind: Some(ir::call::Kind::Action(action)),
                    });
                }
                Some(ir::expr::Kind::FunctionCall(function)) => {
                    calls.push(ir::Call {
                        kind: Some(ir::call::Kind::Function(function)),
                    });
                }
                _ => {
                    return Err(IRParseError(
                        "Parallel block expects action or function calls".to_string(),
                    ));
                }
            }
            self.index += 1;
        }

        if let Some(targets) = targets {
            let expr = ir::Expr {
                kind: Some(ir::expr::Kind::ParallelExpr(ir::ParallelExpr { calls })),
                span: None,
            };
            return Ok(Some(ir::Statement {
                kind: Some(ir::statement::Kind::Assignment(ir::Assignment {
                    targets,
                    value: Some(expr),
                })),
                span: None,
            }));
        }

        Ok(Some(ir::Statement {
            kind: Some(ir::statement::Kind::ParallelBlock(ir::ParallelBlock {
                calls,
            })),
            span: None,
        }))
    }

    fn current_line(&self) -> String {
        self.lines[self.index].clone()
    }

    fn indent_level(&self, line: &str) -> Result<usize, IRParseError> {
        let mut count = 0;
        for ch in line.chars() {
            if ch == ' ' {
                count += 1;
            } else if ch == '\t' {
                return Err(IRParseError(
                    "Tabs are not supported in indentation".to_string(),
                ));
            } else {
                break;
            }
        }
        if count % self.indent.len() != 0 {
            return Err(IRParseError(
                "Indentation is not aligned to the indent width".to_string(),
            ));
        }
        Ok(count / self.indent.len())
    }
}

fn parse_function_header(line: &str) -> Result<(String, Vec<String>, Vec<String>), IRParseError> {
    let pattern = regex::Regex::new(
        r"^fn\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(\s*input:\s*\[(.*?)\]\s*,\s*output:\s*\[(.*?)\]\s*\)\s*:\s*$",
    )
    .unwrap();
    let captures = pattern
        .captures(line.trim())
        .ok_or_else(|| IRParseError(format!("Invalid function header: {line}")))?;
    let name = captures.get(1).unwrap().as_str().to_string();
    let inputs = parse_targets(captures.get(2).unwrap().as_str());
    let outputs = parse_targets(captures.get(3).unwrap().as_str());
    Ok((name, inputs, outputs))
}

fn parse_targets(text: &str) -> Vec<String> {
    let stripped = text.trim();
    if stripped.is_empty() || stripped == "_" {
        return Vec::new();
    }
    stripped
        .split(',')
        .map(|part| part.trim())
        .filter(|part| !part.is_empty())
        .map(|part| part.to_string())
        .collect()
}

fn split_assignment(line: &str) -> Option<(String, String)> {
    let mut depth = 0;
    let mut in_string = false;
    let mut escape = false;
    let mut quote = '\0';
    let chars: Vec<char> = line.chars().collect();
    let len = chars.len();
    let mut idx = 0;
    while idx < len {
        let ch = chars[idx];
        if in_string {
            if escape {
                escape = false;
            } else if ch == '\\' {
                escape = true;
            } else if ch == quote {
                in_string = false;
            }
            idx += 1;
            continue;
        }
        if ch == '\'' || ch == '"' {
            in_string = true;
            quote = ch;
            idx += 1;
            continue;
        }
        if "([{".contains(ch) {
            depth += 1;
            idx += 1;
            continue;
        }
        if ")]}".contains(ch) {
            depth -= 1;
            idx += 1;
            continue;
        }
        if ch == '=' && depth == 0 {
            let prev = if idx > 0 { chars[idx - 1] } else { '\0' };
            let next = if idx + 1 < len { chars[idx + 1] } else { '\0' };
            if "=<>!".contains(prev) || next == '=' {
                idx += 1;
                continue;
            }
            let left = line[..idx].trim().to_string();
            let right = line[idx + 1..].trim().to_string();
            return Some((left, right));
        }
        idx += 1;
    }
    None
}

fn parse_spread(line: &str) -> Result<ir::SpreadAction, IRParseError> {
    if !line.starts_with("spread ") {
        return Err(IRParseError(
            "Spread statement must start with 'spread'".to_string(),
        ));
    }
    let remainder = &line["spread ".len()..];
    let colon_index = find_top_level_char(remainder, ':')
        .ok_or_else(|| IRParseError("Spread missing loop variable separator ':'".to_string()))?;
    let collection_str = remainder[..colon_index].trim();
    let tail = remainder[colon_index + 1..].trim();
    let pattern = regex::Regex::new(r"^([A-Za-z_][A-Za-z0-9_]*)\s*->\s*(.+)$").unwrap();
    let captures = pattern
        .captures(tail)
        .ok_or_else(|| IRParseError("Spread missing '->' action".to_string()))?;
    let loop_var = captures.get(1).unwrap().as_str().to_string();
    let action_str = captures.get(2).unwrap().as_str().trim().to_string();
    let collection = ExprParser::new(collection_str)?.parse()?;
    let action_expr = ExprParser::new(&action_str)?.parse()?;
    let action = match action_expr.kind {
        Some(ir::expr::Kind::ActionCall(action)) => action,
        _ => {
            return Err(IRParseError(
                "Spread action must target an action call".to_string(),
            ));
        }
    };
    Ok(ir::SpreadAction {
        collection: Some(collection),
        loop_var,
        action: Some(action),
    })
}

fn find_top_level_char(text: &str, ch: char) -> Option<usize> {
    let mut depth = 0;
    let mut in_string = false;
    let mut escape = false;
    let mut quote = '\0';
    for (idx, char) in text.chars().enumerate() {
        if in_string {
            if escape {
                escape = false;
            } else if char == '\\' {
                escape = true;
            } else if char == quote {
                in_string = false;
            }
            continue;
        }
        if char == '\'' || char == '"' {
            in_string = true;
            quote = char;
            continue;
        }
        if "([{".contains(char) {
            depth += 1;
            continue;
        }
        if ")]}".contains(char) {
            depth -= 1;
            continue;
        }
        if char == ch && depth == 0 {
            return Some(idx);
        }
    }
    None
}

fn decode_string(value: &str) -> Result<String, IRParseError> {
    if value.len() < 2 {
        return Ok("".to_string());
    }
    let quote = value.chars().next().unwrap_or('"');
    let inner = &value[1..value.len() - 1];
    let mut out = String::new();
    let mut chars = inner.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        if let Some(next) = chars.next() {
            let resolved = match next {
                'n' => '\n',
                't' => '\t',
                'r' => '\r',
                '\\' => '\\',
                '\'' => '\'',
                '"' => '"',
                '0' => '\0',
                'b' => '\u{0008}',
                'f' => '\u{000C}',
                'u' => {
                    let mut hex = String::new();
                    for _ in 0..4 {
                        if let Some(h) = chars.next() {
                            hex.push(h);
                        }
                    }
                    if let Ok(code) = u32::from_str_radix(&hex, 16) {
                        std::char::from_u32(code).unwrap_or('?')
                    } else {
                        '?'
                    }
                }
                _ => next,
            };
            out.push(resolved);
        }
    }
    if quote != '\'' && quote != '"' {
        return Err(IRParseError("Invalid string literal".to_string()));
    }
    Ok(out)
}

/// Convenience wrapper to parse a program.
pub fn parse_program(source: &str) -> Result<ir::Program, IRParseError> {
    IRParser::new("    ").parse_program(source)
}
