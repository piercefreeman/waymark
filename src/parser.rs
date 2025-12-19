//! Recursive descent parser for the Rappel IR language.
//!
//! Parses a token stream from the lexer into an AST represented as proto messages.
//! The parser handles:
//! - Function definitions with explicit I/O declarations
//! - Statements: assignment, action calls, control flow, etc.
//! - Expressions with proper operator precedence
//! - Policy brackets for retry/timeout on actions

use std::fmt;

use crate::lexer::{Lexer, LexerError, Span, SpannedToken, Token};

/// Re-export AST proto types
pub mod ast {
    tonic::include_proto!("rappel.ast");
}

/// Parse error with location information
#[derive(Debug, Clone)]
pub struct ParseError {
    pub message: String,
    pub span: Span,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "parse error at {:?}: {}", self.span, self.message)
    }
}

impl std::error::Error for ParseError {}

impl From<LexerError> for ParseError {
    fn from(err: LexerError) -> Self {
        ParseError {
            message: err.message,
            span: err.span,
        }
    }
}

/// Parser state
pub struct Parser<'source> {
    source: &'source str,
    tokens: Vec<SpannedToken>,
    pos: usize,
}

impl<'source> Parser<'source> {
    /// Create a new parser from source code
    pub fn new(source: &'source str) -> Result<Self, ParseError> {
        let tokens: Vec<SpannedToken> = Lexer::new(source).collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            source,
            tokens,
            pos: 0,
        })
    }

    /// Get the source code
    pub fn source(&self) -> &'source str {
        self.source
    }

    // -------------------------------------------------------------------------
    // Token navigation
    // -------------------------------------------------------------------------

    fn current(&self) -> &SpannedToken {
        static EOF_TOKEN: std::sync::OnceLock<SpannedToken> = std::sync::OnceLock::new();
        self.tokens.get(self.pos).unwrap_or_else(|| {
            EOF_TOKEN.get_or_init(|| SpannedToken {
                token: Token::Eof,
                span: Span::new(0, 0),
            })
        })
    }

    fn peek(&self) -> Token {
        self.current().token.clone()
    }

    fn peek_span(&self) -> Span {
        self.current().span
    }

    fn advance(&mut self) -> SpannedToken {
        let token = self.current().clone();
        if self.pos < self.tokens.len() {
            self.pos += 1;
        }
        token
    }

    fn at_end(&self) -> bool {
        matches!(self.peek(), Token::Eof)
    }

    fn check(&self, token: &Token) -> bool {
        std::mem::discriminant(&self.peek()) == std::mem::discriminant(token)
    }

    fn expect(&mut self, expected: &Token) -> Result<SpannedToken, ParseError> {
        if self.check(expected) {
            Ok(self.advance())
        } else {
            Err(self.error(format!("expected {}, found {}", expected, self.peek())))
        }
    }

    fn expect_ident(&mut self) -> Result<(String, Span), ParseError> {
        match self.peek() {
            Token::Ident(name) => {
                let span = self.peek_span();
                self.advance();
                Ok((name, span))
            }
            _ => Err(self.error(format!("expected identifier, found {}", self.peek()))),
        }
    }

    fn error(&self, message: String) -> ParseError {
        ParseError {
            message,
            span: self.peek_span(),
        }
    }

    fn make_span(&self, start: Span, end: Span) -> Option<ast::Span> {
        // Convert byte offsets to line/column
        let (start_line, start_col) = self.offset_to_line_col(start.start);
        let (end_line, end_col) = self.offset_to_line_col(end.end);
        Some(ast::Span {
            start_line,
            start_col,
            end_line,
            end_col,
        })
    }

    fn offset_to_line_col(&self, offset: usize) -> (u32, u32) {
        let mut line = 1u32;
        let mut col = 1u32;
        for (i, ch) in self.source.char_indices() {
            if i >= offset {
                break;
            }
            if ch == '\n' {
                line += 1;
                col = 1;
            } else {
                col += 1;
            }
        }
        (line, col)
    }

    // -------------------------------------------------------------------------
    // Top-level parsing
    // -------------------------------------------------------------------------

    /// Parse a complete program
    pub fn parse_program(&mut self) -> Result<ast::Program, ParseError> {
        let mut functions = Vec::new();

        while !self.at_end() {
            functions.push(self.parse_function_def()?);
        }

        if functions.is_empty() {
            return Err(self.error("expected at least one function definition".to_string()));
        }

        Ok(ast::Program { functions })
    }

    /// Parse a function definition
    fn parse_function_def(&mut self) -> Result<ast::FunctionDef, ParseError> {
        let start_span = self.peek_span();
        self.expect(&Token::Fn)?;

        let (name, _) = self.expect_ident()?;
        self.expect(&Token::LParen)?;
        let io = self.parse_io_decl()?;
        self.expect(&Token::RParen)?;
        self.expect(&Token::Colon)?;

        let body = self.parse_block()?;
        let end_span = body
            .span
            .clone()
            .map(|s| Span::new(s.start_line as usize, s.end_col as usize))
            .unwrap_or(start_span);

        Ok(ast::FunctionDef {
            name,
            io: Some(io),
            body: Some(body),
            span: self.make_span(start_span, end_span),
        })
    }

    /// Parse I/O declaration: input: [a, b], output: [c]
    fn parse_io_decl(&mut self) -> Result<ast::IoDecl, ParseError> {
        let start_span = self.peek_span();

        self.expect(&Token::Input)?;
        self.expect(&Token::Colon)?;
        self.expect(&Token::LBracket)?;
        let inputs = self.parse_ident_list()?;
        self.expect(&Token::RBracket)?;

        self.expect(&Token::Comma)?;

        self.expect(&Token::Output)?;
        self.expect(&Token::Colon)?;
        self.expect(&Token::LBracket)?;
        let outputs = self.parse_ident_list()?;
        let end_span = self.peek_span();
        self.expect(&Token::RBracket)?;

        Ok(ast::IoDecl {
            inputs,
            outputs,
            span: self.make_span(start_span, end_span),
        })
    }

    /// Parse a comma-separated list of identifiers
    fn parse_ident_list(&mut self) -> Result<Vec<String>, ParseError> {
        let mut idents = Vec::new();

        if let Token::Ident(_) = self.peek() {
            let (name, _) = self.expect_ident()?;
            idents.push(name);

            while self.check(&Token::Comma) {
                self.advance();
                if let Token::Ident(_) = self.peek() {
                    let (name, _) = self.expect_ident()?;
                    idents.push(name);
                } else {
                    break;
                }
            }
        }

        Ok(idents)
    }

    /// Parse a block (INDENT statements+ DEDENT)
    fn parse_block(&mut self) -> Result<ast::Block, ParseError> {
        let start_span = self.peek_span();
        self.expect(&Token::Indent)?;

        let mut statements = Vec::new();
        while !self.check(&Token::Dedent) && !self.at_end() {
            statements.push(self.parse_statement()?);
        }

        let end_span = self.peek_span();
        self.expect(&Token::Dedent)?;

        Ok(ast::Block {
            statements,
            span: self.make_span(start_span, end_span),
        })
    }

    // -------------------------------------------------------------------------
    // Statement parsing
    // -------------------------------------------------------------------------

    fn parse_statement(&mut self) -> Result<ast::Statement, ParseError> {
        let start_span = self.peek_span();

        let kind = match self.peek() {
            Token::If => Some(ast::statement::Kind::Conditional(self.parse_conditional()?)),
            Token::For => Some(ast::statement::Kind::ForLoop(self.parse_for_loop()?)),
            Token::Try => Some(ast::statement::Kind::TryExcept(self.parse_try_except()?)),
            Token::Return => Some(ast::statement::Kind::ReturnStmt(self.parse_return()?)),
            Token::Spread => Some(ast::statement::Kind::SpreadAction(
                self.parse_spread_action()?,
            )),
            Token::Parallel => {
                self.advance(); // Consume 'parallel'
                Some(ast::statement::Kind::ParallelBlock(
                    self.parse_parallel_block()?,
                ))
            }
            Token::Ident(_) => self.parse_assignment_or_expr()?,
            Token::At => Some(ast::statement::Kind::ActionCall(self.parse_action_call()?)),
            _ => return Err(self.error(format!("unexpected token in statement: {}", self.peek()))),
        };

        let end_span = self.peek_span();

        Ok(ast::Statement {
            kind,
            span: self.make_span(start_span, end_span),
        })
    }

    /// Parse assignment or expression statement
    fn parse_assignment_or_expr(&mut self) -> Result<Option<ast::statement::Kind>, ParseError> {
        // Look ahead to determine if this is an assignment
        let (first_ident, first_span) = self.expect_ident()?;

        // Check for tuple unpacking: a, b, c = ...
        let mut targets = vec![first_ident.clone()];
        while self.check(&Token::Comma) {
            self.advance();
            let (name, _) = self.expect_ident()?;
            targets.push(name);
        }

        if self.check(&Token::Eq) {
            self.advance();

            // Check for parallel block: a, b = parallel: ... -> Assignment with ParallelExpr
            if self.check(&Token::Parallel) {
                self.advance();
                let parallel = self.parse_parallel_block()?;
                let parallel_expr = ast::Expr {
                    kind: Some(ast::expr::Kind::ParallelExpr(ast::ParallelExpr {
                        calls: parallel.calls,
                    })),
                    span: None,
                };
                return Ok(Some(ast::statement::Kind::Assignment(ast::Assignment {
                    targets,
                    value: Some(parallel_expr),
                })));
            }

            // Check for spread action: results = spread collection:var -> @action(...)
            // -> Assignment with SpreadExpr
            if self.check(&Token::Spread) {
                let spread = self.parse_spread_action()?;
                let spread_expr = ast::Expr {
                    kind: Some(ast::expr::Kind::SpreadExpr(Box::new(ast::SpreadExpr {
                        collection: spread.collection.map(Box::new),
                        loop_var: spread.loop_var,
                        action: spread.action,
                    }))),
                    span: None,
                };
                return Ok(Some(ast::statement::Kind::Assignment(ast::Assignment {
                    targets,
                    value: Some(spread_expr),
                })));
            }

            let value = self.parse_expr()?;
            Ok(Some(ast::statement::Kind::Assignment(ast::Assignment {
                targets,
                value: Some(value),
            })))
        } else {
            // It's an expression statement - reconstruct the expression
            // This is a simplification; we'd need to handle the ident as part of expr
            let expr = self.build_expr_from_ident_and_rest(first_ident, first_span)?;
            Ok(Some(ast::statement::Kind::ExprStmt(ast::ExprStmt {
                expr: Some(expr),
            })))
        }
    }

    /// Build an expression starting from an already-consumed identifier
    fn build_expr_from_ident_and_rest(
        &mut self,
        name: String,
        span: Span,
    ) -> Result<ast::Expr, ParseError> {
        let mut expr = ast::Expr {
            kind: Some(ast::expr::Kind::Variable(ast::Variable { name })),
            span: self.make_span(span, span),
        };

        // Handle postfix operations (dot access, index, function call)
        loop {
            match self.peek() {
                Token::Dot => {
                    self.advance();
                    let (attr, attr_span) = self.expect_ident()?;
                    expr = ast::Expr {
                        kind: Some(ast::expr::Kind::Dot(Box::new(ast::DotAccess {
                            object: Some(Box::new(expr)),
                            attribute: attr,
                        }))),
                        span: self.make_span(span, attr_span),
                    };
                }
                Token::LBracket => {
                    self.advance();
                    let index = self.parse_expr()?;
                    let end_span = self.peek_span();
                    self.expect(&Token::RBracket)?;
                    expr = ast::Expr {
                        kind: Some(ast::expr::Kind::Index(Box::new(ast::IndexAccess {
                            object: Some(Box::new(expr)),
                            index: Some(Box::new(index)),
                        }))),
                        span: self.make_span(span, end_span),
                    };
                }
                Token::LParen => {
                    // Function call on variable
                    self.advance();
                    let (args, kwargs) = self.parse_call_args()?;
                    let end_span = self.peek_span();
                    self.expect(&Token::RParen)?;

                    if let Some(ast::expr::Kind::Variable(var)) = expr.kind {
                        expr = ast::Expr {
                            kind: Some(ast::expr::Kind::FunctionCall(ast::FunctionCall {
                                name: var.name,
                                args,
                                kwargs,
                            })),
                            span: self.make_span(span, end_span),
                        };
                    }
                }
                _ => break,
            }
        }

        Ok(expr)
    }

    fn parse_conditional(&mut self) -> Result<ast::Conditional, ParseError> {
        // Parse if branch
        let if_start = self.peek_span();
        self.expect(&Token::If)?;
        let if_condition = self.parse_expr()?;
        self.expect(&Token::Colon)?;
        let if_body = self.parse_block()?;
        let if_end = self.peek_span();

        let if_branch = ast::IfBranch {
            condition: Some(if_condition),
            span: self.make_span(if_start, if_end),
            block_body: Some(if_body),
        };

        // Parse elif branches
        let mut elif_branches = Vec::new();
        while self.check(&Token::Elif) {
            let elif_start = self.peek_span();
            self.advance();
            let elif_condition = self.parse_expr()?;
            self.expect(&Token::Colon)?;
            let elif_body = self.parse_block()?;
            let elif_end = self.peek_span();

            elif_branches.push(ast::ElifBranch {
                condition: Some(elif_condition),
                span: self.make_span(elif_start, elif_end),
                block_body: Some(elif_body),
            });
        }

        // Parse else branch
        let else_branch = if self.check(&Token::Else) {
            let else_start = self.peek_span();
            self.advance();
            self.expect(&Token::Colon)?;
            let else_body = self.parse_block()?;
            let else_end = self.peek_span();

            Some(ast::ElseBranch {
                span: self.make_span(else_start, else_end),
                block_body: Some(else_body),
            })
        } else {
            None
        };

        Ok(ast::Conditional {
            if_branch: Some(if_branch),
            elif_branches,
            else_branch,
        })
    }

    fn parse_for_loop(&mut self) -> Result<ast::ForLoop, ParseError> {
        self.expect(&Token::For)?;

        // Parse loop variables (may be tuple unpacking)
        let mut loop_vars = Vec::new();
        let (first_var, _) = self.expect_ident()?;
        loop_vars.push(first_var);

        while self.check(&Token::Comma) {
            self.advance();
            let (var, _) = self.expect_ident()?;
            loop_vars.push(var);
        }

        self.expect(&Token::In)?;
        let iterable = self.parse_expr()?;
        self.expect(&Token::Colon)?;
        let body = self.parse_block()?;

        Ok(ast::ForLoop {
            loop_vars,
            iterable: Some(iterable),
            block_body: Some(body),
        })
    }

    fn parse_try_except(&mut self) -> Result<ast::TryExcept, ParseError> {
        self.expect(&Token::Try)?;
        self.expect(&Token::Colon)?;
        let try_body = self.parse_block()?;

        let mut handlers = Vec::new();
        while self.check(&Token::Except) {
            let handler_start = self.peek_span();
            self.advance();

            // Parse exception types
            let exception_types = if self.check(&Token::Colon) {
                Vec::new() // Bare except:
            } else {
                self.parse_ident_list()?
            };

            self.expect(&Token::Colon)?;
            let handler_body = self.parse_block()?;
            let handler_end = self.peek_span();

            handlers.push(ast::ExceptHandler {
                exception_types,
                span: self.make_span(handler_start, handler_end),
                block_body: Some(handler_body),
            });
        }

        Ok(ast::TryExcept {
            handlers,
            try_block: Some(try_body),
        })
    }

    fn parse_return(&mut self) -> Result<ast::ReturnStmt, ParseError> {
        self.expect(&Token::Return)?;

        let value = if !self.check(&Token::Dedent) && !self.at_end() && !self.check(&Token::Indent)
        {
            // Check if there's an expression to return
            match self.peek() {
                Token::If
                | Token::For
                | Token::Try
                | Token::Return
                | Token::Spread
                | Token::Parallel
                | Token::Fn => None,
                _ => Some(self.parse_expr()?),
            }
        } else {
            None
        };

        Ok(ast::ReturnStmt { value })
    }

    fn parse_spread_action(&mut self) -> Result<ast::SpreadAction, ParseError> {
        self.expect(&Token::Spread)?;
        let collection = self.parse_expr()?;
        self.expect(&Token::Colon)?;
        let (loop_var, _) = self.expect_ident()?;
        self.expect(&Token::Arrow)?;
        let action = self.parse_action_call()?;

        Ok(ast::SpreadAction {
            collection: Some(collection),
            loop_var,
            action: Some(action),
        })
    }

    fn parse_parallel_block(&mut self) -> Result<ast::ParallelBlock, ParseError> {
        self.expect(&Token::Colon)?;
        self.expect(&Token::Indent)?;

        let mut calls = Vec::new();
        while !self.check(&Token::Dedent) && !self.at_end() {
            if self.check(&Token::At) {
                calls.push(ast::Call {
                    kind: Some(ast::call::Kind::Action(self.parse_action_call()?)),
                });
            } else {
                let (name, _) = self.expect_ident()?;
                self.expect(&Token::LParen)?;
                let (args, kwargs) = self.parse_call_args()?;
                self.expect(&Token::RParen)?;
                calls.push(ast::Call {
                    kind: Some(ast::call::Kind::Function(ast::FunctionCall {
                        name,
                        args,
                        kwargs,
                    })),
                });
            }
        }

        self.expect(&Token::Dedent)?;

        Ok(ast::ParallelBlock { calls })
    }

    fn parse_action_call(&mut self) -> Result<ast::ActionCall, ParseError> {
        self.expect(&Token::At)?;
        let (action_name, _) = self.expect_ident()?;
        self.expect(&Token::LParen)?;
        let kwargs = self.parse_kwargs()?;
        self.expect(&Token::RParen)?;

        // Parse policy brackets
        let mut policies = Vec::new();
        while self.check(&Token::LBracket) {
            policies.push(self.parse_policy_bracket()?);
        }

        Ok(ast::ActionCall {
            action_name,
            kwargs,
            policies,
            module_name: None, // Filled in by Python IR builder when registered
        })
    }

    fn parse_kwargs(&mut self) -> Result<Vec<ast::Kwarg>, ParseError> {
        let mut kwargs = Vec::new();

        if let Token::Ident(_) = self.peek() {
            kwargs.push(self.parse_kwarg()?);

            while self.check(&Token::Comma) {
                self.advance();
                if let Token::Ident(_) = self.peek() {
                    kwargs.push(self.parse_kwarg()?);
                } else {
                    break;
                }
            }
        }

        Ok(kwargs)
    }

    /// Parse function call arguments - can be positional or keyword
    /// Returns (positional_args, keyword_args)
    fn parse_call_args(&mut self) -> Result<(Vec<ast::Expr>, Vec<ast::Kwarg>), ParseError> {
        let mut args = Vec::new();
        let mut kwargs = Vec::new();
        let mut seen_kwarg = false;

        if self.check(&Token::RParen) {
            return Ok((args, kwargs));
        }

        loop {
            // Check if this is a kwarg (ident followed by =)
            if let Token::Ident(name) = self.peek() {
                // Look ahead to see if there's an = after the ident
                let save_pos = self.pos;
                self.advance();
                if self.check(&Token::Eq) {
                    // It's a kwarg
                    self.advance();
                    let value = self.parse_expr()?;
                    kwargs.push(ast::Kwarg {
                        name,
                        value: Some(value),
                    });
                    seen_kwarg = true;
                } else {
                    // It's a positional arg - restore position and parse as expr
                    self.pos = save_pos;
                    if seen_kwarg {
                        return Err(
                            self.error("positional argument follows keyword argument".to_string())
                        );
                    }
                    args.push(self.parse_expr()?);
                }
            } else {
                // Not an ident, must be a positional arg expression
                if seen_kwarg {
                    return Err(
                        self.error("positional argument follows keyword argument".to_string())
                    );
                }
                args.push(self.parse_expr()?);
            }

            if self.check(&Token::Comma) {
                self.advance();
                if self.check(&Token::RParen) {
                    break; // Trailing comma
                }
            } else {
                break;
            }
        }

        Ok((args, kwargs))
    }

    fn parse_kwarg(&mut self) -> Result<ast::Kwarg, ParseError> {
        let (name, _) = self.expect_ident()?;
        self.expect(&Token::Eq)?;
        let value = self.parse_expr()?;

        Ok(ast::Kwarg {
            name,
            value: Some(value),
        })
    }

    fn parse_policy_bracket(&mut self) -> Result<ast::PolicyBracket, ParseError> {
        self.expect(&Token::LBracket)?;

        // Check if it's a timeout policy
        if self.check(&Token::Timeout) {
            self.advance();
            self.expect(&Token::Colon)?;
            let duration = self.parse_duration()?;
            self.expect(&Token::RBracket)?;
            return Ok(ast::PolicyBracket {
                kind: Some(ast::policy_bracket::Kind::Timeout(ast::TimeoutPolicy {
                    timeout: Some(duration),
                })),
            });
        }

        // It's a retry policy - check for exception types
        let mut exception_types = Vec::new();

        // Check for exception spec: ExceptionType -> or (Ex1, Ex2) ->
        if let Token::Ident(_) = self.peek() {
            // Could be exception type or "retry" keyword
            if !self.check(&Token::Retry) && !self.check(&Token::Backoff) {
                let (exc_type, _) = self.expect_ident()?;
                exception_types.push(exc_type);

                if self.check(&Token::Comma) {
                    while self.check(&Token::Comma) {
                        self.advance();
                        let (exc_type, _) = self.expect_ident()?;
                        exception_types.push(exc_type);
                    }
                }

                self.expect(&Token::Arrow)?;
            }
        } else if self.check(&Token::LParen) {
            // Tuple of exception types: (Ex1, Ex2) ->
            self.advance();
            exception_types = self.parse_ident_list()?;
            self.expect(&Token::RParen)?;
            self.expect(&Token::Arrow)?;
        }

        // Parse retry params
        let mut max_retries = 0u32;
        let mut backoff = ast::Duration { seconds: 0 };

        loop {
            if self.check(&Token::Retry) {
                self.advance();
                self.expect(&Token::Colon)?;
                if let Token::Int(n) = self.peek() {
                    max_retries = n as u32;
                    self.advance();
                } else {
                    return Err(self.error("expected integer for retry count".to_string()));
                }
            } else if self.check(&Token::Backoff) {
                self.advance();
                self.expect(&Token::Colon)?;
                backoff = self.parse_duration()?;
            } else {
                break;
            }

            if self.check(&Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }

        self.expect(&Token::RBracket)?;

        Ok(ast::PolicyBracket {
            kind: Some(ast::policy_bracket::Kind::Retry(ast::RetryPolicy {
                exception_types,
                max_retries,
                backoff: Some(backoff),
            })),
        })
    }

    fn parse_duration(&mut self) -> Result<ast::Duration, ParseError> {
        match self.peek().clone() {
            Token::Int(n) => {
                self.advance();
                Ok(ast::Duration { seconds: n as u64 })
            }
            Token::Duration(d) => {
                self.advance();
                let seconds = parse_duration_string(&d)?;
                Ok(ast::Duration { seconds })
            }
            _ => Err(self.error("expected duration value".to_string())),
        }
    }

    // -------------------------------------------------------------------------
    // Expression parsing (with precedence)
    // -------------------------------------------------------------------------

    fn parse_expr(&mut self) -> Result<ast::Expr, ParseError> {
        self.parse_or_expr()
    }

    fn parse_or_expr(&mut self) -> Result<ast::Expr, ParseError> {
        let start_span = self.peek_span();
        let mut left = self.parse_and_expr()?;

        while self.check(&Token::Or) {
            self.advance();
            let right = self.parse_and_expr()?;
            let end_span = self.peek_span();
            left = ast::Expr {
                kind: Some(ast::expr::Kind::BinaryOp(Box::new(ast::BinaryOp {
                    left: Some(Box::new(left)),
                    op: ast::BinaryOperator::BinaryOpOr as i32,
                    right: Some(Box::new(right)),
                }))),
                span: self.make_span(start_span, end_span),
            };
        }

        Ok(left)
    }

    fn parse_and_expr(&mut self) -> Result<ast::Expr, ParseError> {
        let start_span = self.peek_span();
        let mut left = self.parse_not_expr()?;

        while self.check(&Token::And) {
            self.advance();
            let right = self.parse_not_expr()?;
            let end_span = self.peek_span();
            left = ast::Expr {
                kind: Some(ast::expr::Kind::BinaryOp(Box::new(ast::BinaryOp {
                    left: Some(Box::new(left)),
                    op: ast::BinaryOperator::BinaryOpAnd as i32,
                    right: Some(Box::new(right)),
                }))),
                span: self.make_span(start_span, end_span),
            };
        }

        Ok(left)
    }

    fn parse_not_expr(&mut self) -> Result<ast::Expr, ParseError> {
        let start_span = self.peek_span();

        if self.check(&Token::Not) {
            self.advance();
            let operand = self.parse_not_expr()?;
            let end_span = self.peek_span();
            return Ok(ast::Expr {
                kind: Some(ast::expr::Kind::UnaryOp(Box::new(ast::UnaryOp {
                    op: ast::UnaryOperator::UnaryOpNot as i32,
                    operand: Some(Box::new(operand)),
                }))),
                span: self.make_span(start_span, end_span),
            });
        }

        self.parse_comparison_expr()
    }

    fn parse_comparison_expr(&mut self) -> Result<ast::Expr, ParseError> {
        let start_span = self.peek_span();
        let mut left = self.parse_additive_expr()?;

        loop {
            let op = match self.peek() {
                Token::EqEq => ast::BinaryOperator::BinaryOpEq,
                Token::NotEq => ast::BinaryOperator::BinaryOpNe,
                Token::Lt => ast::BinaryOperator::BinaryOpLt,
                Token::Le => ast::BinaryOperator::BinaryOpLe,
                Token::Gt => ast::BinaryOperator::BinaryOpGt,
                Token::Ge => ast::BinaryOperator::BinaryOpGe,
                Token::In => ast::BinaryOperator::BinaryOpIn,
                Token::Not => {
                    // "not in"
                    self.advance();
                    if self.check(&Token::In) {
                        self.advance();
                        let right = self.parse_additive_expr()?;
                        let end_span = self.peek_span();
                        left = ast::Expr {
                            kind: Some(ast::expr::Kind::BinaryOp(Box::new(ast::BinaryOp {
                                left: Some(Box::new(left)),
                                op: ast::BinaryOperator::BinaryOpNotIn as i32,
                                right: Some(Box::new(right)),
                            }))),
                            span: self.make_span(start_span, end_span),
                        };
                        continue;
                    } else {
                        return Err(
                            self.error("expected 'in' after 'not' in comparison".to_string())
                        );
                    }
                }
                _ => break,
            };

            self.advance();
            let right = self.parse_additive_expr()?;
            let end_span = self.peek_span();
            left = ast::Expr {
                kind: Some(ast::expr::Kind::BinaryOp(Box::new(ast::BinaryOp {
                    left: Some(Box::new(left)),
                    op: op as i32,
                    right: Some(Box::new(right)),
                }))),
                span: self.make_span(start_span, end_span),
            };
        }

        Ok(left)
    }

    fn parse_additive_expr(&mut self) -> Result<ast::Expr, ParseError> {
        let start_span = self.peek_span();
        let mut left = self.parse_multiplicative_expr()?;

        loop {
            let op = match self.peek() {
                Token::Plus => ast::BinaryOperator::BinaryOpAdd,
                Token::Minus => ast::BinaryOperator::BinaryOpSub,
                _ => break,
            };

            self.advance();
            let right = self.parse_multiplicative_expr()?;
            let end_span = self.peek_span();
            left = ast::Expr {
                kind: Some(ast::expr::Kind::BinaryOp(Box::new(ast::BinaryOp {
                    left: Some(Box::new(left)),
                    op: op as i32,
                    right: Some(Box::new(right)),
                }))),
                span: self.make_span(start_span, end_span),
            };
        }

        Ok(left)
    }

    fn parse_multiplicative_expr(&mut self) -> Result<ast::Expr, ParseError> {
        let start_span = self.peek_span();
        let mut left = self.parse_unary_expr()?;

        loop {
            let op = match self.peek() {
                Token::Star => ast::BinaryOperator::BinaryOpMul,
                Token::Slash => ast::BinaryOperator::BinaryOpDiv,
                Token::DoubleSlash => ast::BinaryOperator::BinaryOpFloorDiv,
                Token::Percent => ast::BinaryOperator::BinaryOpMod,
                _ => break,
            };

            self.advance();
            let right = self.parse_unary_expr()?;
            let end_span = self.peek_span();
            left = ast::Expr {
                kind: Some(ast::expr::Kind::BinaryOp(Box::new(ast::BinaryOp {
                    left: Some(Box::new(left)),
                    op: op as i32,
                    right: Some(Box::new(right)),
                }))),
                span: self.make_span(start_span, end_span),
            };
        }

        Ok(left)
    }

    fn parse_unary_expr(&mut self) -> Result<ast::Expr, ParseError> {
        let start_span = self.peek_span();

        if self.check(&Token::Minus) {
            self.advance();
            let operand = self.parse_unary_expr()?;
            let end_span = self.peek_span();
            return Ok(ast::Expr {
                kind: Some(ast::expr::Kind::UnaryOp(Box::new(ast::UnaryOp {
                    op: ast::UnaryOperator::UnaryOpNeg as i32,
                    operand: Some(Box::new(operand)),
                }))),
                span: self.make_span(start_span, end_span),
            });
        }

        self.parse_postfix_expr()
    }

    fn parse_postfix_expr(&mut self) -> Result<ast::Expr, ParseError> {
        let start_span = self.peek_span();
        let mut expr = self.parse_primary_expr()?;

        loop {
            match self.peek() {
                Token::Dot => {
                    self.advance();
                    let (attr, _) = self.expect_ident()?;
                    let end_span = self.peek_span();
                    expr = ast::Expr {
                        kind: Some(ast::expr::Kind::Dot(Box::new(ast::DotAccess {
                            object: Some(Box::new(expr)),
                            attribute: attr,
                        }))),
                        span: self.make_span(start_span, end_span),
                    };
                }
                Token::LBracket => {
                    self.advance();
                    let index = self.parse_expr()?;
                    let end_span = self.peek_span();
                    self.expect(&Token::RBracket)?;
                    expr = ast::Expr {
                        kind: Some(ast::expr::Kind::Index(Box::new(ast::IndexAccess {
                            object: Some(Box::new(expr)),
                            index: Some(Box::new(index)),
                        }))),
                        span: self.make_span(start_span, end_span),
                    };
                }
                Token::LParen => {
                    // Function call
                    if let Some(ast::expr::Kind::Variable(var)) = &expr.kind {
                        let name = var.name.clone();
                        self.advance();
                        let (args, kwargs) = self.parse_call_args()?;
                        let end_span = self.peek_span();
                        self.expect(&Token::RParen)?;
                        expr = ast::Expr {
                            kind: Some(ast::expr::Kind::FunctionCall(ast::FunctionCall {
                                name,
                                args,
                                kwargs,
                            })),
                            span: self.make_span(start_span, end_span),
                        };
                    } else {
                        break;
                    }
                }
                _ => break,
            }
        }

        Ok(expr)
    }

    fn parse_primary_expr(&mut self) -> Result<ast::Expr, ParseError> {
        let start_span = self.peek_span();

        let kind = match self.peek().clone() {
            Token::Int(n) => {
                self.advance();
                ast::expr::Kind::Literal(ast::Literal {
                    value: Some(ast::literal::Value::IntValue(n)),
                })
            }
            Token::Float(n) => {
                self.advance();
                ast::expr::Kind::Literal(ast::Literal {
                    value: Some(ast::literal::Value::FloatValue(n)),
                })
            }
            Token::String(s) => {
                self.advance();
                ast::expr::Kind::Literal(ast::Literal {
                    value: Some(ast::literal::Value::StringValue(s)),
                })
            }
            Token::True => {
                self.advance();
                ast::expr::Kind::Literal(ast::Literal {
                    value: Some(ast::literal::Value::BoolValue(true)),
                })
            }
            Token::False => {
                self.advance();
                ast::expr::Kind::Literal(ast::Literal {
                    value: Some(ast::literal::Value::BoolValue(false)),
                })
            }
            Token::None_ => {
                self.advance();
                ast::expr::Kind::Literal(ast::Literal {
                    value: Some(ast::literal::Value::IsNone(true)),
                })
            }
            Token::Ident(name) => {
                self.advance();
                ast::expr::Kind::Variable(ast::Variable { name })
            }
            Token::At => {
                // Action call in expression context
                let action = self.parse_action_call()?;
                return Ok(ast::Expr {
                    kind: Some(ast::expr::Kind::ActionCall(action)),
                    span: self.make_span(start_span, self.peek_span()),
                });
            }
            Token::LBracket => {
                self.advance();
                let elements = self.parse_expr_list()?;
                let end_span = self.peek_span();
                self.expect(&Token::RBracket)?;
                return Ok(ast::Expr {
                    kind: Some(ast::expr::Kind::List(ast::ListExpr { elements })),
                    span: self.make_span(start_span, end_span),
                });
            }
            Token::LBrace => {
                self.advance();
                let entries = self.parse_dict_entries()?;
                let end_span = self.peek_span();
                self.expect(&Token::RBrace)?;
                return Ok(ast::Expr {
                    kind: Some(ast::expr::Kind::Dict(ast::DictExpr { entries })),
                    span: self.make_span(start_span, end_span),
                });
            }
            Token::LParen => {
                self.advance();
                let expr = self.parse_expr()?;
                self.expect(&Token::RParen)?;
                return Ok(expr);
            }
            _ => {
                return Err(self.error(format!("unexpected token in expression: {}", self.peek())));
            }
        };

        let end_span = self.peek_span();
        Ok(ast::Expr {
            kind: Some(kind),
            span: self.make_span(start_span, end_span),
        })
    }

    fn parse_expr_list(&mut self) -> Result<Vec<ast::Expr>, ParseError> {
        let mut exprs = Vec::new();

        if !self.check(&Token::RBracket) {
            exprs.push(self.parse_expr()?);

            while self.check(&Token::Comma) {
                self.advance();
                if self.check(&Token::RBracket) {
                    break; // Trailing comma
                }
                exprs.push(self.parse_expr()?);
            }
        }

        Ok(exprs)
    }

    fn parse_dict_entries(&mut self) -> Result<Vec<ast::DictEntry>, ParseError> {
        let mut entries = Vec::new();

        if !self.check(&Token::RBrace) {
            entries.push(self.parse_dict_entry()?);

            while self.check(&Token::Comma) {
                self.advance();
                if self.check(&Token::RBrace) {
                    break; // Trailing comma
                }
                entries.push(self.parse_dict_entry()?);
            }
        }

        Ok(entries)
    }

    fn parse_dict_entry(&mut self) -> Result<ast::DictEntry, ParseError> {
        let key = self.parse_expr()?;
        self.expect(&Token::Colon)?;
        let value = self.parse_expr()?;

        Ok(ast::DictEntry {
            key: Some(key),
            value: Some(value),
        })
    }
}

/// Parse a duration string like "30s", "2m", "1h"
fn parse_duration_string(s: &str) -> Result<u64, ParseError> {
    let len = s.len();
    if len < 2 {
        return Err(ParseError {
            message: format!("invalid duration: {}", s),
            span: Span::new(0, 0),
        });
    }

    let (num_str, unit) = s.split_at(len - 1);
    let num: u64 = num_str.parse().map_err(|_| ParseError {
        message: format!("invalid duration number: {}", num_str),
        span: Span::new(0, 0),
    })?;

    let seconds = match unit {
        "s" => num,
        "m" => num * 60,
        "h" => num * 3600,
        _ => {
            return Err(ParseError {
                message: format!("invalid duration unit: {}", unit),
                span: Span::new(0, 0),
            });
        }
    };

    Ok(seconds)
}

/// Convenience function to parse source code into an AST
pub fn parse(source: &str) -> Result<ast::Program, ParseError> {
    let mut parser = Parser::new(source)?;
    parser.parse_program()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_function() {
        let source = r#"fn greet(input: [name], output: [result]):
    result = @say_hello(name=name)
    return result"#;

        let program = parse(source).unwrap();
        assert_eq!(program.functions.len(), 1);

        let func = &program.functions[0];
        assert_eq!(func.name, "greet");
        assert_eq!(func.io.as_ref().unwrap().inputs, vec!["name"]);
        assert_eq!(func.io.as_ref().unwrap().outputs, vec!["result"]);
    }

    #[test]
    fn test_parse_action_with_policies() {
        let source = r#"fn fetch(input: [url], output: [data]):
    data = @http_get(url=url) [retry: 3, backoff: 30s] [timeout: 60s]
    return data"#;

        let program = parse(source).unwrap();
        let func = &program.functions[0];
        let body = func.body.as_ref().unwrap();
        assert_eq!(body.statements.len(), 2);
    }

    #[test]
    fn test_parse_conditional() {
        let source = r#"fn check(input: [x], output: [result]):
    if x > 0:
        result = @check_positive(x=x)
    elif x < 0:
        result = @check_negative(x=x)
    else:
        result = @check_zero()
    return result"#;

        let program = parse(source).unwrap();
        let func = &program.functions[0];
        let body = func.body.as_ref().unwrap();

        // Should have conditional + return
        assert_eq!(body.statements.len(), 2);
    }

    #[test]
    fn test_parse_conditional_function_call_bodies() {
        let source = r#"fn helper(input: [value], output: [result]):
    result = @do_work(val=value)
    return result

fn runner(input: [], output: [result]):
    if true:
        helper(value=1)
    else:
        result = helper(value=2)
    return result"#;

        let program = parse(source).unwrap();
        let runner = &program.functions[1];
        let body = runner.body.as_ref().unwrap();

        if let Some(ast::statement::Kind::Conditional(cond)) = &body.statements[0].kind {
            let if_body = cond
                .if_branch
                .as_ref()
                .unwrap()
                .block_body
                .as_ref()
                .unwrap();
            assert_eq!(if_body.statements.len(), 1);
            if let Some(ast::statement::Kind::ExprStmt(expr_stmt)) = &if_body.statements[0].kind {
                let expr = expr_stmt.expr.as_ref().unwrap();
                if let Some(ast::expr::Kind::FunctionCall(call)) = &expr.kind {
                    assert_eq!(call.name, "helper");
                } else {
                    panic!("expected function call in if branch");
                }
            } else {
                panic!("expected expression statement in if branch");
            }

            let else_body = cond
                .else_branch
                .as_ref()
                .unwrap()
                .block_body
                .as_ref()
                .unwrap();
            assert_eq!(else_body.statements.len(), 1);
            if let Some(ast::statement::Kind::Assignment(assign)) = &else_body.statements[0].kind {
                assert_eq!(assign.targets, vec!["result"]);
                let expr = assign.value.as_ref().unwrap();
                if let Some(ast::expr::Kind::FunctionCall(call)) = &expr.kind {
                    assert_eq!(call.name, "helper");
                } else {
                    panic!("expected function call assignment in else branch");
                }
            } else {
                panic!("expected assignment statement in else branch");
            }
        } else {
            panic!("expected conditional");
        }
    }

    #[test]
    fn test_parse_for_loop() {
        let source = r#"fn process(input: [items], output: [results]):
    results = []
    for item in items:
        result = @process_item(item=item)
        results = results + [result]
    return results"#;

        let program = parse(source).unwrap();
        let func = &program.functions[0];
        let body = func.body.as_ref().unwrap();
        assert_eq!(body.statements.len(), 3); // assignment, for loop, return
    }

    #[test]
    fn test_parse_spread_action() {
        let source = r#"fn batch(input: [items], output: [results]):
    results = spread items:item -> @process(item=item)
    return results"#;

        let program = parse(source).unwrap();
        assert_eq!(program.functions.len(), 1);
    }

    #[test]
    fn test_parse_expressions() {
        let source = r#"fn calc(input: [a, b], output: [result]):
    result = a + b * 2 - c / d
    return result"#;

        let program = parse(source).unwrap();
        assert_eq!(program.functions.len(), 1);
    }

    #[test]
    fn test_parse_dict_and_list() {
        let source = r#"fn data(input: [], output: [result]):
    x = [1, 2, 3]
    y = {"a": 1, "b": 2}
    result = x[0] + y["a"]
    return result"#;

        let program = parse(source).unwrap();
        assert_eq!(program.functions.len(), 1);
    }

    #[test]
    fn test_parse_try_except() {
        let source = r#"fn safe(input: [x], output: [result]):
    try:
        result = @risky_action(x=x)
    except NetworkError:
        result = @fallback(x=x)
    except:
        result = @default_handler()
    return result"#;

        let program = parse(source).unwrap();
        let func = &program.functions[0];
        let body = func.body.as_ref().unwrap();

        // try/except + return
        assert_eq!(body.statements.len(), 2);
    }

    #[test]
    fn test_parse_for_loop_with_unpacking() {
        let source = r#"fn process(input: [items], output: [results]):
    results = []
    for i, item in enumerate(items):
        result = @process_item(index=i, item=item)
        results = results + [result]
    return results"#;

        let program = parse(source).unwrap();
        let func = &program.functions[0];
        let body = func.body.as_ref().unwrap();

        // Check for loop has two loop vars
        if let Some(ast::statement::Kind::ForLoop(for_loop)) = &body.statements[1].kind {
            assert_eq!(for_loop.loop_vars, vec!["i", "item"]);
        } else {
            panic!("expected for loop");
        }
    }

    #[test]
    fn test_parse_exception_type_arrow_syntax() {
        let source = r#"fn fetch(input: [url], output: [data]):
    data = @http_get(url=url) [NetworkError -> retry: 5, backoff: 2m]
    return data"#;

        let program = parse(source).unwrap();
        let func = &program.functions[0];
        let body = func.body.as_ref().unwrap();

        // Check the action call has a retry policy with exception type
        if let Some(ast::statement::Kind::Assignment(assign)) = &body.statements[0].kind {
            if let Some(ast::expr::Kind::ActionCall(action)) = &assign.value.as_ref().unwrap().kind
            {
                assert_eq!(action.policies.len(), 1);
                if let Some(ast::policy_bracket::Kind::Retry(retry)) = &action.policies[0].kind {
                    assert_eq!(retry.exception_types, vec!["NetworkError"]);
                    assert_eq!(retry.max_retries, 5);
                    assert_eq!(retry.backoff.as_ref().unwrap().seconds, 120); // 2m = 120s
                } else {
                    panic!("expected retry policy");
                }
            } else {
                panic!("expected action call");
            }
        } else {
            panic!("expected assignment");
        }
    }

    #[test]
    fn test_parse_multiple_policy_brackets() {
        let source = r#"fn fetch(input: [url], output: [data]):
    data = @http_get(url=url) [retry: 3, backoff: 30s] [timeout: 60s]
    return data"#;

        let program = parse(source).unwrap();
        let func = &program.functions[0];
        let body = func.body.as_ref().unwrap();

        if let Some(ast::statement::Kind::Assignment(assign)) = &body.statements[0].kind {
            if let Some(ast::expr::Kind::ActionCall(action)) = &assign.value.as_ref().unwrap().kind
            {
                assert_eq!(action.policies.len(), 2);
                // First is retry
                assert!(matches!(
                    &action.policies[0].kind,
                    Some(ast::policy_bracket::Kind::Retry(_))
                ));
                // Second is timeout
                if let Some(ast::policy_bracket::Kind::Timeout(timeout)) = &action.policies[1].kind
                {
                    assert_eq!(timeout.timeout.as_ref().unwrap().seconds, 60);
                } else {
                    panic!("expected timeout policy");
                }
            } else {
                panic!("expected action call");
            }
        } else {
            panic!("expected assignment");
        }
    }

    #[test]
    fn test_parse_parallel_block() {
        let source = r#"fn fetch_all(input: [ids], output: [results]):
    results = parallel:
        @fetch_user(id=1)
        @fetch_user(id=2)
        @fetch_user(id=3)
    return results"#;

        let program = parse(source).unwrap();
        let func = &program.functions[0];
        let body = func.body.as_ref().unwrap();

        // With the new design, "results = parallel:" is an Assignment with ParallelExpr
        if let Some(ast::statement::Kind::Assignment(assign)) = &body.statements[0].kind {
            assert_eq!(assign.targets, vec!["results".to_string()]);
            if let Some(ast::expr::Kind::ParallelExpr(parallel)) =
                assign.value.as_ref().and_then(|v| v.kind.as_ref())
            {
                assert_eq!(parallel.calls.len(), 3);
            } else {
                panic!("expected parallel expression");
            }
        } else {
            panic!("expected assignment");
        }
    }

    #[test]
    fn test_parse_parallel_block_statement() {
        // A parallel block without assignment (side-effect only)
        let source = r#"fn notify_all(input: [ids], output: []):
    parallel:
        @notify(id=1)
        @notify(id=2)
    return"#;

        let program = parse(source).unwrap();
        let func = &program.functions[0];
        let body = func.body.as_ref().unwrap();

        if let Some(ast::statement::Kind::ParallelBlock(parallel)) = &body.statements[0].kind {
            assert_eq!(parallel.calls.len(), 2);
        } else {
            panic!("expected parallel block statement");
        }
    }

    #[test]
    fn test_parse_in_operator() {
        let source = r#"fn check(input: [item, items], output: [result]):
    if item in items:
        result = @found_item(item=item)
    else:
        result = @not_found()
    return result"#;

        let program = parse(source).unwrap();
        let func = &program.functions[0];
        let body = func.body.as_ref().unwrap();

        if let Some(ast::statement::Kind::Conditional(cond)) = &body.statements[0].kind {
            let if_branch = cond.if_branch.as_ref().unwrap();
            if let Some(ast::expr::Kind::BinaryOp(binop)) =
                &if_branch.condition.as_ref().unwrap().kind
            {
                assert_eq!(binop.op, ast::BinaryOperator::BinaryOpIn as i32);
            } else {
                panic!("expected binary op");
            }
        } else {
            panic!("expected conditional");
        }
    }

    #[test]
    fn test_parse_nested_subscript() {
        let source = r#"fn get(input: [data], output: [result]):
    result = data["users"][0]["name"]
    return result"#;

        let program = parse(source).unwrap();
        assert_eq!(program.functions.len(), 1);
    }

    #[test]
    fn test_parse_comprehensive_workflow() {
        // This is the comprehensive example from the IR spec
        let source = r#"fn process_orders(input: [orders, config], output: [summary]):
    inventory = @fetch_inventory(warehouse=config["warehouse"])

    valid_orders = []
    rejected = []
    for order in orders:
        if order["sku"] in inventory and inventory[order["sku"]] >= order["qty"]:
            valid_orders = valid_orders + [order]
        else:
            rejected = rejected + [{"order": order, "reason": "out_of_stock"}]

    if len(valid_orders) > 0:
        payments = spread valid_orders:order -> @process_payment(
            order_id=order["id"],
            amount=order["total"],
            customer=order["customer_id"]
        )

        shipping = spread valid_orders:order -> @get_shipping_quote(
            destination=order["address"],
            weight=order["weight"]
        )

        confirmations = []
        for i, order in enumerate(valid_orders):
            confirmation = {
                "order_id": order["id"],
                "payment": payments[i],
                "shipping": shipping[i],
                "status": "confirmed"
            }
            confirmations = confirmations + [confirmation]
    else:
        confirmations = []

    notification_result = @send_notifications(
        confirmations=confirmations,
        rejected=rejected
    ) [NetworkError -> retry: 3, backoff: 30s] [timeout: 60s]

    summary = {
        "processed": len(confirmations),
        "rejected": len(rejected),
        "notification_id": notification_result["id"]
    }

    return summary"#;

        let program = parse(source).unwrap();
        assert_eq!(program.functions.len(), 1);

        let func = &program.functions[0];
        assert_eq!(func.name, "process_orders");

        let io = func.io.as_ref().unwrap();
        assert_eq!(io.inputs, vec!["orders", "config"]);
        assert_eq!(io.outputs, vec!["summary"]);

        let body = func.body.as_ref().unwrap();
        // inventory assignment, valid_orders, rejected, for loop, if/else, notification, summary, return
        assert!(body.statements.len() >= 7);
    }

    #[test]
    fn test_parse_function_call_in_expr() {
        let source = r#"fn check(input: [items], output: [result]):
    result = len(items) > 0
    return result"#;

        let program = parse(source).unwrap();
        assert_eq!(program.functions.len(), 1);
    }

    #[test]
    fn test_parse_dot_access() {
        let source = r#"fn get_name(input: [user], output: [name]):
    name = user.profile.name
    return name"#;

        let program = parse(source).unwrap();
        assert_eq!(program.functions.len(), 1);
    }

    #[test]
    fn test_parse_empty_io() {
        let source = r#"fn noop(input: [], output: []):
    x = 1
    return x"#;

        let program = parse(source).unwrap();
        let func = &program.functions[0];
        let io = func.io.as_ref().unwrap();
        assert!(io.inputs.is_empty());
        assert!(io.outputs.is_empty());
    }

    #[test]
    fn test_parse_multiline_action_kwargs() {
        let source = r#"fn send(input: [user], output: [result]):
    result = @send_email(
        to=user["email"],
        subject="Hello",
        body="Welcome!"
    )
    return result"#;

        let program = parse(source).unwrap();
        let func = &program.functions[0];
        let body = func.body.as_ref().unwrap();

        if let Some(ast::statement::Kind::Assignment(assign)) = &body.statements[0].kind {
            if let Some(ast::expr::Kind::ActionCall(action)) = &assign.value.as_ref().unwrap().kind
            {
                assert_eq!(action.kwargs.len(), 3);
            } else {
                panic!("expected action call");
            }
        } else {
            panic!("expected assignment");
        }
    }
}
