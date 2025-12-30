//! Lexer for the Rappel IR language.
//!
//! Uses logos for fast tokenization with custom handling for Python-style
//! indentation (INDENT/DEDENT tokens).
//!
//! # Indentation Handling
//!
//! The lexer tracks indentation levels using a stack. At the start of each line:
//! - If indentation increases: emit INDENT
//! - If indentation decreases: emit one or more DEDENT tokens
//! - Blank lines and comments are skipped
//!
//! This converts the indentation-sensitive grammar into a context-free one
//! that the parser can handle with standard recursive descent.

use std::fmt;

use logos::{Logos, SpannedIter};

/// Source span (byte offsets)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

impl Span {
    pub fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }

    /// Merge two spans into one covering both
    pub fn merge(self, other: Span) -> Span {
        Span {
            start: self.start.min(other.start),
            end: self.end.max(other.end),
        }
    }
}

/// A token with its span
#[derive(Debug, Clone, PartialEq)]
pub struct SpannedToken {
    pub token: Token,
    pub span: Span,
}

/// Token types for the Rappel IR language
#[derive(Logos, Debug, Clone, PartialEq)]
#[logos(skip r"[ \t]+")] // Skip horizontal whitespace (not newlines)
pub enum Token {
    // Keywords
    #[token("fn")]
    Fn,
    #[token("input")]
    Input,
    #[token("output")]
    Output,
    #[token("if")]
    If,
    #[token("elif")]
    Elif,
    #[token("else")]
    Else,
    #[token("for")]
    For,
    #[token("in")]
    In,
    #[token("try")]
    Try,
    #[token("except")]
    Except,
    #[token("as")]
    As,
    #[token("return")]
    Return,
    #[token("break")]
    Break,
    #[token("continue")]
    Continue,
    #[token("and")]
    And,
    #[token("or")]
    Or,
    #[token("not")]
    Not,
    #[token("True")]
    True,
    #[token("False")]
    False,
    #[token("None")]
    None_,
    #[token("spread")]
    Spread,
    #[token("parallel")]
    Parallel,
    #[token("retry")]
    Retry,
    #[token("backoff")]
    Backoff,
    #[token("timeout")]
    Timeout,

    // Identifiers
    #[regex(r"[a-zA-Z_][a-zA-Z0-9_]*", |lex| lex.slice().to_string())]
    Ident(String),

    // Literals
    #[regex(r"[0-9]+\.[0-9]+", |lex| lex.slice().parse::<f64>().ok())]
    Float(f64),

    #[regex(r"[0-9]+", priority = 2, callback = |lex| lex.slice().parse::<i64>().ok())]
    Int(i64),

    #[regex(r#""[^"]*""#, |lex| {
        let s = lex.slice();
        s[1..s.len()-1].to_string()
    })]
    String(String),

    // Duration literals: 30s, 2m, 1h
    #[regex(r"[0-9]+[smh]", |lex| lex.slice().to_string())]
    Duration(String),

    // Operators
    #[token("+")]
    Plus,
    #[token("-")]
    Minus,
    #[token("*")]
    Star,
    #[token("/")]
    Slash,
    #[token("//")]
    DoubleSlash,
    #[token("%")]
    Percent,
    #[token("==")]
    EqEq,
    #[token("!=")]
    NotEq,
    #[token("<")]
    Lt,
    #[token(">")]
    Gt,
    #[token("<=")]
    Le,
    #[token(">=")]
    Ge,
    #[token("=")]
    Eq,
    #[token("->")]
    Arrow,

    // Delimiters
    #[token("(")]
    LParen,
    #[token(")")]
    RParen,
    #[token("[")]
    LBracket,
    #[token("]")]
    RBracket,
    #[token("{")]
    LBrace,
    #[token("}")]
    RBrace,
    #[token(",")]
    Comma,
    #[token(":")]
    Colon,
    #[token(".")]
    Dot,
    #[token("@")]
    At,

    // Newline (we need to track these for indentation)
    #[regex(r"\n")]
    Newline,

    // Comment (skip but need to handle for line tracking)
    #[regex(r"#[^\n]*")]
    Comment,

    // Synthetic tokens for indentation (not matched by logos directly)
    Indent,
    Dedent,

    // End of file
    Eof,
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Token::Fn => write!(f, "fn"),
            Token::Input => write!(f, "input"),
            Token::Output => write!(f, "output"),
            Token::If => write!(f, "if"),
            Token::Elif => write!(f, "elif"),
            Token::Else => write!(f, "else"),
            Token::For => write!(f, "for"),
            Token::In => write!(f, "in"),
            Token::Try => write!(f, "try"),
            Token::Except => write!(f, "except"),
            Token::As => write!(f, "as"),
            Token::Return => write!(f, "return"),
            Token::Break => write!(f, "break"),
            Token::Continue => write!(f, "continue"),
            Token::And => write!(f, "and"),
            Token::Or => write!(f, "or"),
            Token::Not => write!(f, "not"),
            Token::True => write!(f, "True"),
            Token::False => write!(f, "False"),
            Token::None_ => write!(f, "None"),
            Token::Spread => write!(f, "spread"),
            Token::Parallel => write!(f, "parallel"),
            Token::Retry => write!(f, "retry"),
            Token::Backoff => write!(f, "backoff"),
            Token::Timeout => write!(f, "timeout"),
            Token::Ident(s) => write!(f, "{}", s),
            Token::Float(n) => write!(f, "{}", n),
            Token::Int(n) => write!(f, "{}", n),
            Token::String(s) => write!(f, "\"{}\"", s),
            Token::Duration(d) => write!(f, "{}", d),
            Token::Plus => write!(f, "+"),
            Token::Minus => write!(f, "-"),
            Token::Star => write!(f, "*"),
            Token::Slash => write!(f, "/"),
            Token::DoubleSlash => write!(f, "//"),
            Token::Percent => write!(f, "%"),
            Token::EqEq => write!(f, "=="),
            Token::NotEq => write!(f, "!="),
            Token::Lt => write!(f, "<"),
            Token::Gt => write!(f, ">"),
            Token::Le => write!(f, "<="),
            Token::Ge => write!(f, ">="),
            Token::Eq => write!(f, "="),
            Token::Arrow => write!(f, "->"),
            Token::LParen => write!(f, "("),
            Token::RParen => write!(f, ")"),
            Token::LBracket => write!(f, "["),
            Token::RBracket => write!(f, "]"),
            Token::LBrace => write!(f, "{{"),
            Token::RBrace => write!(f, "}}"),
            Token::Comma => write!(f, ","),
            Token::Colon => write!(f, ":"),
            Token::Dot => write!(f, "."),
            Token::At => write!(f, "@"),
            Token::Newline => write!(f, "\\n"),
            Token::Comment => write!(f, "# comment"),
            Token::Indent => write!(f, "INDENT"),
            Token::Dedent => write!(f, "DEDENT"),
            Token::Eof => write!(f, "EOF"),
        }
    }
}

/// Lexer error
#[derive(Debug, Clone, PartialEq)]
pub struct LexerError {
    pub message: String,
    pub span: Span,
}

impl fmt::Display for LexerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} at {:?}", self.message, self.span)
    }
}

impl std::error::Error for LexerError {}

/// Lexer wrapper that handles indentation
pub struct Lexer<'source> {
    source: &'source str,
    inner: SpannedIter<'source, Token>,
    indent_stack: Vec<usize>,
    pending_tokens: Vec<SpannedToken>,
    at_line_start: bool,
    current_pos: usize,
    done: bool,
    /// Track bracket nesting depth - no INDENT/DEDENT inside brackets
    bracket_depth: usize,
}

impl<'source> Lexer<'source> {
    pub fn new(source: &'source str) -> Self {
        Self {
            source,
            inner: Token::lexer(source).spanned(),
            indent_stack: vec![0],
            pending_tokens: Vec::new(),
            at_line_start: true,
            current_pos: 0,
            done: false,
            bracket_depth: 0,
        }
    }

    /// Get the source text
    pub fn source(&self) -> &'source str {
        self.source
    }

    /// Measure the indentation at a given position (start of line)
    fn measure_indent(&self, pos: usize) -> usize {
        let mut indent = 0;
        for ch in self.source[pos..].chars() {
            match ch {
                ' ' => indent += 1,
                '\t' => indent += 4, // Treat tabs as 4 spaces
                _ => break,
            }
        }
        indent
    }

    /// Find the start of the current line
    fn line_start(&self, pos: usize) -> usize {
        self.source[..pos].rfind('\n').map(|i| i + 1).unwrap_or(0)
    }

    /// Check if a line is blank or comment-only
    fn is_blank_line(&self, line_start: usize) -> bool {
        for ch in self.source[line_start..].chars() {
            match ch {
                ' ' | '\t' => continue,
                '\n' => return true,
                '#' => return true,
                _ => return false,
            }
        }
        true // End of file
    }

    /// Process indentation at the start of a line
    fn process_indentation(&mut self, line_start: usize, token_start: usize) {
        let indent = self.measure_indent(line_start);
        let current = *self.indent_stack.last().unwrap();

        if indent > current {
            // Indent increased
            self.indent_stack.push(indent);
            self.pending_tokens.push(SpannedToken {
                token: Token::Indent,
                span: Span::new(line_start, token_start),
            });
        } else if indent < current {
            // Indent decreased - may need multiple dedents
            while let Some(&top) = self.indent_stack.last() {
                if top <= indent {
                    break;
                }
                self.indent_stack.pop();
                self.pending_tokens.push(SpannedToken {
                    token: Token::Dedent,
                    span: Span::new(line_start, token_start),
                });
            }
        }
    }

    /// Emit remaining dedents at end of file
    fn emit_final_dedents(&mut self) {
        let pos = self.source.len();
        while self.indent_stack.len() > 1 {
            self.indent_stack.pop();
            self.pending_tokens.push(SpannedToken {
                token: Token::Dedent,
                span: Span::new(pos, pos),
            });
        }
        self.pending_tokens.push(SpannedToken {
            token: Token::Eof,
            span: Span::new(pos, pos),
        });
    }
}

impl<'source> Iterator for Lexer<'source> {
    type Item = Result<SpannedToken, LexerError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Return any pending tokens first
        if let Some(token) = self.pending_tokens.pop() {
            return Some(Ok(token));
        }

        if self.done {
            return None;
        }

        loop {
            match self.inner.next() {
                Some((Ok(token), span)) => {
                    let span = Span::new(span.start, span.end);

                    // Track bracket depth for opening brackets
                    match &token {
                        Token::LParen | Token::LBracket | Token::LBrace => {
                            self.bracket_depth += 1;
                        }
                        Token::RParen | Token::RBracket | Token::RBrace => {
                            self.bracket_depth = self.bracket_depth.saturating_sub(1);
                        }
                        _ => {}
                    }

                    // Handle indentation at line start (only when not inside brackets)
                    if self.at_line_start && self.bracket_depth == 0 {
                        let line_start = self.line_start(span.start);

                        // Skip blank lines
                        if !self.is_blank_line(line_start) {
                            self.process_indentation(line_start, span.start);
                        }
                        self.at_line_start = false;
                    } else if self.at_line_start {
                        // Inside brackets, just clear the flag
                        self.at_line_start = false;
                    }

                    match token {
                        Token::Newline => {
                            self.at_line_start = true;
                            self.current_pos = span.end;
                            // Skip newlines - they're only for indentation tracking
                            continue;
                        }
                        Token::Comment => {
                            // Skip comments
                            continue;
                        }
                        _ => {
                            self.current_pos = span.end;

                            // Return pending indent/dedent tokens first
                            if !self.pending_tokens.is_empty() {
                                self.pending_tokens.reverse();
                                self.pending_tokens.push(SpannedToken { token, span });
                                self.pending_tokens.reverse();
                                return self.pending_tokens.pop().map(Ok);
                            }

                            return Some(Ok(SpannedToken { token, span }));
                        }
                    }
                }
                Some((Err(_), span)) => {
                    return Some(Err(LexerError {
                        message: format!(
                            "unexpected character: '{}'",
                            &self.source[span.start..span.end]
                        ),
                        span: Span::new(span.start, span.end),
                    }));
                }
                None => {
                    // End of input - emit remaining dedents and EOF
                    self.done = true;
                    self.emit_final_dedents();
                    self.pending_tokens.reverse();
                    return self.pending_tokens.pop().map(Ok);
                }
            }
        }
    }
}

/// Convenience function to lex a source string into a vector of tokens
pub fn lex(source: &str) -> Result<Vec<SpannedToken>, LexerError> {
    Lexer::new(source).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token_types(source: &str) -> Vec<Token> {
        lex(source)
            .unwrap()
            .into_iter()
            .map(|st| st.token)
            .collect()
    }

    #[test]
    fn test_keywords() {
        let tokens = token_types("fn if elif else for in return");
        assert_eq!(
            tokens,
            vec![
                Token::Fn,
                Token::If,
                Token::Elif,
                Token::Else,
                Token::For,
                Token::In,
                Token::Return,
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_identifiers() {
        let tokens = token_types("foo bar_baz _private x123");
        assert_eq!(
            tokens,
            vec![
                Token::Ident("foo".to_string()),
                Token::Ident("bar_baz".to_string()),
                Token::Ident("_private".to_string()),
                Token::Ident("x123".to_string()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_literals() {
        let tokens = token_types("42 3.15 \"hello\" True False None");
        assert_eq!(
            tokens,
            vec![
                Token::Int(42),
                Token::Float(3.15),
                Token::String("hello".to_string()),
                Token::True,
                Token::False,
                Token::None_,
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_durations() {
        let tokens = token_types("30s 2m 1h");
        assert_eq!(
            tokens,
            vec![
                Token::Duration("30s".to_string()),
                Token::Duration("2m".to_string()),
                Token::Duration("1h".to_string()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_operators() {
        let tokens = token_types("+ - * / // % == != < > <= >= = ->");
        assert_eq!(
            tokens,
            vec![
                Token::Plus,
                Token::Minus,
                Token::Star,
                Token::Slash,
                Token::DoubleSlash,
                Token::Percent,
                Token::EqEq,
                Token::NotEq,
                Token::Lt,
                Token::Gt,
                Token::Le,
                Token::Ge,
                Token::Eq,
                Token::Arrow,
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_delimiters() {
        let tokens = token_types("( ) [ ] { } , : . @");
        assert_eq!(
            tokens,
            vec![
                Token::LParen,
                Token::RParen,
                Token::LBracket,
                Token::RBracket,
                Token::LBrace,
                Token::RBrace,
                Token::Comma,
                Token::Colon,
                Token::Dot,
                Token::At,
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_simple_indent() {
        let source = "fn foo:\n    x = 1";
        let tokens = token_types(source);
        assert_eq!(
            tokens,
            vec![
                Token::Fn,
                Token::Ident("foo".to_string()),
                Token::Colon,
                Token::Indent,
                Token::Ident("x".to_string()),
                Token::Eq,
                Token::Int(1),
                Token::Dedent,
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_nested_indent() {
        let source = "if x:\n    if y:\n        z = 1\n    a = 2";
        let tokens = token_types(source);
        assert_eq!(
            tokens,
            vec![
                Token::If,
                Token::Ident("x".to_string()),
                Token::Colon,
                Token::Indent,
                Token::If,
                Token::Ident("y".to_string()),
                Token::Colon,
                Token::Indent,
                Token::Ident("z".to_string()),
                Token::Eq,
                Token::Int(1),
                Token::Dedent,
                Token::Ident("a".to_string()),
                Token::Eq,
                Token::Int(2),
                Token::Dedent,
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_multiple_dedents() {
        let source = "if x:\n    if y:\n        z = 1\na = 2";
        let tokens = token_types(source);
        // Should have 2 dedents going from indent 8 to indent 0
        assert!(tokens.contains(&Token::Dedent));
        let dedent_count = tokens.iter().filter(|t| **t == Token::Dedent).count();
        assert_eq!(dedent_count, 2);
    }

    #[test]
    fn test_action_call() {
        let tokens = token_types("result = @fetch_data(url=x)");
        assert_eq!(
            tokens,
            vec![
                Token::Ident("result".to_string()),
                Token::Eq,
                Token::At,
                Token::Ident("fetch_data".to_string()),
                Token::LParen,
                Token::Ident("url".to_string()),
                Token::Eq,
                Token::Ident("x".to_string()),
                Token::RParen,
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_comments_skipped() {
        let tokens = token_types("x = 1  # this is a comment\ny = 2");
        assert_eq!(
            tokens,
            vec![
                Token::Ident("x".to_string()),
                Token::Eq,
                Token::Int(1),
                Token::Ident("y".to_string()),
                Token::Eq,
                Token::Int(2),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_spread_syntax() {
        let tokens = token_types("spread items:item -> @process(x=item)");
        assert_eq!(
            tokens,
            vec![
                Token::Spread,
                Token::Ident("items".to_string()),
                Token::Colon,
                Token::Ident("item".to_string()),
                Token::Arrow,
                Token::At,
                Token::Ident("process".to_string()),
                Token::LParen,
                Token::Ident("x".to_string()),
                Token::Eq,
                Token::Ident("item".to_string()),
                Token::RParen,
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_retry_policy() {
        let tokens = token_types("[retry: 3, backoff: 30s]");
        assert_eq!(
            tokens,
            vec![
                Token::LBracket,
                Token::Retry,
                Token::Colon,
                Token::Int(3),
                Token::Comma,
                Token::Backoff,
                Token::Colon,
                Token::Duration("30s".to_string()),
                Token::RBracket,
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_function_def() {
        let source = r#"fn process_orders(input: [orders], output: [result]):
    x = 1"#;
        let tokens = token_types(source);
        assert!(tokens.contains(&Token::Fn));
        assert!(tokens.contains(&Token::Input));
        assert!(tokens.contains(&Token::Output));
        assert!(tokens.contains(&Token::Indent));
    }
}
