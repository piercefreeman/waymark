#!/usr/bin/env python3
"""
Rappel Language - A DSL with immutable variables, explicit I/O, and first-class actions.

Run with: uv run python rappel_lang.py
Run tests with: uv run pytest rappel_lang.py -v

Language Features:
- Immutable variables assigned with `=`
- List operations: my_list = my_list + [new_item]
- Dict operations: my_dict = my_dict + {"key": "value"}
- Python blocks with explicit input/output vars
- Functions with explicit input/output vars (all calls must use kwargs)
- Actions called with @action_name(kwarg=value) syntax (external, not defined in code)
- For loops iterating single var over list with function body
- Spread operator for variable unpacking
- List/dict key-based access
- No closures, no nested functions
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any


# =============================================================================
# CHECKPOINT 1: Token Types and IR Data Structures
# =============================================================================


class TokenType(Enum):
    """All token types in the Rappel language."""

    # Literals
    IDENTIFIER = auto()
    STRING = auto()
    NUMBER = auto()
    BOOLEAN = auto()

    # Keywords
    FN = auto()  # function definition
    PYTHON = auto()  # python block
    FOR = auto()  # for loop
    IN = auto()  # for x in list
    IF = auto()  # conditional
    ELSE = auto()  # else branch
    RETURN = auto()  # return statement
    INPUT = auto()  # input declaration
    OUTPUT = auto()  # output declaration
    SPREAD = auto()  # spread operator ...
    AT = auto()  # @ for action calls

    # Operators
    ASSIGN = auto()  # =
    PLUS = auto()  # +
    MINUS = auto()  # -
    STAR = auto()  # *
    SLASH = auto()  # /
    DOT = auto()  # .
    COMMA = auto()  # ,
    COLON = auto()  # :
    ARROW = auto()  # ->
    EQ = auto()  # ==
    NEQ = auto()  # !=
    LT = auto()  # <
    GT = auto()  # >
    LTE = auto()  # <=
    GTE = auto()  # >=
    AND = auto()  # and
    OR = auto()  # or
    NOT = auto()  # not

    # Delimiters
    LPAREN = auto()  # (
    RPAREN = auto()  # )
    LBRACKET = auto()  # [
    RBRACKET = auto()  # ]
    LBRACE = auto()  # {
    RBRACE = auto()  # }
    NEWLINE = auto()
    INDENT = auto()
    DEDENT = auto()

    # Special
    EOF = auto()
    PIPE = auto()  # |


@dataclass
class Token:
    """A single token from the lexer."""

    type: TokenType
    value: Any
    line: int
    column: int

    def __repr__(self) -> str:
        return f"Token({self.type.name}, {self.value!r}, L{self.line}:C{self.column})"


# =============================================================================
# IR Data Structures - Immutable by design
# =============================================================================


@dataclass(frozen=True)
class SourceLocation:
    """Source code location for error reporting."""

    line: int
    column: int
    end_line: int | None = None
    end_column: int | None = None


@dataclass(frozen=True)
class RappelValue:
    """Base class for all Rappel values."""

    pass


@dataclass(frozen=True)
class RappelString(RappelValue):
    """String literal value."""

    value: str


@dataclass(frozen=True)
class RappelNumber(RappelValue):
    """Numeric literal value (int or float)."""

    value: int | float


@dataclass(frozen=True)
class RappelBoolean(RappelValue):
    """Boolean literal value."""

    value: bool


@dataclass(frozen=True)
class RappelList(RappelValue):
    """List value - immutable."""

    items: tuple[RappelExpr, ...]


@dataclass(frozen=True)
class RappelDict(RappelValue):
    """Dict value - immutable."""

    pairs: tuple[tuple[str, RappelExpr], ...]


# =============================================================================
# IR Expression Nodes
# =============================================================================


@dataclass(frozen=True)
class RappelLiteral:
    """A literal value (string, number, boolean)."""

    value: RappelValue
    location: SourceLocation | None = None


@dataclass(frozen=True)
class RappelVariable:
    """A variable reference."""

    name: str
    location: SourceLocation | None = None


@dataclass(frozen=True)
class RappelListExpr:
    """A list expression [a, b, c]."""

    items: tuple[RappelExpr, ...]
    location: SourceLocation | None = None


@dataclass(frozen=True)
class RappelDictExpr:
    """A dict expression {"key": value}."""

    pairs: tuple[tuple[RappelExpr, RappelExpr], ...]
    location: SourceLocation | None = None


@dataclass(frozen=True)
class RappelBinaryOp:
    """A binary operation (a + b, a - b, etc.)."""

    op: str
    left: RappelExpr
    right: RappelExpr
    location: SourceLocation | None = None


@dataclass(frozen=True)
class RappelUnaryOp:
    """A unary operation (not x, -x)."""

    op: str
    operand: RappelExpr
    location: SourceLocation | None = None


@dataclass(frozen=True)
class RappelIndexAccess:
    """Index access: list[0] or dict["key"]."""

    target: RappelExpr
    index: RappelExpr
    location: SourceLocation | None = None


@dataclass(frozen=True)
class RappelDotAccess:
    """Dot access: obj.field."""

    target: RappelExpr
    field: str
    location: SourceLocation | None = None


@dataclass(frozen=True)
class RappelSpread:
    """Spread operator: ...variable."""

    target: RappelExpr
    location: SourceLocation | None = None


@dataclass(frozen=True)
class RappelCall:
    """Function call: func(kwarg=value). All args must be kwargs."""

    target: str
    kwargs: tuple[tuple[str, RappelExpr], ...]
    location: SourceLocation | None = None


@dataclass(frozen=True)
class RappelActionCall:
    """Action call: @action_name(kwarg=value). External action, kwargs only."""

    action_name: str
    kwargs: tuple[tuple[str, RappelExpr], ...]
    location: SourceLocation | None = None


# Type alias for expressions
RappelExpr = (
    RappelLiteral
    | RappelVariable
    | RappelListExpr
    | RappelDictExpr
    | RappelBinaryOp
    | RappelUnaryOp
    | RappelIndexAccess
    | RappelDotAccess
    | RappelSpread
    | RappelCall
    | RappelActionCall
)


# =============================================================================
# IR Statement Nodes
# =============================================================================


@dataclass(frozen=True)
class RappelAssignment:
    """Variable assignment: x = expr"""

    target: str
    value: RappelExpr
    location: SourceLocation | None = None


@dataclass(frozen=True)
class RappelMultiAssignment:
    """Multiple assignment (unpacking): a, b, c = expr"""

    targets: tuple[str, ...]
    value: RappelExpr
    location: SourceLocation | None = None


@dataclass(frozen=True)
class RappelReturn:
    """Return statement: return expr or return [a, b, c]"""

    values: tuple[RappelExpr, ...]
    location: SourceLocation | None = None


@dataclass(frozen=True)
class RappelExprStatement:
    """Expression as statement (e.g., action call)."""

    expr: RappelExpr
    location: SourceLocation | None = None


@dataclass(frozen=True)
class RappelPythonBlock:
    """
    Python block with explicit input/output.

    python(input: [x, y], output: [z]):
        z = x + y
    """

    code: str
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]
    location: SourceLocation | None = None


@dataclass(frozen=True)
class RappelFunctionDef:
    """
    Function definition with explicit input/output.

    fn process(input: [x, y], output: [result]):
        result = x + y
        return result
    """

    name: str
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]
    body: tuple[RappelStatement, ...]
    location: SourceLocation | None = None


@dataclass(frozen=True)
class RappelForLoop:
    """
    For loop - must iterate with a function body.

    for item in items -> process_item(input: [item], output: [result]):
        result = transform(item)
    """

    loop_var: str
    iterable: RappelExpr
    body_fn: RappelFunctionDef
    location: SourceLocation | None = None


@dataclass(frozen=True)
class RappelIfStatement:
    """
    If statement with explicit blocks.

    if condition:
        body
    else:
        else_body
    """

    condition: RappelExpr
    then_body: tuple[RappelStatement, ...]
    else_body: tuple[RappelStatement, ...] | None = None
    location: SourceLocation | None = None


# Type alias for statements
RappelStatement = (
    RappelAssignment
    | RappelMultiAssignment
    | RappelReturn
    | RappelExprStatement
    | RappelPythonBlock
    | RappelFunctionDef
    | RappelForLoop
    | RappelIfStatement
)


@dataclass(frozen=True)
class RappelProgram:
    """A complete Rappel program."""

    statements: tuple[RappelStatement, ...]
    location: SourceLocation | None = None


# =============================================================================
# CHECKPOINT 1 TESTS: IR Data Structures
# =============================================================================


def test_ir_literal_values():
    """Test creating literal values."""
    s = RappelString("hello")
    assert s.value == "hello"

    n = RappelNumber(42)
    assert n.value == 42

    f = RappelNumber(3.14)
    assert f.value == 3.14

    b = RappelBoolean(True)
    assert b.value is True


def test_ir_list_and_dict():
    """Test creating list and dict values."""
    # List with items
    items = (
        RappelLiteral(RappelNumber(1)),
        RappelLiteral(RappelNumber(2)),
        RappelLiteral(RappelNumber(3)),
    )
    lst = RappelListExpr(items=items)
    assert len(lst.items) == 3

    # Dict with pairs
    pairs = (
        (RappelLiteral(RappelString("a")), RappelLiteral(RappelNumber(1))),
        (RappelLiteral(RappelString("b")), RappelLiteral(RappelNumber(2))),
    )
    dct = RappelDictExpr(pairs=pairs)
    assert len(dct.pairs) == 2


def test_ir_expressions():
    """Test creating various expressions."""
    # Variable reference
    var = RappelVariable(name="x")
    assert var.name == "x"

    # Binary operation
    add = RappelBinaryOp(
        op="+",
        left=RappelVariable(name="a"),
        right=RappelVariable(name="b"),
    )
    assert add.op == "+"

    # Index access
    idx = RappelIndexAccess(
        target=RappelVariable(name="my_list"),
        index=RappelLiteral(RappelNumber(0)),
    )
    assert isinstance(idx.target, RappelVariable)

    # Spread operator
    spread = RappelSpread(target=RappelVariable(name="items"))
    assert isinstance(spread.target, RappelVariable)


def test_ir_statements():
    """Test creating statements."""
    # Assignment
    assign = RappelAssignment(
        target="x",
        value=RappelLiteral(RappelNumber(42)),
    )
    assert assign.target == "x"

    # Multi-assignment (unpacking)
    multi = RappelMultiAssignment(
        targets=("a", "b", "c"),
        value=RappelVariable(name="result"),
    )
    assert len(multi.targets) == 3


def test_ir_python_block():
    """Test Python block with explicit I/O."""
    block = RappelPythonBlock(
        code="z = x + y",
        inputs=("x", "y"),
        outputs=("z",),
    )
    assert block.inputs == ("x", "y")
    assert block.outputs == ("z",)


def test_ir_function_def():
    """Test function definition with explicit I/O."""
    fn = RappelFunctionDef(
        name="add",
        inputs=("a", "b"),
        outputs=("result",),
        body=(
            RappelAssignment(
                target="result",
                value=RappelBinaryOp(
                    op="+",
                    left=RappelVariable(name="a"),
                    right=RappelVariable(name="b"),
                ),
            ),
            RappelReturn(values=(RappelVariable(name="result"),)),
        ),
    )
    assert fn.name == "add"
    assert fn.inputs == ("a", "b")
    assert fn.outputs == ("result",)


def test_ir_action_call():
    """Test action call expression."""
    action_call = RappelActionCall(
        action_name="fetch_url",
        kwargs=(
            ("url", RappelLiteral(RappelString("https://example.com"))),
        ),
    )
    assert action_call.action_name == "fetch_url"
    assert len(action_call.kwargs) == 1
    assert action_call.kwargs[0][0] == "url"


def test_ir_for_loop():
    """Test for loop with function body."""
    body_fn = RappelFunctionDef(
        name="process_item",
        inputs=("item",),
        outputs=("processed",),
        body=(
            RappelAssignment(
                target="processed",
                value=RappelBinaryOp(
                    op="*",
                    left=RappelVariable(name="item"),
                    right=RappelLiteral(RappelNumber(2)),
                ),
            ),
        ),
    )

    for_loop = RappelForLoop(
        loop_var="item",
        iterable=RappelVariable(name="items"),
        body_fn=body_fn,
    )
    assert for_loop.loop_var == "item"
    assert for_loop.body_fn.name == "process_item"


def test_ir_if_statement():
    """Test if statement."""
    if_stmt = RappelIfStatement(
        condition=RappelBinaryOp(
            op=">",
            left=RappelVariable(name="x"),
            right=RappelLiteral(RappelNumber(0)),
        ),
        then_body=(
            RappelAssignment(
                target="result",
                value=RappelLiteral(RappelString("positive")),
            ),
        ),
        else_body=(
            RappelAssignment(
                target="result",
                value=RappelLiteral(RappelString("non-positive")),
            ),
        ),
    )
    assert if_stmt.condition.op == ">"


def test_ir_immutability():
    """Test that IR nodes are frozen (immutable)."""
    assign = RappelAssignment(
        target="x",
        value=RappelLiteral(RappelNumber(42)),
    )

    # Should raise FrozenInstanceError
    try:
        assign.target = "y"  # type: ignore
        assert False, "Should have raised an error"
    except AttributeError:
        pass  # Expected - frozen dataclass


# =============================================================================
# CHECKPOINT 2: Lexer Implementation
# =============================================================================


class RappelLexer:
    """
    Lexer for the Rappel language.

    Handles:
    - Keywords: fn, action, python, for, in, if, else, return, input, output
    - Operators: =, +, -, *, /, ==, !=, <, >, <=, >=, and, or, not, ->
    - Literals: strings, numbers, booleans
    - Identifiers
    - Indentation-based blocks
    - Spread operator: ...
    """

    KEYWORDS = {
        "fn": TokenType.FN,
        "python": TokenType.PYTHON,
        "for": TokenType.FOR,
        "in": TokenType.IN,
        "if": TokenType.IF,
        "else": TokenType.ELSE,
        "return": TokenType.RETURN,
        "input": TokenType.INPUT,
        "output": TokenType.OUTPUT,
        "and": TokenType.AND,
        "or": TokenType.OR,
        "not": TokenType.NOT,
        "true": TokenType.BOOLEAN,
        "false": TokenType.BOOLEAN,
        "True": TokenType.BOOLEAN,
        "False": TokenType.BOOLEAN,
    }

    SINGLE_CHAR_TOKENS = {
        "(": TokenType.LPAREN,
        ")": TokenType.RPAREN,
        "[": TokenType.LBRACKET,
        "]": TokenType.RBRACKET,
        "{": TokenType.LBRACE,
        "}": TokenType.RBRACE,
        ",": TokenType.COMMA,
        ":": TokenType.COLON,
        "+": TokenType.PLUS,
        "*": TokenType.STAR,
        "/": TokenType.SLASH,
        "|": TokenType.PIPE,
        "@": TokenType.AT,
    }

    def __init__(self, source: str):
        self.source = source
        self.pos = 0
        self.line = 1
        self.column = 1
        self.tokens: list[Token] = []
        self.indent_stack: list[int] = [0]
        self._at_line_start = True

    def tokenize(self) -> list[Token]:
        """Tokenize the entire source."""
        while self.pos < len(self.source):
            self._scan_token()

        # Emit remaining DEDENTs
        while len(self.indent_stack) > 1:
            self.indent_stack.pop()
            self.tokens.append(
                Token(TokenType.DEDENT, None, self.line, self.column)
            )

        self.tokens.append(Token(TokenType.EOF, None, self.line, self.column))
        return self.tokens

    def _scan_token(self) -> None:
        """Scan the next token."""
        # Handle line start indentation
        if self._at_line_start:
            self._handle_indentation()
            self._at_line_start = False
            if self.pos >= len(self.source):
                return

        char = self.source[self.pos]

        # Skip whitespace (but not newlines)
        if char in " \t" and not self._at_line_start:
            self._advance()
            return

        # Comments
        if char == "#":
            self._skip_comment()
            return

        # Newlines
        if char == "\n":
            # Only emit NEWLINE if we have meaningful tokens
            if self.tokens and self.tokens[-1].type not in (
                TokenType.NEWLINE,
                TokenType.INDENT,
                TokenType.DEDENT,
            ):
                self.tokens.append(
                    Token(TokenType.NEWLINE, "\n", self.line, self.column)
                )
            self._advance()
            self.line += 1
            self.column = 1
            self._at_line_start = True
            return

        # Multi-character operators
        if char == "-" and self._peek(1) == ">":
            self.tokens.append(
                Token(TokenType.ARROW, "->", self.line, self.column)
            )
            self._advance()
            self._advance()
            return

        if char == "=" and self._peek(1) == "=":
            self.tokens.append(Token(TokenType.EQ, "==", self.line, self.column))
            self._advance()
            self._advance()
            return

        if char == "!" and self._peek(1) == "=":
            self.tokens.append(
                Token(TokenType.NEQ, "!=", self.line, self.column)
            )
            self._advance()
            self._advance()
            return

        if char == "<" and self._peek(1) == "=":
            self.tokens.append(
                Token(TokenType.LTE, "<=", self.line, self.column)
            )
            self._advance()
            self._advance()
            return

        if char == ">" and self._peek(1) == "=":
            self.tokens.append(
                Token(TokenType.GTE, ">=", self.line, self.column)
            )
            self._advance()
            self._advance()
            return

        if char == "." and self._peek(1) == "." and self._peek(2) == ".":
            self.tokens.append(
                Token(TokenType.SPREAD, "...", self.line, self.column)
            )
            self._advance()
            self._advance()
            self._advance()
            return

        # Single character operators
        if char == "=":
            self.tokens.append(
                Token(TokenType.ASSIGN, "=", self.line, self.column)
            )
            self._advance()
            return

        if char == "-":
            self.tokens.append(
                Token(TokenType.MINUS, "-", self.line, self.column)
            )
            self._advance()
            return

        if char == ".":
            self.tokens.append(Token(TokenType.DOT, ".", self.line, self.column))
            self._advance()
            return

        if char == "<":
            self.tokens.append(Token(TokenType.LT, "<", self.line, self.column))
            self._advance()
            return

        if char == ">":
            self.tokens.append(Token(TokenType.GT, ">", self.line, self.column))
            self._advance()
            return

        if char in self.SINGLE_CHAR_TOKENS:
            self.tokens.append(
                Token(
                    self.SINGLE_CHAR_TOKENS[char], char, self.line, self.column
                )
            )
            self._advance()
            return

        # String literals
        if char in '"\'':
            self._scan_string(char)
            return

        # Number literals
        if char.isdigit() or (char == "-" and self._peek(1).isdigit()):
            self._scan_number()
            return

        # Identifiers and keywords
        if char.isalpha() or char == "_":
            self._scan_identifier()
            return

        # Unknown character
        raise SyntaxError(
            f"Unexpected character '{char}' at line {self.line}, column {self.column}"
        )

    def _handle_indentation(self) -> None:
        """Handle indentation at the start of a line."""
        indent = 0
        while self.pos < len(self.source) and self.source[self.pos] in " \t":
            if self.source[self.pos] == " ":
                indent += 1
            else:  # tab
                indent += 4  # Treat tabs as 4 spaces
            self._advance()

        # Skip blank lines and comment-only lines
        if self.pos < len(self.source) and self.source[self.pos] in "\n#":
            return

        current_indent = self.indent_stack[-1]

        if indent > current_indent:
            self.indent_stack.append(indent)
            self.tokens.append(
                Token(TokenType.INDENT, indent, self.line, self.column)
            )
        elif indent < current_indent:
            while self.indent_stack and indent < self.indent_stack[-1]:
                self.indent_stack.pop()
                self.tokens.append(
                    Token(TokenType.DEDENT, None, self.line, self.column)
                )

            if self.indent_stack[-1] != indent:
                raise SyntaxError(
                    f"Inconsistent indentation at line {self.line}"
                )

    def _scan_string(self, quote: str) -> None:
        """Scan a string literal."""
        start_line = self.line
        start_col = self.column
        self._advance()  # consume opening quote

        value = ""
        while self.pos < len(self.source):
            char = self.source[self.pos]

            if char == quote:
                self._advance()
                self.tokens.append(
                    Token(TokenType.STRING, value, start_line, start_col)
                )
                return

            if char == "\\":
                self._advance()
                if self.pos < len(self.source):
                    escape_char = self.source[self.pos]
                    if escape_char == "n":
                        value += "\n"
                    elif escape_char == "t":
                        value += "\t"
                    elif escape_char == "\\":
                        value += "\\"
                    elif escape_char == quote:
                        value += quote
                    else:
                        value += escape_char
                    self._advance()
            else:
                value += char
                self._advance()

        raise SyntaxError(f"Unterminated string starting at line {start_line}")

    def _scan_number(self) -> None:
        """Scan a number literal."""
        start_col = self.column
        value = ""

        if self.source[self.pos] == "-":
            value += "-"
            self._advance()

        while self.pos < len(self.source) and (
            self.source[self.pos].isdigit() or self.source[self.pos] == "."
        ):
            value += self.source[self.pos]
            self._advance()

        if "." in value:
            self.tokens.append(
                Token(TokenType.NUMBER, float(value), self.line, start_col)
            )
        else:
            self.tokens.append(
                Token(TokenType.NUMBER, int(value), self.line, start_col)
            )

    def _scan_identifier(self) -> None:
        """Scan an identifier or keyword."""
        start_col = self.column
        value = ""

        while self.pos < len(self.source) and (
            self.source[self.pos].isalnum() or self.source[self.pos] == "_"
        ):
            value += self.source[self.pos]
            self._advance()

        if value in self.KEYWORDS:
            token_type = self.KEYWORDS[value]
            if token_type == TokenType.BOOLEAN:
                self.tokens.append(
                    Token(
                        token_type,
                        value in ("true", "True"),
                        self.line,
                        start_col,
                    )
                )
            else:
                self.tokens.append(
                    Token(token_type, value, self.line, start_col)
                )
        else:
            self.tokens.append(
                Token(TokenType.IDENTIFIER, value, self.line, start_col)
            )

    def _skip_comment(self) -> None:
        """Skip a comment until end of line."""
        while self.pos < len(self.source) and self.source[self.pos] != "\n":
            self._advance()

    def _advance(self) -> str:
        """Advance to the next character."""
        char = self.source[self.pos] if self.pos < len(self.source) else ""
        self.pos += 1
        self.column += 1
        return char

    def _peek(self, offset: int = 0) -> str:
        """Peek at a character without advancing."""
        pos = self.pos + offset
        return self.source[pos] if pos < len(self.source) else ""


# =============================================================================
# CHECKPOINT 2 TESTS: Lexer
# =============================================================================


def test_lexer_simple_assignment():
    """Test lexing a simple assignment."""
    lexer = RappelLexer("x = 42")
    tokens = lexer.tokenize()

    assert tokens[0].type == TokenType.IDENTIFIER
    assert tokens[0].value == "x"
    assert tokens[1].type == TokenType.ASSIGN
    assert tokens[2].type == TokenType.NUMBER
    assert tokens[2].value == 42


def test_lexer_string_literal():
    """Test lexing string literals."""
    lexer = RappelLexer('"hello world"')
    tokens = lexer.tokenize()

    assert tokens[0].type == TokenType.STRING
    assert tokens[0].value == "hello world"


def test_lexer_list_literal():
    """Test lexing list literals."""
    lexer = RappelLexer("[1, 2, 3]")
    tokens = lexer.tokenize()

    assert tokens[0].type == TokenType.LBRACKET
    assert tokens[1].type == TokenType.NUMBER
    assert tokens[1].value == 1
    assert tokens[2].type == TokenType.COMMA
    assert tokens[3].type == TokenType.NUMBER
    assert tokens[5].type == TokenType.NUMBER
    assert tokens[6].type == TokenType.RBRACKET


def test_lexer_dict_literal():
    """Test lexing dict literals."""
    lexer = RappelLexer('{"key": "value"}')
    tokens = lexer.tokenize()

    assert tokens[0].type == TokenType.LBRACE
    assert tokens[1].type == TokenType.STRING
    assert tokens[1].value == "key"
    assert tokens[2].type == TokenType.COLON
    assert tokens[3].type == TokenType.STRING
    assert tokens[3].value == "value"
    assert tokens[4].type == TokenType.RBRACE


def test_lexer_keywords():
    """Test lexing keywords."""
    lexer = RappelLexer("fn python for in if else return input output")
    tokens = lexer.tokenize()

    expected = [
        TokenType.FN,
        TokenType.PYTHON,
        TokenType.FOR,
        TokenType.IN,
        TokenType.IF,
        TokenType.ELSE,
        TokenType.RETURN,
        TokenType.INPUT,
        TokenType.OUTPUT,
    ]

    for i, expected_type in enumerate(expected):
        assert tokens[i].type == expected_type


def test_lexer_operators():
    """Test lexing operators."""
    lexer = RappelLexer("+ - * / = == != < > <= >= -> ...")
    tokens = lexer.tokenize()

    expected = [
        TokenType.PLUS,
        TokenType.MINUS,
        TokenType.STAR,
        TokenType.SLASH,
        TokenType.ASSIGN,
        TokenType.EQ,
        TokenType.NEQ,
        TokenType.LT,
        TokenType.GT,
        TokenType.LTE,
        TokenType.GTE,
        TokenType.ARROW,
        TokenType.SPREAD,
    ]

    for i, expected_type in enumerate(expected):
        assert tokens[i].type == expected_type, f"Token {i}: expected {expected_type}, got {tokens[i].type}"


def test_lexer_boolean():
    """Test lexing boolean values."""
    lexer = RappelLexer("true false True False")
    tokens = lexer.tokenize()

    assert tokens[0].type == TokenType.BOOLEAN
    assert tokens[0].value is True
    assert tokens[1].type == TokenType.BOOLEAN
    assert tokens[1].value is False
    assert tokens[2].type == TokenType.BOOLEAN
    assert tokens[2].value is True
    assert tokens[3].type == TokenType.BOOLEAN
    assert tokens[3].value is False


def test_lexer_indentation():
    """Test lexing indentation."""
    source = """if x:
    y = 1
    z = 2
w = 3"""
    lexer = RappelLexer(source)
    tokens = lexer.tokenize()

    # Find INDENT and DEDENT tokens
    indent_count = sum(1 for t in tokens if t.type == TokenType.INDENT)
    dedent_count = sum(1 for t in tokens if t.type == TokenType.DEDENT)

    assert indent_count == 1
    assert dedent_count == 1


def test_lexer_comments():
    """Test that comments are skipped."""
    source = """x = 1  # this is a comment
# full line comment
y = 2"""
    lexer = RappelLexer(source)
    tokens = lexer.tokenize()

    # Should have x, =, 1, newline, y, =, 2, eof
    identifiers = [t for t in tokens if t.type == TokenType.IDENTIFIER]
    assert len(identifiers) == 2
    assert identifiers[0].value == "x"
    assert identifiers[1].value == "y"


def test_lexer_function_def():
    """Test lexing a function definition."""
    source = """fn add(input: [a, b], output: [result]):
    result = a + b
    return result"""
    lexer = RappelLexer(source)
    tokens = lexer.tokenize()

    assert tokens[0].type == TokenType.FN
    assert tokens[1].type == TokenType.IDENTIFIER
    assert tokens[1].value == "add"


def test_lexer_for_loop():
    """Test lexing a for loop."""
    source = "for item in items -> process(input: [item], output: [result]):"
    lexer = RappelLexer(source)
    tokens = lexer.tokenize()

    assert tokens[0].type == TokenType.FOR
    assert tokens[1].type == TokenType.IDENTIFIER
    assert tokens[1].value == "item"
    assert tokens[2].type == TokenType.IN
    assert tokens[3].type == TokenType.IDENTIFIER
    assert tokens[3].value == "items"
    assert tokens[4].type == TokenType.ARROW


def test_lexer_list_concat():
    """Test lexing list concatenation."""
    source = "my_list = my_list + [new_item]"
    lexer = RappelLexer(source)
    tokens = lexer.tokenize()

    assert tokens[0].type == TokenType.IDENTIFIER
    assert tokens[0].value == "my_list"
    assert tokens[1].type == TokenType.ASSIGN
    assert tokens[2].type == TokenType.IDENTIFIER
    assert tokens[3].type == TokenType.PLUS
    assert tokens[4].type == TokenType.LBRACKET


def test_lexer_dict_concat():
    """Test lexing dict concatenation."""
    source = 'my_dict = my_dict + {"key": "new"}'
    lexer = RappelLexer(source)
    tokens = lexer.tokenize()

    assert tokens[0].type == TokenType.IDENTIFIER
    assert tokens[3].type == TokenType.PLUS
    assert tokens[4].type == TokenType.LBRACE


def test_lexer_spread_operator():
    """Test lexing spread operator."""
    source = "call(...args)"
    lexer = RappelLexer(source)
    tokens = lexer.tokenize()

    assert tokens[2].type == TokenType.SPREAD
    assert tokens[3].type == TokenType.IDENTIFIER
    assert tokens[3].value == "args"


def test_lexer_index_access():
    """Test lexing index access."""
    source = 'my_list[0] + my_dict["key"]'
    lexer = RappelLexer(source)
    tokens = lexer.tokenize()

    # my_list [ 0 ] + my_dict [ "key" ]
    assert tokens[0].type == TokenType.IDENTIFIER
    assert tokens[1].type == TokenType.LBRACKET
    assert tokens[2].type == TokenType.NUMBER
    assert tokens[3].type == TokenType.RBRACKET


def test_lexer_python_block():
    """Test lexing python block."""
    source = """python(input: [x, y], output: [z]):
    z = x + y"""
    lexer = RappelLexer(source)
    tokens = lexer.tokenize()

    assert tokens[0].type == TokenType.PYTHON
    assert tokens[1].type == TokenType.LPAREN
    assert tokens[2].type == TokenType.INPUT


def test_lexer_action_call():
    """Test lexing action call with @ syntax."""
    source = "@fetch_url(url=my_url)"
    lexer = RappelLexer(source)
    tokens = lexer.tokenize()

    assert tokens[0].type == TokenType.AT
    assert tokens[1].type == TokenType.IDENTIFIER
    assert tokens[1].value == "fetch_url"
    assert tokens[2].type == TokenType.LPAREN


# =============================================================================
# CHECKPOINT 3: Parser Implementation
# =============================================================================


class RappelParser:
    """
    Parser for the Rappel language.

    Converts tokens into an AST (IR nodes).

    Enforces:
    - No nested function definitions (fn inside fn)
    - No nested python blocks inside fn (python blocks at top level only)
    - For loop body functions are allowed (they're the loop mechanism)
    - All function calls must use kwargs (no positional args)
    - Actions are called with @action_name(kwargs) syntax
    """

    def __init__(self, tokens: list[Token]):
        self.tokens = tokens
        self.pos = 0
        self._in_function = False  # Track if we're inside fn

    def parse(self) -> RappelProgram:
        """Parse a complete program."""
        statements = []

        while not self._is_at_end():
            # Skip newlines at top level
            while self._check(TokenType.NEWLINE):
                self._advance()
                if self._is_at_end():
                    break

            if self._is_at_end():
                break

            stmt = self._parse_statement()
            if stmt:
                statements.append(stmt)

        return RappelProgram(statements=tuple(statements))

    def _parse_statement(self) -> RappelStatement | None:
        """Parse a single statement."""
        if self._check(TokenType.FN):
            if self._in_function:
                raise SyntaxError(
                    f"Nested function definitions are not allowed. "
                    f"Define all functions at the top level. "
                    f"Line {self._peek().line}"
                )
            return self._parse_function_def()

        if self._check(TokenType.PYTHON):
            if self._in_function:
                raise SyntaxError(
                    f"Python blocks are not allowed inside functions. "
                    f"Define python blocks at the top level or use regular assignments. "
                    f"Line {self._peek().line}"
                )
            return self._parse_python_block()

        if self._check(TokenType.FOR):
            return self._parse_for_loop()

        if self._check(TokenType.IF):
            return self._parse_if_statement()

        if self._check(TokenType.RETURN):
            return self._parse_return()

        # Assignment or expression statement
        return self._parse_assignment_or_expr()

    def _parse_function_def(self) -> RappelFunctionDef:
        """Parse a function definition."""
        loc = self._location()
        self._consume(TokenType.FN, "Expected 'fn'")
        name = self._consume(TokenType.IDENTIFIER, "Expected function name").value

        self._consume(TokenType.LPAREN, "Expected '(' after function name")
        inputs, outputs = self._parse_io_spec()
        self._consume(TokenType.RPAREN, "Expected ')' after parameters")

        self._consume(TokenType.COLON, "Expected ':' after function signature")
        self._consume_newline()

        # Set flag to prevent nested definitions
        prev_in_fn = self._in_function
        self._in_function = True
        try:
            body = self._parse_block()
        finally:
            self._in_function = prev_in_fn

        return RappelFunctionDef(
            name=name,
            inputs=tuple(inputs),
            outputs=tuple(outputs),
            body=tuple(body),
            location=loc,
        )

    def _parse_python_block(self) -> RappelPythonBlock:
        """Parse a python block with explicit I/O."""
        loc = self._location()
        self._consume(TokenType.PYTHON, "Expected 'python'")

        self._consume(TokenType.LPAREN, "Expected '(' after 'python'")
        inputs, outputs = self._parse_io_spec()
        self._consume(TokenType.RPAREN, "Expected ')' after python parameters")

        self._consume(TokenType.COLON, "Expected ':' after python signature")
        self._consume_newline()

        # For python blocks, we capture the raw code
        code_lines = self._parse_raw_block()
        code = "\n".join(code_lines)

        return RappelPythonBlock(
            code=code,
            inputs=tuple(inputs),
            outputs=tuple(outputs),
            location=loc,
        )

    def _parse_for_loop(self) -> RappelForLoop:
        """Parse a for loop with function body."""
        loc = self._location()
        self._consume(TokenType.FOR, "Expected 'for'")
        loop_var = self._consume(
            TokenType.IDENTIFIER, "Expected loop variable"
        ).value
        self._consume(TokenType.IN, "Expected 'in'")
        iterable = self._parse_expression()

        self._consume(TokenType.ARROW, "Expected '->' before loop body function")

        # Parse the inline function definition
        fn_name = self._consume(
            TokenType.IDENTIFIER, "Expected function name"
        ).value
        self._consume(TokenType.LPAREN, "Expected '(' after function name")
        inputs, outputs = self._parse_io_spec()
        self._consume(TokenType.RPAREN, "Expected ')' after parameters")

        self._consume(TokenType.COLON, "Expected ':' after function signature")
        self._consume_newline()

        body = self._parse_block()

        body_fn = RappelFunctionDef(
            name=fn_name,
            inputs=tuple(inputs),
            outputs=tuple(outputs),
            body=tuple(body),
            location=loc,
        )

        return RappelForLoop(
            loop_var=loop_var,
            iterable=iterable,
            body_fn=body_fn,
            location=loc,
        )

    def _parse_if_statement(self) -> RappelIfStatement:
        """Parse an if statement."""
        loc = self._location()
        self._consume(TokenType.IF, "Expected 'if'")
        condition = self._parse_expression()

        self._consume(TokenType.COLON, "Expected ':' after condition")
        self._consume_newline()

        then_body = self._parse_block()

        else_body = None
        if self._check(TokenType.ELSE):
            self._advance()
            self._consume(TokenType.COLON, "Expected ':' after 'else'")
            self._consume_newline()
            else_body = self._parse_block()

        return RappelIfStatement(
            condition=condition,
            then_body=tuple(then_body),
            else_body=tuple(else_body) if else_body else None,
            location=loc,
        )

    def _parse_return(self) -> RappelReturn:
        """Parse a return statement."""
        loc = self._location()
        self._consume(TokenType.RETURN, "Expected 'return'")

        values = []
        if not self._check(TokenType.NEWLINE) and not self._is_at_end():
            # Check for list return [a, b, c]
            if self._check(TokenType.LBRACKET):
                self._advance()
                while not self._check(TokenType.RBRACKET):
                    values.append(self._parse_expression())
                    if self._check(TokenType.COMMA):
                        self._advance()
                self._consume(TokenType.RBRACKET, "Expected ']'")
            else:
                values.append(self._parse_expression())

        return RappelReturn(values=tuple(values), location=loc)

    def _parse_assignment_or_expr(self) -> RappelStatement:
        """Parse an assignment or expression statement."""
        loc = self._location()

        # Check for multi-assignment: a, b, c = ...
        if self._check(TokenType.IDENTIFIER):
            first_id = self._advance()
            targets = [first_id.value]

            while self._check(TokenType.COMMA):
                self._advance()
                if self._check(TokenType.IDENTIFIER):
                    targets.append(self._advance().value)
                else:
                    # Not a multi-assignment, backtrack
                    self.pos -= len(targets) * 2 - 1
                    break

            if self._check(TokenType.ASSIGN):
                self._advance()
                value = self._parse_expression()

                if len(targets) > 1:
                    return RappelMultiAssignment(
                        targets=tuple(targets), value=value, location=loc
                    )
                else:
                    return RappelAssignment(
                        target=targets[0], value=value, location=loc
                    )
            else:
                # Not an assignment, backtrack and parse as expression
                self.pos -= len(targets) * 2 - 1

        # Expression statement
        expr = self._parse_expression()
        return RappelExprStatement(expr=expr, location=loc)

    def _parse_expression(self) -> RappelExpr:
        """Parse an expression."""
        return self._parse_or()

    def _parse_or(self) -> RappelExpr:
        """Parse or expressions."""
        left = self._parse_and()

        while self._check(TokenType.OR):
            op = self._advance().value
            right = self._parse_and()
            left = RappelBinaryOp(op=op, left=left, right=right)

        return left

    def _parse_and(self) -> RappelExpr:
        """Parse and expressions."""
        left = self._parse_not()

        while self._check(TokenType.AND):
            op = self._advance().value
            right = self._parse_not()
            left = RappelBinaryOp(op=op, left=left, right=right)

        return left

    def _parse_not(self) -> RappelExpr:
        """Parse not expressions."""
        if self._check(TokenType.NOT):
            op = self._advance().value
            operand = self._parse_not()
            return RappelUnaryOp(op=op, operand=operand)

        return self._parse_comparison()

    def _parse_comparison(self) -> RappelExpr:
        """Parse comparison expressions."""
        left = self._parse_additive()

        while self._check_any(
            TokenType.EQ,
            TokenType.NEQ,
            TokenType.LT,
            TokenType.GT,
            TokenType.LTE,
            TokenType.GTE,
        ):
            op = self._advance().value
            right = self._parse_additive()
            left = RappelBinaryOp(op=op, left=left, right=right)

        return left

    def _parse_additive(self) -> RappelExpr:
        """Parse additive expressions (+, -)."""
        left = self._parse_multiplicative()

        while self._check_any(TokenType.PLUS, TokenType.MINUS):
            op = self._advance().value
            right = self._parse_multiplicative()
            left = RappelBinaryOp(op=op, left=left, right=right)

        return left

    def _parse_multiplicative(self) -> RappelExpr:
        """Parse multiplicative expressions (*, /)."""
        left = self._parse_unary()

        while self._check_any(TokenType.STAR, TokenType.SLASH):
            op = self._advance().value
            right = self._parse_unary()
            left = RappelBinaryOp(op=op, left=left, right=right)

        return left

    def _parse_unary(self) -> RappelExpr:
        """Parse unary expressions."""
        if self._check(TokenType.MINUS):
            op = self._advance().value
            operand = self._parse_unary()
            return RappelUnaryOp(op=op, operand=operand)

        if self._check(TokenType.SPREAD):
            self._advance()
            operand = self._parse_postfix()
            return RappelSpread(target=operand)

        return self._parse_postfix()

    def _parse_postfix(self) -> RappelExpr:
        """Parse postfix expressions (calls, index access, dot access)."""
        expr = self._parse_primary()

        while True:
            if self._check(TokenType.LBRACKET):
                self._advance()
                index = self._parse_expression()
                self._consume(TokenType.RBRACKET, "Expected ']'")
                expr = RappelIndexAccess(target=expr, index=index)
            elif self._check(TokenType.DOT):
                self._advance()
                field = self._consume(
                    TokenType.IDENTIFIER, "Expected field name"
                ).value
                expr = RappelDotAccess(target=expr, field=field)
            elif self._check(TokenType.LPAREN) and isinstance(
                expr, RappelVariable
            ):
                # Function call - kwargs only
                loc = self._location()
                self._advance()
                kwargs = self._parse_kwargs_only()
                self._consume(TokenType.RPAREN, "Expected ')'")
                expr = RappelCall(
                    target=expr.name,
                    kwargs=tuple(kwargs),
                    location=loc,
                )
            else:
                break

        return expr

    def _parse_primary(self) -> RappelExpr:
        """Parse primary expressions."""
        loc = self._location()

        # Action call: @action_name(kwargs)
        if self._check(TokenType.AT):
            self._advance()
            action_name = self._consume(
                TokenType.IDENTIFIER, "Expected action name after '@'"
            ).value
            self._consume(TokenType.LPAREN, "Expected '(' after action name")
            kwargs = self._parse_kwargs_only()
            self._consume(TokenType.RPAREN, "Expected ')'")
            return RappelActionCall(
                action_name=action_name,
                kwargs=tuple(kwargs),
                location=loc,
            )

        # Parenthesized expression
        if self._check(TokenType.LPAREN):
            self._advance()
            expr = self._parse_expression()
            self._consume(TokenType.RPAREN, "Expected ')'")
            return expr

        # List literal
        if self._check(TokenType.LBRACKET):
            return self._parse_list_literal()

        # Dict literal
        if self._check(TokenType.LBRACE):
            return self._parse_dict_literal()

        # String literal
        if self._check(TokenType.STRING):
            value = self._advance().value
            return RappelLiteral(RappelString(value), location=loc)

        # Number literal
        if self._check(TokenType.NUMBER):
            value = self._advance().value
            return RappelLiteral(RappelNumber(value), location=loc)

        # Boolean literal
        if self._check(TokenType.BOOLEAN):
            value = self._advance().value
            return RappelLiteral(RappelBoolean(value), location=loc)

        # Identifier
        if self._check(TokenType.IDENTIFIER):
            name = self._advance().value
            return RappelVariable(name=name, location=loc)

        raise SyntaxError(
            f"Unexpected token {self._peek()} at line {self._peek().line}"
        )

    def _parse_list_literal(self) -> RappelListExpr:
        """Parse a list literal [a, b, c]."""
        loc = self._location()
        self._consume(TokenType.LBRACKET, "Expected '['")

        items = []
        while not self._check(TokenType.RBRACKET):
            items.append(self._parse_expression())
            if self._check(TokenType.COMMA):
                self._advance()

        self._consume(TokenType.RBRACKET, "Expected ']'")
        return RappelListExpr(items=tuple(items), location=loc)

    def _parse_dict_literal(self) -> RappelDictExpr:
        """Parse a dict literal {"key": value}."""
        loc = self._location()
        self._consume(TokenType.LBRACE, "Expected '{'")

        pairs = []
        while not self._check(TokenType.RBRACE):
            key = self._parse_expression()
            self._consume(TokenType.COLON, "Expected ':' after dict key")
            value = self._parse_expression()
            pairs.append((key, value))

            if self._check(TokenType.COMMA):
                self._advance()

        self._consume(TokenType.RBRACE, "Expected '}'")
        return RappelDictExpr(pairs=tuple(pairs), location=loc)

    def _parse_kwargs_only(self) -> list[tuple[str, RappelExpr]]:
        """Parse kwargs-only function/action call arguments."""
        kwargs = []

        while not self._check(TokenType.RPAREN):
            # Must be keyword argument: name=value or name: value
            if not self._check(TokenType.IDENTIFIER):
                raise SyntaxError(
                    f"Expected keyword argument (name=value), got {self._peek().type.name} "
                    f"at line {self._peek().line}. All function/action calls require kwargs."
                )

            name = self._advance().value

            # Accept both = and : for kwargs
            if self._check(TokenType.ASSIGN):
                self._advance()
            elif self._check(TokenType.COLON):
                self._advance()
            else:
                raise SyntaxError(
                    f"Expected '=' or ':' after argument name '{name}' "
                    f"at line {self._peek().line}. All function/action calls require kwargs."
                )

            value = self._parse_expression()
            kwargs.append((name, value))

            if self._check(TokenType.COMMA):
                self._advance()

        return kwargs

    def _parse_io_spec(self) -> tuple[list[str], list[str]]:
        """Parse input/output specification."""
        inputs = []
        outputs = []

        while not self._check(TokenType.RPAREN):
            if self._check(TokenType.INPUT):
                self._advance()
                self._consume(TokenType.COLON, "Expected ':' after 'input'")
                inputs = self._parse_identifier_list()
            elif self._check(TokenType.OUTPUT):
                self._advance()
                self._consume(TokenType.COLON, "Expected ':' after 'output'")
                outputs = self._parse_identifier_list()
            elif self._check(TokenType.COMMA):
                self._advance()
            else:
                break

        return inputs, outputs

    def _parse_identifier_list(self) -> list[str]:
        """Parse a list of identifiers [a, b, c]."""
        self._consume(TokenType.LBRACKET, "Expected '['")

        names = []
        while not self._check(TokenType.RBRACKET):
            names.append(
                self._consume(TokenType.IDENTIFIER, "Expected identifier").value
            )
            if self._check(TokenType.COMMA):
                self._advance()

        self._consume(TokenType.RBRACKET, "Expected ']'")
        return names

    def _parse_block(self) -> list[RappelStatement]:
        """Parse an indented block of statements."""
        statements = []

        self._consume(TokenType.INDENT, "Expected indented block")

        while not self._check(TokenType.DEDENT) and not self._is_at_end():
            # Skip newlines
            while self._check(TokenType.NEWLINE):
                self._advance()
                if self._check(TokenType.DEDENT) or self._is_at_end():
                    break

            if self._check(TokenType.DEDENT) or self._is_at_end():
                break

            stmt = self._parse_statement()
            if stmt:
                statements.append(stmt)

        if self._check(TokenType.DEDENT):
            self._advance()

        return statements

    def _parse_raw_block(self) -> list[str]:
        """Parse raw code block for python blocks."""
        lines = []

        self._consume(TokenType.INDENT, "Expected indented block")

        # For raw blocks, we need to collect all tokens until DEDENT
        # and reconstruct the code
        current_line = ""

        while not self._check(TokenType.DEDENT) and not self._is_at_end():
            token = self._advance()

            if token.type == TokenType.NEWLINE:
                if current_line.strip():
                    lines.append(current_line)
                current_line = ""
            elif token.type == TokenType.INDENT:
                current_line += "    "
            elif token.type == TokenType.DEDENT:
                self.pos -= 1  # Put it back
                break
            else:
                # Reconstruct token value
                if token.type == TokenType.STRING:
                    current_line += f'"{token.value}"'
                elif token.type == TokenType.IDENTIFIER:
                    if current_line and current_line[-1] not in " \t([{,.:":
                        current_line += " "
                    current_line += token.value
                elif token.type in (TokenType.NUMBER, TokenType.BOOLEAN):
                    if current_line and current_line[-1] not in " \t([{,.:":
                        current_line += " "
                    current_line += str(token.value)
                else:
                    current_line += token.value if token.value else ""

        if current_line.strip():
            lines.append(current_line)

        if self._check(TokenType.DEDENT):
            self._advance()

        return lines

    def _consume(self, token_type: TokenType, message: str) -> Token:
        """Consume a token of the expected type."""
        if self._check(token_type):
            return self._advance()
        raise SyntaxError(
            f"{message}. Got {self._peek().type.name} at line {self._peek().line}"
        )

    def _consume_newline(self) -> None:
        """Consume a newline token if present."""
        if self._check(TokenType.NEWLINE):
            self._advance()

    def _check(self, token_type: TokenType) -> bool:
        """Check if current token is of given type."""
        if self._is_at_end():
            return token_type == TokenType.EOF
        return self._peek().type == token_type

    def _check_any(self, *token_types: TokenType) -> bool:
        """Check if current token is any of the given types."""
        return any(self._check(t) for t in token_types)

    def _advance(self) -> Token:
        """Advance to the next token."""
        if not self._is_at_end():
            self.pos += 1
        return self.tokens[self.pos - 1]

    def _peek(self) -> Token:
        """Peek at the current token."""
        return self.tokens[self.pos]

    def _peek_next(self) -> Token | None:
        """Peek at the next token."""
        if self.pos + 1 < len(self.tokens):
            return self.tokens[self.pos + 1]
        return None

    def _is_at_end(self) -> bool:
        """Check if we've reached the end."""
        return self._peek().type == TokenType.EOF

    def _location(self) -> SourceLocation:
        """Get current source location."""
        token = self._peek()
        return SourceLocation(line=token.line, column=token.column)


def parse(source: str) -> RappelProgram:
    """Parse source code into a Rappel program."""
    lexer = RappelLexer(source)
    tokens = lexer.tokenize()
    parser = RappelParser(tokens)
    return parser.parse()


# =============================================================================
# CHECKPOINT 3 TESTS: Parser
# =============================================================================


def test_parser_simple_assignment():
    """Test parsing simple assignment."""
    program = parse("x = 42")

    assert len(program.statements) == 1
    stmt = program.statements[0]
    assert isinstance(stmt, RappelAssignment)
    assert stmt.target == "x"
    assert isinstance(stmt.value, RappelLiteral)
    assert stmt.value.value.value == 42


def test_parser_string_assignment():
    """Test parsing string assignment."""
    program = parse('message = "hello world"')

    stmt = program.statements[0]
    assert isinstance(stmt, RappelAssignment)
    assert isinstance(stmt.value, RappelLiteral)
    assert stmt.value.value.value == "hello world"


def test_parser_list_literal():
    """Test parsing list literal."""
    program = parse("items = [1, 2, 3]")

    stmt = program.statements[0]
    assert isinstance(stmt, RappelAssignment)
    assert isinstance(stmt.value, RappelListExpr)
    assert len(stmt.value.items) == 3


def test_parser_dict_literal():
    """Test parsing dict literal."""
    program = parse('config = {"name": "test", "count": 42}')

    stmt = program.statements[0]
    assert isinstance(stmt, RappelAssignment)
    assert isinstance(stmt.value, RappelDictExpr)
    assert len(stmt.value.pairs) == 2


def test_parser_list_concat():
    """Test parsing list concatenation (immutable update)."""
    program = parse("my_list = my_list + [new_item]")

    stmt = program.statements[0]
    assert isinstance(stmt, RappelAssignment)
    assert isinstance(stmt.value, RappelBinaryOp)
    assert stmt.value.op == "+"
    assert isinstance(stmt.value.left, RappelVariable)
    assert isinstance(stmt.value.right, RappelListExpr)


def test_parser_dict_concat():
    """Test parsing dict concatenation (immutable update)."""
    program = parse('my_dict = my_dict + {"key": "new"}')

    stmt = program.statements[0]
    assert isinstance(stmt, RappelAssignment)
    assert isinstance(stmt.value, RappelBinaryOp)
    assert stmt.value.op == "+"


def test_parser_index_access():
    """Test parsing index access."""
    program = parse("x = my_list[0]")

    stmt = program.statements[0]
    assert isinstance(stmt, RappelAssignment)
    assert isinstance(stmt.value, RappelIndexAccess)
    assert isinstance(stmt.value.target, RappelVariable)
    assert stmt.value.target.name == "my_list"


def test_parser_dict_key_access():
    """Test parsing dict key access."""
    program = parse('x = my_dict["key"]')

    stmt = program.statements[0]
    assert isinstance(stmt, RappelAssignment)
    assert isinstance(stmt.value, RappelIndexAccess)


def test_parser_dot_access():
    """Test parsing dot access."""
    program = parse("x = obj.field")

    stmt = program.statements[0]
    assert isinstance(stmt, RappelAssignment)
    assert isinstance(stmt.value, RappelDotAccess)
    assert stmt.value.field == "field"


def test_parser_function_call():
    """Test parsing function call - must use kwargs."""
    program = parse("result = process(a=x, b=y, c=z)")

    stmt = program.statements[0]
    assert isinstance(stmt, RappelAssignment)
    assert isinstance(stmt.value, RappelCall)
    assert stmt.value.target == "process"
    assert len(stmt.value.kwargs) == 3


def test_parser_function_call_colon_syntax():
    """Test parsing function call with colon syntax for kwargs."""
    program = parse("result = fetch(url: my_url, timeout: 30)")

    stmt = program.statements[0]
    assert isinstance(stmt, RappelAssignment)
    assert isinstance(stmt.value, RappelCall)
    assert len(stmt.value.kwargs) == 2


def test_parser_function_call_rejects_positional():
    """Test that positional args are rejected."""
    import pytest
    with pytest.raises(SyntaxError) as exc_info:
        parse("result = process(a, b, c)")
    assert "kwargs" in str(exc_info.value).lower()


def test_parser_spread_operator():
    """Test parsing spread operator in list."""
    program = parse("result = [...items, extra]")

    stmt = program.statements[0]
    assert isinstance(stmt, RappelAssignment)
    assert isinstance(stmt.value, RappelListExpr)
    # The spread is in the items
    assert len(stmt.value.items) == 2
    assert isinstance(stmt.value.items[0], RappelSpread)


def test_parser_multi_assignment():
    """Test parsing multi-assignment (unpacking)."""
    program = parse("a, b, c = get_values()")

    stmt = program.statements[0]
    assert isinstance(stmt, RappelMultiAssignment)
    assert stmt.targets == ("a", "b", "c")


def test_parser_function_def():
    """Test parsing function definition."""
    source = """fn add(input: [a, b], output: [result]):
    result = a + b
    return result"""

    program = parse(source)

    stmt = program.statements[0]
    assert isinstance(stmt, RappelFunctionDef)
    assert stmt.name == "add"
    assert stmt.inputs == ("a", "b")
    assert stmt.outputs == ("result",)
    assert len(stmt.body) == 2


def test_parser_action_call():
    """Test parsing action call with @ syntax."""
    source = 'result = @fetch_data(url="https://example.com")'

    program = parse(source)

    stmt = program.statements[0]
    assert isinstance(stmt, RappelAssignment)
    assert isinstance(stmt.value, RappelActionCall)
    assert stmt.value.action_name == "fetch_data"
    assert len(stmt.value.kwargs) == 1
    assert stmt.value.kwargs[0][0] == "url"


def test_parser_python_block():
    """Test parsing python block."""
    source = """python(input: [x, y], output: [z]):
    z = x + y"""

    program = parse(source)

    stmt = program.statements[0]
    assert isinstance(stmt, RappelPythonBlock)
    assert stmt.inputs == ("x", "y")
    assert stmt.outputs == ("z",)
    assert "z" in stmt.code and "x" in stmt.code


def test_parser_for_loop():
    """Test parsing for loop with function body."""
    source = """for item in items -> process_item(input: [item], output: [result]):
    result = transform(x=item)"""

    program = parse(source)

    stmt = program.statements[0]
    assert isinstance(stmt, RappelForLoop)
    assert stmt.loop_var == "item"
    assert isinstance(stmt.iterable, RappelVariable)
    assert stmt.iterable.name == "items"
    assert stmt.body_fn.name == "process_item"
    assert stmt.body_fn.inputs == ("item",)
    assert stmt.body_fn.outputs == ("result",)


def test_parser_if_statement():
    """Test parsing if statement."""
    source = """if x > 0:
    result = "positive"
else:
    result = "non-positive" """

    program = parse(source)

    stmt = program.statements[0]
    assert isinstance(stmt, RappelIfStatement)
    assert isinstance(stmt.condition, RappelBinaryOp)
    assert stmt.condition.op == ">"
    assert len(stmt.then_body) == 1
    assert stmt.else_body is not None
    assert len(stmt.else_body) == 1


def test_parser_return_single():
    """Test parsing single return."""
    source = """fn get_value(input: [], output: [x]):
    x = 42
    return x"""

    program = parse(source)
    fn = program.statements[0]
    assert isinstance(fn, RappelFunctionDef)

    ret = fn.body[-1]
    assert isinstance(ret, RappelReturn)
    assert len(ret.values) == 1


def test_parser_return_multiple():
    """Test parsing multiple return values."""
    source = """fn get_values(input: [], output: [a, b, c]):
    a = 1
    b = 2
    c = 3
    return [a, b, c]"""

    program = parse(source)
    fn = program.statements[0]

    ret = fn.body[-1]
    assert isinstance(ret, RappelReturn)
    assert len(ret.values) == 3


def test_parser_comparison_operators():
    """Test parsing comparison operators."""
    program = parse("result = a == b and c != d or e < f")

    stmt = program.statements[0]
    assert isinstance(stmt, RappelAssignment)
    # Should parse as: (a == b) and (c != d) or (e < f)
    # With precedence: ((a == b) and (c != d)) or (e < f)
    assert isinstance(stmt.value, RappelBinaryOp)


def test_parser_arithmetic():
    """Test parsing arithmetic expressions."""
    program = parse("result = a + b * c - d / e")

    stmt = program.statements[0]
    assert isinstance(stmt, RappelAssignment)
    # Should respect precedence: a + (b * c) - (d / e)
    assert isinstance(stmt.value, RappelBinaryOp)


def test_parser_unary_not():
    """Test parsing not operator."""
    program = parse("result = not flag")

    stmt = program.statements[0]
    assert isinstance(stmt, RappelAssignment)
    assert isinstance(stmt.value, RappelUnaryOp)
    assert stmt.value.op == "not"


def test_parser_unary_minus():
    """Test parsing unary minus."""
    program = parse("result = -x")

    stmt = program.statements[0]
    assert isinstance(stmt, RappelAssignment)
    assert isinstance(stmt.value, RappelUnaryOp)
    assert stmt.value.op == "-"


def test_parser_parenthesized():
    """Test parsing parenthesized expressions."""
    program = parse("result = (a + b) * c")

    stmt = program.statements[0]
    assert isinstance(stmt, RappelAssignment)
    assert isinstance(stmt.value, RappelBinaryOp)
    assert stmt.value.op == "*"
    assert isinstance(stmt.value.left, RappelBinaryOp)
    assert stmt.value.left.op == "+"


def test_parser_rejects_nested_function():
    """Test that nested function definitions are rejected."""
    source = """fn outer(input: [x], output: [y]):
    fn inner(input: [a], output: [b]):
        b = a * 2
    y = inner(x)"""

    import pytest
    with pytest.raises(SyntaxError) as exc_info:
        parse(source)
    assert "Nested function definitions are not allowed" in str(exc_info.value)


def test_parser_rejects_python_block_in_function():
    """Test that python blocks inside functions are rejected."""
    source = """fn process(input: [x], output: [y]):
    python(input: [x], output: [y]):
        y = x * 2"""

    import pytest
    with pytest.raises(SyntaxError) as exc_info:
        parse(source)
    assert "Python blocks are not allowed inside functions" in str(exc_info.value)


def test_parser_action_call_in_function():
    """Test that action calls are allowed inside functions."""
    source = """fn process(input: [x], output: [y]):
    y = @fetch_data(url=x)
    return y"""

    # Should not raise - action calls are allowed anywhere
    program = parse(source)
    assert len(program.statements) == 1
    fn = program.statements[0]
    assert isinstance(fn, RappelFunctionDef)


def test_parser_allows_for_loop_in_function():
    """Test that for loops (with their body functions) are allowed inside functions."""
    source = """fn process_all(input: [items], output: [results]):
    results = []
    for item in items -> handle(input: [item], output: [result]):
        result = item * 2
    return results"""

    # Should not raise
    program = parse(source)
    assert len(program.statements) == 1
    fn = program.statements[0]
    assert isinstance(fn, RappelFunctionDef)
    assert fn.name == "process_all"


def test_parser_allows_if_in_function():
    """Test that if statements are allowed inside functions."""
    source = """fn classify(input: [x], output: [label]):
    if x > 0:
        label = "positive"
    else:
        label = "non-positive"
    return label"""

    # Should not raise
    program = parse(source)
    assert len(program.statements) == 1


# =============================================================================
# CHECKPOINT 4: Comprehensive Examples
# =============================================================================


EXAMPLE_IMMUTABLE_VARS = """
# Immutable variable assignment
x = 42
name = "Alice"
is_active = true

# Variables must be reassigned to update
x = x + 1
name = name + " Smith"
"""

EXAMPLE_LIST_OPERATIONS = """
# List initialization
items = []
items = [1, 2, 3]

# Immutable list update (concatenation)
items = items + [4]
items = items + [5, 6]

# List access
first = items[0]
last = items[5]

# List from function
results = []
for item in items -> process(input: [item], output: [result]):
    result = item * 2
results = results + [result]
"""

EXAMPLE_DICT_OPERATIONS = """
# Dict initialization
config = {}
config = {"host": "localhost", "port": 8080}

# Immutable dict update
config = config + {"timeout": 30}
config = config + {"retries": 3, "debug": true}

# Dict access
host = config["host"]
port = config["port"]
"""

EXAMPLE_PYTHON_BLOCK = """
# Python block with explicit I/O
python(input: [x, y], output: [z]):
    z = x + y

# Python block for complex computation
python(input: [data], output: [mean, std]):
    import statistics
    mean = statistics.mean(data)
    std = statistics.stdev(data)
"""

EXAMPLE_FUNCTION_DEF = """
# Function with explicit input/output
fn calculate_area(input: [width, height], output: [area]):
    area = width * height
    return area

# Function with multiple outputs - returns list for unpacking
fn divide_with_remainder(input: [a, b], output: [quotient, remainder]):
    quotient = a / b
    remainder = a - quotient * b
    return [quotient, remainder]

# Simple transformation function
fn double_value(input: [x], output: [result]):
    result = x * 2
    return result

# Using functions - all calls require kwargs
q, r = divide_with_remainder(a=10, b=3)
doubled = double_value(x=5)
area = calculate_area(width=10, height=20)
"""

EXAMPLE_ACTION_CALL = """
# Actions are external - called with @action_name(kwargs) syntax
# No action definitions in code - they are defined externally

# Call an action to fetch data
response = @fetch_url(url="https://api.example.com/data")

# Call an action to save to database
record_id = @save_to_db(data=response, table="responses")

# Chain action calls
user_data = @fetch_user(id=123)
validated = @validate_user(user=user_data)
result = @update_profile(user_id=123, data=validated)
"""

EXAMPLE_FOR_LOOP = """
# For loop with function body
items = [1, 2, 3, 4, 5]
results = []

for item in items -> double(input: [item], output: [doubled]):
    doubled = item * 2

# Collect results
results = results + [doubled]

# Nested data processing
users = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]

for user in users -> process_user(input: [user], output: [processed]):
    name = user["name"]
    age = user["age"]
    processed = {"name": name, "adult": age >= 18}
"""

EXAMPLE_SPREAD_OPERATOR = """
# Spread operator for unpacking in lists
base = [1, 2]
extended = [...base, 3, 4, 5]

# Spread with variables
items = [10, 20, 30]
all_items = [...items, 40, 50]
"""

EXAMPLE_CONDITIONALS = """
# If-else with explicit blocks
if value > 100:
    category = "high"
else:
    category = "low"

# Conditional in function
fn classify(input: [score], output: [grade]):
    if score >= 90:
        grade = "A"
    else:
        if score >= 80:
            grade = "B"
        else:
            if score >= 70:
                grade = "C"
            else:
                grade = "F"
    return grade
"""

EXAMPLE_COMPLEX_WORKFLOW = """
# Complex workflow example combining all features
# Functions defined at top level, actions called with @syntax

# Define validation function
fn validate_item(input: [item], output: [validated]):
    if item > 0:
        validated = item
    else:
        validated = None
    return validated

# Define item processing function
fn process_single_item(input: [item], output: [result, error]):
    validated = validate_item(item=item)
    if validated != None:
        result = validated * 2
        error = None
    else:
        result = None
        error = {"item": item, "reason": "invalid"}
    return [result, error]

# Define batch processing function
fn process_batch(input: [batch], output: [processed, failed]):
    processed = []
    failed = []
    for item in batch -> handle_item(input: [item], output: [result, error]):
        result, error = process_single_item(item=item)
    if result != None:
        processed = processed + [result]
    if error != None:
        failed = failed + [error]
    return [processed, failed]

# Main workflow - initialize state
config = {"api_url": "https://api.example.com", "batch_size": 10}
results = []
errors = []

# Execute workflow - actions called with @syntax
batch = @fetch_batch(url=config["api_url"], offset=0, limit=config["batch_size"])
processed, failed = process_batch(batch=batch)
results = results + processed
errors = errors + failed
"""


def test_example_immutable_vars():
    """Test parsing immutable variable examples."""
    program = parse(EXAMPLE_IMMUTABLE_VARS)
    assert len(program.statements) >= 5


def test_example_list_operations():
    """Test parsing list operation examples."""
    program = parse(EXAMPLE_LIST_OPERATIONS)
    # Should have list init, updates, access, for loop
    assert len(program.statements) >= 5


def test_example_dict_operations():
    """Test parsing dict operation examples."""
    program = parse(EXAMPLE_DICT_OPERATIONS)
    assert len(program.statements) >= 5


def test_example_python_block():
    """Test parsing python block examples."""
    program = parse(EXAMPLE_PYTHON_BLOCK)
    python_blocks = [
        s for s in program.statements if isinstance(s, RappelPythonBlock)
    ]
    assert len(python_blocks) == 2


def test_example_function_def():
    """Test parsing function definition examples."""
    program = parse(EXAMPLE_FUNCTION_DEF)
    functions = [s for s in program.statements if isinstance(s, RappelFunctionDef)]
    assert len(functions) >= 2


def test_example_action_call():
    """Test parsing action call examples."""
    program = parse(EXAMPLE_ACTION_CALL)
    # Find action calls
    action_calls = []
    for stmt in program.statements:
        if isinstance(stmt, RappelAssignment):
            if isinstance(stmt.value, RappelActionCall):
                action_calls.append(stmt.value)
    assert len(action_calls) >= 3


def test_example_for_loop():
    """Test parsing for loop examples."""
    program = parse(EXAMPLE_FOR_LOOP)
    for_loops = [s for s in program.statements if isinstance(s, RappelForLoop)]
    assert len(for_loops) >= 2


def test_example_spread_operator():
    """Test parsing spread operator examples."""
    program = parse(EXAMPLE_SPREAD_OPERATOR)
    # Find statements with spread in list expressions
    has_spread = False
    for stmt in program.statements:
        if isinstance(stmt, RappelAssignment):
            if isinstance(stmt.value, RappelListExpr):
                for item in stmt.value.items:
                    if isinstance(item, RappelSpread):
                        has_spread = True
    assert has_spread


def test_example_conditionals():
    """Test parsing conditional examples."""
    program = parse(EXAMPLE_CONDITIONALS)
    if_stmts = [s for s in program.statements if isinstance(s, RappelIfStatement)]
    assert len(if_stmts) >= 1


def test_example_complex_workflow():
    """Test parsing complex workflow example."""
    program = parse(EXAMPLE_COMPLEX_WORKFLOW)
    # Should have config, functions, and main workflow with action calls
    assert len(program.statements) >= 5

    # Check for variety of statement types
    types_found = set()
    for stmt in program.statements:
        types_found.add(type(stmt).__name__)

    assert "RappelAssignment" in types_found
    assert "RappelFunctionDef" in types_found

    # Check for action calls in assignments
    has_action_call = False
    for stmt in program.statements:
        if isinstance(stmt, RappelAssignment):
            if isinstance(stmt.value, RappelActionCall):
                has_action_call = True
    assert has_action_call, "Should have at least one action call"


# =============================================================================
# Pretty Printer for IR (useful for debugging)
# =============================================================================


class RappelPrettyPrinter:
    """Pretty printer for Rappel IR."""

    def __init__(self):
        self.indent = 0

    def print(self, node: RappelProgram | RappelStatement | RappelExpr) -> str:
        """Print a node as formatted string."""
        return self._print_node(node)

    def _print_node(self, node) -> str:
        """Print any node."""
        method_name = f"_print_{type(node).__name__}"
        method = getattr(self, method_name, self._print_generic)
        return method(node)

    def _print_generic(self, node) -> str:
        return f"<{type(node).__name__}>"

    def _print_RappelProgram(self, node: RappelProgram) -> str:
        lines = []
        for stmt in node.statements:
            lines.append(self._print_node(stmt))
        return "\n".join(lines)

    def _print_RappelAssignment(self, node: RappelAssignment) -> str:
        return f"{node.target} = {self._print_node(node.value)}"

    def _print_RappelMultiAssignment(self, node: RappelMultiAssignment) -> str:
        targets = ", ".join(node.targets)
        return f"{targets} = {self._print_node(node.value)}"

    def _print_RappelLiteral(self, node: RappelLiteral) -> str:
        if isinstance(node.value, RappelString):
            return f'"{node.value.value}"'
        return str(node.value.value)

    def _print_RappelVariable(self, node: RappelVariable) -> str:
        return node.name

    def _print_RappelBinaryOp(self, node: RappelBinaryOp) -> str:
        return f"({self._print_node(node.left)} {node.op} {self._print_node(node.right)})"

    def _print_RappelUnaryOp(self, node: RappelUnaryOp) -> str:
        return f"({node.op} {self._print_node(node.operand)})"

    def _print_RappelListExpr(self, node: RappelListExpr) -> str:
        items = ", ".join(self._print_node(item) for item in node.items)
        return f"[{items}]"

    def _print_RappelDictExpr(self, node: RappelDictExpr) -> str:
        pairs = ", ".join(
            f"{self._print_node(k)}: {self._print_node(v)}"
            for k, v in node.pairs
        )
        return "{" + pairs + "}"

    def _print_RappelIndexAccess(self, node: RappelIndexAccess) -> str:
        return f"{self._print_node(node.target)}[{self._print_node(node.index)}]"

    def _print_RappelDotAccess(self, node: RappelDotAccess) -> str:
        return f"{self._print_node(node.target)}.{node.field}"

    def _print_RappelSpread(self, node: RappelSpread) -> str:
        return f"...{self._print_node(node.target)}"

    def _print_RappelCall(self, node: RappelCall) -> str:
        kwargs = [f"{k}={self._print_node(v)}" for k, v in node.kwargs]
        return f"{node.target}({', '.join(kwargs)})"

    def _print_RappelActionCall(self, node: RappelActionCall) -> str:
        kwargs = [f"{k}={self._print_node(v)}" for k, v in node.kwargs]
        return f"@{node.action_name}({', '.join(kwargs)})"

    def _print_RappelFunctionDef(self, node: RappelFunctionDef) -> str:
        inputs = ", ".join(node.inputs)
        outputs = ", ".join(node.outputs)
        body = self._print_block(node.body)
        return f"fn {node.name}(input: [{inputs}], output: [{outputs}]):\n{body}"

    def _print_RappelPythonBlock(self, node: RappelPythonBlock) -> str:
        inputs = ", ".join(node.inputs)
        outputs = ", ".join(node.outputs)
        return f"python(input: [{inputs}], output: [{outputs}]):\n    {node.code}"

    def _print_RappelForLoop(self, node: RappelForLoop) -> str:
        iterable = self._print_node(node.iterable)
        fn = self._print_node(node.body_fn)
        return f"for {node.loop_var} in {iterable} -> {fn}"

    def _print_RappelIfStatement(self, node: RappelIfStatement) -> str:
        cond = self._print_node(node.condition)
        then_body = self._print_block(node.then_body)
        result = f"if {cond}:\n{then_body}"
        if node.else_body:
            else_body = self._print_block(node.else_body)
            result += f"\nelse:\n{else_body}"
        return result

    def _print_RappelReturn(self, node: RappelReturn) -> str:
        if len(node.values) == 0:
            return "return"
        elif len(node.values) == 1:
            return f"return {self._print_node(node.values[0])}"
        else:
            vals = ", ".join(self._print_node(v) for v in node.values)
            return f"return [{vals}]"

    def _print_RappelExprStatement(self, node: RappelExprStatement) -> str:
        return self._print_node(node.expr)

    def _print_block(self, stmts: tuple) -> str:
        lines = []
        for stmt in stmts:
            line = self._print_node(stmt)
            for subline in line.split("\n"):
                lines.append("    " + subline)
        return "\n".join(lines)


def test_pretty_printer():
    """Test the pretty printer."""
    source = """fn add(input: [a, b], output: [result]):
    result = a + b
    return result"""

    program = parse(source)
    printer = RappelPrettyPrinter()
    output = printer.print(program)

    assert "fn add" in output
    assert "result = (a + b)" in output
    assert "return result" in output


# =============================================================================
# Main entry point
# =============================================================================


# =============================================================================
# New comprehensive example: Action -> Spread -> Loop -> Action
# =============================================================================

EXAMPLE_ACTION_SPREAD_LOOP = """
# Comprehensive example: Fetch list, spread to parallel actions, loop over results
# This demonstrates the full flow of actions, spreading, and iteration

# Step 1: Call an action to get a list of items to process
order_ids = @get_pending_orders(status="pending", limit=100)

# Step 2: Spread the order IDs to fetch full order details in parallel
# Each order ID becomes input to a separate action call
order_details = @fetch_order_details(ids=order_ids)

# Step 3: Initialize accumulators for results
processed_orders = []
failed_orders = []

# Step 4: Define a function to process a single order
fn process_order(input: [order], output: [result, success]):
    # Validate the order
    if order["total"] > 0:
        # Call action to process payment
        payment_result = @process_payment(order_id=order["id"], amount=order["total"])

        if payment_result["status"] == "success":
            # Call action to update order status
            update_result = @update_order_status(order_id=order["id"], status="completed")
            result = {"order_id": order["id"], "payment": payment_result, "update": update_result}
            success = true
        else:
            result = {"order_id": order["id"], "error": payment_result["error"]}
            success = false
    else:
        result = {"order_id": order["id"], "error": "invalid_total"}
        success = false
    return [result, success]

# Step 5: Loop over each order detail, calling the process function
for order in order_details -> handle_order(input: [order], output: [result, success]):
    result, success = process_order(order=order)

# Step 6: Accumulate results based on success/failure
if success:
    processed_orders = processed_orders + [result]
else:
    failed_orders = failed_orders + [result]

# Step 7: Call final action to send notification with summary
summary = {"processed": processed_orders, "failed": failed_orders}
notification = @send_summary_notification(summary=summary, channel="slack")
"""


def main():
    """Main entry point - demonstrates the language."""
    print("=" * 60)
    print("Rappel Language - DSL with Immutable Variables")
    print("=" * 60)
    print()

    # Commented out: individual examples
    # examples = [
    #     ("Immutable Variables", EXAMPLE_IMMUTABLE_VARS),
    #     ("List Operations", EXAMPLE_LIST_OPERATIONS),
    #     ("Dict Operations", EXAMPLE_DICT_OPERATIONS),
    #     ("Python Blocks", EXAMPLE_PYTHON_BLOCK),
    #     ("Function Definitions", EXAMPLE_FUNCTION_DEF),
    #     ("Action Calls", EXAMPLE_ACTION_CALL),
    #     ("For Loops", EXAMPLE_FOR_LOOP),
    #     ("Spread Operator", EXAMPLE_SPREAD_OPERATOR),
    #     ("Conditionals", EXAMPLE_CONDITIONALS),
    #     ("Complex Workflow", EXAMPLE_COMPLEX_WORKFLOW),
    # ]
    #
    # printer = RappelPrettyPrinter()
    #
    # for name, source in examples:
    #     print(f"\n{'=' * 60}")
    #     print(f"Example: {name}")
    #     print("=" * 60)
    #     print("\nSource:")
    #     print("-" * 40)
    #     print(source.strip())
    #     print("-" * 40)
    #
    #     try:
    #         program = parse(source)
    #         print(f"\nParsed {len(program.statements)} statements")
    #         print("\nPretty printed:")
    #         print("-" * 40)
    #         print(printer.print(program))
    #     except SyntaxError as e:
    #         print(f"\nParse error: {e}")

    # New comprehensive example
    print("Example: Action -> Spread -> Loop -> Action Pipeline")
    print("=" * 60)
    print()
    print("This example demonstrates:")
    print("  1. Calling an action to fetch a list")
    print("  2. Spreading results to parallel action calls")
    print("  3. Looping over results with a function body")
    print("  4. Calling actions within the loop")
    print("  5. Accumulating results")
    print("  6. Final action call with summary")
    print()
    print("-" * 60)
    print("SOURCE CODE:")
    print("-" * 60)
    print(EXAMPLE_ACTION_SPREAD_LOOP.strip())
    print("-" * 60)
    print()

    try:
        program = parse(EXAMPLE_ACTION_SPREAD_LOOP)
        print(f"Successfully parsed {len(program.statements)} statements")
        print()

        printer = RappelPrettyPrinter()
        print("-" * 60)
        print("PRETTY PRINTED IR:")
        print("-" * 60)
        print(printer.print(program))
        print("-" * 60)
    except SyntaxError as e:
        print(f"Parse error: {e}")

    print()
    print("=" * 60)
    print("Done!")
    print("=" * 60)


if __name__ == "__main__":
    main()
