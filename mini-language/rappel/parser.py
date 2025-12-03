"""
Rappel Parser - Converts tokens into an AST (IR nodes).

Enforces:
- No nested function definitions (fn inside fn)
- For loop body functions are allowed (they're the loop mechanism)
- All function calls must use kwargs (no positional args)
- Actions are called with @action_name(kwargs) syntax
"""

from __future__ import annotations

from .tokens import Token, TokenType
import re

from .ir import (
    SourceLocation,
    RappelString,
    RappelNumber,
    RappelBoolean,
    RappelLiteral,
    RappelVariable,
    RappelListExpr,
    RappelDictExpr,
    RappelBinaryOp,
    RappelUnaryOp,
    RappelIndexAccess,
    RappelDotAccess,
    RappelSpread,
    RappelCall,
    RappelActionCall,
    RappelRetryPolicy,
    RappelExpr,
    RappelAssignment,
    RappelMultiAssignment,
    RappelReturn,
    RappelExprStatement,
    RappelFunctionDef,
    RappelForLoop,
    RappelIfStatement,
    RappelSpreadAction,
    RappelTryExcept,
    RappelExceptHandler,
    RappelParallelBlock,
    RappelStatement,
    RappelProgram,
)
from .lexer import RappelLexer


class RappelSyntaxError(SyntaxError):
    """Syntax error with source location."""

    def __init__(self, message: str, location: SourceLocation | None = None):
        if location:
            super().__init__(f"{message} at line {location.line}, column {location.column}")
        else:
            super().__init__(message)
        self.location = location


def _parse_duration(value: str) -> float:
    """
    Parse a duration string into seconds.

    Supports:
    - Plain numbers: "30" -> 30 seconds
    - Seconds: "30s" -> 30 seconds
    - Minutes: "2m" -> 120 seconds
    - Hours: "1h" -> 3600 seconds

    Returns seconds as a float.
    """
    value = value.strip()
    if not value:
        raise ValueError("Empty duration")

    # Check for suffix
    match = re.match(r'^(\d+(?:\.\d+)?)(s|m|h)?$', value)
    if not match:
        raise ValueError(f"Invalid duration format: {value}")

    num = float(match.group(1))
    unit = match.group(2) or 's'  # Default to seconds

    if unit == 's':
        return num
    elif unit == 'm':
        return num * 60
    elif unit == 'h':
        return num * 3600
    else:
        raise ValueError(f"Unknown duration unit: {unit}")


class RappelParser:
    """Parser for the Rappel language."""

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

        if self._check(TokenType.FOR):
            return self._parse_for_loop()

        if self._check(TokenType.IF):
            return self._parse_if_statement()

        if self._check(TokenType.TRY):
            return self._parse_try_except()

        if self._check(TokenType.PARALLEL):
            return self._parse_parallel_block()

        if self._check(TokenType.RETURN):
            return self._parse_return()

        if self._check(TokenType.SPREAD):
            return self._parse_spread_action()

        # Assignment or expression statement (may also be spread with assignment)
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

    def _parse_for_loop(self) -> RappelForLoop:
        """
        Parse a for loop with a single function call in body.

        Syntax:
            for item in items:
                result = process_item(x=item)

            for i, item in enumerate(items):
                result = process_item(idx=i, x=item)

        The body must contain exactly one statement: an assignment with a function call.
        """
        loc = self._location()
        self._consume(TokenType.FOR, "Expected 'for'")

        # Parse one or more loop variables separated by commas
        loop_vars = []
        loop_vars.append(
            self._consume(TokenType.IDENTIFIER, "Expected loop variable").value
        )
        while self._check(TokenType.COMMA):
            self._advance()  # consume comma
            loop_vars.append(
                self._consume(TokenType.IDENTIFIER, "Expected loop variable").value
            )

        self._consume(TokenType.IN, "Expected 'in'")
        iterable = self._parse_expression()

        self._consume(TokenType.COLON, "Expected ':' after for loop header")
        self._consume_newline()

        body = self._parse_block()

        # Validate: body must contain exactly one function call
        if len(body) != 1:
            raise RappelSyntaxError(
                f"For loop body must contain exactly one statement, got {len(body)}",
                loc
            )

        stmt = body[0]
        # Must be an assignment with a function call on the right side
        if not isinstance(stmt, RappelAssignment):
            raise RappelSyntaxError(
                "For loop body must be an assignment with a function call",
                loc
            )
        if not isinstance(stmt.value, RappelCall):
            raise RappelSyntaxError(
                "For loop body assignment must have a function call on the right side",
                loc
            )

        return RappelForLoop(
            loop_vars=tuple(loop_vars),
            iterable=iterable,
            body=tuple(body),
            location=loc,
        )

    def _parse_spread_action(self, target: str | None = None) -> RappelSpreadAction:
        """
        Parse a spread action statement.

        Syntax: spread <source_list>:<item_var> -> @action(kwargs)
        Or with assignment: <target> = spread <source_list>:<item_var> -> @action(kwargs)
        """
        loc = self._location()
        self._consume(TokenType.SPREAD, "Expected 'spread'")

        # Parse source_list:item_var
        source_list = self._parse_expression()

        self._consume(TokenType.COLON, "Expected ':' after source list in spread")

        item_var = self._consume(
            TokenType.IDENTIFIER, "Expected item variable name after ':'"
        ).value

        self._consume(TokenType.ARROW, "Expected '->' before action in spread")

        # Parse the action call (must be @action_name(...))
        if not self._check(TokenType.AT):
            raise SyntaxError(
                f"Expected action call (@action_name) after '->' in spread, "
                f"got {self._peek().type.name} at line {self._peek().line}"
            )

        self._advance()  # consume @
        action_name = self._consume(
            TokenType.IDENTIFIER, "Expected action name after '@'"
        ).value
        self._consume(TokenType.LPAREN, "Expected '(' after action name")
        kwargs = self._parse_kwargs_only()
        self._consume(TokenType.RPAREN, "Expected ')'")

        action = RappelActionCall(
            action_name=action_name,
            kwargs=tuple(kwargs),
            location=loc,
        )

        return RappelSpreadAction(
            source_list=source_list,
            item_var=item_var,
            action=action,
            target=target,
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

    def _parse_try_except(self) -> RappelTryExcept:
        """
        Parse a try/except block.

        Syntax:
            try:
                result = @risky_action(...)
            except ErrorType:
                result = @fallback_action(...)
            except:
                result = @default_handler(...)

        Each block (try and except) should be isolated execution units
        containing action calls.
        """
        loc = self._location()
        self._consume(TokenType.TRY, "Expected 'try'")
        self._consume(TokenType.COLON, "Expected ':' after 'try'")
        self._consume_newline()

        # Parse try body
        try_body = self._parse_block()

        # Parse except handlers (at least one required)
        handlers = []
        while self._check(TokenType.EXCEPT):
            handler = self._parse_except_handler()
            handlers.append(handler)

        if not handlers:
            raise RappelSyntaxError(
                "try block must have at least one except handler",
                loc
            )

        return RappelTryExcept(
            try_body=tuple(try_body),
            handlers=tuple(handlers),
            location=loc,
        )

    def _parse_except_handler(self) -> RappelExceptHandler:
        """
        Parse a single except handler.

        Syntax:
            except ErrorType:
                body
            except (ErrorType1, ErrorType2):
                body
            except:
                body  # catch-all
        """
        loc = self._location()
        self._consume(TokenType.EXCEPT, "Expected 'except'")

        # Parse exception types (optional)
        exception_types: list[str] = []

        if not self._check(TokenType.COLON):
            # Check for parenthesized list of exception types
            if self._check(TokenType.LPAREN):
                self._advance()
                while not self._check(TokenType.RPAREN):
                    exc_type = self._consume(
                        TokenType.IDENTIFIER,
                        "Expected exception type name"
                    ).value
                    exception_types.append(exc_type)
                    if self._check(TokenType.COMMA):
                        self._advance()
                self._consume(TokenType.RPAREN, "Expected ')'")
            else:
                # Single exception type
                exc_type = self._consume(
                    TokenType.IDENTIFIER,
                    "Expected exception type name or ':'"
                ).value
                exception_types.append(exc_type)

        self._consume(TokenType.COLON, "Expected ':' after except clause")
        self._consume_newline()

        # Parse handler body
        body = self._parse_block()

        return RappelExceptHandler(
            exception_types=tuple(exception_types),
            body=tuple(body),
            location=loc,
        )

    def _parse_parallel_block(self, target: str | None = None) -> RappelParallelBlock:
        """
        Parse a parallel execution block.

        Syntax:
            parallel:
                @action_a()
                @action_b()
                func_c()

        Or with assignment:
            results = parallel:
                @action_a()
                @action_b()

        Each statement in the block must be an expression statement
        containing either an action call (@action) or function call.
        """
        loc = self._location()
        self._consume(TokenType.PARALLEL, "Expected 'parallel'")
        self._consume(TokenType.COLON, "Expected ':' after 'parallel'")
        self._consume_newline()

        # Parse the block of parallel calls
        self._consume(TokenType.INDENT, "Expected indented block after 'parallel:'")

        calls: list[RappelExpr] = []

        while not self._check(TokenType.DEDENT) and not self._is_at_end():
            # Skip newlines
            while self._check(TokenType.NEWLINE):
                self._advance()
                if self._check(TokenType.DEDENT) or self._is_at_end():
                    break

            if self._check(TokenType.DEDENT) or self._is_at_end():
                break

            # Each line should be an action call or function call
            call_loc = self._location()

            if self._check(TokenType.AT):
                # Action call: @action_name(kwargs)
                self._advance()
                action_name = self._consume(
                    TokenType.IDENTIFIER, "Expected action name after '@'"
                ).value
                self._consume(TokenType.LPAREN, "Expected '(' after action name")
                kwargs = self._parse_kwargs_only()
                self._consume(TokenType.RPAREN, "Expected ')'")
                call = RappelActionCall(
                    action_name=action_name,
                    kwargs=tuple(kwargs),
                    location=call_loc,
                )
                calls.append(call)
            elif self._check(TokenType.IDENTIFIER):
                # Function call: func_name(kwargs)
                func_name = self._advance().value
                self._consume(TokenType.LPAREN, "Expected '(' after function name")
                kwargs = self._parse_kwargs_only()
                self._consume(TokenType.RPAREN, "Expected ')'")
                call = RappelCall(
                    target=func_name,
                    kwargs=tuple(kwargs),
                    location=call_loc,
                )
                calls.append(call)
            else:
                raise RappelSyntaxError(
                    "parallel block must contain action calls (@action) or function calls",
                    call_loc
                )

            # Consume newline after each call
            if self._check(TokenType.NEWLINE):
                self._advance()

        if self._check(TokenType.DEDENT):
            self._advance()

        if not calls:
            raise RappelSyntaxError(
                "parallel block must contain at least one call",
                loc
            )

        return RappelParallelBlock(
            calls=tuple(calls),
            target=target,
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

                # Check if RHS is a spread statement: target = spread ...
                if self._check(TokenType.SPREAD):
                    if len(targets) > 1:
                        raise SyntaxError(
                            f"Spread assignment can only have one target, "
                            f"got {len(targets)} at line {self._peek().line}"
                        )
                    return self._parse_spread_action(target=targets[0])

                # Check if RHS is a parallel block: target = parallel: ...
                if self._check(TokenType.PARALLEL):
                    if len(targets) > 1:
                        raise SyntaxError(
                            f"Parallel assignment can only have one target, "
                            f"got {len(targets)} at line {self._peek().line}"
                        )
                    return self._parse_parallel_block(target=targets[0])

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

        if self._check(TokenType.ELLIPSIS):
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

        # Action call: @action_name(kwargs) [Exception: retry: 3, backoff: 2m, timeout: 30s]
        if self._check(TokenType.AT):
            self._advance()
            action_name = self._consume(
                TokenType.IDENTIFIER, "Expected action name after '@'"
            ).value
            self._consume(TokenType.LPAREN, "Expected '(' after action name")
            kwargs = self._parse_kwargs_only()
            self._consume(TokenType.RPAREN, "Expected ')'")

            # Check for retry policies and timeout: [ValueError -> retry: 3] [timeout: 2m]
            retry_policies, timeout_seconds = self._parse_retry_policies()

            return RappelActionCall(
                action_name=action_name,
                kwargs=tuple(kwargs),
                retry_policies=tuple(retry_policies),
                timeout_seconds=timeout_seconds,
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

    def _parse_retry_policies(self) -> tuple[list[RappelRetryPolicy], float | None]:
        """
        Parse retry policies and timeout after action call.

        Syntax:
            [ValueError -> retry: 3, backoff: 2m]
            [(ValueError, KeyError) -> retry: 3, backoff: 2s]
            [retry: 3, backoff: 2m]  # catch all exceptions
            [timeout: 2m]  # timeout only (separate from retry)

        Multiple brackets can be chained:
            [ValueError -> retry: 3] [KeyError -> retry: 5] [timeout: 2m]

        Returns:
            Tuple of (list of retry policies, timeout in seconds or None)
        """
        policies = []
        timeout_seconds = None

        while self._check(TokenType.LBRACKET):
            self._advance()  # consume '['

            # Determine what kind of bracket this is:
            # 1. [timeout: 2m] - timeout only
            # 2. [retry: 3, backoff: 2m] - catch-all retry
            # 3. [ValueError -> retry: 3] - exception-specific retry
            # 4. [(ValueError, KeyError) -> retry: 3] - tuple of exceptions

            exception_types: list[str] = []

            if self._check(TokenType.LPAREN):
                # Tuple of exception types: (ValueError, KeyError)
                self._advance()  # consume '('
                exception_types.append(
                    self._consume(TokenType.IDENTIFIER, "Expected exception type").value
                )
                while self._check(TokenType.COMMA):
                    self._advance()
                    exception_types.append(
                        self._consume(TokenType.IDENTIFIER, "Expected exception type").value
                    )
                self._consume(TokenType.RPAREN, "Expected ')'")
                self._consume(TokenType.ARROW, "Expected '->' after exception types")

            elif self._check(TokenType.IDENTIFIER):
                # Could be:
                # - Exception type followed by ->: ValueError ->
                # - Param name followed by :: retry: or timeout:
                saved_pos = self.pos
                first_ident = self._advance().value

                if self._check(TokenType.ARROW):
                    # Single exception type: ValueError ->
                    self._advance()  # consume '->'
                    exception_types.append(first_ident)
                elif self._check(TokenType.COLON):
                    # This is a parameter (retry:, backoff:, timeout:)
                    # Backtrack and let the parameter parsing handle it
                    self.pos = saved_pos
                else:
                    raise RappelSyntaxError(
                        f"Expected '->' or ':' after '{first_ident}'",
                        self._peek().line,
                        self._peek().column
                    )

            # Now parse the parameters
            max_retries = 3
            backoff_seconds = 60.0
            is_timeout_bracket = False
            has_retry_params = False

            while not self._check(TokenType.RBRACKET):
                param_name = self._consume(TokenType.IDENTIFIER, "Expected parameter name").value
                self._consume(TokenType.COLON, f"Expected ':' after '{param_name}'")

                if param_name == "retry":
                    if not self._check(TokenType.NUMBER):
                        raise RappelSyntaxError(
                            f"Expected number for 'retry'",
                            self._peek().line,
                            self._peek().column
                        )
                    max_retries = int(self._advance().value)
                    has_retry_params = True
                elif param_name == "backoff":
                    backoff_seconds = self._parse_duration_value()
                    has_retry_params = True
                elif param_name == "timeout":
                    timeout_seconds = self._parse_duration_value()
                    is_timeout_bracket = True
                else:
                    raise RappelSyntaxError(
                        f"Unknown parameter: '{param_name}'",
                        self._peek().line,
                        self._peek().column
                    )

                # Optional comma between parameters
                if self._check(TokenType.COMMA):
                    self._advance()

            self._consume(TokenType.RBRACKET, "Expected ']'")

            # Only create a retry policy if we have retry/backoff params or exception types
            # Timeout-only brackets don't create a policy
            if has_retry_params or exception_types:
                policies.append(RappelRetryPolicy(
                    exception_types=tuple(exception_types),
                    max_retries=max_retries,
                    backoff_seconds=backoff_seconds,
                ))

        return policies, timeout_seconds

    def _parse_duration_value(self) -> float:
        """Parse a duration value (number or duration string like 2m, 30s)."""
        if self._check(TokenType.NUMBER):
            return float(self._advance().value)
        elif self._check(TokenType.STRING):
            return _parse_duration(self._advance().value)
        elif self._check(TokenType.IDENTIFIER):
            # Handle bare identifiers like 2m (lexed as identifier)
            return _parse_duration(self._advance().value)
        else:
            raise RappelSyntaxError(
                "Expected duration value",
                self._peek().line,
                self._peek().column
            )

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
