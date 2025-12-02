"""
Rappel Intermediate Representation (IR)

This module provides:
1. IRParser: Parses Python AST into validated Rappel IR (protobuf messages)
2. IRSerializer: Serializes IR to human-readable text for debugging
3. Helper classes for module indexing and variable analysis

The IR captures the subset of Python that can be translated into a durable
execution DAG. It is produced by parsing Python AST and provides:
- Clear validation (if it parses to IR, it's valid)
- Better error messages with source locations
- A debuggable intermediate form
- Language-agnostic target for future JS/Go frontends

"""

from __future__ import annotations

import ast
import textwrap
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from proto import ir_pb2

if TYPE_CHECKING:
    import inspect


class IRParseError(Exception):
    """Raised when Python code cannot be parsed into valid Rappel IR."""

    def __init__(
        self,
        message: str,
        node: ast.AST | None = None,
        location: ir_pb2.SourceLocation | None = None,
    ):
        self.node = node
        self.location = location or (_source_location_from_node(node) if node else None)
        loc_str = f" ({_format_location(self.location)})" if self.location else ""
        super().__init__(f"{message}{loc_str}")


# AST operator to IR BinaryOp mapping
_BINOP_MAP: dict[type, ir_pb2.BinaryOp.Op] = {
    ast.Add: ir_pb2.BinaryOp.OP_ADD,
    ast.Sub: ir_pb2.BinaryOp.OP_SUB,
    ast.Mult: ir_pb2.BinaryOp.OP_MUL,
    ast.Div: ir_pb2.BinaryOp.OP_DIV,
    ast.Mod: ir_pb2.BinaryOp.OP_MOD,
}

_CMPOP_MAP: dict[type, ir_pb2.BinaryOp.Op] = {
    ast.Eq: ir_pb2.BinaryOp.OP_EQ,
    ast.NotEq: ir_pb2.BinaryOp.OP_NE,
    ast.Lt: ir_pb2.BinaryOp.OP_LT,
    ast.LtE: ir_pb2.BinaryOp.OP_LE,
    ast.Gt: ir_pb2.BinaryOp.OP_GT,
    ast.GtE: ir_pb2.BinaryOp.OP_GE,
    ast.In: ir_pb2.BinaryOp.OP_IN,
    ast.NotIn: ir_pb2.BinaryOp.OP_NOT_IN,
}

_BOOLOP_MAP: dict[type, ir_pb2.BinaryOp.Op] = {
    ast.And: ir_pb2.BinaryOp.OP_AND,
    ast.Or: ir_pb2.BinaryOp.OP_OR,
}

_UNARYOP_MAP: dict[type, ir_pb2.UnaryOp.Op] = {
    ast.Not: ir_pb2.UnaryOp.OP_NOT,
    ast.USub: ir_pb2.UnaryOp.OP_NEG,
}


def _ast_to_expression(node: ast.expr) -> ir_pb2.Expression:
    """Convert a Python AST expression to an IR Expression protobuf."""
    if isinstance(node, ast.Constant):
        return _constant_to_expression(node)

    if isinstance(node, ast.Name):
        return ir_pb2.Expression(variable=node.id)

    if isinstance(node, ast.Subscript):
        return ir_pb2.Expression(
            subscript=ir_pb2.Subscript(
                base=_ast_to_expression(node.value),
                key=_ast_to_expression(node.slice),
            )
        )

    if isinstance(node, ast.Attribute):
        return ir_pb2.Expression(
            attribute=ir_pb2.AttributeAccess(
                base=_ast_to_expression(node.value),
                attribute=node.attr,
            )
        )

    if isinstance(node, ast.List):
        return ir_pb2.Expression(
            array=ir_pb2.ArrayExpr(elements=[_ast_to_expression(e) for e in node.elts])
        )

    if isinstance(node, ast.Dict):
        entries = []
        for k, v in zip(node.keys, node.values):
            if k is None:
                # dict unpacking (**d) - not supported in IR
                raise IRParseError(
                    "Dict unpacking (**) is not supported in expressions",
                    node,
                    _source_location_from_node(node),
                )
            # For now, only support string literal keys
            if isinstance(k, ast.Constant) and isinstance(k.value, str):
                entries.append(ir_pb2.DictEntry(key=k.value, value=_ast_to_expression(v)))
            else:
                raise IRParseError(
                    f"Dict keys must be string literals, got {ast.unparse(k)}",
                    k,
                    _source_location_from_node(k),
                )
        return ir_pb2.Expression(dict=ir_pb2.DictExpr(entries=entries))

    if isinstance(node, ast.BinOp):
        op = _BINOP_MAP.get(type(node.op))
        if op is None:
            raise IRParseError(
                f"Unsupported binary operator: {type(node.op).__name__}",
                node,
                _source_location_from_node(node),
            )
        return ir_pb2.Expression(
            binary_op=ir_pb2.BinaryOp(
                op=op,
                left=_ast_to_expression(node.left),
                right=_ast_to_expression(node.right),
            )
        )

    if isinstance(node, ast.Compare):
        # Handle comparison chains like a < b < c
        if len(node.ops) == 1 and len(node.comparators) == 1:
            op = _CMPOP_MAP.get(type(node.ops[0]))
            if op is None:
                raise IRParseError(
                    f"Unsupported comparison operator: {type(node.ops[0]).__name__}",
                    node,
                    _source_location_from_node(node),
                )
            return ir_pb2.Expression(
                binary_op=ir_pb2.BinaryOp(
                    op=op,
                    left=_ast_to_expression(node.left),
                    right=_ast_to_expression(node.comparators[0]),
                )
            )
        else:
            # Chain comparison like a < b < c becomes (a < b) and (b < c)
            exprs = []
            left = node.left
            for op_node, right in zip(node.ops, node.comparators):
                op = _CMPOP_MAP.get(type(op_node))
                if op is None:
                    raise IRParseError(
                        f"Unsupported comparison operator: {type(op_node).__name__}",
                        node,
                        _source_location_from_node(node),
                    )
                exprs.append(
                    ir_pb2.Expression(
                        binary_op=ir_pb2.BinaryOp(
                            op=op,
                            left=_ast_to_expression(left),
                            right=_ast_to_expression(right),
                        )
                    )
                )
                left = right
            # Combine with AND
            result = exprs[0]
            for expr in exprs[1:]:
                result = ir_pb2.Expression(
                    binary_op=ir_pb2.BinaryOp(
                        op=ir_pb2.BinaryOp.OP_AND,
                        left=result,
                        right=expr,
                    )
                )
            return result

    if isinstance(node, ast.BoolOp):
        op = _BOOLOP_MAP.get(type(node.op))
        if op is None:
            raise IRParseError(
                f"Unsupported boolean operator: {type(node.op).__name__}",
                node,
                _source_location_from_node(node),
            )
        # BoolOp has multiple values, chain them left to right
        result = _ast_to_expression(node.values[0])
        for value in node.values[1:]:
            result = ir_pb2.Expression(
                binary_op=ir_pb2.BinaryOp(
                    op=op,
                    left=result,
                    right=_ast_to_expression(value),
                )
            )
        return result

    if isinstance(node, ast.UnaryOp):
        op = _UNARYOP_MAP.get(type(node.op))
        if op is None:
            raise IRParseError(
                f"Unsupported unary operator: {type(node.op).__name__}",
                node,
                _source_location_from_node(node),
            )
        return ir_pb2.Expression(
            unary_op=ir_pb2.UnaryOp(
                op=op,
                operand=_ast_to_expression(node.operand),
            )
        )

    if isinstance(node, ast.Call):
        # Only support simple function calls like len(x)
        if isinstance(node.func, ast.Name):
            func_name = node.func.id
            args = [_ast_to_expression(arg) for arg in node.args]
            return ir_pb2.Expression(call=ir_pb2.CallExpr(function=func_name, args=args))
        raise IRParseError(
            f"Complex function call not supported in expressions: {ast.unparse(node)}",
            node,
            _source_location_from_node(node),
        )

    if isinstance(node, ast.IfExp):
        # Ternary: a if cond else b
        # Not directly supported in IR - we'd need to add it or raise
        raise IRParseError(
            f"Ternary expressions not supported in IR: {ast.unparse(node)}",
            node,
            _source_location_from_node(node),
        )

    if isinstance(node, ast.JoinedStr):
        # F-strings - store the unparsed representation as a variable/code expression
        # This allows the runtime to evaluate it with the proper context
        return ir_pb2.Expression(variable=ast.unparse(node))

    raise IRParseError(
        f"Unsupported expression type: {type(node).__name__}: {ast.unparse(node)}",
        node,
        _source_location_from_node(node),
    )


def _constant_to_expression(node: ast.Constant) -> ir_pb2.Expression:
    """Convert a Python constant to an IR Expression."""
    value = node.value
    if value is None:
        return ir_pb2.Expression(literal=ir_pb2.Literal(null_value=True))
    if isinstance(value, bool):
        return ir_pb2.Expression(literal=ir_pb2.Literal(bool_value=value))
    if isinstance(value, int):
        return ir_pb2.Expression(literal=ir_pb2.Literal(int_value=value))
    if isinstance(value, float):
        return ir_pb2.Expression(literal=ir_pb2.Literal(float_value=value))
    if isinstance(value, str):
        return ir_pb2.Expression(literal=ir_pb2.Literal(string_value=value))
    raise IRParseError(
        f"Unsupported constant type: {type(value).__name__}",
        node,
        _source_location_from_node(node),
    )


def _expression_to_string(expr: ir_pb2.Expression) -> str:
    """Convert an IR Expression back to a Python code string for display."""
    kind = expr.WhichOneof("kind")
    if kind == "literal":
        lit = expr.literal
        lit_kind = lit.WhichOneof("value")
        if lit_kind == "null_value":
            return "None"
        if lit_kind == "bool_value":
            return "True" if lit.bool_value else "False"
        if lit_kind == "int_value":
            return str(lit.int_value)
        if lit_kind == "float_value":
            return str(lit.float_value)
        if lit_kind == "string_value":
            return repr(lit.string_value)
        return "None"
    if kind == "variable":
        return expr.variable
    if kind == "subscript":
        base = _expression_to_string(expr.subscript.base)
        key = _expression_to_string(expr.subscript.key)
        return f"{base}[{key}]"
    if kind == "attribute":
        base = _expression_to_string(expr.attribute.base)
        return f"{base}.{expr.attribute.attribute}"
    if kind == "array":
        elements = [_expression_to_string(e) for e in expr.array.elements]
        return f"[{', '.join(elements)}]"
    if kind == "dict":
        entries = [f"{repr(e.key)}: {_expression_to_string(e.value)}" for e in expr.dict.entries]
        return f"{{{', '.join(entries)}}}"
    if kind == "binary_op":
        left = _expression_to_string(expr.binary_op.left)
        right = _expression_to_string(expr.binary_op.right)
        op_map = {
            ir_pb2.BinaryOp.OP_ADD: "+",
            ir_pb2.BinaryOp.OP_SUB: "-",
            ir_pb2.BinaryOp.OP_MUL: "*",
            ir_pb2.BinaryOp.OP_DIV: "/",
            ir_pb2.BinaryOp.OP_MOD: "%",
            ir_pb2.BinaryOp.OP_EQ: "==",
            ir_pb2.BinaryOp.OP_NE: "!=",
            ir_pb2.BinaryOp.OP_LT: "<",
            ir_pb2.BinaryOp.OP_LE: "<=",
            ir_pb2.BinaryOp.OP_GT: ">",
            ir_pb2.BinaryOp.OP_GE: ">=",
            ir_pb2.BinaryOp.OP_AND: "and",
            ir_pb2.BinaryOp.OP_OR: "or",
            ir_pb2.BinaryOp.OP_IN: "in",
            ir_pb2.BinaryOp.OP_NOT_IN: "not in",
        }
        op_str = op_map.get(expr.binary_op.op, "?")
        return f"{left} {op_str} {right}"
    if kind == "unary_op":
        operand = _expression_to_string(expr.unary_op.operand)
        op_map = {
            ir_pb2.UnaryOp.OP_NOT: "not ",
            ir_pb2.UnaryOp.OP_NEG: "-",
        }
        op_str = op_map.get(expr.unary_op.op, "?")
        return f"{op_str}{operand}"
    if kind == "call":
        args = [_expression_to_string(a) for a in expr.call.args]
        return f"{expr.call.function}({', '.join(args)})"
    return "?"


def _source_location_from_node(node: ast.AST) -> ir_pb2.SourceLocation:
    """Create a SourceLocation from an AST node."""
    loc = ir_pb2.SourceLocation(
        lineno=getattr(node, "lineno", 0),
        col_offset=getattr(node, "col_offset", 0),
    )
    end_lineno = getattr(node, "end_lineno", None)
    end_col_offset = getattr(node, "end_col_offset", None)
    if end_lineno is not None:
        loc.end_lineno = end_lineno
    if end_col_offset is not None:
        loc.end_col_offset = end_col_offset
    return loc


def _format_location(loc: ir_pb2.SourceLocation | None) -> str:
    """Format a SourceLocation for display."""
    if loc is None:
        return ""
    if loc.HasField("end_lineno") and loc.end_lineno != loc.lineno:
        return f"lines {loc.lineno}-{loc.end_lineno}"
    return f"line {loc.lineno}, col {loc.col_offset}"


@dataclass
class ActionDefinition:
    """Metadata about an action for module resolution and arg mapping."""

    name: str
    module: str | None = None
    signature: inspect.Signature | None = None
    param_names: list[str] = field(default_factory=list)


class ModuleIndex:
    """Indexes a module's imports and definitions for capture in python blocks."""

    def __init__(self, module_source: str):
        self._source = module_source
        self._imports: dict[str, str] = {}
        self._definitions: dict[str, str] = {}
        self._definition_deps: dict[str, set[str]] = {}

        tree = ast.parse(module_source)
        for node in tree.body:
            snippet = ast.get_source_segment(module_source, node)
            if snippet is None:
                # Defensive: ast.get_source_segment can return None for AST nodes
                # without source positions. This is rare but possible with synthetic
                # or malformed AST nodes.
                continue
            text = textwrap.dedent(snippet)

            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                self._definitions[node.name] = text
                self._definition_deps[node.name] = self._extract_deps(node)
            elif isinstance(node, (ast.Import, ast.ImportFrom)):
                for alias in node.names:
                    exposed = alias.asname or alias.name.split(".")[0]
                    self._imports[exposed] = text

    def _extract_deps(self, node: ast.AST) -> set[str]:
        """Extract names referenced in a definition."""
        deps: set[str] = set()
        for sub in ast.walk(node):
            if isinstance(sub, ast.Name) and isinstance(sub.ctx, ast.Load):
                deps.add(sub.id)
        return deps

    @property
    def symbols(self) -> set[str]:
        return set(self._imports) | set(self._definitions)

    def resolve(self, names: set[str]) -> tuple[list[str], list[str]]:
        """Resolve names to import and definition blocks, with transitive deps."""
        import_blocks: list[str] = []
        definition_blocks: list[str] = []
        resolved: set[str] = set()
        to_resolve = list(names)

        while to_resolve:
            name = to_resolve.pop()
            if name in resolved:
                # Skip already-processed names to avoid duplicates when
                # multiple symbols depend on the same definition.
                continue
            resolved.add(name)

            if name in self._imports:
                import_blocks.append(self._imports[name])
            elif name in self._definitions:
                definition_blocks.append(self._definitions[name])
                for dep in self._definition_deps.get(name, set()):
                    if dep not in resolved and dep in self.symbols:
                        to_resolve.append(dep)

        return sorted(set(import_blocks)), sorted(set(definition_blocks))


class VariableAnalyzer(ast.NodeVisitor):
    """Analyzes Python code to determine inputs (reads) and outputs (writes)."""

    def __init__(self, known_vars: set[str] | None = None):
        self.reads: set[str] = set()
        self.writes: set[str] = set()
        self._known_vars = known_vars or set()
        self._local_scope: set[str] = set()

    def analyze(self, nodes: list[ast.stmt]) -> tuple[list[str], list[str]]:
        """Analyze statements and return (inputs, outputs)."""
        for node in nodes:
            self.visit(node)
        inputs = sorted(self.reads - self._local_scope)
        outputs = sorted(self.writes)
        return inputs, outputs

    def visit_Name(self, node: ast.Name) -> None:
        if isinstance(node.ctx, ast.Load):
            self.reads.add(node.id)
        elif isinstance(node.ctx, (ast.Store, ast.Del)):
            self.writes.add(node.id)
            self._local_scope.add(node.id)
        self.generic_visit(node)

    def visit_Assign(self, node: ast.Assign) -> None:
        self.visit(node.value)
        for target in node.targets:
            self.visit(target)

    def visit_AugAssign(self, node: ast.AugAssign) -> None:
        if isinstance(node.target, ast.Name):
            self.reads.add(node.target.id)
            self.writes.add(node.target.id)
        self.visit(node.value)

    def visit_For(self, node: ast.For) -> None:
        """Analyze for loops within Python blocks for variable flow.

        Note: This is called when analyzing Python blocks that contain for loops
        (e.g., within conditionals without actions). Top-level for loops with
        actions are handled by _parse_for_loop instead.
        """
        self.visit(node.iter)
        self.visit(node.target)
        for stmt in node.body:
            self.visit(stmt)
        for stmt in node.orelse:
            # Handle for-else constructs
            self.visit(stmt)

    def visit_Expr(self, node: ast.Expr) -> None:
        """Detect mutating method calls like list.append().

        When we see a call like `outputs.append(value)`, the list `outputs` is mutated.
        We mark it as both a read (to access the list) and a write (since it's modified).
        """
        if isinstance(node.value, ast.Call):
            call = node.value
            # Check for method calls like var.append(), var.extend(), var.clear(), etc.
            if isinstance(call.func, ast.Attribute):
                attr = call.func
                # List/dict mutating methods
                mutating_methods = {
                    "append",
                    "extend",
                    "insert",
                    "remove",
                    "pop",
                    "clear",
                    "reverse",
                    "sort",
                    "update",
                    "setdefault",
                }
                if attr.attr in mutating_methods and isinstance(attr.value, ast.Name):
                    var_name = attr.value.id
                    self.reads.add(var_name)
                    self.writes.add(var_name)
        self.generic_visit(node)

    def visit_comprehension(self, node: ast.comprehension) -> None:
        self.visit(node.iter)
        if isinstance(node.target, ast.Name):
            self._local_scope.add(node.target.id)
        for if_ in node.ifs:
            self.visit(if_)


class GuardValidator(ast.NodeVisitor):
    """Validates that guard expressions are safe and well-formed."""

    ALLOWED_FUNCS = {
        "len",
        "str",
        "int",
        "float",
        "bool",
        "abs",
        "min",
        "max",
        "sum",
        "any",
        "all",
        "isinstance",
        "hasattr",
        "getattr",
    }

    def __init__(self, known_vars: set[str]):
        self.known_vars = known_vars
        self.errors: list[str] = []
        self.referenced_vars: set[str] = set()

    def validate(self, guard_expr: str, location: ir_pb2.SourceLocation | None = None) -> list[str]:
        """Validate a guard expression. Returns list of errors (empty if valid)."""
        self.errors = []
        self.referenced_vars = set()

        try:
            tree = ast.parse(guard_expr, mode="eval")
            self.visit(tree.body)
        except SyntaxError as e:
            # Guard expressions come from unparsed AST nodes, so syntax errors
            # should be very rare. This handles edge cases where ast.unparse
            # produces invalid Python syntax.
            loc_str = f" at {_format_location(location)}" if location else ""
            self.errors.append(f"Invalid guard syntax{loc_str}: {e}")

        return self.errors

    def visit_Name(self, node: ast.Name) -> None:
        if isinstance(node.ctx, ast.Load):
            self.referenced_vars.add(node.id)
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        if isinstance(node.func, ast.Name):
            if node.func.id not in self.ALLOWED_FUNCS:
                self.errors.append(
                    f"Function '{node.func.id}' not allowed in guard expressions. "
                    f"Allowed: {', '.join(sorted(self.ALLOWED_FUNCS))}"
                )
        elif not isinstance(node.func, ast.Attribute):
            self.errors.append(f"Complex function call not allowed in guard: {ast.unparse(node)}")
        self.generic_visit(node)

    def visit_Lambda(self, node: ast.Lambda) -> None:
        self.errors.append("Lambda expressions not allowed in guards")

    def visit_Await(self, node: ast.Await) -> None:
        self.errors.append("Await expressions not allowed in guards")

    def visit_Yield(self, node: ast.Yield) -> None:
        self.errors.append("Yield expressions not allowed in guards")

    def visit_YieldFrom(self, node: ast.YieldFrom) -> None:
        self.errors.append("Yield expressions not allowed in guards")


class IRParser:
    """Parses a Python async def into Rappel IR (protobuf messages)."""

    def __init__(
        self,
        action_defs: dict[str, ActionDefinition],
        module_index: ModuleIndex | None = None,
        source: str | None = None,
    ):
        self.action_defs = action_defs
        self.action_names = set(action_defs.keys())
        self.module_index = module_index
        self.source = source

        self._known_collections: dict[str, list[str]] = {}
        self._known_vars: set[str] = set()
        self._guard_validator = GuardValidator(self._known_vars)

    def parse_workflow(self, func: ast.AsyncFunctionDef) -> ir_pb2.Workflow:
        """Parse an async function definition into a Workflow protobuf."""
        workflow = ir_pb2.Workflow(name=func.name)

        for arg in func.args.args:
            if arg.arg == "self":
                continue
            param = ir_pb2.WorkflowParam(name=arg.arg)
            if arg.annotation:
                param.type_annotation = ast.unparse(arg.annotation)
            workflow.params.append(param)
            self._known_vars.add(arg.arg)

        if func.returns:
            workflow.return_type = ast.unparse(func.returns)

        for stmt in self._parse_body(func.body):
            workflow.body.append(stmt)

        return workflow

    def _parse_body(self, stmts: list[ast.stmt]) -> list[ir_pb2.Statement]:
        """Parse a list of statements into IR Statements."""
        result: list[ir_pb2.Statement] = []

        for stmt in stmts:
            if self._is_empty_list_init(stmt):
                if isinstance(stmt, ast.Assign):
                    for t in stmt.targets:
                        if isinstance(t, ast.Name):
                            self._known_vars.add(t.id)
                continue

            parsed = self._parse_statement(stmt)
            if parsed is not None:
                result.append(parsed)
                self._track_statement_outputs(parsed)

        return result

    def _track_statement_outputs(self, stmt: ir_pb2.Statement) -> None:
        """Track variables produced by a statement."""
        kind = stmt.WhichOneof("kind")
        if kind == "action_call" and stmt.action_call.HasField("target"):
            self._known_vars.add(stmt.action_call.target)
        elif kind == "gather" and stmt.gather.HasField("target"):
            self._known_vars.add(stmt.gather.target)
        elif kind == "python_block":
            self._known_vars.update(stmt.python_block.outputs)
        elif kind == "loop":
            self._known_vars.add(stmt.loop.accumulator)
        elif kind == "conditional" and stmt.conditional.HasField("target"):
            self._known_vars.add(stmt.conditional.target)
        elif kind == "spread" and stmt.spread.HasField("target"):
            self._known_vars.add(stmt.spread.target)

    def _parse_statement(self, stmt: ast.stmt) -> ir_pb2.Statement | None:
        """Parse a single statement into IR Statement."""
        location = _source_location_from_node(stmt)

        if isinstance(stmt, ast.Return):
            return ir_pb2.Statement(return_stmt=self._parse_return(stmt, location))

        if isinstance(stmt, ast.Expr):
            return self._parse_expr_statement(stmt, location)

        if isinstance(stmt, ast.Assign):
            return self._parse_assignment(stmt, location)

        if isinstance(stmt, ast.For):
            return ir_pb2.Statement(loop=self._parse_for_loop(stmt, location))

        if isinstance(stmt, ast.If):
            return self._parse_conditional(stmt, location)

        if isinstance(stmt, ast.Try):
            return ir_pb2.Statement(try_except=self._parse_try_except(stmt, location))

        return ir_pb2.Statement(python_block=self._make_python_block([stmt], location))

    def _parse_return(self, stmt: ast.Return, location: ir_pb2.SourceLocation) -> ir_pb2.Return:
        """Parse a return statement."""
        ret = ir_pb2.Return()
        ret.location.CopyFrom(location)

        if stmt.value is None:
            return ret

        action_call = self._extract_action_call(stmt.value)
        if action_call is not None:
            action_call.target = "__workflow_return"
            ret.action.CopyFrom(action_call)
            return ret

        gather = self._extract_gather(stmt.value)
        if gather is not None:
            gather.target = "__workflow_return"
            ret.gather.CopyFrom(gather)
            return ret

        ret.expression.CopyFrom(_ast_to_expression(stmt.value))
        return ret

    def _parse_expr_statement(
        self, stmt: ast.Expr, location: ir_pb2.SourceLocation
    ) -> ir_pb2.Statement | None:
        """Parse an expression statement."""
        expr = stmt.value

        action_call = self._extract_action_call(expr)
        if action_call is not None:
            action_call.location.CopyFrom(location)
            return ir_pb2.Statement(action_call=action_call)

        gather = self._extract_gather(expr)
        if gather is not None:
            gather.location.CopyFrom(location)
            return ir_pb2.Statement(gather=gather)

        sleep = self._extract_sleep(expr)
        if sleep is not None:
            sleep.location.CopyFrom(location)
            return ir_pb2.Statement(sleep=sleep)

        # Fallback: expression statements that aren't action calls, gathers, or
        # sleeps become Python blocks. This handles arbitrary expressions.
        return ir_pb2.Statement(python_block=self._make_python_block([stmt], location))

    def _parse_assignment(
        self, stmt: ast.Assign, location: ir_pb2.SourceLocation
    ) -> ir_pb2.Statement | None:
        """Parse an assignment statement."""
        if len(stmt.targets) != 1:
            return ir_pb2.Statement(python_block=self._make_python_block([stmt], location))

        target = stmt.targets[0]
        if not isinstance(target, ast.Name):
            return ir_pb2.Statement(python_block=self._make_python_block([stmt], location))

        target_name = target.id

        if self._is_empty_list_init(stmt):
            return None

        action_call = self._extract_action_call(stmt.value)
        if action_call is not None:
            action_call.target = target_name
            action_call.location.CopyFrom(location)
            return ir_pb2.Statement(action_call=action_call)

        gather = self._extract_gather(stmt.value)
        if gather is not None:
            gather.target = target_name
            gather.location.CopyFrom(location)
            self._known_collections[target_name] = [
                f"{target_name}__item{i}" for i in range(len(gather.calls))
            ]
            return ir_pb2.Statement(gather=gather)

        # Check for async list comprehension pattern: [await action(x) for x in collection]
        # Transform this into a Loop IR
        loop = self._extract_async_list_comp(stmt.value, target_name, location)
        if loop is not None:
            return ir_pb2.Statement(loop=loop)

        spread = self._extract_spread(stmt.value, target_name)
        if spread is not None:
            spread.location.CopyFrom(location)
            return ir_pb2.Statement(spread=spread)

        return ir_pb2.Statement(python_block=self._make_python_block([stmt], location))

    def _parse_for_loop(self, stmt: ast.For, location: ir_pb2.SourceLocation) -> ir_pb2.Loop:
        """Parse a for loop.

        The loop body is parsed as a sub-graph of statements. The accumulator is
        detected from append() calls in the body - these become PythonBlock statements.
        """
        if stmt.orelse:
            raise IRParseError("for/else is not supported", stmt, location)

        if not isinstance(stmt.target, ast.Name):
            raise IRParseError("for loop target must be a simple variable", stmt, location)

        loop_var = stmt.target.id

        # Track variables available in loop body (includes loop var)
        loop_known_vars = self._known_vars | {loop_var}

        # Detect accumulator from append statements
        accumulator: str | None = None
        for body_stmt in stmt.body:
            append_info = self._extract_append(body_stmt)
            if append_info is not None:
                acc_name, _ = append_info
                if accumulator is None:
                    accumulator = acc_name
                elif accumulator != acc_name:
                    raise IRParseError(
                        f"Loop can only append to one accumulator, found both '{accumulator}' and '{acc_name}'",
                        body_stmt,
                        _source_location_from_node(body_stmt),
                    )

        if accumulator is None:
            raise IRParseError("for loop must append to an accumulator", stmt, location)

        # Parse body statements as a sub-graph
        body_statements: list[ir_pb2.Statement] = []
        has_action = False

        for body_stmt in stmt.body:
            body_location = _source_location_from_node(body_stmt)

            # Check for append - becomes a PythonBlock
            append_info = self._extract_append(body_stmt)
            if append_info is not None:
                block = self._make_python_block([body_stmt], body_location, loop_known_vars)
                body_statements.append(ir_pb2.Statement(python_block=block))
                continue

            # Check for action call with assignment
            if isinstance(body_stmt, ast.Assign) and len(body_stmt.targets) == 1:
                target = body_stmt.targets[0]
                if isinstance(target, ast.Name):
                    action_call = self._extract_action_call(body_stmt.value)
                    if action_call is not None:
                        action_call.target = target.id
                        action_call.location.CopyFrom(body_location)
                        body_statements.append(ir_pb2.Statement(action_call=action_call))
                        loop_known_vars.add(target.id)
                        has_action = True
                        continue

            # Check for action call as expression
            if isinstance(body_stmt, ast.Expr):
                action_call = self._extract_action_call(body_stmt.value)
                if action_call is not None:
                    action_call.location.CopyFrom(body_location)
                    body_statements.append(ir_pb2.Statement(action_call=action_call))
                    has_action = True
                    continue

            # Everything else becomes a PythonBlock
            block = self._make_python_block([body_stmt], body_location, loop_known_vars)
            body_statements.append(ir_pb2.Statement(python_block=block))
            loop_known_vars.update(block.outputs)

        if not has_action:
            raise IRParseError("for loop must contain at least one action call", stmt, location)

        loop = ir_pb2.Loop(
            loop_var=loop_var,
            accumulator=accumulator,
        )
        loop.iterator.CopyFrom(_ast_to_expression(stmt.iter))
        loop.body.extend(body_statements)
        loop.location.CopyFrom(location)

        return loop

    def _parse_conditional(self, stmt: ast.If, location: ir_pb2.SourceLocation) -> ir_pb2.Statement:
        """Parse an if/elif/else statement."""
        if not self._contains_action(stmt):
            return ir_pb2.Statement(python_block=self._make_python_block([stmt], location))

        if not stmt.orelse:
            raise IRParseError("Conditional with actions requires an else branch", stmt, location)

        branches = self._extract_conditional_branches(stmt, parent_guard=None)

        for branch in branches:
            # Convert Expression back to string for validation
            guard_str = _expression_to_string(branch.guard)
            errors = self._guard_validator.validate(guard_str, branch.location)
            if errors:
                raise IRParseError(
                    f"Invalid guard expression: {'; '.join(errors)}",
                    location=branch.location,
                )

        targets = {
            b.actions[-1].target for b in branches if b.actions and b.actions[-1].HasField("target")
        }
        target = targets.pop() if len(targets) == 1 else None

        cond = ir_pb2.Conditional()
        cond.branches.extend(branches)
        if target:
            cond.target = target
        cond.location.CopyFrom(location)

        return ir_pb2.Statement(conditional=cond)

    def _extract_conditional_branches(
        self, stmt: ast.If, parent_guard: ast.expr | None
    ) -> list[ir_pb2.Branch]:
        """Extract all branches from an if/elif/else chain."""
        branches: list[ir_pb2.Branch] = []
        location = _source_location_from_node(stmt)

        # Build guard expression (combining with parent if present)
        if parent_guard is not None:
            true_guard = ast.BoolOp(op=ast.And(), values=[parent_guard, stmt.test])
        else:
            true_guard = stmt.test

        true_branch = self._parse_branch_body(stmt.body, true_guard, location)
        branches.append(true_branch)

        if len(stmt.orelse) == 1 and isinstance(stmt.orelse[0], ast.If):
            # Negated guard for elif chain
            negated = ast.UnaryOp(op=ast.Not(), operand=stmt.test)
            if parent_guard is not None:
                negated = ast.BoolOp(op=ast.And(), values=[parent_guard, negated])
            branches.extend(self._extract_conditional_branches(stmt.orelse[0], negated))
        elif stmt.orelse:
            # Else branch - negate the condition
            negated = ast.UnaryOp(op=ast.Not(), operand=stmt.test)
            if parent_guard is not None:
                negated = ast.BoolOp(op=ast.And(), values=[parent_guard, negated])
            else_location = _source_location_from_node(stmt.orelse[0])
            false_branch = self._parse_branch_body(stmt.orelse, negated, else_location)
            branches.append(false_branch)

        return branches

    def _parse_branch_body(
        self, body: list[ast.stmt], guard: ast.expr, location: ir_pb2.SourceLocation
    ) -> ir_pb2.Branch:
        """Parse a branch body into preamble, actions, postamble."""
        preamble: list[ir_pb2.PythonBlock] = []
        actions: list[ir_pb2.ActionCall] = []
        postamble: list[ir_pb2.PythonBlock] = []

        branch_known_vars = set(self._known_vars)
        found_first_action = False
        in_postamble = False

        for stmt in body:
            stmt_location = _source_location_from_node(stmt)
            stmt_action = self._extract_statement_action(stmt)

            if stmt_action is not None:
                if in_postamble:
                    raise IRParseError(
                        "Actions cannot appear after non-action statements in a branch",
                        stmt,
                        stmt_location,
                    )
                stmt_action.location.CopyFrom(stmt_location)
                actions.append(stmt_action)
                if stmt_action.HasField("target"):
                    branch_known_vars.add(stmt_action.target)
                found_first_action = True
            elif not found_first_action:
                block = self._make_python_block([stmt], stmt_location, branch_known_vars)
                preamble.append(block)
                branch_known_vars.update(block.outputs)
            else:
                in_postamble = True
                block = self._make_python_block([stmt], stmt_location, branch_known_vars)
                postamble.append(block)
                branch_known_vars.update(block.outputs)

        if not actions:
            guard_str = ast.unparse(guard)
            raise IRParseError(
                f"Conditional branch with guard '{guard_str}' must have at least one action",
                location=location,
            )

        branch = ir_pb2.Branch()
        branch.guard.CopyFrom(_ast_to_expression(guard))
        branch.preamble.extend(preamble)
        branch.actions.extend(actions)
        branch.postamble.extend(postamble)
        branch.location.CopyFrom(location)

        return branch

    def _extract_statement_action(self, stmt: ast.stmt) -> ir_pb2.ActionCall | None:
        """Extract action call from a statement if present."""
        if isinstance(stmt, ast.Assign) and len(stmt.targets) == 1:
            target = stmt.targets[0]
            if isinstance(target, ast.Name):
                action = self._extract_action_call(stmt.value)
                if action:
                    action.target = target.id
                    return action
        elif isinstance(stmt, ast.Expr):
            return self._extract_action_call(stmt.value)
        return None

    def _parse_try_except(self, stmt: ast.Try, location: ir_pb2.SourceLocation) -> ir_pb2.TryExcept:
        """Parse a try/except statement."""
        if stmt.finalbody:
            raise IRParseError("finally blocks are not supported", stmt, location)
        if stmt.orelse:
            raise IRParseError("try/else is not supported", stmt, location)
        if not stmt.handlers:
            raise IRParseError("try must have except handlers", stmt, location)

        # Parse try body with preamble/postamble support
        try_preamble, try_actions, try_postamble = self._parse_try_or_handler_body(
            stmt.body, set(self._known_vars), "try block"
        )

        if not try_actions:
            raise IRParseError("try block must contain at least one action", stmt, location)

        # Track variables from try block for handler parsing
        try_known_vars = set(self._known_vars)
        for action in try_actions:
            if action.HasField("target"):
                try_known_vars.add(action.target)
        for block in try_postamble:
            try_known_vars.update(block.outputs)

        handlers: list[ir_pb2.ExceptHandler] = []
        for handler in stmt.handlers:
            handler_location = _source_location_from_node(handler)

            if handler.name:
                raise IRParseError(
                    "except clauses cannot bind exception to variable",
                    handler,
                    handler_location,
                )

            exc_types = self._extract_exception_types(handler.type)

            # Parse handler body with preamble/postamble support
            handler_preamble, handler_actions, handler_postamble = self._parse_try_or_handler_body(
                handler.body, try_known_vars, "except block"
            )

            exc_handler = ir_pb2.ExceptHandler()
            exc_handler.exception_types.extend(exc_types)
            exc_handler.preamble.extend(handler_preamble)
            exc_handler.body.extend(handler_actions)
            exc_handler.postamble.extend(handler_postamble)
            exc_handler.location.CopyFrom(handler_location)
            handlers.append(exc_handler)

        try_except = ir_pb2.TryExcept()
        try_except.try_preamble.extend(try_preamble)
        try_except.try_body.extend(try_actions)
        try_except.try_postamble.extend(try_postamble)
        try_except.handlers.extend(handlers)
        try_except.location.CopyFrom(location)

        return try_except

    def _parse_try_or_handler_body(
        self, body: list[ast.stmt], known_vars: set[str], block_name: str
    ) -> tuple[list[ir_pb2.PythonBlock], list[ir_pb2.ActionCall], list[ir_pb2.PythonBlock]]:
        """Parse a try or except handler body into preamble, actions, postamble.

        Similar to _parse_branch_body but for try/except blocks.
        """
        preamble: list[ir_pb2.PythonBlock] = []
        actions: list[ir_pb2.ActionCall] = []
        postamble: list[ir_pb2.PythonBlock] = []

        block_known_vars = set(known_vars)
        found_first_action = False
        in_postamble = False

        for stmt in body:
            stmt_location = _source_location_from_node(stmt)
            stmt_action = self._extract_statement_action(stmt)

            if stmt_action is not None:
                if in_postamble:
                    raise IRParseError(
                        f"Actions cannot appear after non-action statements in {block_name}",
                        stmt,
                        stmt_location,
                    )
                stmt_action.location.CopyFrom(stmt_location)
                actions.append(stmt_action)
                if stmt_action.HasField("target"):
                    block_known_vars.add(stmt_action.target)
                found_first_action = True
            elif not found_first_action:
                block = self._make_python_block([stmt], stmt_location, block_known_vars)
                preamble.append(block)
                block_known_vars.update(block.outputs)
            else:
                in_postamble = True
                block = self._make_python_block([stmt], stmt_location, block_known_vars)
                postamble.append(block)
                block_known_vars.update(block.outputs)

        return preamble, actions, postamble

    def _extract_exception_types(self, node: ast.expr | None) -> list[ir_pb2.ExceptionType]:
        """Extract exception types from except clause."""
        if node is None:
            return [ir_pb2.ExceptionType()]

        if isinstance(node, ast.Tuple):
            return [self._parse_exception_type(elt) for elt in node.elts]

        return [self._parse_exception_type(node)]

    def _parse_exception_type(self, node: ast.expr) -> ir_pb2.ExceptionType:
        """Parse a single exception type into ExceptionType."""
        if isinstance(node, ast.Name):
            return ir_pb2.ExceptionType(name=node.id)
        if isinstance(node, ast.Attribute):
            module_parts = []
            current = node.value
            while isinstance(current, ast.Attribute):
                module_parts.insert(0, current.attr)
                current = current.value
            if isinstance(current, ast.Name):
                module_parts.insert(0, current.id)
            return ir_pb2.ExceptionType(module=".".join(module_parts), name=node.attr)
        raise IRParseError(
            f"Unsupported exception type: {ast.unparse(node)}",
            node,
            _source_location_from_node(node),
        )

    def _extract_action_call(self, expr: ast.expr) -> ir_pb2.ActionCall | None:
        """Extract an action call from an expression."""
        if isinstance(expr, ast.Await):
            expr = expr.value

        if not isinstance(expr, ast.Call):
            return None

        run_action_info = self._extract_run_action(expr)
        if run_action_info:
            return run_action_info

        action_name = self._get_action_name(expr.func)
        if action_name is None or action_name not in self.action_names:
            return None

        action_def = self.action_defs.get(action_name)
        module = action_def.module if action_def else None

        args = self._extract_args_with_signature(expr, action_def)

        action_call = ir_pb2.ActionCall(
            action=action_def.name if action_def else action_name,
        )
        if module:
            action_call.module = module
        action_call.args.extend(args)
        action_call.location.CopyFrom(_source_location_from_node(expr))

        return action_call

    def _extract_args_with_signature(
        self, call: ast.Call, action_def: ActionDefinition | None
    ) -> list[ir_pb2.KwArg]:
        """Extract args as KwArg list, mapping positional args using signature."""
        args: list[ir_pb2.KwArg] = []
        seen_names: set[str] = set()

        # First process keyword arguments
        for kw in call.keywords:
            if kw.arg is not None:
                args.append(
                    ir_pb2.KwArg(
                        name=kw.arg,
                        value=_ast_to_expression(kw.value),
                    )
                )
                seen_names.add(kw.arg)

        # Then process positional arguments
        if call.args:
            if action_def and action_def.param_names:
                param_names = action_def.param_names
                for i, arg in enumerate(call.args):
                    if i < len(param_names):
                        param_name = param_names[i]
                        if param_name not in seen_names:
                            args.append(
                                ir_pb2.KwArg(
                                    name=param_name,
                                    value=_ast_to_expression(arg),
                                )
                            )
                            seen_names.add(param_name)
                    else:
                        args.append(
                            ir_pb2.KwArg(
                                name=f"__arg{i}",
                                value=_ast_to_expression(arg),
                            )
                        )
            else:
                for i, arg in enumerate(call.args):
                    args.append(
                        ir_pb2.KwArg(
                            name=f"__arg{i}",
                            value=_ast_to_expression(arg),
                        )
                    )

        return args

    def _extract_run_action(self, call: ast.Call) -> ir_pb2.ActionCall | None:
        """Extract action from self.run_action(action(...), ...) pattern."""
        func = call.func
        if not (
            isinstance(func, ast.Attribute)
            and func.attr == "run_action"
            and isinstance(func.value, ast.Name)
            and func.value.id == "self"
        ):
            return None

        if not call.args:
            raise IRParseError(
                "run_action requires an action argument",
                call,
                _source_location_from_node(call),
            )

        inner = call.args[0]
        if isinstance(inner, ast.Await):
            inner = inner.value

        if not isinstance(inner, ast.Call):
            raise IRParseError(
                "run_action argument must be an action call",
                call,
                _source_location_from_node(call),
            )

        action_name = self._get_action_name(inner.func)
        if action_name is None or action_name not in self.action_names:
            raise IRParseError(
                f"Unknown action in run_action: {ast.unparse(inner.func)}",
                call,
                _source_location_from_node(call),
            )

        action_def = self.action_defs.get(action_name)
        args = self._extract_args_with_signature(inner, action_def)
        config = self._extract_run_action_config(call.keywords)

        action_call = ir_pb2.ActionCall(
            action=action_def.name if action_def else action_name,
        )
        if action_def and action_def.module:
            action_call.module = action_def.module
        action_call.args.extend(args)
        if config:
            action_call.config.CopyFrom(config)
        action_call.location.CopyFrom(_source_location_from_node(call))

        return action_call

    def _extract_run_action_config(
        self, keywords: list[ast.keyword]
    ) -> ir_pb2.RunActionConfig | None:
        """Extract timeout/retry/backoff config from run_action keywords."""
        config = ir_pb2.RunActionConfig()
        has_config = False

        for kw in keywords:
            if kw.arg == "timeout":
                config.timeout_seconds = self._eval_timeout(kw.value)
                has_config = True
            elif kw.arg == "retry":
                config.max_retries = self._eval_retry(kw.value)
                has_config = True
            elif kw.arg == "backoff":
                backoff = self._eval_backoff(kw.value)
                if backoff:
                    config.backoff.CopyFrom(backoff)
                    has_config = True

        return config if has_config else None

    def _eval_timeout(self, node: ast.expr) -> int:
        """Evaluate timeout value."""
        if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
            return int(node.value)
        if isinstance(node, ast.Call):
            func_name = self._get_simple_name(node.func)
            if func_name == "timedelta":
                return self._eval_timedelta(node)
        raise IRParseError(
            f"Cannot evaluate timeout: {ast.unparse(node)}",
            node,
            _source_location_from_node(node),
        )

    def _eval_timedelta(self, call: ast.Call) -> int:
        """Evaluate timedelta to seconds."""
        seconds = 0.0
        conversions = {
            "days": 86400,
            "hours": 3600,
            "minutes": 60,
            "seconds": 1,
            "milliseconds": 0.001,
            "microseconds": 0.000001,
        }

        for kw in call.keywords:
            if kw.arg in conversions and isinstance(kw.value, ast.Constant):
                seconds += kw.value.value * conversions[kw.arg]

        pos_keys = ["days", "seconds", "microseconds"]
        for i, arg in enumerate(call.args):
            if i < len(pos_keys) and isinstance(arg, ast.Constant):
                seconds += arg.value * conversions[pos_keys[i]]

        return int(seconds)

    def _eval_retry(self, node: ast.expr) -> int:
        """Evaluate retry value."""
        if isinstance(node, ast.Constant):
            if node.value is None:
                return 2_147_483_647
            if isinstance(node.value, int):
                return node.value
        if isinstance(node, ast.Call):
            for kw in node.keywords:
                if kw.arg in ("attempts", "max_attempts"):
                    if isinstance(kw.value, ast.Constant):
                        if kw.value.value is None:
                            return 2_147_483_647
                        return kw.value.value
        raise IRParseError(
            f"Cannot evaluate retry: {ast.unparse(node)}",
            node,
            _source_location_from_node(node),
        )

    def _eval_backoff(self, node: ast.expr) -> ir_pb2.BackoffConfig | None:
        """Evaluate backoff config."""
        if isinstance(node, ast.Constant) and node.value is None:
            return None
        if not isinstance(node, ast.Call):
            raise IRParseError(
                f"Cannot evaluate backoff: {ast.unparse(node)}",
                node,
                _source_location_from_node(node),
            )

        func_name = self._get_simple_name(node.func)
        if func_name == "LinearBackoff":
            base_delay = self._get_keyword_int(node, "base_delay_ms")
            return ir_pb2.BackoffConfig(
                kind=ir_pb2.BackoffConfig.KIND_LINEAR, base_delay_ms=base_delay
            )
        elif func_name == "ExponentialBackoff":
            base_delay = self._get_keyword_int(node, "base_delay_ms")
            multiplier = self._get_keyword_float(node, "multiplier", 2.0)
            return ir_pb2.BackoffConfig(
                kind=ir_pb2.BackoffConfig.KIND_EXPONENTIAL,
                base_delay_ms=base_delay,
                multiplier=multiplier,
            )

        raise IRParseError(
            f"Unknown backoff type: {func_name}",
            node,
            _source_location_from_node(node),
        )

    def _get_keyword_int(self, call: ast.Call, name: str, default: int = 0) -> int:
        for kw in call.keywords:
            if kw.arg == name and isinstance(kw.value, ast.Constant):
                return int(kw.value.value)
        return default

    def _get_keyword_float(self, call: ast.Call, name: str, default: float = 0.0) -> float:
        for kw in call.keywords:
            if kw.arg == name and isinstance(kw.value, ast.Constant):
                return float(kw.value.value)
        return default

    def _extract_gather(self, expr: ast.expr) -> ir_pb2.Gather | None:
        """Extract an asyncio.gather call."""
        if isinstance(expr, ast.Await):
            expr = expr.value

        if not isinstance(expr, ast.Call):
            return None

        func = expr.func
        if not (
            isinstance(func, ast.Attribute)
            and func.attr == "gather"
            and isinstance(func.value, ast.Name)
            and func.value.id == "asyncio"
        ):
            return None

        calls: list[ir_pb2.GatherCall] = []
        for arg in expr.args:
            gather_call = self._extract_gather_call(arg)
            if gather_call is None:
                raise IRParseError(
                    f"gather argument must be an action or subgraph call: {ast.unparse(arg)}",
                    arg,
                    _source_location_from_node(arg),
                )
            calls.append(gather_call)

        gather = ir_pb2.Gather()
        gather.calls.extend(calls)
        gather.location.CopyFrom(_source_location_from_node(expr))

        return gather

    def _extract_gather_call(self, arg: ast.expr) -> ir_pb2.GatherCall | None:
        """Extract a single gather call - either an action or a subgraph call."""
        # First try to extract as an action call
        action = self._extract_action_call(arg)
        if action is not None:
            return ir_pb2.GatherCall(action=action)

        # Then try to extract as a subgraph call (self.method())
        subgraph = self._extract_subgraph_call(arg)
        if subgraph is not None:
            return ir_pb2.GatherCall(subgraph=subgraph)

        return None

    def _extract_subgraph_call(self, expr: ast.expr) -> ir_pb2.SubgraphCall | None:
        """Extract a self.method() call as a subgraph invocation."""
        if isinstance(expr, ast.Await):
            expr = expr.value

        if not isinstance(expr, ast.Call):
            return None

        func = expr.func
        # Check for self.method pattern
        if not (
            isinstance(func, ast.Attribute)
            and isinstance(func.value, ast.Name)
            and func.value.id == "self"
        ):
            return None

        method_name = func.attr

        # Skip run_action - that's handled elsewhere
        if method_name == "run_action":
            return None

        # Extract args as KwArg list
        args: list[ir_pb2.KwArg] = []
        for kw in expr.keywords:
            if kw.arg is not None:
                args.append(
                    ir_pb2.KwArg(
                        name=kw.arg,
                        value=_ast_to_expression(kw.value),
                    )
                )

        # Handle positional args as __argN
        for i, arg in enumerate(expr.args):
            args.append(
                ir_pb2.KwArg(
                    name=f"__arg{i}",
                    value=_ast_to_expression(arg),
                )
            )

        subgraph = ir_pb2.SubgraphCall(method_name=method_name)
        subgraph.args.extend(args)
        subgraph.location.CopyFrom(_source_location_from_node(expr))

        return subgraph

    def _extract_sleep(self, expr: ast.expr) -> ir_pb2.Sleep | None:
        """Extract an asyncio.sleep call."""
        if isinstance(expr, ast.Await):
            expr = expr.value

        if not isinstance(expr, ast.Call):
            return None

        func = expr.func
        if not (
            isinstance(func, ast.Attribute)
            and func.attr == "sleep"
            and isinstance(func.value, ast.Name)
            and func.value.id == "asyncio"
        ):
            return None

        if not expr.args:
            raise IRParseError(
                "asyncio.sleep requires a duration argument",
                expr,
                _source_location_from_node(expr),
            )

        sleep = ir_pb2.Sleep()
        sleep.duration.CopyFrom(_ast_to_expression(expr.args[0]))
        sleep.location.CopyFrom(_source_location_from_node(expr))

        return sleep

    def _extract_async_list_comp(
        self, expr: ast.expr, target: str, location: ir_pb2.SourceLocation
    ) -> ir_pb2.Loop | None:
        """Transform async list comprehension into Loop IR.

        Transforms:
            values = [await action(x) for x in collection]

        Into equivalent Loop IR for:
            values = []
            for x in collection:
                __temp = await action(x)
                values.append(__temp)

        This allows async list comprehensions to be properly scheduled
        as sequential action executions.
        """
        if not isinstance(expr, ast.ListComp):
            return None

        if len(expr.generators) != 1:
            return None

        gen = expr.generators[0]
        # We specifically DON'T check gen.is_async here because
        # [await action(x) for x in items] is NOT an async comprehension
        # (that would be [x async for x in async_iter]).
        # What we're looking for is a sync comprehension with an await in the element.
        if gen.ifs:
            # Don't support filtered comprehensions for now
            return None

        if not isinstance(gen.target, ast.Name):
            return None

        loop_var = gen.target.id

        # Check if the element contains an await action call
        action = self._extract_action_call(expr.elt)
        if action is None:
            return None

        # Generate a temp variable for the action result
        temp_var = f"__{target}_item"
        action.target = temp_var

        # Track variables available in loop body
        loop_known_vars = self._known_vars | {loop_var, temp_var}

        # Create the loop body:
        # 1. Action call that produces temp_var
        # 2. Append temp_var to accumulator
        body_statements: list[ir_pb2.Statement] = []

        # Add the action call
        action.location.CopyFrom(location)
        body_statements.append(ir_pb2.Statement(action_call=action))

        # Create the append statement as a PythonBlock
        append_code = f"{target}.append({temp_var})"
        append_block = ir_pb2.PythonBlock(code=append_code)
        append_block.inputs.extend([target, temp_var])
        append_block.outputs.append(target)
        append_block.location.CopyFrom(location)
        body_statements.append(ir_pb2.Statement(python_block=append_block))

        # Create the Loop IR
        loop = ir_pb2.Loop(
            loop_var=loop_var,
            accumulator=target,
        )
        loop.iterator.CopyFrom(_ast_to_expression(gen.iter))
        loop.body.extend(body_statements)
        loop.location.CopyFrom(location)

        # Mark that we need an empty list initialization
        # This is handled by _is_empty_list_init returning true for explicit
        # empty list inits, but for async list comp we need to synthesize one.
        # We'll track this so the caller knows to add the init.
        self._known_vars.add(target)

        return loop

    def _extract_spread(self, expr: ast.expr, target: str) -> ir_pb2.Spread | None:
        """Extract [await action(x=v) for v in collection] pattern."""
        if not isinstance(expr, ast.ListComp):
            return None

        if len(expr.generators) != 1:
            return None

        gen = expr.generators[0]
        if gen.ifs or gen.is_async:
            return None

        if not isinstance(gen.target, ast.Name):
            return None

        loop_var = gen.target.id

        action = self._extract_action_call(expr.elt)
        if action is None:
            return None

        spread = ir_pb2.Spread(loop_var=loop_var, target=target)
        spread.iterable.CopyFrom(_ast_to_expression(gen.iter))
        spread.action.CopyFrom(action)
        spread.location.CopyFrom(_source_location_from_node(expr))

        return spread

    def _extract_append(self, stmt: ast.stmt) -> tuple[str, str] | None:
        """Extract accumulator.append(expr) pattern."""
        if not isinstance(stmt, ast.Expr):
            return None

        call = stmt.value
        if not isinstance(call, ast.Call):
            return None

        func = call.func
        if not isinstance(func, ast.Attribute) or func.attr != "append":
            # Not an append call (e.g., extend, other method calls)
            return None

        if not isinstance(func.value, ast.Name):
            # Append on attribute access (e.g., self.results.append) not supported
            # for loop accumulator pattern
            return None

        if len(call.args) != 1:
            # Malformed append call - standard list.append takes exactly 1 arg
            return None

        accumulator = func.value.id
        source_expr = ast.unparse(call.args[0])
        return (accumulator, source_expr)

    def _get_action_name(self, node: ast.expr) -> str | None:
        """Get action name from a call's func."""
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            return node.attr
        return None

    def _get_simple_name(self, node: ast.expr) -> str | None:
        """Get simple variable name from expression.

        Returns None for complex expressions like attribute access or
        subscript operations. This is used for spread pattern matching
        where we need a simple iterable variable name.
        """
        if isinstance(node, ast.Name):
            return node.id
        return None

    def _is_empty_list_init(self, stmt: ast.stmt) -> bool:
        """Check if statement is `x = []`."""
        if not isinstance(stmt, ast.Assign):
            return False
        if len(stmt.targets) != 1:
            return False
        if not isinstance(stmt.targets[0], ast.Name):
            return False
        if not isinstance(stmt.value, ast.List):
            return False
        return len(stmt.value.elts) == 0

    def _contains_action(self, node: ast.AST) -> bool:
        """Check if AST node contains any action calls."""
        for child in ast.walk(node):
            if isinstance(child, ast.Call):
                name = self._get_action_name(child.func)
                if name and name in self.action_names:
                    return True
        return False

    def _make_python_block(
        self,
        stmts: list[ast.stmt],
        location: ir_pb2.SourceLocation | None = None,
        known_vars: set[str] | None = None,
    ) -> ir_pb2.PythonBlock:
        """Create a python block from statements with full analysis."""
        code = "\n".join(ast.unparse(s) for s in stmts)

        analyzer = VariableAnalyzer(known_vars or self._known_vars)
        inputs, outputs = analyzer.analyze(stmts)

        imports: list[str] = []
        definitions: list[str] = []

        if self.module_index:
            referenced: set[str] = set()
            for stmt in stmts:
                for node in ast.walk(stmt):
                    if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
                        referenced.add(node.id)
                    elif isinstance(node, ast.Attribute):
                        root = node
                        while isinstance(root, ast.Attribute):
                            root = root.value
                        if isinstance(root, ast.Name):
                            referenced.add(root.id)

            to_resolve = referenced & self.module_index.symbols
            if to_resolve:
                imports, definitions = self.module_index.resolve(to_resolve)

        block = ir_pb2.PythonBlock(code=code)
        block.imports.extend(imports)
        block.definitions.extend(definitions)
        block.inputs.extend(inputs)
        block.outputs.extend(outputs)
        if location:
            block.location.CopyFrom(location)

        return block


class IRSerializer:
    """Serialize Rappel IR to a human-readable text format."""

    def __init__(self, include_locations: bool = False):
        self.include_locations = include_locations

    def serialize(self, workflow: ir_pb2.Workflow) -> str:
        lines: list[str] = []

        params_str = ", ".join(
            f"{p.name}: {p.type_annotation}" if p.HasField("type_annotation") else p.name
            for p in workflow.params
        )
        ret_str = f" -> {workflow.return_type}" if workflow.HasField("return_type") else ""
        lines.append(f"workflow {workflow.name}({params_str}){ret_str}:")

        for stmt in workflow.body:
            stmt_lines = self._serialize_statement(stmt)
            for line in stmt_lines:
                lines.append("    " + line)

        return "\n".join(lines)

    def _loc_comment(self, location: ir_pb2.SourceLocation | None) -> str:
        if self.include_locations and location:
            return f"  # {_format_location(location)}"
        return ""

    def _serialize_statement(self, stmt: ir_pb2.Statement) -> list[str]:
        kind = stmt.WhichOneof("kind")
        if kind == "action_call":
            return self._serialize_action(stmt.action_call)
        if kind == "gather":
            return self._serialize_gather(stmt.gather)
        if kind == "python_block":
            return self._serialize_python_block(stmt.python_block)
        if kind == "loop":
            return self._serialize_loop(stmt.loop)
        if kind == "conditional":
            return self._serialize_conditional(stmt.conditional)
        if kind == "try_except":
            return self._serialize_try_except(stmt.try_except)
        if kind == "sleep":
            return self._serialize_sleep(stmt.sleep)
        if kind == "return_stmt":
            return self._serialize_return(stmt.return_stmt)
        if kind == "spread":
            return self._serialize_spread(stmt.spread)
        # Defensive: handle unknown statement kinds for forward compatibility
        # with new IR statement types.
        return [f"# UNKNOWN: {kind}"]

    def _serialize_action(self, action: ir_pb2.ActionCall) -> list[str]:
        args_str = ", ".join(
            f"{arg.name}={_expression_to_string(arg.value)}" for arg in action.args
        )
        module_prefix = f"{action.module}." if action.HasField("module") else ""
        call_str = f"@{module_prefix}{action.action}({args_str})"

        if action.HasField("config"):
            config_parts = []
            if action.config.HasField("timeout_seconds"):
                config_parts.append(f"timeout={action.config.timeout_seconds}s")
            if action.config.HasField("max_retries"):
                config_parts.append(f"retry={action.config.max_retries}")
            if action.config.HasField("backoff"):
                b = action.config.backoff
                if b.kind == ir_pb2.BackoffConfig.KIND_LINEAR:
                    config_parts.append(f"backoff=linear({b.base_delay_ms}ms)")
                else:
                    config_parts.append(f"backoff=exp({b.base_delay_ms}ms, {b.multiplier}x)")
            if config_parts:
                call_str += f" [policy: {', '.join(config_parts)}]"

        loc = self._loc_comment(action.location if action.HasField("location") else None)
        if action.HasField("target"):
            return [f"{action.target} = {call_str}{loc}"]
        return [f"{call_str}{loc}"]

    def _serialize_gather(self, gather: ir_pb2.Gather) -> list[str]:
        calls = []
        for gather_call in gather.calls:
            call_str = self._serialize_gather_call(gather_call)
            calls.append(call_str)

        loc = self._loc_comment(gather.location if gather.HasField("location") else None)
        if gather.HasField("target"):
            return [f"{gather.target} = parallel({', '.join(calls)}){loc}"]
        return [f"parallel({', '.join(calls)}){loc}"]

    def _serialize_gather_call(self, gather_call: ir_pb2.GatherCall) -> str:
        """Serialize a single gather call (action or subgraph)."""
        kind = gather_call.WhichOneof("kind")
        if kind == "action":
            call = gather_call.action
            args_str = ", ".join(
                f"{arg.name}={_expression_to_string(arg.value)}" for arg in call.args
            )
            module_prefix = f"{call.module}." if call.HasField("module") else ""
            return f"@{module_prefix}{call.action}({args_str})"
        elif kind == "subgraph":
            call = gather_call.subgraph
            args_str = ", ".join(
                f"{arg.name}={_expression_to_string(arg.value)}" for arg in call.args
            )
            return f"self.{call.method_name}({args_str})"
        else:
            return f"# UNKNOWN gather call: {kind}"

    def _serialize_python_block(self, block: ir_pb2.PythonBlock) -> list[str]:
        lines = []

        io_parts = []
        if block.inputs:
            io_parts.append(f"reads: {', '.join(block.inputs)}")
        if block.outputs:
            io_parts.append(f"writes: {', '.join(block.outputs)}")
        io_str = f" ({'; '.join(io_parts)})" if io_parts else ""

        lines.append(f"python{io_str} {{")

        if block.imports:
            for imp in block.imports:
                lines.append(f"    # import: {imp.strip()}")

        if block.definitions:
            for defn in block.definitions:
                first_line = defn.split("\n")[0]
                lines.append(f"    # def: {first_line}...")

        for line in block.code.split("\n"):
            lines.append("    " + line)
        lines.append("}")
        return lines

    def _serialize_loop(self, loop: ir_pb2.Loop) -> list[str]:
        lines = []

        loc = self._loc_comment(loop.location if loop.HasField("location") else None)
        iterator_str = _expression_to_string(loop.iterator) if loop.HasField("iterator") else "?"
        lines.append(f"loop {loop.loop_var} in {iterator_str} -> {loop.accumulator}:{loc}")

        for stmt in loop.body:
            stmt_lines = self._serialize_statement(stmt)
            for line in stmt_lines:
                lines.append("    " + line)

        return lines

    def _serialize_conditional(self, cond: ir_pb2.Conditional) -> list[str]:
        lines = []
        for i, branch in enumerate(cond.branches):
            loc = self._loc_comment(branch.location if branch.HasField("location") else None)
            guard_str = _expression_to_string(branch.guard) if branch.HasField("guard") else "?"
            if i == 0:
                lines.append(f"branch if {guard_str}:{loc}")
            elif i == len(cond.branches) - 1 and "not " in guard_str:
                lines.append(f"branch else:{loc}")
            else:
                lines.append(f"branch elif {guard_str}:{loc}")

            if branch.preamble:
                for pre in branch.preamble:
                    io_parts = []
                    if pre.inputs:
                        io_parts.append(f"reads: {', '.join(pre.inputs)}")
                    if pre.outputs:
                        io_parts.append(f"writes: {', '.join(pre.outputs)}")
                    io_str = f" ({'; '.join(io_parts)})" if io_parts else ""
                    lines.append(f"    # preamble{io_str}")
                    for line in pre.code.split("\n"):
                        lines.append("    " + line)

            for action in branch.actions:
                action_lines = self._serialize_action(action)
                for line in action_lines:
                    lines.append("    " + line)

            if branch.postamble:
                for post in branch.postamble:
                    io_parts = []
                    if post.inputs:
                        io_parts.append(f"reads: {', '.join(post.inputs)}")
                    if post.outputs:
                        io_parts.append(f"writes: {', '.join(post.outputs)}")
                    io_str = f" ({'; '.join(io_parts)})" if io_parts else ""
                    lines.append(f"    # postamble{io_str}")
                    for line in post.code.split("\n"):
                        lines.append("    " + line)

        return lines

    def _serialize_try_except(self, te: ir_pb2.TryExcept) -> list[str]:
        loc = self._loc_comment(te.location if te.HasField("location") else None)
        lines = [f"try:{loc}"]

        # Serialize try preamble
        if te.try_preamble:
            for pre in te.try_preamble:
                io_parts = []
                if pre.inputs:
                    io_parts.append(f"reads: {', '.join(pre.inputs)}")
                if pre.outputs:
                    io_parts.append(f"writes: {', '.join(pre.outputs)}")
                io_str = f" ({'; '.join(io_parts)})" if io_parts else ""
                lines.append(f"    # preamble{io_str}")
                for line in pre.code.split("\n"):
                    lines.append("    " + line)

        # Serialize try body actions
        for action in te.try_body:
            action_lines = self._serialize_action(action)
            for line in action_lines:
                lines.append("    " + line)

        # Serialize try postamble
        if te.try_postamble:
            for post in te.try_postamble:
                io_parts = []
                if post.inputs:
                    io_parts.append(f"reads: {', '.join(post.inputs)}")
                if post.outputs:
                    io_parts.append(f"writes: {', '.join(post.outputs)}")
                io_str = f" ({'; '.join(io_parts)})" if io_parts else ""
                lines.append(f"    # postamble{io_str}")
                for line in post.code.split("\n"):
                    lines.append("    " + line)

        for handler in te.handlers:
            hloc = self._loc_comment(handler.location if handler.HasField("location") else None)
            if not handler.exception_types or (
                len(handler.exception_types) == 1
                and not handler.exception_types[0].HasField("name")
            ):
                lines.append(f"except:{hloc}")
            else:
                types = []
                for et in handler.exception_types:
                    if et.HasField("module"):
                        types.append(f"{et.module}.{et.name}")
                    elif et.HasField("name"):
                        types.append(et.name)
                if len(types) == 1:
                    lines.append(f"except {types[0]}:{hloc}")
                else:
                    lines.append(f"except ({', '.join(types)}):{hloc}")

            # Serialize handler preamble
            if handler.preamble:
                for pre in handler.preamble:
                    io_parts = []
                    if pre.inputs:
                        io_parts.append(f"reads: {', '.join(pre.inputs)}")
                    if pre.outputs:
                        io_parts.append(f"writes: {', '.join(pre.outputs)}")
                    io_str = f" ({'; '.join(io_parts)})" if io_parts else ""
                    lines.append(f"    # preamble{io_str}")
                    for line in pre.code.split("\n"):
                        lines.append("    " + line)

            # Serialize handler body actions
            for action in handler.body:
                action_lines = self._serialize_action(action)
                for line in action_lines:
                    lines.append("    " + line)

            # Serialize handler postamble
            if handler.postamble:
                for post in handler.postamble:
                    io_parts = []
                    if post.inputs:
                        io_parts.append(f"reads: {', '.join(post.inputs)}")
                    if post.outputs:
                        io_parts.append(f"writes: {', '.join(post.outputs)}")
                    io_str = f" ({'; '.join(io_parts)})" if io_parts else ""
                    lines.append(f"    # postamble{io_str}")
                    for line in post.code.split("\n"):
                        lines.append("    " + line)

        return lines

    def _serialize_sleep(self, sleep: ir_pb2.Sleep) -> list[str]:
        loc = self._loc_comment(sleep.location if sleep.HasField("location") else None)
        duration_str = _expression_to_string(sleep.duration) if sleep.HasField("duration") else "?"
        return [f"@sleep({duration_str}){loc}"]

    def _serialize_return(self, ret: ir_pb2.Return) -> list[str]:
        loc = self._loc_comment(ret.location if ret.HasField("location") else None)
        value_kind = ret.WhichOneof("value")
        if value_kind is None:
            return [f"return{loc}"]
        if value_kind == "expression":
            return [f"return {_expression_to_string(ret.expression)}{loc}"]
        if value_kind == "action":
            action_lines = self._serialize_action(ret.action)
            return [f"return {action_lines[0]}"]
        if value_kind == "gather":
            gather_lines = self._serialize_gather(ret.gather)
            return [f"return {gather_lines[0]}"]
        # Defensive: handle unknown return value kinds for forward compatibility.
        return [f"return{loc}"]

    def _serialize_spread(self, spread: ir_pb2.Spread) -> list[str]:
        action = spread.action
        args_str = ", ".join(
            f"{arg.name}={_expression_to_string(arg.value)}" for arg in action.args
        )
        module_prefix = f"{action.module}." if action.HasField("module") else ""
        call_str = f"@{module_prefix}{action.action}({args_str})"
        loc = self._loc_comment(spread.location if spread.HasField("location") else None)
        iterable_str = (
            _expression_to_string(spread.iterable) if spread.HasField("iterable") else "?"
        )
        if spread.HasField("target"):
            return [
                f"{spread.target} = spread {call_str} over {iterable_str} as {spread.loop_var}{loc}"
            ]
        # Spread without target - parallel action over collection without collecting results.
        # This is a valid pattern when the actions have side effects but results aren't needed.
        return [f"spread {call_str} over {iterable_str} as {spread.loop_var}{loc}"]
