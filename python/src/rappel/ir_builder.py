"""
IR Builder - Converts Python workflow AST to Rappel IR (ast.proto).

This module parses Python workflow classes and produces the IR representation
that can be sent to the Rust runtime for execution.

The IR builder performs deep transformations to convert Python patterns into
valid Rappel IR structures. Control flow bodies (try, for, if branches) are
represented as full blocks, so they can contain arbitrary statements and
`return` behaves consistently with Python (returns from the entry function end
the workflow).

Validation:
The IR builder proactively detects unsupported Python patterns and raises
UnsupportedPatternError with clear recommendations for how to rewrite the code.
"""

from __future__ import annotations

import ast
import copy
import inspect
import textwrap
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Union

from proto import ast_pb2 as ir


class UnsupportedPatternError(Exception):
    """Raised when the IR builder encounters an unsupported Python pattern.

    This error includes a recommendation for how to rewrite the code to use
    supported patterns.
    """

    def __init__(
        self,
        message: str,
        recommendation: str,
        line: Optional[int] = None,
        col: Optional[int] = None,
        filename: Optional[str] = None,
    ):
        self.message = message
        self.recommendation = recommendation
        self.line = line
        self.col = col
        self.filename = filename

        location_parts: List[str] = []
        if filename:
            location_parts.append(filename)
        if line:
            location_parts.append(f"line {line}")
        if col is not None:
            location_parts.append(f"col {col}")
        location = f" ({', '.join(location_parts)})" if location_parts else ""
        full_message = f"{message}{location}\n\nRecommendation: {recommendation}"
        super().__init__(full_message)


# Recommendations for common unsupported patterns
RECOMMENDATIONS = {
    "constructor_return": (
        "Returning a class constructor (like MyModel(...)) directly is not supported.\n"
        "The workflow IR cannot serialize arbitrary object instantiation.\n\n"
        "Use an @action to create the object:\n\n"
        "    @action\n"
        "    async def build_result(items: list, count: int) -> MyResult:\n"
        "        return MyResult(items=items, count=count)\n\n"
        "    # In workflow:\n"
        "    return await build_result(items, count)"
    ),
    "constructor_assignment": (
        "Assigning a class constructor result (like x = MyClass(...)) is not supported.\n"
        "The workflow IR cannot serialize arbitrary object instantiation.\n\n"
        "Use an @action to create the object:\n\n"
        "    @action\n"
        "    async def create_config(value: int) -> Config:\n"
        "        return Config(value=value)\n\n"
        "    # In workflow:\n"
        "    config = await create_config(value)"
    ),
    "non_action_call": (
        "Calling a function that is not decorated with @action is not supported.\n"
        "Only @action decorated functions can be awaited in workflow code.\n\n"
        "Add the @action decorator to your function:\n\n"
        "    @action\n"
        "    async def my_function(x: int) -> int:\n"
        "        return x * 2"
    ),
    "sync_function_call": (
        "Calling a synchronous function directly in workflow code is not supported.\n"
        "All computation must happen inside @action decorated async functions.\n\n"
        "Wrap your logic in an @action:\n\n"
        "    @action\n"
        "    async def compute(x: int) -> int:\n"
        "        return some_sync_function(x)"
    ),
    "method_call_non_self": (
        "Calling methods on objects other than 'self' is not supported in workflow code.\n"
        "Use an @action to perform method calls:\n\n"
        "    @action\n"
        "    async def call_method(obj: MyClass) -> Result:\n"
        "        return obj.some_method()"
    ),
    "builtin_call": (
        "Calling built-in functions like len(), str(), int() directly is not supported.\n"
        "Use an @action to perform these operations:\n\n"
        "    @action\n"
        "    async def get_length(items: list) -> int:\n"
        "        return len(items)"
    ),
    "fstring": (
        "F-strings are not supported in workflow code because they require "
        "runtime string interpolation.\n"
        "Use an @action to perform string formatting:\n\n"
        "    @action\n"
        "    async def format_message(value: int) -> str:\n"
        "        return f'Result: {value}'"
    ),
    "delete": (
        "The 'del' statement is not supported in workflow code.\n"
        "Use an @action to perform mutations:\n\n"
        "    @action\n"
        "    async def remove_key(data: dict, key: str) -> dict:\n"
        "        del data[key]\n"
        "        return data"
    ),
    "while_loop": (
        "While loops are not supported in workflow code because they can run "
        "indefinitely.\n"
        "Use a for loop with a fixed range, or restructure as recursive workflow calls."
    ),
    "with_statement": (
        "Context managers (with statements) are not supported in workflow code.\n"
        "Use an @action to handle resource management:\n\n"
        "    @action\n"
        "    async def read_file(path: str) -> str:\n"
        "        with open(path) as f:\n"
        "            return f.read()"
    ),
    "raise_statement": (
        "The 'raise' statement is not supported directly in workflow code.\n"
        "Use an @action that raises exceptions, or return error values."
    ),
    "assert_statement": (
        "Assert statements are not supported in workflow code.\n"
        "Use an @action for validation, or use if statements with explicit error handling."
    ),
    "lambda": (
        "Lambda expressions are not supported in workflow code.\n"
        "Use an @action to define the function logic."
    ),
    "list_comprehension": (
        "List comprehensions are only supported when assigned directly to a variable "
        "or inside asyncio.gather(*[...]).\n"
        "For other cases, use a for loop or an @action."
    ),
    "dict_comprehension": (
        "Dict comprehensions are only supported when assigned directly to a variable.\n"
        "For other cases, use a for loop or an @action:\n\n"
        "    @action\n"
        "    async def build_dict(items: list) -> dict:\n"
        "        return {k: v for k, v in items}"
    ),
    "set_comprehension": (
        "Set comprehensions are not supported in workflow code.\nUse an @action to build sets."
    ),
    "generator": (
        "Generator expressions are not supported in workflow code.\n"
        "Use a list or an @action instead."
    ),
    "walrus": (
        "The walrus operator (:=) is not supported in workflow code.\n"
        "Use separate assignment statements instead."
    ),
    "match": (
        "Match statements are not supported in workflow code.\nUse if/elif/else chains instead."
    ),
    "gather_variable_spread": (
        "Spreading a variable in asyncio.gather() is not supported because it requires "
        "data flow analysis to determine the contents.\n"
        "Use a list comprehension directly in gather:\n\n"
        "    # Instead of:\n"
        "    tasks = []\n"
        "    for i in range(count):\n"
        "        tasks.append(process(value=i))\n"
        "    results = await asyncio.gather(*tasks)\n\n"
        "    # Use:\n"
        "    results = await asyncio.gather(*[process(value=i) for i in range(count)])"
    ),
    "for_loop_append_pattern": (
        "Building a task list in a for loop then spreading in asyncio.gather() is not "
        "supported.\n"
        "Use a list comprehension directly in gather:\n\n"
        "    # Instead of:\n"
        "    tasks = []\n"
        "    for i in range(count):\n"
        "        tasks.append(process(value=i))\n"
        "    results = await asyncio.gather(*tasks)\n\n"
        "    # Use:\n"
        "    results = await asyncio.gather(*[process(value=i) for i in range(count)])"
    ),
    "global_statement": (
        "Global statements are not supported in workflow code.\n"
        "Use workflow state or pass values explicitly."
    ),
    "nonlocal_statement": (
        "Nonlocal statements are not supported in workflow code.\n"
        "Use explicit parameter passing instead."
    ),
    "import_statement": (
        "Import statements inside workflow run() are not supported.\n"
        "Place imports at the module level."
    ),
    "class_def": (
        "Class definitions inside workflow run() are not supported.\n"
        "Define classes at the module level."
    ),
    "function_def": (
        "Nested function definitions inside workflow run() are not supported.\n"
        "Define functions at the module level or use @action."
    ),
    "yield_statement": (
        "Yield statements are not supported in workflow code.\n"
        "Workflows must return a complete result, not generate values incrementally."
    ),
    "unsupported_expression": (
        "This expression type is not supported in workflow code.\n"
        "Move the logic into an @action or rewrite using supported expressions."
    ),
    "unsupported_literal": (
        "This literal type is not supported in workflow code.\n"
        "Convert the value to a supported literal type inside an @action."
    ),
}


if TYPE_CHECKING:
    from .workflow import Workflow


@dataclass
class ActionDefinition:
    """Definition of an action function."""

    action_name: str
    module_name: Optional[str]
    signature: inspect.Signature


@dataclass
class ModuleContext:
    """Cached IRBuilder context derived from a module."""

    action_defs: Dict[str, ActionDefinition]
    imported_names: Dict[str, "ImportedName"]
    module_functions: Set[str]
    model_defs: Dict[str, "ModelDefinition"]


@dataclass
class TransformContext:
    """Context for IR transformations."""

    # Counter for generating unique function names
    implicit_fn_counter: int = 0
    # Implicit functions generated during transformation
    implicit_functions: List[ir.FunctionDef] = None  # type: ignore

    def __post_init__(self) -> None:
        if self.implicit_functions is None:
            self.implicit_functions = []

    def next_implicit_fn_name(self, prefix: str = "implicit") -> str:
        """Generate a unique implicit function name."""
        self.implicit_fn_counter += 1
        return f"__{prefix}_{self.implicit_fn_counter}__"


def build_workflow_ir(workflow_cls: type["Workflow"]) -> ir.Program:
    """Build an IR Program from a workflow class.

    Args:
        workflow_cls: The workflow class to convert.

    Returns:
        An IR Program proto message.
    """
    original_run = getattr(workflow_cls, "__workflow_run_impl__", None)
    if original_run is None:
        original_run = workflow_cls.__dict__.get("run")
    if original_run is None:
        raise ValueError(f"workflow {workflow_cls!r} missing run implementation")

    module = inspect.getmodule(original_run)
    if module is None:
        raise ValueError(f"unable to locate module for workflow {workflow_cls!r}")

    module_contexts: Dict[str, ModuleContext] = {}

    def get_module_context(target_module: Any) -> ModuleContext:
        module_name = target_module.__name__
        if module_name not in module_contexts:
            module_contexts[module_name] = ModuleContext(
                action_defs=_discover_action_names(target_module),
                imported_names=_discover_module_imports(target_module),
                module_functions=_discover_module_functions(target_module),
                model_defs=_discover_model_definitions(target_module),
            )
        return module_contexts[module_name]

    # Build the IR with transformation context
    ctx = TransformContext()
    program = ir.Program()
    function_defs: Dict[str, ir.FunctionDef] = {}

    def parse_function(fn: Any) -> tuple[ast.AST, Optional[str], int]:
        source_lines, start_line = inspect.getsourcelines(fn)
        function_source = textwrap.dedent("".join(source_lines))
        filename = inspect.getsourcefile(fn)
        if filename is None:
            filename = inspect.getfile(fn)
        return ast.parse(function_source, filename=filename or "<unknown>"), filename, start_line

    def _with_source_location(
        err: UnsupportedPatternError,
        filename: Optional[str],
        start_line: int,
    ) -> UnsupportedPatternError:
        line = err.line
        col = err.col
        if line is not None:
            line = start_line + line - 1
        if col is not None:
            col = col + 1
        return UnsupportedPatternError(
            err.message,
            err.recommendation,
            line=line,
            col=col,
            filename=filename,
        )

    def add_function_def(
        fn: Any,
        fn_tree: ast.AST,
        filename: Optional[str],
        start_line: int,
        override_name: Optional[str] = None,
    ) -> None:
        fn_module = inspect.getmodule(fn)
        if fn_module is None:
            raise ValueError(f"unable to locate module for function {fn!r}")

        ctx_data = get_module_context(fn_module)
        builder = IRBuilder(
            ctx_data.action_defs,
            ctx,
            ctx_data.imported_names,
            ctx_data.module_functions,
            ctx_data.model_defs,
        )
        try:
            builder.visit(fn_tree)
        except UnsupportedPatternError as err:
            raise _with_source_location(err, filename, start_line) from err
        if builder.function_def:
            if override_name:
                builder.function_def.name = override_name
            function_defs[builder.function_def.name] = builder.function_def

    # Build the entry function from run() and map it to main in the IR.
    run_tree, run_filename, run_start_line = parse_function(original_run)
    add_function_def(original_run, run_tree, run_filename, run_start_line, "main")

    # Include helper methods reachable via self.method() calls
    pending = list(_collect_self_method_calls(run_tree))
    visited: Set[str] = set()
    skip_methods = {"run_action"}

    while pending:
        method_name = pending.pop()
        if method_name in visited or method_name == "run" or method_name in skip_methods:
            continue
        visited.add(method_name)

        method = _find_workflow_method(workflow_cls, method_name)
        if method is None:
            continue

        method_tree, method_filename, method_start_line = parse_function(method)
        add_function_def(method, method_tree, method_filename, method_start_line)

        pending.extend(_collect_self_method_calls(method_tree))

    # Add implicit functions first (they may be called by the main function)
    for implicit_fn in ctx.implicit_functions:
        program.functions.append(implicit_fn)

    # Add all function definitions (run + reachable helper methods)
    for fn_def in function_defs.values():
        program.functions.append(fn_def)

    return program


def _collect_self_method_calls(tree: ast.AST) -> Set[str]:
    """Collect self.method(...) call names from a parsed function AST."""
    calls: Set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
            if func.value.id == "self":
                calls.add(func.attr)
    return calls


def _find_workflow_method(workflow_cls: type["Workflow"], name: str) -> Optional[Any]:
    """Find a workflow method by name across the class MRO."""
    for base in workflow_cls.__mro__:
        if name not in base.__dict__:
            continue
        value = base.__dict__[name]
        if isinstance(value, staticmethod) or isinstance(value, classmethod):
            return value.__func__
        if inspect.isfunction(value):
            return value
    return None


def _discover_action_names(module: Any) -> Dict[str, ActionDefinition]:
    """Discover all @action decorated functions in a module."""
    names: Dict[str, ActionDefinition] = {}
    for attr_name in dir(module):
        attr = getattr(module, attr_name)
        action_name = getattr(attr, "__rappel_action_name__", None)
        action_module = getattr(attr, "__rappel_action_module__", None)
        if callable(attr) and action_name:
            signature = inspect.signature(attr)
            names[attr_name] = ActionDefinition(
                action_name=action_name,
                module_name=action_module or module.__name__,
                signature=signature,
            )
    return names


def _discover_module_functions(module: Any) -> Set[str]:
    """Discover all async function names defined in a module.

    This is used to detect when users await functions in the same module
    that are NOT decorated with @action.
    """
    function_names: Set[str] = set()
    for attr_name in dir(module):
        try:
            attr = getattr(module, attr_name)
        except AttributeError:
            continue

        # Only include functions defined in THIS module (not imported)
        if not callable(attr):
            continue
        if not inspect.iscoroutinefunction(attr):
            continue

        # Check if the function is defined in this module
        func_module = getattr(attr, "__module__", None)
        if func_module == module.__name__:
            function_names.add(attr_name)

    return function_names


@dataclass
class ImportedName:
    """Tracks an imported name and its source module."""

    local_name: str  # Name used in code (e.g., "sleep")
    module: str  # Source module (e.g., "asyncio")
    original_name: str  # Original name in source module (e.g., "sleep")


@dataclass
class ModelFieldDefinition:
    """Definition of a field in a Pydantic model or dataclass."""

    name: str
    has_default: bool
    default_value: Any = None  # Only set if has_default is True


@dataclass
class ModelDefinition:
    """Definition of a Pydantic model or dataclass that can be used in workflows.

    These are data classes that can be instantiated in workflow code and will
    be converted to dictionary expressions in the IR.
    """

    class_name: str
    fields: Dict[str, ModelFieldDefinition]
    is_pydantic: bool  # True for Pydantic models, False for dataclasses


def _is_simple_pydantic_model(cls: type) -> bool:
    """Check if a class is a simple Pydantic model without custom validators.

    A simple Pydantic model:
    - Inherits from pydantic.BaseModel
    - Has no field_validator or model_validator decorators
    - Has no custom __init__ method

    Returns False if pydantic is not installed or cls is not a Pydantic model.
    """
    try:
        from pydantic import BaseModel
    except ImportError:
        return False

    if not isinstance(cls, type) or not issubclass(cls, BaseModel):
        return False

    # Check for validators - Pydantic v2 uses __pydantic_decorators__
    decorators = getattr(cls, "__pydantic_decorators__", None)
    if decorators is not None:
        # Check for field validators
        if hasattr(decorators, "field_validators") and decorators.field_validators:
            return False
        # Check for model validators
        if hasattr(decorators, "model_validators") and decorators.model_validators:
            return False

    # Check for custom __init__ (not the one from BaseModel)
    if "__init__" in cls.__dict__:
        return False

    return True


def _is_simple_dataclass(cls: type) -> bool:
    """Check if a class is a simple dataclass without custom logic.

    A simple dataclass:
    - Is decorated with @dataclass
    - Has no custom __init__ method (uses the generated one)
    - Has no __post_init__ method

    Returns False if cls is not a dataclass.
    """
    import dataclasses

    if not dataclasses.is_dataclass(cls):
        return False

    # Check for __post_init__ which could have custom logic
    if hasattr(cls, "__post_init__") and "__post_init__" in cls.__dict__:
        return False

    # Dataclasses generate __init__, so check if there's a custom one
    # that overrides it (unlikely but possible)
    # The dataclass decorator sets __init__ so we can't easily detect override
    # We'll trust that dataclasses without __post_init__ are simple

    return True


def _get_pydantic_model_fields(cls: type) -> Dict[str, ModelFieldDefinition]:
    """Extract field definitions from a Pydantic model."""
    try:
        from pydantic import BaseModel
        from pydantic.fields import FieldInfo
    except ImportError:
        return {}

    if not issubclass(cls, BaseModel):
        return {}

    fields: Dict[str, ModelFieldDefinition] = {}

    # Pydantic v2 uses model_fields
    model_fields = getattr(cls, "model_fields", {})
    for field_name, field_info in model_fields.items():
        has_default = False
        default_value = None

        if isinstance(field_info, FieldInfo):
            # Check if field has a default value
            # PydanticUndefined means no default
            from pydantic_core import PydanticUndefined

            if field_info.default is not PydanticUndefined:
                has_default = True
                default_value = field_info.default
            elif field_info.default_factory is not None:
                # We can't serialize factory functions, so treat as no default
                has_default = False

        fields[field_name] = ModelFieldDefinition(
            name=field_name,
            has_default=has_default,
            default_value=default_value,
        )

    return fields


def _get_dataclass_fields(cls: type) -> Dict[str, ModelFieldDefinition]:
    """Extract field definitions from a dataclass."""
    import dataclasses

    if not dataclasses.is_dataclass(cls):
        return {}

    fields: Dict[str, ModelFieldDefinition] = {}
    for field in dataclasses.fields(cls):
        has_default = False
        default_value = None

        if field.default is not dataclasses.MISSING:
            has_default = True
            default_value = field.default
        elif field.default_factory is not dataclasses.MISSING:
            # We can't serialize factory functions, so treat as no default
            has_default = False

        fields[field.name] = ModelFieldDefinition(
            name=field.name,
            has_default=has_default,
            default_value=default_value,
        )

    return fields


def _discover_model_definitions(module: Any) -> Dict[str, ModelDefinition]:
    """Discover all Pydantic models and dataclasses that can be used in workflows.

    Only discovers "simple" models without custom validators or __post_init__.
    """
    models: Dict[str, ModelDefinition] = {}

    for attr_name in dir(module):
        try:
            attr = getattr(module, attr_name)
        except AttributeError:
            continue

        if not isinstance(attr, type):
            continue

        # Check if this class is defined in this module or imported
        # We want to include both, as models might be imported
        if _is_simple_pydantic_model(attr):
            fields = _get_pydantic_model_fields(attr)
            models[attr_name] = ModelDefinition(
                class_name=attr_name,
                fields=fields,
                is_pydantic=True,
            )
        elif _is_simple_dataclass(attr):
            fields = _get_dataclass_fields(attr)
            models[attr_name] = ModelDefinition(
                class_name=attr_name,
                fields=fields,
                is_pydantic=False,
            )

    return models


def _discover_module_imports(module: Any) -> Dict[str, ImportedName]:
    """Discover imports in a module by parsing its source.

    Tracks imports like:
    - from asyncio import sleep  -> {"sleep": ImportedName("sleep", "asyncio", "sleep")}
    - from asyncio import sleep as s -> {"s": ImportedName("s", "asyncio", "sleep")}
    """
    imported: Dict[str, ImportedName] = {}

    try:
        source = inspect.getsource(module)
        tree = ast.parse(source)
    except (OSError, TypeError):
        # Can't get source (e.g., built-in module)
        return imported

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            for alias in node.names:
                local_name = alias.asname if alias.asname else alias.name
                imported[local_name] = ImportedName(
                    local_name=local_name,
                    module=node.module,
                    original_name=alias.name,
                )

    return imported


class IRBuilder(ast.NodeVisitor):
    """Builds IR from Python AST with deep transformations."""

    def __init__(
        self,
        action_defs: Dict[str, ActionDefinition],
        ctx: TransformContext,
        imported_names: Optional[Dict[str, ImportedName]] = None,
        module_functions: Optional[Set[str]] = None,
        model_defs: Optional[Dict[str, ModelDefinition]] = None,
    ):
        self._action_defs = action_defs
        self._ctx = ctx
        self._imported_names = imported_names or {}
        self._module_functions = module_functions or set()
        self._model_defs = model_defs or {}
        self.function_def: Optional[ir.FunctionDef] = None
        self._statements: List[ir.Statement] = []

    def visit_FunctionDef(self, node: ast.FunctionDef) -> Any:
        """Visit a function definition (the workflow's run method)."""
        inputs = self._collect_function_inputs(node)

        # Create the function definition
        self.function_def = ir.FunctionDef(
            name=node.name,
            io=ir.IoDecl(inputs=inputs, outputs=[]),
            span=_make_span(node),
        )

        # Visit the body - _visit_statement now returns a list
        self._statements = []
        for stmt in node.body:
            ir_stmts = self._visit_statement(stmt)
            self._statements.extend(ir_stmts)

        # Set the body
        self.function_def.body.CopyFrom(ir.Block(statements=self._statements))

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> Any:
        """Visit an async function definition (the workflow's run method)."""
        # Handle async the same way as sync for IR building
        inputs = self._collect_function_inputs(node)

        self.function_def = ir.FunctionDef(
            name=node.name,
            io=ir.IoDecl(inputs=inputs, outputs=[]),
            span=_make_span(node),
        )

        self._statements = []
        for stmt in node.body:
            ir_stmts = self._visit_statement(stmt)
            self._statements.extend(ir_stmts)

        self.function_def.body.CopyFrom(ir.Block(statements=self._statements))

    def _visit_statement(self, node: ast.stmt) -> List[ir.Statement]:
        """Convert a Python statement to IR Statement(s).

        Returns a list because some transformations (like try block hoisting)
        may expand a single Python statement into multiple IR statements.

        Raises UnsupportedPatternError for unsupported statement types.
        """
        if isinstance(node, ast.Assign):
            dict_expanded = self._expand_dict_comprehension_assignment(node)
            if dict_expanded is not None:
                return dict_expanded
            expanded = self._expand_list_comprehension_assignment(node)
            if expanded is not None:
                return expanded
            result = self._visit_assign(node)
            return [result] if result else []
        elif isinstance(node, ast.Expr):
            result = self._visit_expr_stmt(node)
            return [result] if result else []
        elif isinstance(node, ast.For):
            return self._visit_for(node)
        elif isinstance(node, ast.If):
            return self._visit_if(node)
        elif isinstance(node, ast.Try):
            return self._visit_try(node)
        elif isinstance(node, ast.Return):
            return self._visit_return(node)
        elif isinstance(node, ast.AugAssign):
            return self._visit_aug_assign(node)
        elif isinstance(node, ast.Pass):
            # Pass statements are fine, they just don't produce IR
            return []

        # Check for unsupported statement types
        self._check_unsupported_statement(node)

        return []

    def _check_unsupported_statement(self, node: ast.stmt) -> None:
        """Check for unsupported statement types and raise descriptive errors."""
        line = getattr(node, "lineno", None)
        col = getattr(node, "col_offset", None)

        if isinstance(node, ast.While):
            raise UnsupportedPatternError(
                "While loops are not supported",
                RECOMMENDATIONS["while_loop"],
                line=line,
                col=col,
            )
        elif isinstance(node, (ast.With, ast.AsyncWith)):
            raise UnsupportedPatternError(
                "Context managers (with statements) are not supported",
                RECOMMENDATIONS["with_statement"],
                line=line,
                col=col,
            )
        elif isinstance(node, ast.Raise):
            raise UnsupportedPatternError(
                "The 'raise' statement is not supported",
                RECOMMENDATIONS["raise_statement"],
                line=line,
                col=col,
            )
        elif isinstance(node, ast.Assert):
            raise UnsupportedPatternError(
                "Assert statements are not supported",
                RECOMMENDATIONS["assert_statement"],
                line=line,
                col=col,
            )
        elif isinstance(node, ast.Delete):
            raise UnsupportedPatternError(
                "The 'del' statement is not supported",
                RECOMMENDATIONS["delete"],
                line=line,
                col=col,
            )
        elif isinstance(node, ast.Global):
            raise UnsupportedPatternError(
                "Global statements are not supported",
                RECOMMENDATIONS["global_statement"],
                line=line,
                col=col,
            )
        elif isinstance(node, ast.Nonlocal):
            raise UnsupportedPatternError(
                "Nonlocal statements are not supported",
                RECOMMENDATIONS["nonlocal_statement"],
                line=line,
                col=col,
            )
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            raise UnsupportedPatternError(
                "Import statements inside workflow run() are not supported",
                RECOMMENDATIONS["import_statement"],
                line=line,
                col=col,
            )
        elif isinstance(node, ast.ClassDef):
            raise UnsupportedPatternError(
                "Class definitions inside workflow run() are not supported",
                RECOMMENDATIONS["class_def"],
                line=line,
                col=col,
            )
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            raise UnsupportedPatternError(
                "Nested function definitions are not supported",
                RECOMMENDATIONS["function_def"],
                line=line,
                col=col,
            )
        elif hasattr(ast, "Match") and isinstance(node, ast.Match):
            raise UnsupportedPatternError(
                "Match statements are not supported",
                RECOMMENDATIONS["match"],
                line=line,
                col=col,
            )

    def _expand_list_comprehension_assignment(
        self, node: ast.Assign
    ) -> Optional[List[ir.Statement]]:
        """Expand a list comprehension assignment into loop-based statements.

        Example:
            active_users = [user for user in users if user.active]

        Becomes:
            active_users = []
            for user in users:
                if user.active:
                    active_users = active_users + [user]
        """
        if not isinstance(node.value, ast.ListComp):
            return None

        if len(node.targets) != 1 or not isinstance(node.targets[0], ast.Name):
            line = getattr(node, "lineno", None)
            col = getattr(node, "col_offset", None)
            raise UnsupportedPatternError(
                "List comprehension assignments must target a single variable",
                "Assign the comprehension to a simple variable like `results = [x for x in items]`",
                line=line,
                col=col,
            )

        listcomp = node.value
        if len(listcomp.generators) != 1:
            line = getattr(listcomp, "lineno", None)
            col = getattr(listcomp, "col_offset", None)
            raise UnsupportedPatternError(
                "List comprehensions with multiple generators are not supported",
                "Use nested for loops instead of combining multiple generators in one comprehension",
                line=line,
                col=col,
            )

        gen = listcomp.generators[0]
        if gen.is_async:
            line = getattr(listcomp, "lineno", None)
            col = getattr(listcomp, "col_offset", None)
            raise UnsupportedPatternError(
                "Async list comprehensions are not supported",
                "Rewrite using an explicit async for loop",
                line=line,
                col=col,
            )

        target_name = node.targets[0].id

        # Initialize the accumulator list: active_users = []
        init_assign_ast = ast.Assign(
            targets=[ast.Name(id=target_name, ctx=ast.Store())],
            value=ast.List(elts=[], ctx=ast.Load()),
            type_comment=None,
        )
        ast.copy_location(init_assign_ast, node)
        ast.fix_missing_locations(init_assign_ast)

        def _make_append_assignment(value_expr: ast.expr) -> ast.Assign:
            append_assign = ast.Assign(
                targets=[ast.Name(id=target_name, ctx=ast.Store())],
                value=ast.BinOp(
                    left=ast.Name(id=target_name, ctx=ast.Load()),
                    op=ast.Add(),
                    right=ast.List(elts=[copy.deepcopy(value_expr)], ctx=ast.Load()),
                ),
                type_comment=None,
            )
            ast.copy_location(append_assign, node.value)
            ast.fix_missing_locations(append_assign)
            return append_assign

        append_statements: List[ast.stmt] = []
        if isinstance(listcomp.elt, ast.IfExp):
            then_assign = _make_append_assignment(listcomp.elt.body)
            else_assign = _make_append_assignment(listcomp.elt.orelse)
            branch_if = ast.If(
                test=copy.deepcopy(listcomp.elt.test),
                body=[then_assign],
                orelse=[else_assign],
            )
            ast.copy_location(branch_if, listcomp.elt)
            ast.fix_missing_locations(branch_if)
            append_statements.append(branch_if)
        else:
            append_statements.append(_make_append_assignment(listcomp.elt))

        loop_body: List[ast.stmt] = append_statements
        if gen.ifs:
            condition: ast.expr
            if len(gen.ifs) == 1:
                condition = copy.deepcopy(gen.ifs[0])
            else:
                condition = ast.BoolOp(op=ast.And(), values=[copy.deepcopy(iff) for iff in gen.ifs])
                ast.copy_location(condition, gen.ifs[0])
            if_stmt = ast.If(test=condition, body=append_statements, orelse=[])
            ast.copy_location(if_stmt, gen.ifs[0])
            ast.fix_missing_locations(if_stmt)
            loop_body = [if_stmt]

        loop_ast = ast.For(
            target=copy.deepcopy(gen.target),
            iter=copy.deepcopy(gen.iter),
            body=loop_body,
            orelse=[],
            type_comment=None,
        )
        ast.copy_location(loop_ast, node)
        ast.fix_missing_locations(loop_ast)

        statements: List[ir.Statement] = []
        init_stmt = self._visit_assign(init_assign_ast)
        if init_stmt:
            statements.append(init_stmt)
        statements.extend(self._visit_for(loop_ast))

        return statements

    def _expand_dict_comprehension_assignment(
        self, node: ast.Assign
    ) -> Optional[List[ir.Statement]]:
        """Expand a dict comprehension assignment into loop-based statements."""
        if not isinstance(node.value, ast.DictComp):
            return None

        if len(node.targets) != 1 or not isinstance(node.targets[0], ast.Name):
            line = getattr(node, "lineno", None)
            col = getattr(node, "col_offset", None)
            raise UnsupportedPatternError(
                "Dict comprehension assignments must target a single variable",
                "Assign the comprehension to a simple variable like `result = {k: v for k, v in pairs}`",
                line=line,
                col=col,
            )

        dictcomp = node.value
        if len(dictcomp.generators) != 1:
            line = getattr(dictcomp, "lineno", None)
            col = getattr(dictcomp, "col_offset", None)
            raise UnsupportedPatternError(
                "Dict comprehensions with multiple generators are not supported",
                "Use nested for loops instead of combining multiple generators in one comprehension",
                line=line,
                col=col,
            )

        gen = dictcomp.generators[0]
        if gen.is_async:
            line = getattr(dictcomp, "lineno", None)
            col = getattr(dictcomp, "col_offset", None)
            raise UnsupportedPatternError(
                "Async dict comprehensions are not supported",
                "Rewrite using an explicit async for loop",
                line=line,
                col=col,
            )

        target_name = node.targets[0].id

        # Initialize accumulator: result = {}
        init_assign_ast = ast.Assign(
            targets=[ast.Name(id=target_name, ctx=ast.Store())],
            value=ast.Dict(keys=[], values=[]),
            type_comment=None,
        )
        ast.copy_location(init_assign_ast, node)
        ast.fix_missing_locations(init_assign_ast)

        # result[key] = value
        subscript_target = ast.Subscript(
            value=ast.Name(id=target_name, ctx=ast.Load()),
            slice=copy.deepcopy(dictcomp.key),
            ctx=ast.Store(),
        )
        append_assign_ast = ast.Assign(
            targets=[subscript_target],
            value=copy.deepcopy(dictcomp.value),
            type_comment=None,
        )
        ast.copy_location(append_assign_ast, node.value)
        ast.fix_missing_locations(append_assign_ast)

        loop_body: List[ast.stmt] = []
        if gen.ifs:
            condition: ast.expr
            if len(gen.ifs) == 1:
                condition = copy.deepcopy(gen.ifs[0])
            else:
                condition = ast.BoolOp(op=ast.And(), values=[copy.deepcopy(iff) for iff in gen.ifs])
                ast.copy_location(condition, gen.ifs[0])
            if_stmt = ast.If(test=condition, body=[append_assign_ast], orelse=[])
            ast.copy_location(if_stmt, gen.ifs[0])
            ast.fix_missing_locations(if_stmt)
            loop_body.append(if_stmt)
        else:
            loop_body.append(append_assign_ast)

        loop_ast = ast.For(
            target=copy.deepcopy(gen.target),
            iter=copy.deepcopy(gen.iter),
            body=loop_body,
            orelse=[],
            type_comment=None,
        )
        ast.copy_location(loop_ast, node)
        ast.fix_missing_locations(loop_ast)

        statements: List[ir.Statement] = []
        init_stmt = self._visit_assign(init_assign_ast)
        if init_stmt:
            statements.append(init_stmt)
        statements.extend(self._visit_for(loop_ast))

        return statements

    def _visit_assign(self, node: ast.Assign) -> Optional[ir.Statement]:
        """Convert assignment to IR.

        All assignments with targets use the Assignment statement type.
        This provides uniform unpacking support for:
        - Action calls: a, b = @get_pair()
        - Parallel blocks: a, b = parallel: @x() @y()
        - Regular expressions: a, b = some_list

        Raises UnsupportedPatternError for:
        - Constructor calls: x = MyClass(...)
        - Non-action await: x = await some_func()
        """
        stmt = ir.Statement(span=_make_span(node))
        targets = self._get_assign_targets(node.targets)

        # Check for Pydantic model or dataclass constructor calls
        # These are converted to dict expressions
        model_name = self._is_model_constructor(node.value)
        if model_name and isinstance(node.value, ast.Call):
            value_expr = self._convert_model_constructor_to_dict(node.value, model_name)
            assign = ir.Assignment(targets=targets, value=value_expr)
            stmt.assignment.CopyFrom(assign)
            return stmt

        # Check for constructor calls in assignment (e.g., x = MyModel(...))
        # This must come AFTER the model constructor check since models are allowed
        self._check_constructor_in_assignment(node.value)

        # Check for asyncio.gather() - convert to parallel or spread expression
        if isinstance(node.value, ast.Await) and isinstance(node.value.value, ast.Call):
            gather_call = node.value.value
            if self._is_asyncio_gather_call(gather_call):
                gather_result = self._convert_asyncio_gather(gather_call)
                if gather_result is not None:
                    if isinstance(gather_result, ir.ParallelExpr):
                        value = ir.Expr(parallel_expr=gather_result, span=_make_span(node))
                    else:
                        # SpreadExpr
                        value = ir.Expr(spread_expr=gather_result, span=_make_span(node))
                    assign = ir.Assignment(targets=targets, value=value)
                    stmt.assignment.CopyFrom(assign)
                    return stmt

        # Check if this is an action call - wrap in Assignment for uniform unpacking
        action_call = self._extract_action_call(node.value)
        if action_call:
            value = ir.Expr(action_call=action_call, span=_make_span(node))
            assign = ir.Assignment(targets=targets, value=value)
            stmt.assignment.CopyFrom(assign)
            return stmt

        # Regular assignment (variables, literals, expressions)
        value_expr = _expr_to_ir(node.value)
        if value_expr:
            assign = ir.Assignment(targets=targets, value=value_expr)
            stmt.assignment.CopyFrom(assign)
            return stmt

        return None

    def _visit_expr_stmt(self, node: ast.Expr) -> Optional[ir.Statement]:
        """Convert expression statement to IR (side effect only, no assignment)."""
        stmt = ir.Statement(span=_make_span(node))

        # Check for asyncio.gather() - convert to parallel block statement (side effect)
        if isinstance(node.value, ast.Await) and isinstance(node.value.value, ast.Call):
            gather_call = node.value.value
            if self._is_asyncio_gather_call(gather_call):
                gather_result = self._convert_asyncio_gather(gather_call)
                if gather_result is not None:
                    if isinstance(gather_result, ir.ParallelExpr):
                        # Side effect only - use ParallelBlock statement
                        parallel = ir.ParallelBlock()
                        parallel.calls.extend(gather_result.calls)
                        stmt.parallel_block.CopyFrom(parallel)
                        return stmt
                    else:
                        # SpreadExpr as side effect - wrap in assignment with no targets
                        # This handles: await asyncio.gather(*[action(x) for x in items])
                        value = ir.Expr(spread_expr=gather_result, span=_make_span(node))
                        assign = ir.Assignment(targets=[], value=value)
                        stmt.assignment.CopyFrom(assign)
                        return stmt

        # Check if this is an action call (side effect only)
        action_call = self._extract_action_call(node.value)
        if action_call:
            stmt.action_call.CopyFrom(action_call)
            return stmt

        # Convert list.append(x) to list = list + [x]
        # This makes the mutation explicit so data flows correctly through the DAG
        if isinstance(node.value, ast.Call):
            call = node.value
            if (
                isinstance(call.func, ast.Attribute)
                and call.func.attr == "append"
                and isinstance(call.func.value, ast.Name)
                and len(call.args) == 1
            ):
                list_name = call.func.value.id
                append_value = call.args[0]
                # Create: list = list + [value]
                list_var = ir.Expr(variable=ir.Variable(name=list_name), span=_make_span(node))
                value_expr = _expr_to_ir(append_value)
                if value_expr:
                    # Create [value] as a list literal
                    list_literal = ir.Expr(
                        list=ir.ListExpr(elements=[value_expr]), span=_make_span(node)
                    )
                    # Create list + [value]
                    concat_expr = ir.Expr(
                        binary_op=ir.BinaryOp(
                            op=ir.BinaryOperator.BINARY_OP_ADD, left=list_var, right=list_literal
                        ),
                        span=_make_span(node),
                    )
                    assign = ir.Assignment(targets=[list_name], value=concat_expr)
                    stmt.assignment.CopyFrom(assign)
                    return stmt

        # Regular expression
        expr = _expr_to_ir(node.value)
        if expr:
            stmt.expr_stmt.CopyFrom(ir.ExprStmt(expr=expr))
            return stmt

        return None

    def _visit_for(self, node: ast.For) -> List[ir.Statement]:
        """Convert for loop to IR.

        The loop body is emitted as a full block so it can contain multiple
        statements/calls and early `return`.
        """
        # Get loop variables
        loop_vars: List[str] = []
        if isinstance(node.target, ast.Name):
            loop_vars.append(node.target.id)
        elif isinstance(node.target, ast.Tuple):
            for elt in node.target.elts:
                if isinstance(elt, ast.Name):
                    loop_vars.append(elt.id)

        # Get iterable
        iterable = _expr_to_ir(node.iter)
        if not iterable:
            return []

        # Build body statements (recursively transforms nested structures)
        body_stmts: List[ir.Statement] = []
        for body_node in node.body:
            stmts = self._visit_statement(body_node)
            body_stmts.extend(stmts)

        stmt = ir.Statement(span=_make_span(node))
        for_loop = ir.ForLoop(
            loop_vars=loop_vars,
            iterable=iterable,
            block_body=ir.Block(statements=body_stmts, span=_make_span(node)),
        )
        stmt.for_loop.CopyFrom(for_loop)
        return [stmt]

    def _detect_accumulator_targets(
        self, stmts: List[ir.Statement], in_scope_vars: set
    ) -> List[str]:
        """Detect out-of-scope variable modifications in for loop body.

        Scans statements for patterns that modify variables defined outside the loop.
        Returns a list of accumulator variable names that should be set as targets.

        Supported patterns:
        1. List append: results.append(value) -> "results"
        2. Dict subscript: result[key] = value -> "result"
        3. List/set update methods: results.extend(...), results.add(...) -> "results"

        Note: Patterns like `results = results + [x]` and `count = count + 1` create
        new assignments which are tracked via in_scope_vars and don't need special
        detection here - they're handled by the regular assignment target logic.
        """
        accumulators: List[str] = []
        seen: set = set()

        for stmt in stmts:
            var_name = self._extract_accumulator_from_stmt(stmt, in_scope_vars)
            if var_name and var_name not in seen:
                accumulators.append(var_name)
                seen.add(var_name)

            # Check conditionals for accumulator targets in branch bodies
            if stmt.HasField("conditional"):
                cond = stmt.conditional
                branch_blocks: list[ir.Block] = []
                if cond.HasField("if_branch") and cond.if_branch.HasField("block_body"):
                    branch_blocks.append(cond.if_branch.block_body)
                for branch in cond.elif_branches:
                    if branch.HasField("block_body"):
                        branch_blocks.append(branch.block_body)
                if cond.HasField("else_branch") and cond.else_branch.HasField("block_body"):
                    branch_blocks.append(cond.else_branch.block_body)

                for block in branch_blocks:
                    for var in self._detect_accumulator_targets(
                        list(block.statements), in_scope_vars
                    ):
                        if var not in seen:
                            accumulators.append(var)
                            seen.add(var)

        return accumulators

    def _extract_accumulator_from_stmt(
        self, stmt: ir.Statement, in_scope_vars: set
    ) -> Optional[str]:
        """Extract accumulator variable name from a single statement.

        Returns the variable name if this statement modifies an out-of-scope variable,
        None otherwise.
        """
        # Pattern 1: Method calls like list.append(), dict.update(), set.add()
        if stmt.HasField("expr_stmt"):
            expr = stmt.expr_stmt.expr
            if expr.HasField("function_call"):
                fn_name = expr.function_call.name
                # Check for mutating method calls: x.append, x.extend, x.add, x.update, etc.
                mutating_methods = {
                    ".append",
                    ".extend",
                    ".add",
                    ".update",
                    ".insert",
                    ".pop",
                    ".remove",
                    ".clear",
                }
                for method in mutating_methods:
                    if fn_name.endswith(method):
                        var_name = fn_name[: len(fn_name) - len(method)]
                        # Only return if it's an out-of-scope variable
                        if var_name and var_name not in in_scope_vars:
                            return var_name

        # Pattern 2: Subscript assignment like dict[key] = value
        if stmt.HasField("assignment"):
            for target in stmt.assignment.targets:
                # Check if target is a subscript pattern (contains '[')
                if "[" in target:
                    # Extract base variable name (before '[')
                    var_name = target.split("[")[0]
                    if var_name and var_name not in in_scope_vars:
                        return var_name

        # Pattern 3: Self-referential assignment like x = x + [y]
        # The target variable is used on the RHS, so it must come from outside.
        # Note: We don't check in_scope_vars here because the assignment itself
        # would have added the target to in_scope_vars, but it still needs its
        # previous value from outside the loop body.
        if stmt.HasField("assignment"):
            assign = stmt.assignment
            rhs_vars = self._collect_variables_from_expr(assign.value)
            for target in assign.targets:
                if target in rhs_vars:
                    return target

        return None

    def _collect_variables_from_expr(self, expr: ir.Expr) -> set:
        """Recursively collect all variable names used in an expression."""
        vars_found: set = set()

        if expr.HasField("variable"):
            vars_found.add(expr.variable.name)
        elif expr.HasField("binary_op"):
            vars_found.update(self._collect_variables_from_expr(expr.binary_op.left))
            vars_found.update(self._collect_variables_from_expr(expr.binary_op.right))
        elif expr.HasField("unary_op"):
            vars_found.update(self._collect_variables_from_expr(expr.unary_op.operand))
        elif expr.HasField("list"):
            for elem in expr.list.elements:
                vars_found.update(self._collect_variables_from_expr(elem))
        elif expr.HasField("dict"):
            for entry in expr.dict.entries:
                vars_found.update(self._collect_variables_from_expr(entry.key))
                vars_found.update(self._collect_variables_from_expr(entry.value))
        elif expr.HasField("index"):
            vars_found.update(self._collect_variables_from_expr(expr.index.value))
            vars_found.update(self._collect_variables_from_expr(expr.index.index))
        elif expr.HasField("dot"):
            vars_found.update(self._collect_variables_from_expr(expr.dot.object))
        elif expr.HasField("function_call"):
            for kwarg in expr.function_call.kwargs:
                vars_found.update(self._collect_variables_from_expr(kwarg.value))
        elif expr.HasField("action_call"):
            for kwarg in expr.action_call.kwargs:
                vars_found.update(self._collect_variables_from_expr(kwarg.value))

        return vars_found

    def _visit_if(self, node: ast.If) -> List[ir.Statement]:
        """Convert if statement to IR.

        Normalizes patterns like:
            if await some_action(...):
                ...
        into:
            __if_cond_n__ = await some_action(...)
            if __if_cond_n__:
                ...
        """

        def normalize_condition(test: ast.expr) -> tuple[List[ir.Statement], Optional[ir.Expr]]:
            action_call = self._extract_action_call(test)
            if action_call is None:
                return ([], _expr_to_ir(test))

            if not isinstance(test, ast.Await):
                line = getattr(test, "lineno", None)
                col = getattr(test, "col_offset", None)
                raise UnsupportedPatternError(
                    "Action calls inside boolean expressions are not supported in if conditions",
                    "Assign the awaited action result to a variable, then use the variable in the if condition.",
                    line=line,
                    col=col,
                )

            cond_var = self._ctx.next_implicit_fn_name(prefix="if_cond")
            assign_stmt = ir.Statement(span=_make_span(test))
            assign_stmt.assignment.CopyFrom(
                ir.Assignment(
                    targets=[cond_var],
                    value=ir.Expr(action_call=action_call, span=_make_span(test)),
                )
            )
            cond_expr = ir.Expr(variable=ir.Variable(name=cond_var), span=_make_span(test))
            return ([assign_stmt], cond_expr)

        def visit_body(nodes: list[ast.stmt]) -> List[ir.Statement]:
            stmts: List[ir.Statement] = []
            for body_node in nodes:
                stmts.extend(self._visit_statement(body_node))
            return stmts

        # Collect if/elif branches as (test_expr, body_nodes)
        branches: list[tuple[ast.expr, list[ast.stmt], ast.AST]] = [(node.test, node.body, node)]
        current = node
        while current.orelse and len(current.orelse) == 1 and isinstance(current.orelse[0], ast.If):
            elif_node = current.orelse[0]
            branches.append((elif_node.test, elif_node.body, elif_node))
            current = elif_node

        else_nodes = current.orelse

        normalized: list[
            tuple[List[ir.Statement], Optional[ir.Expr], List[ir.Statement], ast.AST]
        ] = []
        for test_expr, body_nodes, span_node in branches:
            prefix, cond = normalize_condition(test_expr)
            normalized.append((prefix, cond, visit_body(body_nodes), span_node))

        else_body = visit_body(else_nodes) if else_nodes else []

        # If any non-first branch needs normalization, preserve Python semantics by nesting.
        requires_nested = any(prefix for prefix, _, _, _ in normalized[1:])

        def build_conditional_stmt(
            condition: ir.Expr,
            then_body: List[ir.Statement],
            else_body_statements: List[ir.Statement],
            span_node: ast.AST,
        ) -> ir.Statement:
            conditional_stmt = ir.Statement(span=_make_span(span_node))
            if_branch = ir.IfBranch(
                condition=condition,
                block_body=ir.Block(statements=then_body, span=_make_span(span_node)),
                span=_make_span(span_node),
            )
            conditional = ir.Conditional(if_branch=if_branch)
            if else_body_statements:
                else_branch = ir.ElseBranch(
                    block_body=ir.Block(
                        statements=else_body_statements,
                        span=_make_span(span_node),
                    ),
                    span=_make_span(span_node),
                )
                conditional.else_branch.CopyFrom(else_branch)
            conditional_stmt.conditional.CopyFrom(conditional)
            return conditional_stmt

        if requires_nested:
            nested_else: List[ir.Statement] = else_body
            for prefix, cond, then_body, span_node in reversed(normalized):
                if cond is None:
                    continue
                nested_if_stmt = build_conditional_stmt(
                    condition=cond,
                    then_body=then_body,
                    else_body_statements=nested_else,
                    span_node=span_node,
                )
                nested_else = [*prefix, nested_if_stmt]
            return nested_else

        # Flat conditional with elif/else (original behavior), plus optional prefix for the if guard.
        if_prefix, if_condition, if_body, if_span_node = normalized[0]
        if if_condition is None:
            return []

        conditional_stmt = ir.Statement(span=_make_span(if_span_node))
        if_branch = ir.IfBranch(
            condition=if_condition,
            block_body=ir.Block(statements=if_body, span=_make_span(if_span_node)),
            span=_make_span(if_span_node),
        )
        conditional = ir.Conditional(if_branch=if_branch)

        for _, elif_condition, elif_body, elif_span_node in normalized[1:]:
            if elif_condition is None:
                continue
            elif_branch = ir.ElifBranch(
                condition=elif_condition,
                block_body=ir.Block(statements=elif_body, span=_make_span(elif_span_node)),
                span=_make_span(elif_span_node),
            )
            conditional.elif_branches.append(elif_branch)

        if else_body:
            else_branch = ir.ElseBranch(
                block_body=ir.Block(statements=else_body, span=_make_span(if_span_node)),
                span=_make_span(if_span_node),
            )
            conditional.else_branch.CopyFrom(else_branch)

        conditional_stmt.conditional.CopyFrom(conditional)
        return [*if_prefix, conditional_stmt]

    def _collect_assigned_vars(self, stmts: List[ir.Statement]) -> set:
        """Collect all variable names assigned in a list of statements."""
        assigned = set()
        for stmt in stmts:
            if stmt.HasField("assignment"):
                assigned.update(stmt.assignment.targets)
        return assigned

    def _collect_assigned_vars_in_order(self, stmts: List[ir.Statement]) -> list[str]:
        """Collect assigned variable names in statement order (deduplicated)."""
        assigned: list[str] = []
        seen: set[str] = set()

        for stmt in stmts:
            if stmt.HasField("assignment"):
                for target in stmt.assignment.targets:
                    if target not in seen:
                        seen.add(target)
                        assigned.append(target)

            if stmt.HasField("conditional"):
                cond = stmt.conditional
                if cond.HasField("if_branch") and cond.if_branch.HasField("block_body"):
                    for target in self._collect_assigned_vars_in_order(
                        list(cond.if_branch.block_body.statements)
                    ):
                        if target not in seen:
                            seen.add(target)
                            assigned.append(target)
                for elif_branch in cond.elif_branches:
                    if elif_branch.HasField("block_body"):
                        for target in self._collect_assigned_vars_in_order(
                            list(elif_branch.block_body.statements)
                        ):
                            if target not in seen:
                                seen.add(target)
                                assigned.append(target)
                if cond.HasField("else_branch") and cond.else_branch.HasField("block_body"):
                    for target in self._collect_assigned_vars_in_order(
                        list(cond.else_branch.block_body.statements)
                    ):
                        if target not in seen:
                            seen.add(target)
                            assigned.append(target)

            if stmt.HasField("for_loop") and stmt.for_loop.HasField("block_body"):
                for target in self._collect_assigned_vars_in_order(
                    list(stmt.for_loop.block_body.statements)
                ):
                    if target not in seen:
                        seen.add(target)
                        assigned.append(target)

            if stmt.HasField("try_except"):
                try_block = stmt.try_except.try_block
                if try_block.HasField("span"):
                    for target in self._collect_assigned_vars_in_order(list(try_block.statements)):
                        if target not in seen:
                            seen.add(target)
                            assigned.append(target)
                for handler in stmt.try_except.handlers:
                    if handler.HasField("block_body"):
                        for target in self._collect_assigned_vars_in_order(
                            list(handler.block_body.statements)
                        ):
                            if target not in seen:
                                seen.add(target)
                                assigned.append(target)

        return assigned

    def _collect_variables_from_block(self, block: ir.Block) -> list[str]:
        return self._collect_variables_from_statements(list(block.statements))

    def _collect_variables_from_statements(self, stmts: List[ir.Statement]) -> list[str]:
        """Collect variable references from statements in encounter order."""
        vars_found: list[str] = []
        seen: set[str] = set()

        for stmt in stmts:
            if stmt.HasField("assignment") and stmt.assignment.HasField("value"):
                for var in self._collect_variables_from_expr(stmt.assignment.value):
                    if var not in seen:
                        seen.add(var)
                        vars_found.append(var)

            if stmt.HasField("return_stmt") and stmt.return_stmt.HasField("value"):
                for var in self._collect_variables_from_expr(stmt.return_stmt.value):
                    if var not in seen:
                        seen.add(var)
                        vars_found.append(var)

            if stmt.HasField("action_call"):
                expr = ir.Expr(action_call=stmt.action_call, span=stmt.span)
                for var in self._collect_variables_from_expr(expr):
                    if var not in seen:
                        seen.add(var)
                        vars_found.append(var)

            if stmt.HasField("expr_stmt"):
                for var in self._collect_variables_from_expr(stmt.expr_stmt.expr):
                    if var not in seen:
                        seen.add(var)
                        vars_found.append(var)

            if stmt.HasField("conditional"):
                cond = stmt.conditional
                if cond.HasField("if_branch"):
                    if cond.if_branch.HasField("condition"):
                        for var in self._collect_variables_from_expr(cond.if_branch.condition):
                            if var not in seen:
                                seen.add(var)
                                vars_found.append(var)
                    if cond.if_branch.HasField("block_body"):
                        for var in self._collect_variables_from_block(cond.if_branch.block_body):
                            if var not in seen:
                                seen.add(var)
                                vars_found.append(var)
                for elif_branch in cond.elif_branches:
                    if elif_branch.HasField("condition"):
                        for var in self._collect_variables_from_expr(elif_branch.condition):
                            if var not in seen:
                                seen.add(var)
                                vars_found.append(var)
                    if elif_branch.HasField("block_body"):
                        for var in self._collect_variables_from_block(elif_branch.block_body):
                            if var not in seen:
                                seen.add(var)
                                vars_found.append(var)
                if cond.HasField("else_branch") and cond.else_branch.HasField("block_body"):
                    for var in self._collect_variables_from_block(cond.else_branch.block_body):
                        if var not in seen:
                            seen.add(var)
                            vars_found.append(var)

            if stmt.HasField("for_loop"):
                fl = stmt.for_loop
                if fl.HasField("iterable"):
                    for var in self._collect_variables_from_expr(fl.iterable):
                        if var not in seen:
                            seen.add(var)
                            vars_found.append(var)
                if fl.HasField("block_body"):
                    for var in self._collect_variables_from_block(fl.block_body):
                        if var not in seen:
                            seen.add(var)
                            vars_found.append(var)

            if stmt.HasField("try_except"):
                te = stmt.try_except
                if te.HasField("try_block"):
                    for var in self._collect_variables_from_block(te.try_block):
                        if var not in seen:
                            seen.add(var)
                            vars_found.append(var)
                for handler in te.handlers:
                    if handler.HasField("block_body"):
                        for var in self._collect_variables_from_block(handler.block_body):
                            if var not in seen:
                                seen.add(var)
                                vars_found.append(var)

            if stmt.HasField("parallel_block"):
                for call in stmt.parallel_block.calls:
                    if call.HasField("action"):
                        for kwarg in call.action.kwargs:
                            for var in self._collect_variables_from_expr(kwarg.value):
                                if var not in seen:
                                    seen.add(var)
                                    vars_found.append(var)
                    elif call.HasField("function"):
                        for kwarg in call.function.kwargs:
                            for var in self._collect_variables_from_expr(kwarg.value):
                                if var not in seen:
                                    seen.add(var)
                                    vars_found.append(var)

            if stmt.HasField("spread_action"):
                spread = stmt.spread_action
                if spread.HasField("collection"):
                    for var in self._collect_variables_from_expr(spread.collection):
                        if var not in seen:
                            seen.add(var)
                            vars_found.append(var)
                if spread.HasField("action"):
                    for kwarg in spread.action.kwargs:
                        for var in self._collect_variables_from_expr(kwarg.value):
                            if var not in seen:
                                seen.add(var)
                                vars_found.append(var)

        return vars_found

    def _visit_try(self, node: ast.Try) -> List[ir.Statement]:
        """Convert try/except to IR with full block bodies."""
        # Build try body statements (recursively transforms nested structures)
        try_body: List[ir.Statement] = []
        for body_node in node.body:
            stmts = self._visit_statement(body_node)
            try_body.extend(stmts)

        # Build exception handlers (with wrapping if needed)
        handlers: List[ir.ExceptHandler] = []
        for handler in node.handlers:
            exception_types: List[str] = []
            if handler.type:
                if isinstance(handler.type, ast.Name):
                    exception_types.append(handler.type.id)
                elif isinstance(handler.type, ast.Tuple):
                    for elt in handler.type.elts:
                        if isinstance(elt, ast.Name):
                            exception_types.append(elt.id)

            # Build handler body (recursively transforms nested structures)
            handler_body: List[ir.Statement] = []
            for handler_node in handler.body:
                stmts = self._visit_statement(handler_node)
                handler_body.extend(stmts)

            except_handler = ir.ExceptHandler(
                exception_types=exception_types,
                block_body=ir.Block(statements=handler_body, span=_make_span(handler)),
                span=_make_span(handler),
            )
            handlers.append(except_handler)

        # Build the try/except statement
        try_stmt = ir.Statement(span=_make_span(node))
        try_except = ir.TryExcept(
            handlers=handlers,
            try_block=ir.Block(statements=try_body, span=_make_span(node)),
        )
        try_stmt.try_except.CopyFrom(try_except)

        return [try_stmt]

    def _count_calls(self, stmts: List[ir.Statement]) -> int:
        """Count action calls and function calls in statements.

        Both action calls and function calls (including synthetic functions)
        count toward the limit of one call per control flow body.
        """
        count = 0
        for stmt in stmts:
            if stmt.HasField("action_call"):
                count += 1
            elif stmt.HasField("assignment"):
                # Check if assignment value is an action call or function call
                if stmt.assignment.value.HasField("action_call"):
                    count += 1
                elif stmt.assignment.value.HasField("function_call"):
                    count += 1
            elif stmt.HasField("expr_stmt"):
                # Check if expression is a function call
                if stmt.expr_stmt.expr.HasField("function_call"):
                    count += 1
        return count

    def _wrap_body_as_function(
        self,
        body: List[ir.Statement],
        prefix: str,
        node: ast.AST,
        inputs: Optional[List[str]] = None,
        modified_vars: Optional[List[str]] = None,
    ) -> List[ir.Statement]:
        """Wrap a body with multiple calls into a synthetic function.

        Args:
            body: The statements to wrap
            prefix: Name prefix for the synthetic function
            node: AST node for span information
            inputs: Variables to pass as inputs (e.g., loop variables)
            modified_vars: Out-of-scope variables modified in the body.
                          These are added as inputs AND returned as outputs,
                          enabling functional transformation of external state.

        Returns a list containing a single function call statement (or assignment
        if modified_vars are present).
        """
        fn_name = self._ctx.next_implicit_fn_name(prefix)
        fn_inputs = list(inputs or [])

        # Add modified variables as inputs (they need to be passed in)
        modified_vars = modified_vars or []
        for var in modified_vars:
            if var not in fn_inputs:
                fn_inputs.append(var)

        # If there are modified variables, add a return statement for them
        wrapped_body = list(body)
        if modified_vars:
            # Create return statement: return (var1, var2, ...) or return var1
            if len(modified_vars) == 1:
                return_expr = ir.Expr(
                    variable=ir.Variable(name=modified_vars[0]),
                    span=_make_span(node),
                )
            else:
                # Return as list (tuples are represented as lists in IR)
                return_expr = ir.Expr(
                    list=ir.ListExpr(
                        elements=[ir.Expr(variable=ir.Variable(name=var)) for var in modified_vars]
                    ),
                    span=_make_span(node),
                )
            return_stmt = ir.Statement(span=_make_span(node))
            return_stmt.return_stmt.CopyFrom(ir.ReturnStmt(value=return_expr))
            wrapped_body.append(return_stmt)

        # Create the synthetic function
        implicit_fn = ir.FunctionDef(
            name=fn_name,
            io=ir.IoDecl(inputs=fn_inputs, outputs=modified_vars),
            body=ir.Block(statements=wrapped_body),
            span=_make_span(node),
        )
        self._ctx.implicit_functions.append(implicit_fn)

        # Create a function call expression
        kwargs = [
            ir.Kwarg(name=var, value=ir.Expr(variable=ir.Variable(name=var))) for var in fn_inputs
        ]
        fn_call_expr = ir.Expr(
            function_call=ir.FunctionCall(name=fn_name, kwargs=kwargs),
            span=_make_span(node),
        )

        # If there are modified variables, create an assignment statement
        # so the returned values are assigned back to the variables
        call_stmt = ir.Statement(span=_make_span(node))
        if modified_vars:
            # Create assignment: var1, var2 = fn(...) or var1 = fn(...)
            assign = ir.Assignment(value=fn_call_expr)
            assign.targets.extend(modified_vars)
            call_stmt.assignment.CopyFrom(assign)
        else:
            call_stmt.expr_stmt.CopyFrom(ir.ExprStmt(expr=fn_call_expr))

        return [call_stmt]

    def _visit_return(self, node: ast.Return) -> List[ir.Statement]:
        """Convert return statement to IR.

        Return statements should only contain variables or literals, not action calls.
        If the return contains an action call, we normalize it:
            return await action()
        becomes:
            _return_tmp = await action()
            return _return_tmp

        Constructor calls (like return MyModel(...)) are not supported and will
        raise an error with a recommendation to use an @action instead.
        """
        if node.value:
            # Check for constructor calls in return (e.g., return MyModel(...))
            self._check_constructor_in_return(node.value)

            # Check if returning an action call - normalize to assignment + return
            action_call = self._extract_action_call(node.value)
            if action_call:
                # Create a temporary variable for the action result
                tmp_var = "_return_tmp"

                # Create assignment: _return_tmp = await action()
                assign_stmt = ir.Statement(span=_make_span(node))
                value = ir.Expr(action_call=action_call, span=_make_span(node))
                assign = ir.Assignment(targets=[tmp_var], value=value)
                assign_stmt.assignment.CopyFrom(assign)

                # Create return: return _return_tmp
                return_stmt = ir.Statement(span=_make_span(node))
                var_expr = ir.Expr(variable=ir.Variable(name=tmp_var), span=_make_span(node))
                ret = ir.ReturnStmt(value=var_expr)
                return_stmt.return_stmt.CopyFrom(ret)

                return [assign_stmt, return_stmt]

            # Normalize return of function calls into assignment + return
            expr = _expr_to_ir(node.value)
            if expr and expr.HasField("function_call"):
                tmp_var = self._ctx.next_implicit_fn_name(prefix="return_tmp")

                assign_stmt = ir.Statement(span=_make_span(node))
                assign_stmt.assignment.CopyFrom(ir.Assignment(targets=[tmp_var], value=expr))

                return_stmt = ir.Statement(span=_make_span(node))
                var_expr = ir.Expr(variable=ir.Variable(name=tmp_var), span=_make_span(node))
                return_stmt.return_stmt.CopyFrom(ir.ReturnStmt(value=var_expr))
                return [assign_stmt, return_stmt]

            # Regular return with expression (variable, literal, etc.)
            if expr:
                stmt = ir.Statement(span=_make_span(node))
                return_stmt = ir.ReturnStmt(value=expr)
                stmt.return_stmt.CopyFrom(return_stmt)
                return [stmt]

        # Return with no value
        stmt = ir.Statement(span=_make_span(node))
        stmt.return_stmt.CopyFrom(ir.ReturnStmt())
        return [stmt]

    def _visit_aug_assign(self, node: ast.AugAssign) -> List[ir.Statement]:
        """Convert augmented assignment (+=, -=, etc.) to IR."""
        # For now, we can represent this as a regular assignment with binary op
        # target op= value  ->  target = target op value
        stmt = ir.Statement(span=_make_span(node))

        targets: List[str] = []
        if isinstance(node.target, ast.Name):
            targets.append(node.target.id)

        left = _expr_to_ir(node.target)
        right = _expr_to_ir(node.value)
        if right and right.HasField("function_call"):
            tmp_var = self._ctx.next_implicit_fn_name(prefix="aug_tmp")

            assign_tmp = ir.Statement(span=_make_span(node))
            assign_tmp.assignment.CopyFrom(
                ir.Assignment(
                    targets=[tmp_var],
                    value=ir.Expr(function_call=right.function_call, span=_make_span(node)),
                )
            )

            if left:
                op = _bin_op_to_ir(node.op)
                if op:
                    binary = ir.BinaryOp(
                        left=left,
                        op=op,
                        right=ir.Expr(variable=ir.Variable(name=tmp_var)),
                    )
                    value = ir.Expr(binary_op=binary)
                    assign = ir.Assignment(targets=targets, value=value)
                    stmt.assignment.CopyFrom(assign)
                    return [assign_tmp, stmt]
            return [assign_tmp]

        if left and right:
            op = _bin_op_to_ir(node.op)
            if op:
                binary = ir.BinaryOp(left=left, op=op, right=right)
                value = ir.Expr(binary_op=binary)
                assign = ir.Assignment(targets=targets, value=value)
                stmt.assignment.CopyFrom(assign)
                return [stmt]

        return []

    def _collect_function_inputs(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> List[str]:
        """Collect workflow inputs from function parameters, including kw-only args."""
        args: List[str] = []
        seen: set[str] = set()

        ordered_args = list(node.args.posonlyargs) + list(node.args.args)
        if ordered_args and ordered_args[0].arg == "self":
            ordered_args = ordered_args[1:]

        for arg in ordered_args:
            if arg.arg not in seen:
                args.append(arg.arg)
                seen.add(arg.arg)

        if node.args.vararg and node.args.vararg.arg not in seen:
            args.append(node.args.vararg.arg)
            seen.add(node.args.vararg.arg)

        for arg in node.args.kwonlyargs:
            if arg.arg not in seen:
                args.append(arg.arg)
                seen.add(arg.arg)

        if node.args.kwarg and node.args.kwarg.arg not in seen:
            args.append(node.args.kwarg.arg)
            seen.add(node.args.kwarg.arg)

        return args

    def _check_constructor_in_return(self, node: ast.expr) -> None:
        """Check for constructor calls in return statements.

        Raises UnsupportedPatternError if the return value is a class instantiation
        like: return MyModel(field=value)

        This is not supported because the workflow IR cannot serialize arbitrary
        object instantiation. Users should use an @action to create objects.
        """
        # Skip if it's an await (action call) - those are fine
        if isinstance(node, ast.Await):
            return

        # Check for direct Call that looks like a constructor
        if isinstance(node, ast.Call):
            func_name = self._get_constructor_name(node.func)
            if func_name and self._looks_like_constructor(func_name, node):
                line = getattr(node, "lineno", None)
                col = getattr(node, "col_offset", None)
                raise UnsupportedPatternError(
                    f"Returning constructor call '{func_name}(...)' is not supported",
                    RECOMMENDATIONS["constructor_return"],
                    line=line,
                    col=col,
                )

    def _check_constructor_in_assignment(self, node: ast.expr) -> None:
        """Check for constructor calls in assignments.

        Raises UnsupportedPatternError if the assignment value is a class instantiation
        like: result = MyModel(field=value)

        This is not supported because the workflow IR cannot serialize arbitrary
        object instantiation. Users should use an @action to create objects.
        """
        # Skip if it's an await (action call) - those are fine
        if isinstance(node, ast.Await):
            return

        # Check for direct Call that looks like a constructor
        if isinstance(node, ast.Call):
            func_name = self._get_constructor_name(node.func)
            if func_name and self._looks_like_constructor(func_name, node):
                line = getattr(node, "lineno", None)
                col = getattr(node, "col_offset", None)
                raise UnsupportedPatternError(
                    f"Assigning constructor call '{func_name}(...)' is not supported",
                    RECOMMENDATIONS["constructor_assignment"],
                    line=line,
                    col=col,
                )

    def _get_constructor_name(self, func: ast.expr) -> Optional[str]:
        """Get the name from a function expression if it looks like a constructor."""
        if isinstance(func, ast.Name):
            return func.id
        elif isinstance(func, ast.Attribute):
            return func.attr
        return None

    def _looks_like_constructor(self, func_name: str, call: ast.Call) -> bool:
        """Check if a function call looks like a class constructor.

        A constructor is identified by:
        1. Name starts with uppercase (PEP8 convention for classes)
        2. It's not a known action
        3. It's not a known builtin like String operations
        4. It's not a known Pydantic model or dataclass (those are allowed)

        This is a heuristic - we can't perfectly distinguish constructors
        from functions without full type information.
        """
        # Check if first letter is uppercase (class naming convention)
        if not func_name or not func_name[0].isupper():
            return False

        # If it's a known action, it's not a constructor
        if func_name in self._action_defs:
            return False

        # If it's a known Pydantic model or dataclass, allow it
        # (it will be converted to a dict expression)
        if func_name in self._model_defs:
            return False

        # Common builtins that start with uppercase but aren't constructors
        # (these are rarely used in workflow code but let's be safe)
        builtin_exceptions = {"True", "False", "None", "Ellipsis"}
        if func_name in builtin_exceptions:
            return False

        return True

    def _is_model_constructor(self, node: ast.expr) -> Optional[str]:
        """Check if an expression is a Pydantic model or dataclass constructor call.

        Returns the model name if it is, None otherwise.
        """
        if not isinstance(node, ast.Call):
            return None

        func_name = self._get_constructor_name(node.func)
        if func_name and func_name in self._model_defs:
            return func_name

        return None

    def _convert_model_constructor_to_dict(self, node: ast.Call, model_name: str) -> ir.Expr:
        """Convert a Pydantic model or dataclass constructor call to a dict expression.

        For example:
            MyModel(field1=value1, field2=value2)
        becomes:
            {"field1": value1, "field2": value2}

        Default values from the model definition are included for fields not
        explicitly provided in the constructor call.
        """
        model_def = self._model_defs[model_name]
        entries: List[ir.DictEntry] = []

        # Track which fields were explicitly provided
        provided_fields: Set[str] = set()

        # First, add all explicitly provided kwargs
        for kw in node.keywords:
            if kw.arg is None:
                # **kwargs expansion - not supported
                line = getattr(node, "lineno", None)
                col = getattr(node, "col_offset", None)
                raise UnsupportedPatternError(
                    f"Model constructor '{model_name}' with **kwargs is not supported",
                    "Use explicit keyword arguments instead of **kwargs.",
                    line=line,
                    col=col,
                )

            provided_fields.add(kw.arg)
            key_expr = ir.Expr()
            key_literal = ir.Literal()
            key_literal.string_value = kw.arg
            key_expr.literal.CopyFrom(key_literal)

            value_expr = _expr_to_ir(kw.value)
            if value_expr is None:
                # If we can't convert the value, we need to raise an error
                line = getattr(node, "lineno", None)
                col = getattr(node, "col_offset", None)
                raise UnsupportedPatternError(
                    f"Cannot convert value for field '{kw.arg}' in '{model_name}'",
                    "Use simpler expressions (literals, variables, dicts, lists).",
                    line=line,
                    col=col,
                )

            entries.append(ir.DictEntry(key=key_expr, value=value_expr))

        # Handle positional arguments (dataclasses support this)
        if node.args:
            # For dataclasses, positional args map to fields in order
            field_names = list(model_def.fields.keys())
            for i, arg in enumerate(node.args):
                if i >= len(field_names):
                    line = getattr(node, "lineno", None)
                    col = getattr(node, "col_offset", None)
                    raise UnsupportedPatternError(
                        f"Too many positional arguments for '{model_name}'",
                        "Use keyword arguments for clarity.",
                        line=line,
                        col=col,
                    )

                field_name = field_names[i]
                provided_fields.add(field_name)

                key_expr = ir.Expr()
                key_literal = ir.Literal()
                key_literal.string_value = field_name
                key_expr.literal.CopyFrom(key_literal)

                value_expr = _expr_to_ir(arg)
                if value_expr is None:
                    line = getattr(node, "lineno", None)
                    col = getattr(node, "col_offset", None)
                    raise UnsupportedPatternError(
                        f"Cannot convert positional argument for field '{field_name}' in '{model_name}'",
                        "Use simpler expressions (literals, variables, dicts, lists).",
                        line=line,
                        col=col,
                    )

                entries.append(ir.DictEntry(key=key_expr, value=value_expr))

        # Add default values for fields not explicitly provided
        for field_name, field_def in model_def.fields.items():
            if field_name in provided_fields:
                continue

            if field_def.has_default:
                key_expr = ir.Expr()
                key_literal = ir.Literal()
                key_literal.string_value = field_name
                key_expr.literal.CopyFrom(key_literal)

                # Convert the default value to an IR literal
                default_literal = _constant_to_literal(field_def.default_value)
                if default_literal is None:
                    # Can't serialize this default - skip it
                    # (it's probably a complex object like a list factory)
                    continue

                value_expr = ir.Expr()
                value_expr.literal.CopyFrom(default_literal)

                entries.append(ir.DictEntry(key=key_expr, value=value_expr))

        result = ir.Expr(span=_make_span(node))
        result.dict.CopyFrom(ir.DictExpr(entries=entries))
        return result

    def _check_non_action_await(self, node: ast.Await) -> None:
        """Check if an await is for a non-action function.

        Note: We can only reliably detect non-action awaits for functions defined
        in the same module. Actions imported from other modules will pass through
        and may fail at runtime if they're not actually actions.

        For now, we only check against common builtins and known non-action patterns.
        A runtime check will catch functions that aren't registered actions.
        """
        awaited = node.value
        if not isinstance(awaited, ast.Call):
            return

        # Skip special cases that are handled elsewhere
        if self._is_run_action_call(awaited):
            return
        if self._is_asyncio_sleep_call(awaited):
            return
        if self._is_asyncio_gather_call(awaited):
            return

        # Get the function name
        func_name = None
        if isinstance(awaited.func, ast.Name):
            func_name = awaited.func.id
        elif isinstance(awaited.func, ast.Attribute):
            func_name = awaited.func.attr

        if not func_name:
            return

        # Only raise error for functions defined in THIS module that we know
        # are NOT actions (i.e., async functions without @action decorator)
        # We can't reliably detect imported non-actions without full type info.
        #
        # The check works by looking at _module_functions which contains
        # functions defined in the same module as the workflow.
        if func_name in getattr(self, "_module_functions", set()):
            if func_name not in self._action_defs:
                line = getattr(node, "lineno", None)
                col = getattr(node, "col_offset", None)
                raise UnsupportedPatternError(
                    f"Awaiting non-action function '{func_name}()' is not supported",
                    RECOMMENDATIONS["non_action_call"],
                    line=line,
                    col=col,
                )

    def _check_sync_function_call(self, node: ast.Call) -> None:
        """Check for synchronous function calls that should be in actions.

        Common patterns like len(), str(), etc. are not supported in workflow code.
        """
        func_name = None
        if isinstance(node.func, ast.Name):
            func_name = node.func.id
        elif isinstance(node.func, ast.Attribute):
            # Method calls on objects - check the method name
            func_name = node.func.attr

        if not func_name:
            return

        # Builtins that users commonly try to use
        common_builtins = {
            "len",
            "str",
            "int",
            "float",
            "bool",
            "list",
            "dict",
            "set",
            "tuple",
            "sum",
            "min",
            "max",
            "sorted",
            "reversed",
            "enumerate",
            "zip",
            "map",
            "filter",
            "range",
            "abs",
            "round",
            "print",
            "type",
            "isinstance",
            "hasattr",
            "getattr",
            "setattr",
            "open",
            "format",
        }

        if func_name in common_builtins:
            line = getattr(node, "lineno", None)
            col = getattr(node, "col_offset", None)
            raise UnsupportedPatternError(
                f"Calling built-in function '{func_name}()' directly is not supported",
                RECOMMENDATIONS["builtin_call"],
                line=line,
                col=col,
            )

    def _extract_action_call(self, node: ast.expr) -> Optional[ir.ActionCall]:
        """Extract an action call from an expression if present.

        Also validates that awaited calls are actually @action decorated functions.
        Raises UnsupportedPatternError if awaiting a non-action function.
        """
        if not isinstance(node, ast.Await):
            return None

        awaited = node.value
        # Handle self.run_action(...) wrapper
        if isinstance(awaited, ast.Call):
            if self._is_run_action_call(awaited):
                # Extract the actual action call from run_action
                if awaited.args:
                    action_call = self._extract_action_call_from_awaitable(awaited.args[0])
                    if action_call:
                        # Extract policies from run_action kwargs (retry, timeout)
                        self._extract_policies_from_run_action(awaited, action_call)
                    return action_call
            # Check for asyncio.sleep() - convert to @sleep action
            if self._is_asyncio_sleep_call(awaited):
                return self._convert_asyncio_sleep_to_action(awaited)
            # Try to extract as action call
            action_call = self._extract_action_call_from_call(awaited)
            if action_call:
                return action_call

            # If we get here, it's an await of a non-action function
            self._check_non_action_await(node)
            return None

        return None

    def _is_run_action_call(self, node: ast.Call) -> bool:
        """Check if this is a self.run_action(...) call."""
        if isinstance(node.func, ast.Attribute):
            return node.func.attr == "run_action"
        return False

    def _extract_policies_from_run_action(
        self, run_action_call: ast.Call, action_call: ir.ActionCall
    ) -> None:
        """Extract retry and timeout policies from run_action kwargs.

        Parses patterns like:
        - self.run_action(action(), retry=RetryPolicy(attempts=3))
        - self.run_action(action(), timeout=timedelta(seconds=30))
        - self.run_action(action(), timeout=60)
        """
        for kw in run_action_call.keywords:
            if kw.arg == "retry":
                retry_policy = self._parse_retry_policy(kw.value)
                if retry_policy:
                    policy_bracket = ir.PolicyBracket()
                    policy_bracket.retry.CopyFrom(retry_policy)
                    action_call.policies.append(policy_bracket)
            elif kw.arg == "timeout":
                timeout_policy = self._parse_timeout_policy(kw.value)
                if timeout_policy:
                    policy_bracket = ir.PolicyBracket()
                    policy_bracket.timeout.CopyFrom(timeout_policy)
                    action_call.policies.append(policy_bracket)

    def _parse_retry_policy(self, node: ast.expr) -> Optional[ir.RetryPolicy]:
        """Parse a RetryPolicy(...) call into IR.

        Supports:
        - RetryPolicy(attempts=3)
        - RetryPolicy(attempts=3, exception_types=["ValueError"])
        - RetryPolicy(attempts=3, backoff_seconds=5)
        """
        if not isinstance(node, ast.Call):
            return None

        # Check if it's a RetryPolicy call
        func_name = None
        if isinstance(node.func, ast.Name):
            func_name = node.func.id
        elif isinstance(node.func, ast.Attribute):
            func_name = node.func.attr

        if func_name != "RetryPolicy":
            return None

        policy = ir.RetryPolicy()

        for kw in node.keywords:
            if kw.arg == "attempts" and isinstance(kw.value, ast.Constant):
                policy.max_retries = kw.value.value
            elif kw.arg == "exception_types" and isinstance(kw.value, ast.List):
                for elt in kw.value.elts:
                    if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                        policy.exception_types.append(elt.value)
            elif kw.arg == "backoff_seconds" and isinstance(kw.value, ast.Constant):
                policy.backoff.seconds = int(kw.value.value)

        return policy

    def _parse_timeout_policy(self, node: ast.expr) -> Optional[ir.TimeoutPolicy]:
        """Parse a timeout value into IR.

        Supports:
        - timeout=60 (int seconds)
        - timeout=30.5 (float seconds)
        - timeout=timedelta(seconds=30)
        - timeout=timedelta(minutes=2)
        """
        policy = ir.TimeoutPolicy()

        # Direct numeric value (seconds)
        if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
            policy.timeout.seconds = int(node.value)
            return policy

        # timedelta(...) call
        if isinstance(node, ast.Call):
            func_name = None
            if isinstance(node.func, ast.Name):
                func_name = node.func.id
            elif isinstance(node.func, ast.Attribute):
                func_name = node.func.attr

            if func_name == "timedelta":
                total_seconds = 0
                for kw in node.keywords:
                    if isinstance(kw.value, ast.Constant):
                        val = kw.value.value
                        if kw.arg == "seconds":
                            total_seconds += int(val)
                        elif kw.arg == "minutes":
                            total_seconds += int(val) * 60
                        elif kw.arg == "hours":
                            total_seconds += int(val) * 3600
                        elif kw.arg == "days":
                            total_seconds += int(val) * 86400
                policy.timeout.seconds = total_seconds
                return policy

        return None

    def _is_asyncio_sleep_call(self, node: ast.Call) -> bool:
        """Check if this is an asyncio.sleep(...) call.

        Supports both patterns:
        - import asyncio; asyncio.sleep(1)
        - from asyncio import sleep; sleep(1)
        - from asyncio import sleep as s; s(1)
        """
        if isinstance(node.func, ast.Attribute):
            # asyncio.sleep(...) pattern
            if node.func.attr == "sleep" and isinstance(node.func.value, ast.Name):
                return node.func.value.id == "asyncio"
        elif isinstance(node.func, ast.Name):
            # sleep(...) pattern - check if it's imported from asyncio
            func_name = node.func.id
            if func_name in self._imported_names:
                imported = self._imported_names[func_name]
                return imported.module == "asyncio" and imported.original_name == "sleep"
        return False

    def _convert_asyncio_sleep_to_action(self, node: ast.Call) -> ir.ActionCall:
        """Convert asyncio.sleep(duration) to @sleep(duration=X) action call.

        This creates a built-in sleep action that the scheduler handles as a
        durable sleep - stored in the DB with a future scheduled_at time.
        """
        action_call = ir.ActionCall(action_name="sleep")

        # Extract duration argument (positional or keyword)
        if node.args:
            # asyncio.sleep(1) - positional
            expr = _expr_to_ir(node.args[0])
            if expr:
                action_call.kwargs.append(ir.Kwarg(name="duration", value=expr))
        elif node.keywords:
            # asyncio.sleep(seconds=1) - keyword (less common)
            for kw in node.keywords:
                if kw.arg in ("seconds", "delay", "duration"):
                    expr = _expr_to_ir(kw.value)
                    if expr:
                        action_call.kwargs.append(ir.Kwarg(name="duration", value=expr))
                    break

        return action_call

    def _is_asyncio_gather_call(self, node: ast.Call) -> bool:
        """Check if this is an asyncio.gather(...) call.

        Supports both patterns:
        - import asyncio; asyncio.gather(a(), b())
        - from asyncio import gather; gather(a(), b())
        - from asyncio import gather as g; g(a(), b())
        """
        if isinstance(node.func, ast.Attribute):
            # asyncio.gather(...) pattern
            if node.func.attr == "gather" and isinstance(node.func.value, ast.Name):
                return node.func.value.id == "asyncio"
        elif isinstance(node.func, ast.Name):
            # gather(...) pattern - check if it's imported from asyncio
            func_name = node.func.id
            if func_name in self._imported_names:
                imported = self._imported_names[func_name]
                return imported.module == "asyncio" and imported.original_name == "gather"
        return False

    def _convert_asyncio_gather(
        self, node: ast.Call
    ) -> Optional[Union[ir.ParallelExpr, ir.SpreadExpr]]:
        """Convert asyncio.gather(...) to ParallelExpr or SpreadExpr IR.

        Handles two patterns:
        1. Static gather: asyncio.gather(a(), b(), c()) -> ParallelExpr
        2. Spread gather: asyncio.gather(*[action(x) for x in items]) -> SpreadExpr

        Args:
            node: The asyncio.gather() Call node

        Returns:
            A ParallelExpr, SpreadExpr, or None if conversion fails.
        """
        # Check for starred expressions - spread pattern
        if len(node.args) == 1 and isinstance(node.args[0], ast.Starred):
            starred = node.args[0]
            # Only list comprehensions are supported for spread
            if isinstance(starred.value, ast.ListComp):
                return self._convert_listcomp_to_spread_expr(starred.value)
            else:
                # Spreading a variable or other expression is not supported
                line = getattr(node, "lineno", None)
                col = getattr(node, "col_offset", None)
                if isinstance(starred.value, ast.Name):
                    var_name = starred.value.id
                    raise UnsupportedPatternError(
                        f"Spreading variable '{var_name}' in asyncio.gather() is not supported",
                        RECOMMENDATIONS["gather_variable_spread"],
                        line=line,
                        col=col,
                    )
                else:
                    raise UnsupportedPatternError(
                        "Spreading non-list-comprehension expressions in asyncio.gather() is not supported",
                        RECOMMENDATIONS["gather_variable_spread"],
                        line=line,
                        col=col,
                    )

        # Standard case: gather(a(), b(), c()) -> ParallelExpr
        parallel = ir.ParallelExpr()

        # Each argument to gather() should be an action call
        for arg in node.args:
            call = self._convert_gather_arg_to_call(arg)
            if call:
                parallel.calls.append(call)

        # Only return if we have calls
        if not parallel.calls:
            return None

        return parallel

    def _convert_listcomp_to_spread_expr(self, listcomp: ast.ListComp) -> Optional[ir.SpreadExpr]:
        """Convert a list comprehension to SpreadExpr IR.

        Handles patterns like:
            [action(x=item) for item in collection]

        The comprehension must have exactly one generator with no conditions,
        and the element must be an action call.

        Args:
            listcomp: The ListComp AST node

        Returns:
            A SpreadExpr, or None if conversion fails.
        """
        # Only support simple list comprehensions with one generator
        if len(listcomp.generators) != 1:
            line = getattr(listcomp, "lineno", None)
            col = getattr(listcomp, "col_offset", None)
            raise UnsupportedPatternError(
                "Spread pattern only supports a single loop variable",
                "Use a simple list comprehension: [action(x) for x in items]",
                line=line,
                col=col,
            )

        gen = listcomp.generators[0]

        # Check for conditions - not supported
        if gen.ifs:
            line = getattr(listcomp, "lineno", None)
            col = getattr(listcomp, "col_offset", None)
            raise UnsupportedPatternError(
                "Spread pattern does not support conditions in list comprehension",
                "Remove the 'if' clause from the comprehension",
                line=line,
                col=col,
            )

        # Get the loop variable name
        if not isinstance(gen.target, ast.Name):
            line = getattr(listcomp, "lineno", None)
            col = getattr(listcomp, "col_offset", None)
            raise UnsupportedPatternError(
                "Spread pattern requires a simple loop variable",
                "Use a simple variable: [action(x) for x in items]",
                line=line,
                col=col,
            )
        loop_var = gen.target.id

        # Get the collection expression
        collection_expr = _expr_to_ir(gen.iter)
        if not collection_expr:
            line = getattr(listcomp, "lineno", None)
            col = getattr(listcomp, "col_offset", None)
            raise UnsupportedPatternError(
                "Could not convert collection expression in spread pattern",
                "Ensure the collection is a simple variable or expression",
                line=line,
                col=col,
            )

        # The element must be an action call
        if not isinstance(listcomp.elt, ast.Call):
            line = getattr(listcomp, "lineno", None)
            col = getattr(listcomp, "col_offset", None)
            raise UnsupportedPatternError(
                "Spread pattern requires an action call in the list comprehension",
                "Use: [action(x=item) for item in items]",
                line=line,
                col=col,
            )

        action_call = self._extract_action_call_from_call(listcomp.elt)
        if not action_call:
            line = getattr(listcomp, "lineno", None)
            col = getattr(listcomp, "col_offset", None)
            raise UnsupportedPatternError(
                "Spread pattern element must be an @action call",
                "Ensure the function is decorated with @action",
                line=line,
                col=col,
            )

        # Build the SpreadExpr
        spread = ir.SpreadExpr()
        spread.collection.CopyFrom(collection_expr)
        spread.loop_var = loop_var
        spread.action.CopyFrom(action_call)

        return spread

    def _convert_gather_arg_to_call(self, node: ast.expr) -> Optional[ir.Call]:
        """Convert a gather argument to an IR Call.

        Handles both action calls and regular function calls.
        """
        if not isinstance(node, ast.Call):
            return None

        # Try to extract as an action call first
        action_call = self._extract_action_call_from_call(node)
        if action_call:
            call = ir.Call()
            call.action.CopyFrom(action_call)
            return call

        # Fall back to regular function call
        func_call = self._convert_to_function_call(node)
        if func_call:
            call = ir.Call()
            call.function.CopyFrom(func_call)
            return call

        return None

    def _convert_to_function_call(self, node: ast.Call) -> Optional[ir.FunctionCall]:
        """Convert an AST Call to IR FunctionCall."""
        func_name = self._get_func_name(node.func)
        if not func_name:
            return None

        fn_call = ir.FunctionCall(name=func_name)

        # Add positional args
        for arg in node.args:
            expr = _expr_to_ir(arg)
            if expr:
                fn_call.args.append(expr)

        # Add keyword args
        for kw in node.keywords:
            if kw.arg:
                expr = _expr_to_ir(kw.value)
                if expr:
                    fn_call.kwargs.append(ir.Kwarg(name=kw.arg, value=expr))

        return fn_call

    def _get_func_name(self, node: ast.expr) -> Optional[str]:
        """Get function name from a func node."""
        if isinstance(node, ast.Name):
            return node.id
        elif isinstance(node, ast.Attribute):
            # Handle chained attributes like obj.method
            parts = []
            current = node
            while isinstance(current, ast.Attribute):
                parts.append(current.attr)
                current = current.value
            if isinstance(current, ast.Name):
                parts.append(current.id)
            name = ".".join(reversed(parts))
            if name.startswith("self."):
                return name[5:]
            return name
        return None

    def _expr_to_ir_with_model_coercion(self, node: ast.expr) -> Optional[ir.Expr]:
        """Convert an AST expression to IR, converting model constructors to dicts.

        This is used for action arguments where Pydantic models or dataclass
        constructors should be converted to dict expressions that Rust can evaluate.

        If the expression is a model constructor (e.g., MyModel(field=value)),
        it is converted to a dict expression. Otherwise, falls back to the
        standard _expr_to_ir conversion.
        """
        # Check if this is a model constructor call
        if isinstance(node, ast.Call):
            model_name = self._is_model_constructor(node)
            if model_name:
                return self._convert_model_constructor_to_dict(node, model_name)

        # Fall back to standard expression conversion
        return _expr_to_ir(node)

    def _extract_action_call_from_awaitable(self, node: ast.expr) -> Optional[ir.ActionCall]:
        """Extract action call from an awaitable expression."""
        if isinstance(node, ast.Call):
            return self._extract_action_call_from_call(node)
        return None

    def _extract_action_call_from_call(self, node: ast.Call) -> Optional[ir.ActionCall]:
        """Extract action call info from a Call node.

        Converts positional arguments to keyword arguments using the action's
        signature introspection. This ensures all arguments are named in the IR.

        Pydantic models and dataclass constructors passed as arguments are
        automatically converted to dict expressions.
        """
        action_name = self._get_action_name(node.func)
        if not action_name:
            return None

        if action_name not in self._action_defs:
            return None

        action_def = self._action_defs[action_name]
        action_call = ir.ActionCall(action_name=action_def.action_name)

        # Set the module name so the worker knows where to find the action
        if action_def.module_name:
            action_call.module_name = action_def.module_name

        # Get parameter names from signature for positional arg conversion
        param_names = list(action_def.signature.parameters.keys())

        # Convert positional args to kwargs using signature introspection
        # Model constructors are converted to dict expressions
        for i, arg in enumerate(node.args):
            if i < len(param_names):
                expr = self._expr_to_ir_with_model_coercion(arg)
                if expr:
                    kwarg = ir.Kwarg(name=param_names[i], value=expr)
                    action_call.kwargs.append(kwarg)

        # Add explicit kwargs
        # Model constructors are converted to dict expressions
        for kw in node.keywords:
            if kw.arg:
                expr = self._expr_to_ir_with_model_coercion(kw.value)
                if expr:
                    kwarg = ir.Kwarg(name=kw.arg, value=expr)
                    action_call.kwargs.append(kwarg)

        return action_call

    def _get_action_name(self, func: ast.expr) -> Optional[str]:
        """Get the action name from a function expression."""
        if isinstance(func, ast.Name):
            return func.id
        elif isinstance(func, ast.Attribute):
            return func.attr
        return None

    def _get_assign_target(self, targets: List[ast.expr]) -> Optional[str]:
        """Get the target variable name from assignment targets (single target only)."""
        if targets and isinstance(targets[0], ast.Name):
            return targets[0].id
        return None

    def _get_assign_targets(self, targets: List[ast.expr]) -> List[str]:
        """Get all target variable names from assignment targets (including tuple unpacking)."""
        result: List[str] = []
        for t in targets:
            if isinstance(t, ast.Name):
                result.append(t.id)
            elif isinstance(t, ast.Subscript):
                formatted = _format_subscript_target(t)
                if formatted:
                    result.append(formatted)
            elif isinstance(t, ast.Tuple):
                for elt in t.elts:
                    if isinstance(elt, ast.Name):
                        result.append(elt.id)
        return result


def _make_span(node: ast.AST) -> ir.Span:
    """Create a Span from an AST node."""
    return ir.Span(
        start_line=getattr(node, "lineno", 0),
        start_col=getattr(node, "col_offset", 0),
        end_line=getattr(node, "end_lineno", 0) or 0,
        end_col=getattr(node, "end_col_offset", 0) or 0,
    )


def _expr_to_ir(expr: ast.AST) -> Optional[ir.Expr]:
    """Convert Python AST expression to IR Expr."""
    result = ir.Expr(span=_make_span(expr))

    if isinstance(expr, ast.Name):
        result.variable.CopyFrom(ir.Variable(name=expr.id))
        return result

    if isinstance(expr, ast.Constant):
        literal = _constant_to_literal(expr.value)
        if literal:
            result.literal.CopyFrom(literal)
            return result

    if isinstance(expr, ast.BinOp):
        left = _expr_to_ir(expr.left)
        right = _expr_to_ir(expr.right)
        op = _bin_op_to_ir(expr.op)
        if left and right and op:
            result.binary_op.CopyFrom(ir.BinaryOp(left=left, op=op, right=right))
            return result

    if isinstance(expr, ast.UnaryOp):
        operand = _expr_to_ir(expr.operand)
        op = _unary_op_to_ir(expr.op)
        if operand and op:
            result.unary_op.CopyFrom(ir.UnaryOp(op=op, operand=operand))
            return result

    if isinstance(expr, ast.Compare):
        left = _expr_to_ir(expr.left)
        if not left:
            return None
        # For simplicity, handle single comparison
        if expr.ops and expr.comparators:
            op = _cmp_op_to_ir(expr.ops[0])
            right = _expr_to_ir(expr.comparators[0])
            if op and right:
                result.binary_op.CopyFrom(ir.BinaryOp(left=left, op=op, right=right))
                return result

    if isinstance(expr, ast.BoolOp):
        values = [_expr_to_ir(v) for v in expr.values]
        if all(v for v in values):
            op = _bool_op_to_ir(expr.op)
            if op and len(values) >= 2:
                # Chain boolean ops: a and b and c -> (a and b) and c
                result_expr = values[0]
                for v in values[1:]:
                    if result_expr and v:
                        new_result = ir.Expr()
                        new_result.binary_op.CopyFrom(ir.BinaryOp(left=result_expr, op=op, right=v))
                        result_expr = new_result
                return result_expr

    if isinstance(expr, ast.List):
        elements = [_expr_to_ir(e) for e in expr.elts]
        if all(e for e in elements):
            list_expr = ir.ListExpr(elements=[e for e in elements if e])
            result.list.CopyFrom(list_expr)
            return result

    if isinstance(expr, ast.Dict):
        entries: List[ir.DictEntry] = []
        for k, v in zip(expr.keys, expr.values, strict=False):
            if k:
                key_expr = _expr_to_ir(k)
                value_expr = _expr_to_ir(v)
                if key_expr and value_expr:
                    entries.append(ir.DictEntry(key=key_expr, value=value_expr))
        result.dict.CopyFrom(ir.DictExpr(entries=entries))
        return result

    if isinstance(expr, ast.Subscript):
        obj = _expr_to_ir(expr.value)
        index = _expr_to_ir(expr.slice) if isinstance(expr.slice, ast.AST) else None
        if obj and index:
            result.index.CopyFrom(ir.IndexAccess(object=obj, index=index))
            return result

    if isinstance(expr, ast.Attribute):
        obj = _expr_to_ir(expr.value)
        if obj:
            result.dot.CopyFrom(ir.DotAccess(object=obj, attribute=expr.attr))
            return result

    if isinstance(expr, ast.Await) and isinstance(expr.value, ast.Call):
        func_name = _get_func_name(expr.value.func)
        if func_name:
            args = [_expr_to_ir(a) for a in expr.value.args]
            kwargs: List[ir.Kwarg] = []
            for kw in expr.value.keywords:
                if kw.arg:
                    kw_expr = _expr_to_ir(kw.value)
                    if kw_expr:
                        kwargs.append(ir.Kwarg(name=kw.arg, value=kw_expr))
            func_call = ir.FunctionCall(
                name=func_name,
                args=[a for a in args if a],
                kwargs=kwargs,
            )
            result.function_call.CopyFrom(func_call)
            return result

    if isinstance(expr, ast.Call):
        # Function call
        func_name = _get_func_name(expr.func)
        if func_name:
            args = [_expr_to_ir(a) for a in expr.args]
            kwargs: List[ir.Kwarg] = []
            for kw in expr.keywords:
                if kw.arg:
                    kw_expr = _expr_to_ir(kw.value)
                    if kw_expr:
                        kwargs.append(ir.Kwarg(name=kw.arg, value=kw_expr))
            func_call = ir.FunctionCall(
                name=func_name,
                args=[a for a in args if a],
                kwargs=kwargs,
            )
            result.function_call.CopyFrom(func_call)
            return result

    if isinstance(expr, ast.Tuple):
        # Handle tuple as list for now
        elements = [_expr_to_ir(e) for e in expr.elts]
        if all(e for e in elements):
            list_expr = ir.ListExpr(elements=[e for e in elements if e])
            result.list.CopyFrom(list_expr)
            return result

    # Check for unsupported expression types
    _check_unsupported_expression(expr)

    return None


def _check_unsupported_expression(expr: ast.AST) -> None:
    """Check for unsupported expression types and raise descriptive errors."""
    line = getattr(expr, "lineno", None)
    col = getattr(expr, "col_offset", None)

    if isinstance(expr, ast.Constant):
        if _constant_to_literal(expr.value) is None:
            raise UnsupportedPatternError(
                f"Unsupported literal type '{type(expr.value).__name__}'",
                RECOMMENDATIONS["unsupported_literal"],
                line=line,
                col=col,
            )

    if isinstance(expr, ast.JoinedStr):
        raise UnsupportedPatternError(
            "F-strings are not supported",
            RECOMMENDATIONS["fstring"],
            line=line,
            col=col,
        )
    elif isinstance(expr, ast.Lambda):
        raise UnsupportedPatternError(
            "Lambda expressions are not supported",
            RECOMMENDATIONS["lambda"],
            line=line,
            col=col,
        )
    elif isinstance(expr, ast.ListComp):
        raise UnsupportedPatternError(
            "List comprehensions are not supported in this context",
            RECOMMENDATIONS["list_comprehension"],
            line=line,
            col=col,
        )
    elif isinstance(expr, ast.DictComp):
        raise UnsupportedPatternError(
            "Dict comprehensions are not supported in this context",
            RECOMMENDATIONS["dict_comprehension"],
            line=line,
            col=col,
        )
    elif isinstance(expr, ast.SetComp):
        raise UnsupportedPatternError(
            "Set comprehensions are not supported",
            RECOMMENDATIONS["set_comprehension"],
            line=line,
            col=col,
        )
    elif isinstance(expr, ast.GeneratorExp):
        raise UnsupportedPatternError(
            "Generator expressions are not supported",
            RECOMMENDATIONS["generator"],
            line=line,
            col=col,
        )
    elif isinstance(expr, ast.NamedExpr):
        raise UnsupportedPatternError(
            "The walrus operator (:=) is not supported",
            RECOMMENDATIONS["walrus"],
            line=line,
            col=col,
        )
    elif isinstance(expr, ast.Yield) or isinstance(expr, ast.YieldFrom):
        raise UnsupportedPatternError(
            "Yield expressions are not supported",
            RECOMMENDATIONS["yield_statement"],
            line=line,
            col=col,
        )
    elif isinstance(expr, ast.expr):
        raise UnsupportedPatternError(
            f"Unsupported expression type '{type(expr).__name__}'",
            RECOMMENDATIONS["unsupported_expression"],
            line=line,
            col=col,
        )


def _format_subscript_target(target: ast.Subscript) -> Optional[str]:
    """Convert a subscript target to a string representation for tracking."""
    if not isinstance(target.value, ast.Name):
        return None

    base = target.value.id
    try:
        index_str = ast.unparse(target.slice)
    except Exception:
        return None

    return f"{base}[{index_str}]"


def _constant_to_literal(value: Any) -> Optional[ir.Literal]:
    """Convert a Python constant to IR Literal."""
    literal = ir.Literal()
    if value is None:
        literal.is_none = True
    elif isinstance(value, bool):
        literal.bool_value = value
    elif isinstance(value, int):
        literal.int_value = value
    elif isinstance(value, float):
        literal.float_value = value
    elif isinstance(value, str):
        literal.string_value = value
    else:
        return None
    return literal


def _bin_op_to_ir(op: ast.operator) -> Optional[ir.BinaryOperator]:
    """Convert Python binary operator to IR BinaryOperator."""
    mapping = {
        ast.Add: ir.BinaryOperator.BINARY_OP_ADD,
        ast.Sub: ir.BinaryOperator.BINARY_OP_SUB,
        ast.Mult: ir.BinaryOperator.BINARY_OP_MUL,
        ast.Div: ir.BinaryOperator.BINARY_OP_DIV,
        ast.FloorDiv: ir.BinaryOperator.BINARY_OP_FLOOR_DIV,
        ast.Mod: ir.BinaryOperator.BINARY_OP_MOD,
    }
    return mapping.get(type(op))


def _unary_op_to_ir(op: ast.unaryop) -> Optional[ir.UnaryOperator]:
    """Convert Python unary operator to IR UnaryOperator."""
    mapping = {
        ast.USub: ir.UnaryOperator.UNARY_OP_NEG,
        ast.Not: ir.UnaryOperator.UNARY_OP_NOT,
    }
    return mapping.get(type(op))


def _cmp_op_to_ir(op: ast.cmpop) -> Optional[ir.BinaryOperator]:
    """Convert Python comparison operator to IR BinaryOperator."""
    mapping = {
        ast.Eq: ir.BinaryOperator.BINARY_OP_EQ,
        ast.NotEq: ir.BinaryOperator.BINARY_OP_NE,
        ast.Lt: ir.BinaryOperator.BINARY_OP_LT,
        ast.LtE: ir.BinaryOperator.BINARY_OP_LE,
        ast.Gt: ir.BinaryOperator.BINARY_OP_GT,
        ast.GtE: ir.BinaryOperator.BINARY_OP_GE,
        ast.In: ir.BinaryOperator.BINARY_OP_IN,
        ast.NotIn: ir.BinaryOperator.BINARY_OP_NOT_IN,
    }
    return mapping.get(type(op))


def _bool_op_to_ir(op: ast.boolop) -> Optional[ir.BinaryOperator]:
    """Convert Python boolean operator to IR BinaryOperator."""
    mapping = {
        ast.And: ir.BinaryOperator.BINARY_OP_AND,
        ast.Or: ir.BinaryOperator.BINARY_OP_OR,
    }
    return mapping.get(type(op))


def _get_func_name(func: ast.expr) -> Optional[str]:
    """Get function name from a Call's func attribute."""
    if isinstance(func, ast.Name):
        return func.id
    elif isinstance(func, ast.Attribute):
        # For method calls like obj.method, return full dotted name
        parts = []
        current = func
        while isinstance(current, ast.Attribute):
            parts.append(current.attr)
            current = current.value
        if isinstance(current, ast.Name):
            parts.append(current.id)
        name = ".".join(reversed(parts))
        if name.startswith("self."):
            return name[5:]
        return name
    return None
