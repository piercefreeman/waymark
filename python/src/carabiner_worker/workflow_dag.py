from __future__ import annotations

import ast
import copy
import inspect
import textwrap
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Set, Tuple

if TYPE_CHECKING:  # pragma: no cover
    from .workflow import Workflow


@dataclass
class DagNode:
    id: str
    action: str
    kwargs: Dict[str, Any]
    module: Optional[str] = None
    depends_on: List[str] = field(default_factory=list)
    wait_for_sync: List[str] = field(default_factory=list)
    produces: List[str] = field(default_factory=list)
    guard: Optional[str] = None
    exception_edges: List["ExceptionEdge"] = field(default_factory=list)
    timeout_seconds: Optional[int] = None
    max_retries: Optional[int] = None
    timeout_retry_limit: Optional[int] = None


RETURN_VARIABLE = "__workflow_return"
UNLIMITED_RETRIES = 2_147_483_647


@dataclass
class WorkflowDag:
    nodes: List[DagNode]
    return_variable: Optional[str] = None


@dataclass
class ActionDefinition:
    action_name: str
    module_name: Optional[str]
    signature: inspect.Signature


@dataclass
class RunActionConfig:
    timeout_seconds: Optional[int] = None
    max_retries: Optional[int] = None
    timeout_retry_limit: Optional[int] = None


@dataclass
class ParsedActionCall:
    call: ast.Call
    config: Optional[RunActionConfig] = None


@dataclass
class ExceptionEdge:
    source_node_id: str
    exception_type: Optional[str] = None
    exception_module: Optional[str] = None


def _extract_action_name(expr: ast.AST) -> Optional[str]:
    if isinstance(expr, ast.Name):
        return expr.id
    if isinstance(expr, ast.Attribute):
        return expr.attr
    return None


class _ActionReferenceValidator(ast.NodeVisitor):
    def __init__(self, action_names: Set[str]) -> None:
        self._action_names = action_names
        self._stack: List[ast.AST] = []

    def visit(self, node: ast.AST) -> Any:  # type: ignore[override]
        self._stack.append(node)
        try:
            return super().visit(node)
        finally:
            self._stack.pop()

    def visit_Name(self, node: ast.Name) -> Any:  # type: ignore[override]
        if isinstance(node.ctx, ast.Load) and node.id in self._action_names:
            if not self._is_allowed_reference(node):
                self._raise_error(node.id, node)
        return self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> Any:  # type: ignore[override]
        name = _extract_action_name(node)
        if name and name in self._action_names and not self._is_allowed_reference(node):
            self._raise_error(name, node)
        return self.generic_visit(node)

    def _is_allowed_reference(self, node: ast.AST) -> bool:
        parent = self._parent()
        if isinstance(parent, ast.Call):
            if parent.func is node:
                return True
            if self._is_run_action_call(parent) and parent.args and parent.args[0] is node:
                return True
        return False

    def _is_run_action_call(self, call: ast.Call) -> bool:
        func = call.func
        if isinstance(func, ast.Attribute):
            return func.attr == "run_action"
        if isinstance(func, ast.Name):
            return func.id == "run_action"
        return False

    def _parent(self) -> Optional[ast.AST]:
        if len(self._stack) < 2:
            return None
        return self._stack[-2]

    def _raise_error(self, name: str, node: ast.AST) -> None:
        line = getattr(node, "lineno", "?")
        column = getattr(node, "col_offset", "?")
        raise ValueError(
            f"action '{name}' is referenced outside supported call or list comprehension "
            f"contexts (line {line}, column {column})"
        )


@dataclass
class _ConditionalBranch:
    call: ParsedActionCall
    target: Optional[str]


@dataclass
class _ExceptionTypeSpec:
    module: Optional[str]
    type_name: Optional[str]


@dataclass
class _TryScope:
    nodes: List[str] = field(default_factory=list)


@dataclass
class _ExceptionHandlerContext:
    try_nodes: List[str]
    specs: List[_ExceptionTypeSpec]


def build_workflow_dag(workflow_cls: type["Workflow"]) -> WorkflowDag:
    original_run = getattr(workflow_cls, "__workflow_run_impl__", None)
    if original_run is None:
        original_run = workflow_cls.__dict__.get("run")
    if original_run is None:
        raise ValueError(f"workflow {workflow_cls!r} missing run implementation")
    module = inspect.getmodule(original_run)
    if module is None:
        raise ValueError(f"unable to locate module for workflow {workflow_cls!r}")
    module_source = inspect.getsource(module)
    module_index = ModuleIndex(module_source)
    action_defs = _discover_action_names(module)
    function_source = textwrap.dedent(inspect.getsource(original_run))
    tree = ast.parse(function_source)
    validator = _ActionReferenceValidator(set(action_defs))
    validator.visit(tree)
    builder = WorkflowDagBuilder(action_defs, module_index, function_source)
    builder.visit(tree)
    builder.ensure_return_variable()
    return WorkflowDag(nodes=builder.nodes, return_variable=builder.return_variable)


def _discover_action_names(module: Any) -> Dict[str, ActionDefinition]:
    names: Dict[str, ActionDefinition] = {}
    for attr_name in dir(module):
        attr = getattr(module, attr_name)
        action_name = getattr(attr, "__carabiner_action_name__", None)
        action_module = getattr(attr, "__carabiner_action_module__", None)
        if callable(attr) and action_name:
            signature = inspect.signature(attr)
            names[attr_name] = ActionDefinition(
                action_name=action_name,
                module_name=action_module or module.__name__,
                signature=signature,
            )
    return names


class ModuleIndex:
    def __init__(self, module_source: str) -> None:
        self._source = module_source
        self._imports: Dict[str, str] = {}
        self._definitions: Dict[str, str] = {}
        tree = ast.parse(module_source)
        for node in tree.body:
            snippet = ast.get_source_segment(module_source, node)
            if snippet is None:
                continue
            text = textwrap.dedent(snippet)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                self._definitions[node.name] = text
            elif isinstance(node, (ast.Import, ast.ImportFrom)):
                for alias in node.names:
                    exposed = alias.asname or alias.name.split(".")[0]
                    self._imports[exposed] = text

    @property
    def symbols(self) -> Set[str]:
        return set(self._imports) | set(self._definitions)

    def resolve(self, names: Iterable[str]) -> Tuple[List[str], List[str]]:
        import_blocks: List[str] = []
        definition_blocks: List[str] = []
        for name in names:
            if name in self._imports:
                import_blocks.append(self._imports[name])
            elif name in self._definitions:
                definition_blocks.append(self._definitions[name])
        return sorted(set(import_blocks)), sorted(set(definition_blocks))


class WorkflowDagBuilder(ast.NodeVisitor):
    def __init__(
        self,
        action_defs: Dict[str, ActionDefinition],
        module_index: ModuleIndex,
        source: str,
    ) -> None:
        self.nodes: List[DagNode] = []
        self._var_to_node: Dict[str, str] = {}
        self._counter = 0
        self._action_defs = action_defs
        self._module_index = module_index
        self._source = source
        self._last_node_id: Optional[str] = None
        self._collections: Dict[str, List[str]] = {}
        self._return_variable: Optional[str] = None
        self._try_stack: List[_TryScope] = []
        self._handler_stack: List[_ExceptionHandlerContext] = []

    # pylint: disable=too-many-return-statements
    def visit_Assign(self, node: ast.Assign) -> Any:
        action_call = self._extract_action_call(node.value)
        if action_call is not None:
            target = self._extract_target(node.targets)
            dag_node = self._build_node(action_call, target)
            if target:
                self._var_to_node[target] = dag_node.id
                self._collections.pop(target, None)
            self._append_node(dag_node)
            return
        gather = self._match_gather_call(node.value)
        if gather is not None:
            self._handle_gather(node.targets, gather)
            return
        if self._handle_action_list_comprehension(node.targets, node.value):
            return
        if self._is_complex_block(node.value):
            self._capture_block(node)
            return
        self._capture_block(node)

    def visit_Expr(self, node: ast.Expr) -> Any:
        action_call = self._extract_action_call(node.value)
        if action_call is not None:
            self._append_node(self._build_node(action_call, target=None))
            return
        gather = self._match_gather_call(node.value)
        if gather is not None:
            self._handle_gather([], gather)
            return
        if self._is_complex_block(node.value):
            self._capture_block(node.value)
            return
        self.generic_visit(node)

    def visit_For(self, node: ast.For) -> Any:
        self._capture_block(node)

    def visit_If(self, node: ast.If) -> Any:
        if self._handle_conditional_actions(node):
            return
        self._capture_block(node)

    def visit_Try(self, node: ast.Try) -> Any:
        if not node.handlers:
            line = getattr(node, "lineno", "?")
            raise ValueError(f"try statement without except handler (line {line})")
        if node.finalbody:
            line = getattr(node, "lineno", "?")
            raise ValueError(f"finally blocks are not supported (line {line})")
        scope = _TryScope()
        self._try_stack.append(scope)
        for stmt in node.body:
            self.visit(stmt)
        self._try_stack.pop()
        try_nodes = list(scope.nodes)
        if not try_nodes:
            line = getattr(node, "lineno", "?")
            raise ValueError(f"try block must contain at least one action (line {line})")
        for handler in node.handlers:
            if handler.name:
                line = getattr(handler, "lineno", "?")
                raise ValueError(
                    f"except clauses cannot bind exceptions (line {line}); use explicit actions"
                )
            specs = self._parse_exception_specs(handler.type)
            context = _ExceptionHandlerContext(try_nodes=try_nodes, specs=specs)
            self._handler_stack.append(context)
            for stmt in handler.body:
                self.visit(stmt)
            self._handler_stack.pop()
        if node.orelse:
            line = getattr(node, "lineno", "?")
            raise ValueError(f"try/else clauses are not supported (line {line})")

    def visit_Return(self, node: ast.Return) -> Any:
        if self._return_variable is not None:
            line = getattr(node, "lineno", "?")
            raise ValueError(f"multiple return statements are not supported (line {line})")
        if node.value is None:
            self._return_variable = RETURN_VARIABLE
            self._append_python_block(f"{RETURN_VARIABLE} = None", [RETURN_VARIABLE])
            return
        if isinstance(node.value, ast.Await):
            line = getattr(node, "lineno", "?")
            raise ValueError(
                f"return statements cannot directly await values (line {line}); "
                "assign to a variable first"
            )
        if isinstance(node.value, ast.Name) and node.value.id in self._var_to_node:
            self._return_variable = node.value.id
            return
        snippet = f"{RETURN_VARIABLE} = {ast.unparse(node.value)}"
        self._return_variable = RETURN_VARIABLE
        self._append_python_block(snippet, [RETURN_VARIABLE])

    def _handle_conditional_actions(self, node: ast.If) -> bool:
        if not node.orelse:
            if self._branch_contains_action(node.body):
                line = getattr(node, "lineno", "?")
                raise ValueError(f"conditional action at line {line} requires an else branch")
            return False
        if len(node.body) != 1 or len(node.orelse) != 1:
            if self._branch_contains_action(node.body) or self._branch_contains_action(node.orelse):
                line = getattr(node, "lineno", "?")
                raise ValueError(
                    f"conditional actions must contain a single action call per branch (line {line})"
                )
            return False
        true_branch = self._extract_branch_statement(node.body[0])
        false_branch = self._extract_branch_statement(node.orelse[0])
        if true_branch is None or false_branch is None:
            if self._branch_contains_action(node.body) or self._branch_contains_action(node.orelse):
                line = getattr(node, "lineno", "?")
                raise ValueError(
                    f"unsupported conditional action structure near line {line}; "
                    "only direct action calls are allowed in each branch"
                )
            return False
        if bool(true_branch.target) != bool(false_branch.target):
            line = getattr(node, "lineno", "?")
            raise ValueError(
                f"conditional branches must both assign to the same target or neither (line {line})"
            )
        if true_branch.target and false_branch.target and true_branch.target != false_branch.target:
            line = getattr(node, "lineno", "?")
            raise ValueError(
                f"conditional branches assign to different targets near line {line}; "
                "use a shared variable name"
            )
        condition_expr = f"({ast.unparse(node.test).strip()})"
        condition_deps = sorted(set(self._dependencies_from_expr(node.test)))
        true_guard = condition_expr
        false_guard = f"not ({condition_expr})"
        true_node = self._build_node(
            true_branch.call,
            target=None,
            guard=true_guard,
            extra_dependencies=condition_deps,
        )
        false_node = self._build_node(
            false_branch.call,
            target=None,
            guard=false_guard,
            extra_dependencies=condition_deps,
        )
        temp_true = temp_false = None
        if true_branch.target:
            temp_true = f"__branch_{true_branch.target}_{true_node.id}"
            temp_false = f"__branch_{false_branch.target}_{false_node.id}"
            true_node.produces = [temp_true]
            false_node.produces = [temp_false]
        self._append_node(true_node)
        self._append_node(false_node)
        if temp_true and temp_false and true_branch.target:
            merge_node = self._build_branch_merge_node(
                true_branch.target, temp_true, temp_false, [true_node.id, false_node.id]
            )
            self._var_to_node[true_branch.target] = merge_node.id
            self._collections.pop(true_branch.target, None)
            self._append_node(merge_node)
        return True

    def _extract_branch_statement(self, stmt: ast.stmt) -> Optional[_ConditionalBranch]:
        if isinstance(stmt, ast.Assign):
            call = self._extract_action_call(stmt.value)
            if call is None:
                return None
            target = self._extract_target(stmt.targets)
            if target is None:
                return None
            return _ConditionalBranch(call=call, target=target)
        if isinstance(stmt, ast.Expr):
            call = self._extract_action_call(stmt.value)
            if call is None:
                return None
            return _ConditionalBranch(call=call, target=None)
        return None

    def _branch_contains_action(self, statements: List[ast.stmt]) -> bool:
        for stmt in statements:
            for sub in ast.walk(stmt):
                if self._extract_action_call(sub) is not None:
                    return True
        return False

    def _build_branch_merge_node(
        self, target_name: str, true_var: str, false_var: str, dependencies: List[str]
    ) -> DagNode:
        merge_code = textwrap.dedent(
            f"""
            if "{true_var}" in locals():
                {target_name} = {true_var}
            elif "{false_var}" in locals():
                {target_name} = {false_var}
            else:
                raise RuntimeError("conditional branch produced no value for {target_name}")
            """
        ).strip()
        return DagNode(
            id=self._new_node_id(),
            action="python_block",
            kwargs={"code": merge_code, "imports": "", "definitions": ""},
            depends_on=sorted(set(dependencies)),
            produces=[target_name],
        )

    def _extract_action_call(self, expr: ast.AST) -> Optional[ParsedActionCall]:
        if isinstance(expr, ast.Await):
            expr = expr.value
        if not isinstance(expr, ast.Call):
            return None
        if self._is_action_func(expr.func):
            return ParsedActionCall(call=expr)
        run_action = self._convert_run_action(expr)
        if run_action is not None:
            return run_action
        return None

    def _convert_run_action(self, call: ast.Call) -> Optional[ParsedActionCall]:
        func = call.func
        if not self._is_run_action_invocation(func):
            return None
        if not call.args:
            raise ValueError("run_action requires an action coroutine argument")
        if len(call.args) > 1:
            raise ValueError("run_action accepts a single positional action argument")
        action_expr = call.args[0]
        if isinstance(action_expr, ast.Await):
            action_expr = action_expr.value
        if not isinstance(action_expr, ast.Call):
            raise ValueError("run_action expects an action call as its first argument")
        if not self._is_action_func(action_expr.func):
            return None
        config = self._parse_run_action_metadata(call.keywords)
        return ParsedActionCall(call=action_expr, config=config)

    def _is_run_action_invocation(self, func: ast.AST) -> bool:
        if isinstance(func, ast.Attribute):
            return func.attr == "run_action"
        if isinstance(func, ast.Name):
            return func.id == "run_action"
        return False

    def _is_action_func(self, func: ast.AST) -> bool:
        name = _extract_action_name(func)
        return bool(name and name in self._action_defs)

    def _match_gather_call(self, expr: ast.AST) -> Optional[ast.Call]:
        if not isinstance(expr, ast.Await):
            return None
        call = expr.value
        if not isinstance(call, ast.Call):
            return None
        func = call.func
        is_gather = False
        if isinstance(func, ast.Attribute):
            is_gather = (
                isinstance(func.value, ast.Name)
                and func.value.id == "asyncio"
                and func.attr == "gather"
            )
        elif isinstance(func, ast.Name):
            is_gather = func.id == "gather"
        return call if is_gather else None

    def _handle_gather(self, targets: List[ast.expr], call: ast.Call) -> None:
        flat_targets = self._flatten_targets(targets)
        if len(flat_targets) == 1 and flat_targets[0]:
            target_name = flat_targets[0]
            item_vars: List[str] = []
            for idx, arg in enumerate(call.args):
                action_call = self._extract_action_call(arg)
                if action_call is None:
                    continue
                item_var = self._collection_item_name(target_name, idx)
                dag_node = self._build_node(action_call, item_var)
                self._var_to_node[item_var] = dag_node.id
                item_vars.append(item_var)
                self._append_node(dag_node)
            self._collections[target_name] = list(item_vars)
            self._append_collection_node(target_name, item_vars)
            return
        for idx, arg in enumerate(call.args):
            action_call = self._extract_action_call(arg)
            if action_call is None:
                continue
            target_name = flat_targets[idx] if idx < len(flat_targets) else None
            dag_node = self._build_node(action_call, target_name)
            if target_name:
                self._var_to_node[target_name] = dag_node.id
                self._collections.pop(target_name, None)
            self._append_node(dag_node)

    def _is_complex_block(self, expr: ast.AST) -> bool:
        return (
            isinstance(expr, ast.Call)
            and isinstance(expr.func, ast.Name)
            and expr.func.id in {"list", "dict"}
        )

    def _capture_block(self, node: ast.AST) -> None:
        snippet = ast.get_source_segment(self._source, node)
        snippet = self._normalize_block_snippet(snippet, node)
        if not snippet:
            return
        block_id = self._new_node_id()
        deps = self._dependencies_from_node(node)
        references = self._resolve_module_references(node)
        imports, definitions = self._module_index.resolve(references)
        imports_text = "\n".join(imports)
        definitions_text = "\n".join(definitions)
        block_node = DagNode(
            id=block_id,
            action="python_block",
            kwargs={
                "code": snippet,
                "imports": imports_text,
                "definitions": definitions_text,
            },
            depends_on=sorted(set(deps)),
            produces=sorted(self._collect_mutated_names(node)),
        )
        for name in block_node.produces:
            self._var_to_node[name] = block_id
            self._collections.pop(name, None)
        self._append_node(block_node)

    def _normalize_block_snippet(self, snippet: Optional[str], node: ast.AST) -> str:
        text = snippet or ast.unparse(node)
        text = textwrap.dedent(text).strip("\n")
        if not text:
            return text
        try:
            compile(text, "<workflow_block>", "exec")
            return text
        except SyntaxError:
            fallback = textwrap.dedent(ast.unparse(node)).strip("\n")
            compile(fallback, "<workflow_block>", "exec")
            return fallback

    def _append_python_block(self, code: str, produces: List[str]) -> str:
        snippet = textwrap.dedent(code).strip()
        if not snippet:
            raise ValueError("python block requires non-empty code")
        block_id = self._new_node_id()
        deps: List[str] = []
        parsed = ast.parse(snippet)
        for stmt in parsed.body:
            deps.extend(self._dependencies_from_node(stmt))
        block_node = DagNode(
            id=block_id,
            action="python_block",
            kwargs={"code": snippet, "imports": "", "definitions": ""},
            depends_on=sorted(set(deps)),
            produces=list(produces),
        )
        for name in produces:
            self._var_to_node[name] = block_id
        self._append_node(block_node)
        return block_id

    def _append_node(self, node: DagNode) -> None:
        if self._last_node_id is not None:
            node.wait_for_sync = [self._last_node_id]
        self._apply_try_guards(node)
        self._apply_handler_metadata(node)
        self.nodes.append(node)
        self._last_node_id = node.id

    def _apply_try_guards(self, node: DagNode) -> None:
        if not self._try_stack:
            return
        prior_ids: List[List[str]] = [list(scope.nodes) for scope in self._try_stack]
        clauses = [self._format_no_exception_guard(ids) for ids in prior_ids if ids]
        guard_expr = self._combine_guards(clauses)
        if guard_expr:
            node.guard = self._combine_guard(node.guard, guard_expr)
        for scope in self._try_stack:
            scope.nodes.append(node.id)

    def _apply_handler_metadata(self, node: DagNode) -> None:
        if not self._handler_stack:
            return
        handler = self._handler_stack[-1]
        guard_expr = self._format_handler_guard(handler)
        if guard_expr:
            node.guard = self._combine_guard(node.guard, guard_expr)
        node.exception_edges.extend(self._build_exception_edges(handler))

    def _combine_guard(self, existing: Optional[str], extra: Optional[str]) -> Optional[str]:
        if not extra:
            return existing
        if not existing:
            return extra
        return f"({existing}) and ({extra})"

    def _combine_guards(self, clauses: List[Optional[str]]) -> Optional[str]:
        active = [clause for clause in clauses if clause]
        if not active:
            return None
        expr = active[0]
        for clause in active[1:]:
            expr = f"({expr}) and ({clause})"
        return expr

    def _format_no_exception_guard(self, node_ids: List[str]) -> Optional[str]:
        if not node_ids:
            return None
        if len(node_ids) == 1:
            node_id = node_ids[0]
            return f"__workflow_exceptions.get({node_id!r}) is None"
        joined = ", ".join(repr(node_id) for node_id in node_ids)
        return f"all(__workflow_exceptions.get(__node) is None for __node in ({joined},))"

    def _format_handler_guard(self, handler: _ExceptionHandlerContext) -> Optional[str]:
        clauses: List[str] = []
        for source in handler.try_nodes:
            base = f"__workflow_exceptions.get({source!r})"
            if not handler.specs:
                clauses.append(f"({base} is not None)")
                continue
            for spec in handler.specs:
                segments = [f"{base} is not None"]
                if spec.type_name:
                    segments.append(f"{base}.get('type') == {spec.type_name!r}")
                if spec.module:
                    segments.append(f"{base}.get('module') == {spec.module!r}")
                clause = " and ".join(segments)
                clauses.append(f"({clause})")
        if not clauses:
            return None
        expr = clauses[0]
        for clause in clauses[1:]:
            expr = f"({expr}) or ({clause})"
        return expr

    def _build_exception_edges(self, handler: _ExceptionHandlerContext) -> List[ExceptionEdge]:
        edges: List[ExceptionEdge] = []
        for source in handler.try_nodes:
            if not handler.specs:
                edges.append(ExceptionEdge(source_node_id=source))
                continue
            for spec in handler.specs:
                edges.append(
                    ExceptionEdge(
                        source_node_id=source,
                        exception_type=spec.type_name,
                        exception_module=spec.module,
                    )
                )
        return edges

    def _parse_exception_specs(self, expr: Optional[ast.expr]) -> List[_ExceptionTypeSpec]:
        if expr is None:
            return [_ExceptionTypeSpec(module=None, type_name=None)]
        if isinstance(expr, ast.Tuple):
            specs: List[_ExceptionTypeSpec] = []
            for element in expr.elts:
                specs.extend(self._parse_exception_specs(element))
            return specs
        if isinstance(expr, ast.Name):
            return [_ExceptionTypeSpec(module=None, type_name=expr.id)]
        if isinstance(expr, ast.Attribute):
            parts: List[str] = []
            current: ast.AST = expr
            while isinstance(current, ast.Attribute):
                parts.append(current.attr)
                current = current.value
            if isinstance(current, ast.Name):
                parts.append(current.id)
                parts.reverse()
                module = ".".join(parts[:-1]) if len(parts) > 1 else None
                type_name = parts[-1]
                return [_ExceptionTypeSpec(module=module, type_name=type_name)]
        line = getattr(expr, "lineno", "?")
        raise ValueError(f"unsupported exception reference near line {line}")

    def ensure_return_variable(self) -> None:
        if self._return_variable is not None:
            return
        self._return_variable = RETURN_VARIABLE
        self._append_python_block(f"{RETURN_VARIABLE} = None", [RETURN_VARIABLE])

    @property
    def return_variable(self) -> Optional[str]:
        return self._return_variable

    def _flatten_targets(self, targets: List[ast.expr]) -> List[Optional[str]]:
        flat: List[Optional[str]] = []
        for target in targets:
            flat.extend(self._flatten_target(target))
        return flat

    def _flatten_target(self, target: ast.expr) -> List[Optional[str]]:
        if isinstance(target, ast.Name):
            return [target.id]
        if isinstance(target, (ast.Tuple, ast.List)):
            nested: List[Optional[str]] = []
            for elt in target.elts:
                nested.extend(self._flatten_target(elt))
            return nested
        return [None]

    def _extract_target(self, targets: List[ast.expr]) -> Optional[str]:
        if len(targets) != 1:
            return None
        target = targets[0]
        if isinstance(target, ast.Name):
            return target.id
        return None

    def _collection_item_name(self, base: str, index: int) -> str:
        return f"{base}__item{index}"

    def _append_collection_node(self, target: str, items: List[str]) -> None:
        if items:
            code = f"{target} = [{', '.join(items)}]"
        else:
            code = f"{target} = []"
        self._append_python_block(code, [target])

    def _handle_action_list_comprehension(self, targets: List[ast.expr], value: ast.AST) -> bool:
        if not isinstance(value, ast.ListComp):
            return False
        target = self._extract_target(targets)
        if target is None or len(value.generators) != 1:
            return False
        generator = value.generators[0]
        if generator.ifs or generator.is_async or not isinstance(generator.target, ast.Name):
            return False
        iter_name = self._resolve_collection_name(generator.iter)
        if iter_name is None:
            return False
        sources = self._collections.get(iter_name)
        if not sources:
            return False
        action_call = self._extract_action_call(value.elt)
        if action_call is None:
            return False
        loop_var = generator.target.id
        produced: List[str] = []
        for idx, source in enumerate(sources):
            substituted_call = self._substitute_call(action_call.call, {loop_var: source})
            parsed = ParsedActionCall(call=substituted_call, config=action_call.config)
            item_var = self._collection_item_name(target, idx)
            dag_node = self._build_node(parsed, item_var)
            self._var_to_node[item_var] = dag_node.id
            produced.append(item_var)
            self._append_node(dag_node)
        self._collections[target] = list(produced)
        self._append_collection_node(target, produced)
        return True

    def _resolve_collection_name(self, expr: ast.AST) -> Optional[str]:
        if isinstance(expr, ast.Name):
            return expr.id
        return None

    def _substitute_call(self, call: ast.Call, replacements: Dict[str, str]) -> ast.Call:
        mapping: Dict[str, ast.AST] = {}
        for name, expr in replacements.items():
            mapping[name] = ast.Name(id=expr, ctx=ast.Load())
        substituter = _NameSubstituter(mapping)
        return substituter.visit(copy.deepcopy(call))

    def _parse_run_action_metadata(self, keywords: List[ast.keyword]) -> Optional[RunActionConfig]:
        timeout_seconds: Optional[int] = None
        max_retries: Optional[int] = None
        for kw in keywords:
            name = kw.arg
            if name == "timeout":
                timeout_seconds = self._evaluate_timeout_literal(kw.value)
            elif name == "retry":
                max_retries = self._evaluate_retry_literal(kw.value)
            elif name == "backoff":
                self._validate_backoff_literal(kw.value)
            else:
                raise ValueError(f"unsupported run_action keyword argument '{name}'")
        if timeout_seconds is None and max_retries is None:
            return None
        return RunActionConfig(timeout_seconds=timeout_seconds, max_retries=max_retries)

    def _evaluate_timeout_literal(self, expr: ast.AST) -> Optional[int]:
        if isinstance(expr, ast.Constant):
            value = expr.value
            if value is None:
                return None
            if isinstance(value, (int, float)):
                if value < 0:
                    raise ValueError("timeout must be non-negative")
                return int(value)
        if isinstance(expr, ast.Call) and self._is_timedelta_constructor(expr.func):
            seconds = self._compute_timedelta_seconds(expr)
            if seconds < 0:
                raise ValueError("timeout must be non-negative")
            return int(seconds)
        raise ValueError("timeout must be a numeric literal or datetime.timedelta")

    def _is_timedelta_constructor(self, func: ast.AST) -> bool:
        if isinstance(func, ast.Name):
            return func.id == "timedelta"
        if isinstance(func, ast.Attribute):
            return func.attr == "timedelta"
        return False

    def _compute_timedelta_seconds(self, call: ast.Call) -> float:
        positional = ["days", "seconds", "microseconds"]
        total = 0.0
        for idx, arg in enumerate(call.args):
            if idx >= len(positional):
                raise ValueError("timedelta positional args limited to days, seconds, microseconds")
            value = self._literal_number(arg)
            if value is None:
                raise ValueError("timedelta positional args must be numeric literals")
            total += value * self._timedelta_factor(positional[idx])
        for kw in call.keywords:
            if kw.arg is None:
                continue
            value = self._literal_number(kw.value)
            if value is None:
                raise ValueError("timedelta keyword args must be numeric literals")
            total += value * self._timedelta_factor(kw.arg)
        return total

    def _timedelta_factor(self, name: str) -> float:
        factors = {
            "weeks": 7 * 24 * 60 * 60,
            "days": 24 * 60 * 60,
            "hours": 60 * 60,
            "minutes": 60,
            "seconds": 1,
            "milliseconds": 1e-3,
            "microseconds": 1e-6,
        }
        if name not in factors:
            raise ValueError(f"unsupported timedelta keyword '{name}'")
        return factors[name]

    def _evaluate_retry_literal(self, expr: ast.AST) -> Optional[int]:
        if isinstance(expr, ast.Constant):
            value = expr.value
            if value is None:
                return None
            if isinstance(value, int):
                return self._normalize_retry_attempts(value)
        if isinstance(expr, ast.Call) and self._is_retry_policy_constructor(expr.func):
            attempts_expr: Optional[ast.AST] = None
            for kw in expr.keywords:
                if kw.arg in {"attempts", "max_attempts"}:
                    attempts_expr = kw.value
                else:
                    raise ValueError(f"RetryPolicy received unsupported argument '{kw.arg}'")
            if attempts_expr is None and expr.args:
                attempts_expr = expr.args[0]
            if attempts_expr is None:
                return None
            if isinstance(attempts_expr, ast.Constant) and attempts_expr.value is None:
                return UNLIMITED_RETRIES
            literal = self._literal_number(attempts_expr)
            if literal is None:
                raise ValueError("RetryPolicy attempts must be numeric")
            return self._normalize_retry_attempts(int(literal))
        raise ValueError("retry must be RetryPolicy(...) or None")

    def _normalize_retry_attempts(self, value: int) -> int:
        if value < 0:
            raise ValueError("retry attempts must be >= 0")
        if value == 0:
            return 0
        return min(value, UNLIMITED_RETRIES)

    def _is_retry_policy_constructor(self, func: ast.AST) -> bool:
        if isinstance(func, ast.Name):
            return func.id == "RetryPolicy"
        if isinstance(func, ast.Attribute):
            return func.attr == "RetryPolicy"
        return False

    def _validate_backoff_literal(self, expr: ast.AST) -> None:
        if isinstance(expr, ast.Constant) and expr.value is None:
            return
        if isinstance(expr, ast.Call) and self._is_backoff_policy_constructor(expr.func):
            return
        raise ValueError("backoff must be BackoffPolicy(...) or None")

    def _is_backoff_policy_constructor(self, func: ast.AST) -> bool:
        if isinstance(func, ast.Name):
            return func.id == "BackoffPolicy"
        if isinstance(func, ast.Attribute):
            return func.attr == "BackoffPolicy"
        return False

    def _literal_number(self, expr: ast.AST) -> Optional[float]:
        if isinstance(expr, ast.Constant) and isinstance(expr.value, (int, float)):
            return float(expr.value)
        if isinstance(expr, ast.UnaryOp) and isinstance(expr.op, ast.USub):
            operand = expr.operand
            if isinstance(operand, ast.Constant) and isinstance(operand.value, (int, float)):
                return -float(operand.value)
        return None

    def _build_node(
        self,
        parsed_call: ParsedActionCall,
        target: Optional[str],
        *,
        guard: Optional[str] = None,
        extra_dependencies: Optional[List[str]] = None,
    ) -> DagNode:
        call = parsed_call.call
        name = self._format_call_name(call.func)
        node_id = self._new_node_id()
        kwargs: Dict[str, str] = {}
        dependencies: List[str] = []
        for kw in call.keywords:
            if kw.arg is None:
                continue
            kwargs[kw.arg] = ast.unparse(kw.value)
            dependencies.extend(self._dependencies_from_expr(kw.value))
        action_def = self._action_defs.get(name)
        used_keys = set(kwargs)
        if call.args:
            if action_def is None:
                line = getattr(call, "lineno", "?")
                raise ValueError(
                    f"action '{name}' uses positional arguments but metadata is unavailable (line {line})"
                )
            arg_mappings = self._map_positional_args(call, action_def, used_keys)
            for key, expr in arg_mappings:
                kwargs[key] = expr
                dependencies.extend(self._dependencies_from_expr(ast.parse(expr, mode="eval").body))
        if action_def:
            action_name = action_def.action_name
            module_name = action_def.module_name
        else:
            action_name = name
            module_name = None
        if extra_dependencies:
            dependencies.extend(extra_dependencies)
        dag_node = DagNode(
            id=node_id,
            action=action_name,
            module=module_name,
            kwargs=kwargs,
            depends_on=sorted(set(dependencies)),
            produces=[target] if target else [],
            guard=guard,
        )
        if parsed_call.config:
            if parsed_call.config.timeout_seconds is not None:
                dag_node.timeout_seconds = parsed_call.config.timeout_seconds
            if parsed_call.config.max_retries is not None:
                dag_node.max_retries = parsed_call.config.max_retries
        return dag_node

    def _map_positional_args(
        self,
        call: ast.Call,
        action_def: ActionDefinition,
        used_keys: Set[str],
    ) -> List[Tuple[str, str]]:
        mappings: List[Tuple[str, str]] = []
        params_iter = iter(action_def.signature.parameters.values())
        for arg in call.args:
            param = self._next_positional_param(params_iter, used_keys)
            if param is None:
                line = getattr(call, "lineno", "?")
                raise ValueError(
                    f"too many positional arguments for action '{action_def.action_name}' (line {line})"
                )
            expr = ast.unparse(arg)
            mappings.append((param.name, expr))
            used_keys.add(param.name)
        return mappings

    def _next_positional_param(
        self,
        params: Iterable[inspect.Parameter],
        used_keys: Set[str],
    ) -> Optional[inspect.Parameter]:
        for param in params:
            if param.kind in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            ):
                if param.name in used_keys:
                    continue
                return param
            if param.kind == inspect.Parameter.VAR_POSITIONAL:
                return param
        return None

    def _new_node_id(self) -> str:
        node_id = f"node_{self._counter}"
        self._counter += 1
        return node_id

    def _format_call_name(self, func_ref: ast.AST) -> str:
        name = _extract_action_name(func_ref)
        if name is not None:
            return name
        return ast.unparse(func_ref)

    def _dependencies_from_expr(self, expr: ast.AST) -> List[str]:
        deps: List[str] = []
        for node in ast.walk(expr):
            base: Optional[str] = None
            if isinstance(node, ast.Name):
                base = node.id
            elif isinstance(node, ast.Attribute):
                root = node
                while isinstance(root, ast.Attribute):
                    root = root.value  # type: ignore[assignment]
                if isinstance(root, ast.Name):
                    base = root.id
            if base and base in self._var_to_node:
                deps.append(self._var_to_node[base])
        return deps

    def _dependencies_from_node(self, node: ast.AST) -> List[str]:
        produced = {name for name in self._collect_mutated_names(node)}
        deps: List[str] = []
        for sub in ast.walk(node):
            if isinstance(sub, ast.Name) and isinstance(sub.ctx, ast.Load):
                if sub.id in produced:
                    continue
                if sub.id in self._var_to_node:
                    deps.append(self._var_to_node[sub.id])
        return deps

    def _collect_mutated_names(self, node: ast.AST) -> Set[str]:
        produced: Set[str] = set()
        comprehension_targets: Set[str] = set()
        for sub in ast.walk(node):
            if isinstance(sub, ast.Name) and isinstance(sub.ctx, ast.Store):
                produced.add(sub.id)
            elif isinstance(sub, ast.Attribute) and isinstance(sub.value, ast.Name):
                if sub.attr in {"append", "extend", "update", "add"}:
                    produced.add(sub.value.id)
            elif isinstance(sub, ast.comprehension):
                comprehension_targets.update(self._collect_comprehension_targets(sub.target))
        produced.difference_update(comprehension_targets)
        return produced

    def _collect_comprehension_targets(self, target: ast.AST) -> Set[str]:
        names: Set[str] = set()
        for sub in ast.walk(target):
            if isinstance(sub, ast.Name):
                names.add(sub.id)
        return names

    def _resolve_module_references(self, node: ast.AST) -> Set[str]:
        local_assignments = self._collect_mutated_names(node)
        refs: Set[str] = set()
        for sub in ast.walk(node):
            if isinstance(sub, ast.Name) and isinstance(sub.ctx, ast.Load):
                if sub.id in local_assignments or sub.id in self._var_to_node:
                    continue
                if sub.id in self._module_index.symbols:
                    refs.add(sub.id)
        return refs


class _NameSubstituter(ast.NodeTransformer):
    def __init__(self, replacements: Dict[str, ast.AST]) -> None:
        self._replacements = replacements

    def visit_Name(self, node: ast.Name) -> Any:  # type: ignore[override]
        if isinstance(node.ctx, ast.Load) and node.id in self._replacements:
            return copy.deepcopy(self._replacements[node.id])
        return self.generic_visit(node)
