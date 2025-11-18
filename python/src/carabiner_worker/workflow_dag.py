from __future__ import annotations

import ast
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


@dataclass
class WorkflowDag:
    nodes: List[DagNode]


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
    call: ast.Call
    target: Optional[str]


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
    return WorkflowDag(builder.nodes)


def _discover_action_names(module: Any) -> Dict[str, Tuple[str, str]]:
    names: Dict[str, Tuple[str, str]] = {}
    for attr_name in dir(module):
        attr = getattr(module, attr_name)
        action_name = getattr(attr, "__carabiner_action_name__", None)
        action_module = getattr(attr, "__carabiner_action_module__", None)
        if callable(attr) and action_name:
            names[attr_name] = (action_name, action_module or module.__name__)
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
        action_defs: Dict[str, Tuple[str, str]],
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

    # pylint: disable=too-many-return-statements
    def visit_Assign(self, node: ast.Assign) -> Any:
        action_call = self._extract_action_call(node.value)
        if action_call is not None:
            target = self._extract_target(node.targets)
            dag_node = self._build_node(action_call, target)
            if target:
                self._var_to_node[target] = dag_node.id
            self._append_node(dag_node)
            return
        gather = self._match_gather_call(node.value)
        if gather is not None:
            self._handle_gather(node.targets, gather)
            return
        if self._is_complex_block(node.value):
            self._capture_block(node.value)
            return
        self.generic_visit(node)

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
        self.generic_visit(node)

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

    def _extract_action_call(self, expr: ast.AST) -> Optional[ast.Call]:
        if isinstance(expr, ast.Await):
            expr = expr.value
        if not isinstance(expr, ast.Call):
            return None
        if self._is_action_func(expr.func):
            return expr
        run_action = self._convert_run_action(expr)
        if run_action is not None:
            return run_action
        return None

    def _convert_run_action(self, call: ast.Call) -> Optional[ast.Call]:
        func = call.func
        if not (isinstance(func, ast.Attribute) and func.attr == "run_action" and call.args):
            return None
        action_ref = call.args[0]
        if not self._is_action_func(action_ref):
            return None
        new_call = ast.Call(
            func=action_ref,
            args=[],
            keywords=call.keywords,
        )
        return new_call

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
        for idx, arg in enumerate(call.args):
            action_call = self._extract_action_call(arg)
            if action_call is None and isinstance(arg, ast.Await):
                action_call = self._extract_action_call(arg.value)
            if action_call is None:
                continue
            target_name = flat_targets[idx] if idx < len(flat_targets) else None
            dag_node = self._build_node(action_call, target_name)
            if target_name:
                self._var_to_node[target_name] = dag_node.id
            self._append_node(dag_node)

    def _is_complex_block(self, expr: ast.AST) -> bool:
        return (
            isinstance(expr, ast.Call)
            and isinstance(expr.func, ast.Name)
            and expr.func.id in {"list", "dict"}
        )

    def _capture_block(self, node: ast.AST) -> None:
        snippet = ast.get_source_segment(self._source, node)
        if snippet is None:
            snippet = ast.unparse(node)
        snippet = textwrap.dedent(snippet).strip()
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
        self._append_node(block_node)

    def _append_node(self, node: DagNode) -> None:
        if self._last_node_id is not None:
            node.wait_for_sync = [self._last_node_id]
        self.nodes.append(node)
        self._last_node_id = node.id

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

    def _build_node(
        self,
        call: ast.Call,
        target: Optional[str],
        *,
        guard: Optional[str] = None,
        extra_dependencies: Optional[List[str]] = None,
    ) -> DagNode:
        name = self._format_call_name(call.func)
        node_id = self._new_node_id()
        kwargs: Dict[str, str] = {}
        dependencies: List[str] = []
        for kw in call.keywords:
            if kw.arg is None:
                continue
            kwargs[kw.arg] = ast.unparse(kw.value)
            dependencies.extend(self._dependencies_from_expr(kw.value))
        if extra_dependencies:
            dependencies.extend(extra_dependencies)
        action_name, module_name = self._action_defs.get(name, (name, None))
        return DagNode(
            id=node_id,
            action=action_name,
            module=module_name,
            kwargs=kwargs,
            depends_on=sorted(set(dependencies)),
            produces=[target] if target else [],
            guard=guard,
        )

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
        for sub in ast.walk(node):
            if isinstance(sub, ast.Name) and isinstance(sub.ctx, ast.Store):
                produced.add(sub.id)
            elif isinstance(sub, ast.Attribute) and isinstance(sub.value, ast.Name):
                if sub.attr in {"append", "extend", "update", "add"}:
                    produced.add(sub.value.id)
        return produced

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
