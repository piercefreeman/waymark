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
    depends_on: List[str] = field(default_factory=list)


@dataclass
class WorkflowDag:
    nodes: List[DagNode]


def build_workflow_dag(workflow_cls: type["Workflow"]) -> WorkflowDag:
    module = inspect.getmodule(workflow_cls)
    if module is None:
        raise ValueError(f"unable to locate module for workflow {workflow_cls!r}")
    module_source = inspect.getsource(module)
    module_index = ModuleIndex(module_source)
    action_names = _discover_action_names(module)
    function_source = textwrap.dedent(inspect.getsource(workflow_cls.run))
    builder = WorkflowDagBuilder(action_names, module_index, function_source)
    builder.visit(ast.parse(function_source))
    return WorkflowDag(builder.nodes)


def _discover_action_names(module: Any) -> Set[str]:
    names: Set[str] = set()
    for attr_name in dir(module):
        attr = getattr(module, attr_name)
        if callable(attr) and getattr(attr, "__carabiner_action_name__", None):
            names.add(attr_name)
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
    def __init__(self, action_names: Set[str], module_index: ModuleIndex, source: str) -> None:
        self.nodes: List[DagNode] = []
        self._var_to_node: Dict[str, str] = {}
        self._counter = 0
        self._action_names = action_names
        self._module_index = module_index
        self._source = source

    # pylint: disable=too-many-return-statements
    def visit_Assign(self, node: ast.Assign) -> Any:
        action_call = self._extract_action_call(node.value)
        if action_call is not None:
            target = self._extract_target(node.targets)
            dag_node = self._build_node(action_call, target)
            if target:
                self._var_to_node[target] = dag_node.id
            self.nodes.append(dag_node)
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
            self.nodes.append(self._build_node(action_call, target=None))
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
        name = self._extract_name(func)
        return bool(name and name in self._action_names)

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
            self.nodes.append(dag_node)

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
        block_node = DagNode(
            id=block_id,
            action="python_block",
            kwargs={"code": snippet, "imports": imports, "definitions": definitions},
            depends_on=sorted(set(deps)),
        )
        for name in self._collect_mutated_names(node):
            self._var_to_node[name] = block_id
        self.nodes.append(block_node)

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

    def _build_node(self, call: ast.Call, target: Optional[str]) -> DagNode:
        name = self._format_call_name(call.func)
        node_id = self._new_node_id()
        kwargs: Dict[str, str] = {}
        dependencies: List[str] = []
        for kw in call.keywords:
            if kw.arg is None:
                continue
            kwargs[kw.arg] = ast.unparse(kw.value)
            dependencies.extend(self._dependencies_from_expr(kw.value))
        return DagNode(
            id=node_id,
            action=name,
            kwargs=kwargs,
            depends_on=sorted(set(dependencies)),
        )

    def _new_node_id(self) -> str:
        node_id = f"node_{self._counter}"
        self._counter += 1
        return node_id

    def _format_call_name(self, func_ref: ast.AST) -> str:
        name = self._extract_name(func_ref)
        if name is not None:
            return name
        return ast.unparse(func_ref)

    def _extract_name(self, expr: ast.AST) -> Optional[str]:
        if isinstance(expr, ast.Name):
            return expr.id
        if isinstance(expr, ast.Attribute):
            return expr.attr
        return None

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
