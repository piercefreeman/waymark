"""Self-contained demo: parse a workflow's run() AST into a DAG."""
from __future__ import annotations

import ast
import asyncio
import inspect
import textwrap
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set


# ----- Action + Workflow primitives (stub implementations) -----

ACTION_NAMES: Set[str] = set()


def action(func):
    """Decorator that tags a callable as a workflow action."""
    setattr(func, "__is_action__", True)
    ACTION_NAMES.add(func.__name__)
    return func


class Workflow:
    """Minimal workflow base."""

    async def run(self):  # pragma: no cover - enforced by subclasses
        raise NotImplementedError


# ----- Demo workflow definition -----


def _format_currency(value: float) -> str:
    return f"${value:,.2f}"  # regular helper (not an action)


def _is_high_value(record: "TransactionRecord") -> bool:
    return record.amount > 100


@dataclass
class Profile:
    user_id: str
    status: str


@dataclass
class Transactions:
    total: float
    count: int
    records: List["TransactionRecord"]


@dataclass
class Summary:
    lifetime_value: float
    transactions: Transactions


@dataclass
class EnrichedSummary:
    summary: Summary
    tier: str


@dataclass
class TransactionRecord:
    amount: float
    currency: str = "USD"


@action
async def fetch_profile(user_id: str) -> Profile:
    ...


@action
async def load_transactions(user_id: str) -> Transactions:
    ...


@action
async def compute_summary(profile: Profile, txns: Transactions) -> Summary:
    ...


@action
async def enrich_summary(summary: Summary) -> EnrichedSummary:
    ...


@action
async def persist_summary(
    user_id: str, summary: EnrichedSummary, pretty: str, total_spent: float
) -> None:
    ...


@action
async def record_high_value(user_id: str, top_spenders: List[float]) -> None:
    ...


class ModuleIndex:
    def __init__(self, module_source: str) -> None:
        self.source = module_source
        self._imports: Dict[str, str] = {}
        self._definitions: Dict[str, str] = {}
        tree = ast.parse(module_source)
        for node in tree.body:
            segment = ast.get_source_segment(module_source, node)
            if segment is None:
                continue
            snippet = textwrap.dedent(segment)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                self._definitions[node.name] = snippet
            elif isinstance(node, (ast.Import, ast.ImportFrom)):
                for alias in node.names:
                    exposed = alias.asname or alias.name.split(".")[0]
                    self._imports[exposed] = snippet

    @property
    def symbols(self) -> Set[str]:
        return set(self._imports) | set(self._definitions)

    def resolve(self, names: Set[str]) -> tuple[List[str], List[str]]:
        import_blocks: List[str] = []
        definition_blocks: List[str] = []
        for name in names:
            if name in self._imports:
                import_blocks.append(self._imports[name])
            elif name in self._definitions:
                definition_blocks.append(self._definitions[name])
        return sorted(set(import_blocks)), sorted(set(definition_blocks))


class CustomerSummaryWorkflow(Workflow):
    def __init__(self, user_id: str):
        self.user_id = user_id

    async def run(self) -> EnrichedSummary:
        profile, txns = await asyncio.gather(
            fetch_profile(user_id=self.user_id),
            load_transactions(user_id=self.user_id),
        )
        summary = await compute_summary(
            profile=profile,
            txns=txns,
        )
        enriched = await enrich_summary(summary=summary)
        pretty = _format_currency(summary.transactions.total)
        top_spenders: List[float] = []
        for record in summary.transactions.records:
            if _is_high_value(record):
                top_spenders.append(record.amount)
        await persist_summary(
            user_id=self.user_id,
            summary=enriched,
            pretty=pretty,
            total_spent=summary.transactions.total,
        )
        await record_high_value(user_id=self.user_id, top_spenders=top_spenders)
        return enriched


# ----- AST → DAG experiment -----

@dataclass
class DagNode:
    id: str
    action: str
    kwargs: Dict[str, str]
    depends_on: List[str] = field(default_factory=list)


class WorkflowDagBuilder(ast.NodeVisitor):
    def __init__(self, action_names: Set[str], module_index: ModuleIndex, source: str) -> None:
        self.nodes: List[DagNode] = []
        self._var_to_node: Dict[str, str] = {}
        self._counter = 0
        self._action_names = action_names
        self._source = source
        self._module_index = module_index

    def visit_Assign(self, node: ast.Assign) -> Any:
        action_call = self._extract_action_call(node.value)
        if action_call is not None:
            target = self._extract_target(node.targets)
            dag_node = self._build_node(action_call, target)
            if target:
                self._var_to_node[target] = dag_node.id
            self.nodes.append(dag_node)
            return
        gather_call = self._match_gather_call(node.value)
        if gather_call is not None:
            self._handle_gather(node.targets, gather_call)
            return
        if self._is_complex_block(node.value):
            self._capture_block(node.value)
            return
        self.generic_visit(node)

    def visit_Expr(self, node: ast.Expr) -> Any:
        action_call = self._extract_action_call(node.value)
        if action_call is not None:
            dag_node = self._build_node(action_call, target=None)
            self.nodes.append(dag_node)
            return
        gather_call = self._match_gather_call(node.value)
        if gather_call is not None:
            self._handle_gather([], gather_call)
            return
        if self._is_complex_block(node.value):
            self._capture_block(node.value)
            return
        self.generic_visit(node)

    def visit_For(self, node: ast.For) -> Any:
        self._capture_block(node)

    def _is_complex_block(self, expr: ast.AST) -> bool:
        return isinstance(expr, ast.Call) and isinstance(expr.func, ast.Name) and expr.func.id in {"list", "dict"}

    # ----- helpers -----
    def _extract_action_call(self, expr: ast.AST, *, unwrap_await: bool = True) -> Optional[ast.Call]:
        if unwrap_await and isinstance(expr, ast.Await):
            expr = expr.value
        if isinstance(expr, ast.Call) and self._is_action_func(expr.func):
            return expr
        return None

    def _is_action_func(self, func: ast.AST) -> bool:
        if isinstance(func, ast.Name):
            return func.id in self._action_names
        if isinstance(func, ast.Attribute):
            return isinstance(func.value, ast.Name) and func.attr in self._action_names
        return False

    def _match_gather_call(self, expr: ast.AST) -> Optional[ast.Call]:
        if not isinstance(expr, ast.Await):
            return None
        call = expr.value
        if not isinstance(call, ast.Call):
            return None
        func = call.func
        is_gather = False
        if isinstance(func, ast.Attribute):
            is_gather = isinstance(func.value, ast.Name) and func.value.id == "asyncio" and func.attr == "gather"
        elif isinstance(func, ast.Name):
            is_gather = func.id == "gather"
        return call if is_gather else None

    def _handle_gather(self, targets: List[ast.expr], call: ast.Call) -> None:
        flat_targets = self._flatten_targets(targets)
        args = call.args
        for idx, arg in enumerate(args):
            action_call = self._extract_action_call(arg, unwrap_await=False)
            if action_call is None and isinstance(arg, ast.Await):
                action_call = self._extract_action_call(arg.value, unwrap_await=False)
            if action_call is None:
                continue
            target_name = flat_targets[idx] if idx < len(flat_targets) else None
            dag_node = self._build_node(action_call, target_name)
            if target_name:
                self._var_to_node[target_name] = dag_node.id
            self.nodes.append(dag_node)

    def _capture_block(self, node: ast.AST) -> None:
        snippet = textwrap.dedent(ast.get_source_segment(self._source, node) or ast.unparse(node)).strip()
        if not snippet:
            return
        node_id = self._new_node_id()
        deps = self._dependencies_from_node(node)
        references = self._resolve_module_references(node)
        import_blocks, definition_blocks = self._module_index.resolve(references)
        logic_node = DagNode(
            id=node_id,
            action="python_block",
            kwargs={
                "code": snippet,
                "imports": import_blocks,
                "definitions": definition_blocks,
            },
            depends_on=sorted(set(deps)),
        )
        produced = self._collect_mutated_names(node)
        for name in produced:
            self._var_to_node[name] = node_id
        self.nodes.append(logic_node)

    def _flatten_targets(self, targets: List[ast.expr]) -> List[Optional[str]]:
        names: List[Optional[str]] = []
        for target in targets:
            names.extend(self._flatten_target(target))
        return names

    def _flatten_target(self, target: ast.expr) -> List[Optional[str]]:
        if isinstance(target, ast.Name):
            return [target.id]
        if isinstance(target, (ast.Tuple, ast.List)):
            flat: List[Optional[str]] = []
            for elt in target.elts:
                flat.extend(self._flatten_target(elt))
            return flat
        return [None]

    def _extract_target(self, targets: List[ast.expr]) -> Optional[str]:
        if len(targets) != 1:
            return None
        target = targets[0]
        if isinstance(target, ast.Name):
            return target.id
        return None

    def _build_node(self, call: ast.Call, target: Optional[str]) -> DagNode:
        action_name = self._format_call_name(call.func)
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
            action=action_name,
            kwargs=kwargs,
            depends_on=sorted(set(dependencies)),
        )

    def _new_node_id(self) -> str:
        node_id = f"node_{self._counter}"
        self._counter += 1
        return node_id

    def _format_call_name(self, func_ref: ast.AST) -> str:
        if isinstance(func_ref, ast.Name):
            return func_ref.id
        return ast.unparse(func_ref)

    def _dependencies_from_expr(self, expr: ast.AST) -> List[str]:
        deps: List[str] = []
        for node in ast.walk(expr):
            base = None
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

    def _resolve_module_references(self, node: ast.AST) -> Set[str]:
        produced = self._collect_mutated_names(node)
        references: Set[str] = set()
        for sub in ast.walk(node):
            if isinstance(sub, ast.Name) and isinstance(sub.ctx, ast.Load):
                if sub.id in produced or sub.id in self._var_to_node:
                    continue
                if sub.id in self._module_index.symbols:
                    references.add(sub.id)
        return references

    def _dependencies_from_node(self, node: ast.AST) -> List[str]:
        assigned = {
            n.id
            for n in ast.walk(node)
            if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Store)
        }
        deps: List[str] = []
        for sub in ast.walk(node):
            if isinstance(sub, ast.Name) and isinstance(sub.ctx, ast.Load):
                if sub.id in self._var_to_node and sub.id not in assigned:
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


def build_workflow_dag(workflow_cls: type[Workflow]) -> List[DagNode]:
    module_index = ModuleIndex(Path(__file__).read_text())
    source = textwrap.dedent(inspect.getsource(workflow_cls.run))
    fn_ast = ast.parse(source)
    builder = WorkflowDagBuilder(ACTION_NAMES, module_index, source)
    builder.visit(fn_ast)
    return builder.nodes


if __name__ == "__main__":
    dag = build_workflow_dag(CustomerSummaryWorkflow)
    print("Workflow DAG:")
    for node in dag:
        print(
            f"- {node.id}: action={node.action} kwargs={node.kwargs} depends_on={node.depends_on}"
        )
