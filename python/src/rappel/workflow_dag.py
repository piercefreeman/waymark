import ast
import copy
import inspect
import textwrap
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Set, Tuple, cast

if TYPE_CHECKING:  # pragma: no cover
    from .workflow import BackoffPolicy, ExponentialBackoff, LinearBackoff, Workflow


class EdgeType:
    """Edge types for DAG traversal."""

    FORWARD = "forward"  # Normal dependency edge
    CONTINUE = "continue"  # Loop head -> body (more iterations)
    BREAK = "break"  # Loop head -> exit (iterator exhausted)
    BACK = "back"  # Loop body tail -> loop head (iteration complete)


class NodeType:
    """Node types for distinguishing action nodes from synthetic control nodes."""

    ACTION = "action"  # Regular action node executed by workers
    LOOP_HEAD = "loop_head"  # Synthetic loop control node (evaluated by scheduler)


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
    loop: Optional["LoopSpec"] = None
    multi_action_loop: Optional["MultiActionLoopSpec"] = None  # New: multi-action loops
    sleep_duration_expr: Optional[str] = None  # Expression evaluated at scheduling time
    backoff: Optional["BackoffPolicy"] = None
    # Cycle-based loop support
    node_type: str = NodeType.ACTION  # "action" or "loop_head"
    loop_head_meta: Optional["LoopHeadMeta"] = None  # Metadata for loop_head nodes
    loop_id: Optional[str] = None  # For body nodes: which loop they belong to


RETURN_VARIABLE = "__workflow_return"
UNLIMITED_RETRIES = 2_147_483_647


@dataclass
class DagEdge:
    """Represents an edge in the DAG with its type."""

    from_node: str
    to_node: str
    edge_type: str = EdgeType.FORWARD


@dataclass
class PreambleOp:
    """Pure computation operations evaluated by scheduler."""

    op_type: str  # "set_iterator_index", "set_iterator_value", "set_accumulator_len"
    var: str
    accumulator: Optional[str] = None  # For set_accumulator_len


@dataclass
class AccumulatorSpec:
    """Specifies how to collect values into an accumulator across iterations."""

    var: str  # Python variable name (e.g., "results")
    source_node: Optional[str] = None  # Node whose result to append
    source_expr: Optional[str] = None  # Expression to evaluate


@dataclass
class LoopHeadMeta:
    """Metadata for loop_head synthetic nodes."""

    iterator_source: str  # Node ID that produces the iterator
    loop_var: str  # Variable name for current item
    body_entry: List[str]  # Entry node(s) for loop body
    body_tail: str  # Last node before back edge
    exit_target: str  # Node to execute after loop completes
    accumulators: List[AccumulatorSpec]  # Variables to collect across iterations
    preamble: List[PreambleOp]  # Pure computations before each iteration
    body_nodes: List[str] = field(default_factory=list)  # All nodes in loop body


@dataclass
class WorkflowDag:
    nodes: List[DagNode]
    return_variable: Optional[str] = None
    edges: List[DagEdge] = field(default_factory=list)  # Explicit edges with types


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
    backoff: Optional["BackoffPolicy"] = None


@dataclass
class ParsedActionCall:
    call: ast.Call
    config: Optional[RunActionConfig] = None


@dataclass
class ExceptionEdge:
    source_node_id: str
    exception_type: Optional[str] = None
    exception_module: Optional[str] = None


@dataclass
class LoopSpec:
    iterable_expr: str
    loop_var: str
    accumulator: str
    body_action: str
    body_kwargs: Dict[str, str]
    preamble: Optional[str] = None
    body_module: Optional[str] = None


@dataclass
class LoopBodyNode:
    """A node within a loop body sub-DAG."""

    id: str  # e.g., "phase_0"
    action: str  # Action name
    module: Optional[str]  # Module containing the action
    kwargs: Dict[str, str]  # Kwargs as expression strings
    depends_on: List[str]  # Dependencies within body graph (phase IDs)
    output_var: str  # Variable to capture result
    timeout_seconds: Optional[int] = None
    max_retries: Optional[int] = None


@dataclass
class LoopBodyGraph:
    """A sub-DAG executed per loop iteration, supporting multiple actions."""

    nodes: List[LoopBodyNode]
    result_variable: str  # Variable from final node to append to accumulator


@dataclass
class MultiActionLoopSpec:
    """Loop specification supporting multiple actions per iteration."""

    iterable_expr: str
    loop_var: str
    accumulator: str
    body_graph: LoopBodyGraph
    preamble: Optional[str] = None  # Python code before first action


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
    """Represents one branch of an if/elif/else with an action call."""

    guard: str  # The guard expression for this branch
    preamble: List[ast.stmt]  # Statements before the action call
    action_call: ParsedActionCall  # The action call
    action_target: Optional[str]  # Variable assigned by the action
    postamble: List[ast.stmt]  # Statements after the action call


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
    return WorkflowDag(
        nodes=builder.nodes,
        return_variable=builder.return_variable,
        edges=builder.edges,
    )


def _discover_action_names(module: Any) -> Dict[str, ActionDefinition]:
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


class ModuleIndex:
    def __init__(self, module_source: str) -> None:
        self._source = module_source
        self._imports: Dict[str, str] = {}
        self._definitions: Dict[str, str] = {}
        self._definition_deps: Dict[str, Set[str]] = {}  # Track dependencies of each definition
        tree = ast.parse(module_source)
        for node in tree.body:
            snippet = ast.get_source_segment(module_source, node)
            if snippet is None:
                continue
            text = textwrap.dedent(snippet)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                self._definitions[node.name] = text
                # Extract dependencies from the definition (base classes, type hints, etc.)
                self._definition_deps[node.name] = self._extract_definition_dependencies(node)
            elif isinstance(node, (ast.Import, ast.ImportFrom)):
                for alias in node.names:
                    exposed = alias.asname or alias.name.split(".")[0]
                    self._imports[exposed] = text

    def _extract_definition_dependencies(self, node: ast.AST) -> Set[str]:
        """Extract names referenced in a class/function definition."""
        deps: Set[str] = set()
        for sub in ast.walk(node):
            if isinstance(sub, ast.Name) and isinstance(sub.ctx, ast.Load):
                deps.add(sub.id)
        return deps

    @property
    def symbols(self) -> Set[str]:
        return set(self._imports) | set(self._definitions)

    def resolve(self, names: Iterable[str]) -> Tuple[List[str], List[str]]:
        """Resolve names to their import and definition blocks.

        This method transitively resolves dependencies - if a class definition
        references other imports or definitions (like base classes), those are
        included as well.
        """
        import_blocks: List[str] = []
        definition_blocks: List[str] = []
        resolved: Set[str] = set()
        to_resolve = list(names)

        while to_resolve:
            name = to_resolve.pop()
            if name in resolved:
                continue
            resolved.add(name)

            if name in self._imports:
                import_blocks.append(self._imports[name])
            elif name in self._definitions:
                definition_blocks.append(self._definitions[name])
                # Add transitive dependencies from this definition
                deps = self._definition_deps.get(name, set())
                for dep in deps:
                    if dep not in resolved and dep in self.symbols:
                        to_resolve.append(dep)

        return sorted(set(import_blocks)), sorted(set(definition_blocks))


class WorkflowDagBuilder(ast.NodeVisitor):
    def __init__(
        self,
        action_defs: Dict[str, ActionDefinition],
        module_index: ModuleIndex,
        source: str,
    ) -> None:
        self.nodes: List[DagNode] = []
        self.edges: List[DagEdge] = []  # Explicit edges for cycle-based loops
        # Track the latest producer of each variable for dependency lookup
        self._var_to_node: Dict[str, str] = {}
        # Track ALL producers of each variable (for convergent branches like try/except, if/else)
        self._var_all_producers: Dict[str, List[str]] = {}
        self._counter = 0
        self._action_defs = action_defs
        self._module_index = module_index
        self._source = source
        self._last_node_id: Optional[str] = None
        self._collections: Dict[str, List[str]] = {}
        self._return_variable: Optional[str] = None
        self._try_stack: List[_TryScope] = []
        self._handler_stack: List[_ExceptionHandlerContext] = []
        self._loop_counter = 0  # Counter for generating unique loop IDs

    # pylint: disable=too-many-return-statements
    def visit_Assign(self, node: ast.Assign) -> Any:
        action_call = self._extract_action_call(node.value)
        if action_call is not None:
            target = self._extract_target(node.targets)
            dag_node = self._build_node(action_call, target)
            if target:
                self._record_variable_producer(target, dag_node.id)
                self._collections.pop(target, None)
            self._append_node(dag_node)
            return
        gather = self._match_gather_call(node.value)
        if gather is not None:
            if self._handle_gather(node.targets, gather):
                return
            # Gather couldn't be unrolled - fall through to capture_block
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
            if self._handle_gather([], gather):
                return
            # Gather couldn't be unrolled - fall through
        sleep_seconds = self._match_sleep_call(node.value)
        if sleep_seconds is not None:
            self._append_sleep_node(sleep_seconds)
            return
        if self._is_complex_block(node.value):
            self._capture_block(node.value)
            return
        # Capture expression statements that have side effects (e.g., method calls like append)
        if self._is_side_effect_expr(node.value):
            self._capture_block(node)
            return
        self.generic_visit(node)

    def visit_For(self, node: ast.For) -> Any:
        # Try multi-action loop first (more general)
        if self._handle_multi_action_loop(node):
            return
        # Fall back to single-action loop (legacy)
        if self._handle_loop_controller(node):
            return
        if self._handle_action_for_loop(node):
            return
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
        action_call = self._extract_action_call(node.value)
        if action_call is not None:
            self._return_variable = RETURN_VARIABLE
            dag_node = self._build_node(action_call, RETURN_VARIABLE)
            self._record_variable_producer(RETURN_VARIABLE, dag_node.id)
            self._collections.pop(RETURN_VARIABLE, None)
            self._append_node(dag_node)
            return
        gather = self._match_gather_call(node.value)
        if gather is not None:
            self._return_variable = RETURN_VARIABLE
            target = ast.Name(id=RETURN_VARIABLE, ctx=ast.Store())
            if self._handle_gather([target], gather):
                return
            # Gather couldn't be unrolled - fall through
        if isinstance(node.value, ast.Await):
            line = getattr(node, "lineno", "?")
            raise ValueError(
                f"return await is only supported for action calls (line {line}); "
                "assign the awaited value to a variable or return a plain expression"
            )
        if isinstance(node.value, ast.Name) and node.value.id in self._var_to_node:
            self._return_variable = node.value.id
            return
        snippet = f"{RETURN_VARIABLE} = {ast.unparse(node.value)}"
        self._return_variable = RETURN_VARIABLE
        self._append_python_block(snippet, [RETURN_VARIABLE])

    def _handle_conditional_actions(self, node: ast.If) -> bool:
        """Handle if/elif/else statements containing action calls.

        This method supports multi-statement branches where each branch can have:
        - Preamble: statements before the action call
        - Action: exactly one action call per branch
        - Postamble: statements after the action call

        For elif chains, each condition builds up to exclude previous conditions.
        """
        if not node.orelse:
            if self._branch_contains_action(node.body):
                line = getattr(node, "lineno", "?")
                raise ValueError(f"conditional action at line {line} requires an else branch")
            return False

        # Check if any branch contains an action
        if not self._conditional_contains_action(node):
            return False

        # Extract all branches from the if/elif/else chain
        branches = self._extract_all_branches(node, parent_guard=None)
        if branches is None:
            line = getattr(node, "lineno", "?")
            raise ValueError(
                f"conditional with actions must have exactly one action call per branch (line {line})"
            )

        # Validate that all branches have consistent targets
        action_targets = {b.action_target for b in branches}
        if len(action_targets) > 1 and None in action_targets:
            line = getattr(node, "lineno", "?")
            raise ValueError(
                f"conditional branches must all assign to a target or none should (line {line})"
            )
        non_none_targets = {t for t in action_targets if t is not None}
        if len(non_none_targets) > 1:
            line = getattr(node, "lineno", "?")
            raise ValueError(f"conditional branches must assign to the same target (line {line})")

        # Get the common action target (if any)
        action_target = next(iter(non_none_targets), None)

        # Track all generated nodes for the merge
        branch_node_ids: List[str] = []
        branch_temp_vars: List[str] = []
        branch_postambles: List[Tuple[str, List[ast.stmt]]] = []

        for branch in branches:
            guard_deps = self._dependencies_from_guard_expr(branch.guard)

            # Create preamble node if needed
            preamble_node_id: Optional[str] = None
            if branch.preamble:
                preamble_node_id = self._create_guarded_preamble(
                    branch.preamble, branch.guard, guard_deps
                )

            # Create the guarded action node
            action_deps = list(guard_deps)
            if preamble_node_id:
                action_deps.append(preamble_node_id)

            action_node = self._build_node(
                branch.action_call,
                target=None,
                guard=branch.guard,
                extra_dependencies=action_deps,
            )

            # If the action has a target, use a temp variable
            temp_var: Optional[str] = None
            if action_target:
                temp_var = f"__branch_{action_target}_{action_node.id}"
                action_node.produces = [temp_var]
                branch_temp_vars.append(temp_var)

            self._append_node(action_node)
            branch_node_ids.append(action_node.id)

            # Track postamble for the merge node
            if branch.postamble:
                branch_postambles.append((temp_var or action_node.id, branch.postamble))

        # Create the merge node that handles result assignment and postambles
        if action_target or branch_postambles:
            merge_node = self._build_multi_branch_merge_node(
                action_target,
                branch_temp_vars,
                branch_postambles,
                branch_node_ids,
            )
            if action_target:
                self._record_variable_producer(action_target, merge_node.id)
                self._collections.pop(action_target, None)
            # Also track any variables produced by postambles
            for name in merge_node.produces:
                if name != action_target:
                    self._record_variable_producer(name, merge_node.id)
            self._append_node(merge_node)

        return True

    def _conditional_contains_action(self, node: ast.If) -> bool:
        """Check if an if/elif/else chain contains any action calls."""
        if self._branch_contains_action(node.body):
            return True
        for stmt in node.orelse:
            if isinstance(stmt, ast.If):
                if self._conditional_contains_action(stmt):
                    return True
            elif self._branch_contains_action([stmt]):
                return True
        return self._branch_contains_action(node.orelse)

    def _extract_all_branches(
        self, node: ast.If, parent_guard: Optional[str]
    ) -> Optional[List[_ConditionalBranch]]:
        """Extract all branches from an if/elif/else chain.

        Returns None if any branch has invalid structure (not exactly one action).
        """
        branches: List[_ConditionalBranch] = []

        # Build the guard for this branch
        condition_expr = f"({ast.unparse(node.test).strip()})"
        if parent_guard:
            true_guard = f"({parent_guard}) and {condition_expr}"
        else:
            true_guard = condition_expr

        # Extract the true branch
        true_branch = self._parse_branch_body(node.body, true_guard)
        if true_branch is None:
            return None
        branches.append(true_branch)

        # Handle else/elif
        if len(node.orelse) == 1 and isinstance(node.orelse[0], ast.If):
            # This is an elif - recurse with negated guard
            elif_node = node.orelse[0]
            negated_guard = (
                f"not {condition_expr}"
                if not parent_guard
                else f"({parent_guard}) and not {condition_expr}"
            )
            sub_branches = self._extract_all_branches(elif_node, negated_guard)
            if sub_branches is None:
                return None
            branches.extend(sub_branches)
        elif node.orelse:
            # This is an else branch
            if parent_guard:
                false_guard = f"({parent_guard}) and not {condition_expr}"
            else:
                false_guard = f"not {condition_expr}"
            false_branch = self._parse_branch_body(node.orelse, false_guard)
            if false_branch is None:
                return None
            branches.append(false_branch)

        return branches

    def _parse_branch_body(self, body: List[ast.stmt], guard: str) -> Optional[_ConditionalBranch]:
        """Parse a branch body to extract preamble, action, and postamble.

        Returns None if the branch doesn't have exactly one action call.
        """
        preamble: List[ast.stmt] = []
        action_call: Optional[ParsedActionCall] = None
        action_target: Optional[str] = None
        postamble: List[ast.stmt] = []

        found_action = False
        for stmt in body:
            stmt_action = self._extract_statement_action(stmt)
            if stmt_action is not None:
                if found_action:
                    # Multiple actions in one branch - not supported
                    return None
                found_action = True
                action_call, action_target = stmt_action
            elif found_action:
                postamble.append(stmt)
            else:
                preamble.append(stmt)

        if action_call is None:
            return None

        return _ConditionalBranch(
            guard=guard,
            preamble=preamble,
            action_call=action_call,
            action_target=action_target,
            postamble=postamble,
        )

    def _extract_statement_action(
        self, stmt: ast.stmt
    ) -> Optional[Tuple[ParsedActionCall, Optional[str]]]:
        """Extract action call and target from a statement."""
        if isinstance(stmt, ast.Assign):
            call = self._extract_action_call(stmt.value)
            if call is not None:
                target = self._extract_target(stmt.targets)
                return (call, target)
        elif isinstance(stmt, ast.Expr):
            call = self._extract_action_call(stmt.value)
            if call is not None:
                return (call, None)
        return None

    def _dependencies_from_guard_expr(self, guard: str) -> List[str]:
        """Extract dependencies from a guard expression string."""
        try:
            parsed = ast.parse(guard, mode="eval")
            return self._dependencies_from_expr(parsed.body)
        except SyntaxError:
            return []

    def _create_guarded_preamble(
        self, preamble: List[ast.stmt], guard: str, guard_deps: List[str]
    ) -> str:
        """Create a guarded python block for preamble statements."""
        snippets: List[str] = []
        all_deps: List[str] = list(guard_deps)
        all_produces: Set[str] = set()

        for stmt in preamble:
            snippet = ast.get_source_segment(self._source, stmt) or ast.unparse(stmt)
            normalized = self._normalize_block_snippet(snippet, stmt)
            if normalized:
                snippets.append(normalized)
            all_deps.extend(self._dependencies_from_node(stmt))
            all_produces.update(self._collect_mutated_names(stmt))

        code = "\n".join(snippets)
        references = set()
        for stmt in preamble:
            references.update(self._resolve_module_references(stmt))
        imports, definitions = self._module_index.resolve(references)

        block_id = self._new_node_id()
        block_node = DagNode(
            id=block_id,
            action="python_block",
            kwargs={
                "code": code,
                "imports": "\n".join(imports),
                "definitions": "\n".join(definitions),
            },
            depends_on=sorted(set(all_deps)),
            produces=sorted(all_produces),
            guard=guard,
        )

        for name in all_produces:
            self._record_variable_producer(name, block_id)

        self._append_node(block_node)
        return block_id

    def _build_multi_branch_merge_node(
        self,
        action_target: Optional[str],
        temp_vars: List[str],
        postambles: List[Tuple[str, List[ast.stmt]]],
        dependencies: List[str],
    ) -> DagNode:
        """Build a merge node that handles multiple branches with postambles."""
        code_lines: List[str] = []
        all_produces: Set[str] = set()
        all_references: Set[str] = set()

        if action_target:
            all_produces.add(action_target)

        # Collect all variables produced by postambles
        for _, stmts in postambles:
            for stmt in stmts:
                all_produces.update(self._collect_mutated_names(stmt))
                all_references.update(self._resolve_module_references(stmt))

        # Generate merge code
        for i, temp_var in enumerate(temp_vars):
            # Check that the variable exists AND is not None
            # (skipped guarded nodes may pass None for their temp variables)
            condition = f'locals().get("{temp_var}") is not None'
            if i == 0:
                code_lines.append(f"if {condition}:")
            else:
                code_lines.append(f"elif {condition}:")

            if action_target:
                code_lines.append(f"    {action_target} = {temp_var}")

            # Add postamble for this branch if it exists
            for branch_id, stmts in postambles:
                if branch_id == temp_var:
                    for stmt in stmts:
                        snippet = ast.get_source_segment(self._source, stmt) or ast.unparse(stmt)
                        for line in snippet.split("\n"):
                            code_lines.append(f"    {line}")
                    break

        # Add else clause for error handling if we have temp vars
        if temp_vars:
            code_lines.append("else:")
            if action_target:
                code_lines.append(
                    f'    raise RuntimeError("conditional branch produced no value for {action_target}")'
                )
            else:
                code_lines.append('    raise RuntimeError("no conditional branch executed")')

        merge_code = "\n".join(code_lines)
        imports, definitions = self._module_index.resolve(all_references)

        return DagNode(
            id=self._new_node_id(),
            action="python_block",
            kwargs={
                "code": merge_code,
                "imports": "\n".join(imports),
                "definitions": "\n".join(definitions),
            },
            depends_on=sorted(set(dependencies)),
            produces=sorted(all_produces),
        )

    def _handle_loop_controller(self, node: ast.For) -> bool:
        if node.orelse or not isinstance(node.target, ast.Name):
            return False
        parsed = self._parse_loop_controller_body(node.body)
        if parsed is None:
            return False
        preamble, action_call, accumulator = parsed
        loop_var = node.target.id
        iterable_expr = ast.unparse(node.iter)
        dependencies = self._dependencies_from_expr(node.iter)
        for stmt in preamble:
            dependencies.extend(self._dependencies_from_node(stmt))
        body_kwargs, kw_deps, action_name, action_module = self._loop_body_metadata(action_call)
        dependencies.extend(kw_deps)

        # Generate unique loop ID
        loop_id = self._new_loop_id()

        # 1. Create iterator source node (computes the iterable and initializes accumulator)
        iter_source_id = self._new_node_id()
        iter_source_code = f"__iter_{loop_id} = list({iterable_expr})\n{accumulator} = []"
        iter_source_node = DagNode(
            id=iter_source_id,
            action="python_block",
            kwargs={"code": iter_source_code, "imports": "", "definitions": ""},
            depends_on=sorted(set(dependencies)),
            produces=[f"__iter_{loop_id}", accumulator],
        )
        self.nodes.append(iter_source_node)
        self._record_variable_producer(f"__iter_{loop_id}", iter_source_id)
        self._record_variable_producer(accumulator, iter_source_id)

        # 2. Create loop_head synthetic node
        loop_head_id = self._new_node_id()

        # 2b. Create preamble node if there are preamble statements
        preamble_node_id: Optional[str] = None
        if preamble:
            preamble_node_id = self._new_node_id()
            preamble_code = "\n".join(ast.unparse(stmt) for stmt in preamble)
            preamble_produces: List[str] = []
            for stmt in preamble:
                if isinstance(stmt, ast.Assign):
                    for target in stmt.targets:
                        if isinstance(target, ast.Name):
                            preamble_produces.append(target.id)
                elif isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
                    preamble_produces.append(stmt.target.id)
            preamble_node = DagNode(
                id=preamble_node_id,
                action="python_block",
                kwargs={"code": preamble_code, "imports": "", "definitions": ""},
                depends_on=[loop_head_id],
                produces=preamble_produces,
                loop_id=loop_id,
            )
            self.nodes.append(preamble_node)
            for var in preamble_produces:
                self._record_variable_producer(var, preamble_node_id)

        # Determine what the body action depends on
        body_depends_on = preamble_node_id if preamble_node_id else loop_head_id

        # 3. Create body action node
        body_node_id = self._new_node_id()
        body_node = DagNode(
            id=body_node_id,
            action=action_name,
            kwargs=body_kwargs,
            module=action_module,
            depends_on=[body_depends_on],  # Depends on preamble or loop_head
            produces=[],  # Result captured by accumulator update
            loop_id=loop_id,
        )
        if action_call.config:
            if action_call.config.timeout_seconds is not None:
                body_node.timeout_seconds = action_call.config.timeout_seconds
            if action_call.config.max_retries is not None:
                body_node.max_retries = action_call.config.max_retries
            if action_call.config.backoff is not None:
                body_node.backoff = action_call.config.backoff
        self.nodes.append(body_node)

        # 4. Create exit node (finalizes the accumulator)
        exit_node_id = self._new_node_id()
        exit_node = DagNode(
            id=exit_node_id,
            action="python_block",
            kwargs={"code": f"pass  # Loop {loop_id} complete", "imports": "", "definitions": ""},
            depends_on=[loop_head_id],  # Will be unlocked via break edge
            produces=[],
        )
        self.nodes.append(exit_node)

        # 5. Create loop_head node with metadata
        # body_entry is the first node after continue edge (preamble or body action)
        # body_tail is the last node before back edge (always the body action)
        body_entry_id = preamble_node_id if preamble_node_id else body_node_id
        # Collect all body nodes for reset on back edge
        all_body_nodes = [preamble_node_id, body_node_id] if preamble_node_id else [body_node_id]
        loop_head_meta = LoopHeadMeta(
            iterator_source=iter_source_id,
            loop_var=loop_var,
            body_entry=[body_entry_id],
            body_tail=body_node_id,
            exit_target=exit_node_id,
            accumulators=[AccumulatorSpec(var=accumulator, source_node=body_node_id)],
            preamble=[
                PreambleOp(op_type="set_iterator_index", var=f"__idx_{loop_id}"),
                PreambleOp(op_type="set_iterator_value", var=loop_var),
            ],
            body_nodes=all_body_nodes,
        )
        loop_head_node = DagNode(
            id=loop_head_id,
            action="loop_head",
            kwargs={},
            module="rappel.workflow_runtime",
            depends_on=[iter_source_id],
            produces=[loop_var, f"__idx_{loop_id}"],
            node_type=NodeType.LOOP_HEAD,
            loop_head_meta=loop_head_meta,
            loop_id=loop_id,
        )
        self.nodes.append(loop_head_node)

        # 6. Add edges
        self._add_edge(iter_source_id, loop_head_id, EdgeType.FORWARD)
        # Continue edge goes to body_entry (preamble or body action)
        self._add_edge(loop_head_id, body_entry_id, EdgeType.CONTINUE)
        # If we have a preamble, add forward edge from preamble to body action
        if preamble_node_id:
            self._add_edge(preamble_node_id, body_node_id, EdgeType.FORWARD)
        # Back edge comes from body_tail (body action)
        self._add_edge(body_node_id, loop_head_id, EdgeType.BACK)
        self._add_edge(loop_head_id, exit_node_id, EdgeType.BREAK)

        # Update variable tracking - accumulator is "produced" by exit node for downstream deps
        self._record_variable_producer(accumulator, exit_node_id)
        self._collections.pop(accumulator, None)
        self._last_node_id = exit_node_id
        return True

    def _handle_multi_action_loop(self, node: ast.For) -> bool:
        """Handle loops with multiple action calls in the body.

        Pattern:
            results = []
            for item in items:
                # optional preamble (pure Python)
                a = await action_one(x=item)
                b = await action_two(y=a)
                # ... more actions
                results.append(final_var)

        Emits cycle-based loop structure with multiple body action nodes.
        """
        if node.orelse or not isinstance(node.target, ast.Name):
            return False

        parsed = self._parse_multi_action_loop_body(node.body)
        if parsed is None:
            return False

        preamble_stmts, action_stmts, accumulators = parsed

        # Use multi-action loop for:
        # - Multiple actions in body, OR
        # - Multiple accumulators (even with single action)
        # Single action with single accumulator is handled by _handle_loop_controller
        if len(action_stmts) < 2 and len(accumulators) < 2:
            return False

        loop_var = node.target.id
        iterable_expr = ast.unparse(node.iter)

        # Build dependencies from iterable
        dependencies = self._dependencies_from_expr(node.iter)

        # Extract dependencies from preamble statements
        for stmt in preamble_stmts:
            dependencies.extend(self._dependencies_from_node(stmt))

        # Generate unique loop ID
        loop_id = self._new_loop_id()

        # 1. Create iterator source node (computes the iterable and initializes accumulators)
        iter_source_id = self._new_node_id()
        accumulator_inits = "\n".join(f"{acc} = []" for acc, _ in accumulators)
        iter_source_code = f"__iter_{loop_id} = list({iterable_expr})\n{accumulator_inits}"
        accumulator_names = [acc for acc, _ in accumulators]
        iter_source_node = DagNode(
            id=iter_source_id,
            action="python_block",
            kwargs={"code": iter_source_code, "imports": "", "definitions": ""},
            depends_on=sorted(set(dependencies)),
            produces=[f"__iter_{loop_id}"] + accumulator_names,
        )
        self.nodes.append(iter_source_node)
        self._record_variable_producer(f"__iter_{loop_id}", iter_source_id)
        for acc in accumulator_names:
            self._record_variable_producer(acc, iter_source_id)

        # 2. Create loop_head synthetic node (ID reserved, added later)
        loop_head_id = self._new_node_id()

        # 3. Create body action nodes
        body_node_ids: List[str] = []
        prev_body_node_id: Optional[str] = None

        for idx, (target_var, action_call) in enumerate(action_stmts):
            call = action_call.call

            # Get action metadata
            action_name = self._format_call_name(call.func)
            action_def = self._action_defs.get(action_name)

            # Build kwargs
            kwargs: Dict[str, str] = {}
            for kw in call.keywords:
                if kw.arg is None:
                    continue
                kwargs[kw.arg] = ast.unparse(kw.value)

            # Handle positional arguments
            if call.args:
                if action_def is None:
                    line = call.lineno if hasattr(call, "lineno") else "?"
                    raise ValueError(
                        f"action '{action_name}' uses positional arguments "
                        f"but metadata is unavailable (line {line})"
                    )
                used_keys = set(kwargs)
                arg_mappings = self._map_positional_args(call, action_def, used_keys)
                for key, expr in arg_mappings:
                    kwargs[key] = expr

            # Determine module
            if action_def:
                module_name = action_def.module_name
                resolved_action_name = action_def.action_name
            else:
                module_name = None
                resolved_action_name = action_name

            body_node_id = self._new_node_id()
            body_node_ids.append(body_node_id)

            # First body node depends on loop_head, subsequent nodes depend on previous
            if idx == 0:
                node_deps = [loop_head_id]
            else:
                node_deps = [prev_body_node_id] if prev_body_node_id else []

            body_node = DagNode(
                id=body_node_id,
                action=resolved_action_name,
                kwargs=kwargs,
                module=module_name,
                depends_on=node_deps,
                produces=[target_var],
                loop_id=loop_id,
            )

            # Apply timeout/retry config if present
            config = action_call.config
            if config:
                if config.timeout_seconds is not None:
                    body_node.timeout_seconds = config.timeout_seconds
                if config.max_retries is not None:
                    body_node.max_retries = config.max_retries
                if config.backoff is not None:
                    body_node.backoff = config.backoff

            self.nodes.append(body_node)
            self._record_variable_producer(target_var, body_node_id)
            prev_body_node_id = body_node_id

        # The last body node is the tail
        body_tail_id = body_node_ids[-1]

        # 4. Create exit node (finalizes the accumulator)
        exit_node_id = self._new_node_id()
        exit_node = DagNode(
            id=exit_node_id,
            action="python_block",
            kwargs={"code": f"pass  # Loop {loop_id} complete", "imports": "", "definitions": ""},
            depends_on=[loop_head_id],  # Will be unlocked via break edge
            produces=[],
        )
        self.nodes.append(exit_node)

        # 5. Create loop_head node with metadata
        # Build AccumulatorSpec for each accumulator with its source expression
        accumulator_specs = [
            AccumulatorSpec(var=acc_name, source_node=body_tail_id, source_expr=source_expr)
            for acc_name, source_expr in accumulators
        ]
        loop_head_meta = LoopHeadMeta(
            iterator_source=iter_source_id,
            loop_var=loop_var,
            body_entry=[body_node_ids[0]],  # First body node
            body_tail=body_tail_id,
            exit_target=exit_node_id,
            accumulators=accumulator_specs,
            preamble=[
                PreambleOp(op_type="set_iterator_index", var=f"__idx_{loop_id}"),
                PreambleOp(op_type="set_iterator_value", var=loop_var),
            ],
            body_nodes=body_node_ids,  # All body nodes for reset on back edge
        )
        loop_head_node = DagNode(
            id=loop_head_id,
            action="loop_head",
            kwargs={},
            module="rappel.workflow_runtime",
            depends_on=[iter_source_id],
            produces=[loop_var, f"__idx_{loop_id}"],
            node_type=NodeType.LOOP_HEAD,
            loop_head_meta=loop_head_meta,
            loop_id=loop_id,
        )
        self.nodes.append(loop_head_node)

        # 6. Add edges
        self._add_edge(iter_source_id, loop_head_id, EdgeType.FORWARD)
        self._add_edge(loop_head_id, body_node_ids[0], EdgeType.CONTINUE)

        # Add forward edges between consecutive body nodes
        for i in range(len(body_node_ids) - 1):
            self._add_edge(body_node_ids[i], body_node_ids[i + 1], EdgeType.FORWARD)

        # Back edge from last body node to loop head
        self._add_edge(body_tail_id, loop_head_id, EdgeType.BACK)
        self._add_edge(loop_head_id, exit_node_id, EdgeType.BREAK)

        # Update variable tracking - all accumulators are "produced" by exit node for downstream deps
        for acc_name in accumulator_names:
            self._record_variable_producer(acc_name, exit_node_id)
            self._collections.pop(acc_name, None)
        self._last_node_id = exit_node_id
        return True

    def _parse_multi_action_loop_body(
        self, body: List[ast.stmt]
    ) -> Optional[Tuple[List[ast.stmt], List[Tuple[str, ParsedActionCall]], List[Tuple[str, str]]]]:
        """Parse a loop body with potentially multiple action calls.

        Returns:
            Tuple of (preamble_stmts, action_stmts, accumulators) or None
            - preamble_stmts: statements before any action
            - action_stmts: list of (target_var, ParsedActionCall) tuples
            - accumulators: list of (accumulator_name, source_var) tuples
        """
        if len(body) < 2:
            return None

        # Extract trailing append statements (one or more)
        append_stmts: List[Tuple[str, str]] = []  # (accumulator, source_var)
        non_append_body = list(body)

        while non_append_body:
            append_target = self._extract_append_target(non_append_body[-1])
            if append_target is None:
                break
            accumulator, appended_value = append_target
            # Appended value can be a name or an attribute access (e.g., processed["result"])
            if isinstance(appended_value, ast.Name):
                source_var = appended_value.id
            elif isinstance(appended_value, ast.Subscript):
                # Allow subscript access like processed["result"]
                source_var = ast.unparse(appended_value)
            else:
                break
            append_stmts.insert(0, (accumulator, source_var))
            non_append_body = non_append_body[:-1]

        # Need at least one append
        if not append_stmts:
            return None

        # Scan body (excluding appends) for action calls
        action_stmts: List[Tuple[str, ParsedActionCall]] = []
        preamble_stmts: List[ast.stmt] = []
        found_first_action = False

        for stmt in non_append_body:
            if isinstance(stmt, ast.Assign):
                if len(stmt.targets) == 1 and isinstance(stmt.targets[0], ast.Name):
                    action_call = self._extract_action_call(stmt.value)
                    if action_call is not None:
                        found_first_action = True
                        target_var = stmt.targets[0].id
                        action_stmts.append((target_var, action_call))
                        continue

            # If we haven't found first action yet, this is preamble
            if not found_first_action:
                # Preamble cannot contain action calls
                if self._branch_contains_action([stmt]):
                    return None
                preamble_stmts.append(stmt)
            else:
                # After first action, we don't support interleaved pure Python
                # (for now - could add support later)
                if self._branch_contains_action([stmt]):
                    # It's an action but not in expected format
                    return None
                # Non-action statement after first action - not supported
                return None

        # Must have at least one action
        if not action_stmts:
            return None

        return preamble_stmts, action_stmts, append_stmts

    def _parse_loop_controller_body(
        self, body: List[ast.stmt]
    ) -> Optional[Tuple[List[ast.stmt], ParsedActionCall, str]]:
        if len(body) < 2:
            return None
        append_target = self._extract_append_target(body[-1])
        if append_target is None:
            return None
        accumulator, appended_value = append_target
        if not isinstance(appended_value, ast.Name):
            return None
        action_stmt = body[-2]
        if not isinstance(action_stmt, ast.Assign):
            return None
        if len(action_stmt.targets) != 1 or not isinstance(action_stmt.targets[0], ast.Name):
            return None
        if action_stmt.targets[0].id != appended_value.id:
            return None
        action_call = self._extract_action_call(action_stmt.value)
        if action_call is None:
            return None
        preamble = body[:-2]
        for stmt in preamble:
            if self._branch_contains_action([stmt]):
                return None
        return preamble, action_call, accumulator

    def _extract_append_target(self, stmt: ast.stmt) -> Optional[Tuple[str, ast.AST]]:
        if not isinstance(stmt, ast.Expr):
            return None
        call = stmt.value
        if not isinstance(call, ast.Call):
            return None
        func = call.func
        if not isinstance(func, ast.Attribute):
            return None
        if func.attr != "append" or not isinstance(func.value, ast.Name):
            return None
        if not call.args or len(call.args) != 1:
            return None
        return func.value.id, call.args[0]

    def _loop_body_metadata(
        self, parsed_call: ParsedActionCall
    ) -> Tuple[Dict[str, str], List[str], str, Optional[str]]:
        call = parsed_call.call
        name = self._format_call_name(call.func)
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
                line = call.lineno if hasattr(call, "lineno") else "?"
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
        return kwargs, dependencies, action_name, module_name

    def _format_preamble_snippet(self, preamble: List[ast.stmt]) -> str:
        snippets: List[str] = []
        for stmt in preamble:
            snippet = ast.get_source_segment(self._source, stmt) or ast.unparse(stmt)
            normalized = self._normalize_block_snippet(snippet, stmt)
            if normalized:
                snippets.append(normalized)
        return "\n".join(snippets)

    def _handle_action_for_loop(self, node: ast.For) -> bool:
        if node.orelse:
            return False
        iter_name = self._resolve_collection_name(node.iter)
        if iter_name is None:
            return False
        sources = self._collections.get(iter_name)
        if not sources:
            return False
        if not isinstance(node.target, ast.Name):
            return False
        if not self._branch_contains_action(node.body):
            return False
        loop_var = node.target.id
        for source in sources:
            for stmt in node.body:
                substituted = self._substitute_node(stmt, {loop_var: source})
                self._attach_snippet_override(substituted)
                self._visit_loop_body_statement(substituted)
        last_source = sources[-1]
        if last_source in self._var_to_node:
            self._record_variable_producer(loop_var, self._var_to_node[last_source])
        return True

    def _visit_loop_body_statement(self, stmt: ast.stmt) -> None:
        if isinstance(stmt, ast.Expr):
            action_call = self._extract_action_call(stmt.value)
            if action_call is not None:
                self._append_node(self._build_node(action_call, target=None))
                return
            gather = self._match_gather_call(stmt.value)
            if gather is not None:
                if self._handle_gather([], gather):
                    return
                # Gather couldn't be unrolled - fall through to capture_block
            if self._is_complex_block(stmt.value):
                self._capture_block(stmt)
                return
            self._capture_block(stmt)
            return
        self.visit(stmt)

    def _branch_contains_action(self, statements: List[ast.stmt]) -> bool:
        for stmt in statements:
            for sub in ast.walk(stmt):
                if self._extract_action_call(sub) is not None:
                    return True
        return False

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

    def _match_sleep_call(self, expr: ast.AST) -> Optional[str]:
        """Match `await asyncio.sleep(duration)` and return the duration expression as a string."""
        if not isinstance(expr, ast.Await):
            return None
        call = expr.value
        if not isinstance(call, ast.Call):
            return None
        func = call.func
        is_sleep = False
        if isinstance(func, ast.Attribute):
            is_sleep = (
                isinstance(func.value, ast.Name)
                and func.value.id == "asyncio"
                and func.attr == "sleep"
            )
        elif isinstance(func, ast.Name):
            is_sleep = func.id == "sleep"
        if not is_sleep:
            return None
        if not call.args:
            line = getattr(call, "lineno", "?")
            raise ValueError(f"asyncio.sleep() requires a duration argument (line {line})")
        duration_expr = call.args[0]
        self._validate_sleep_duration(duration_expr)
        return ast.unparse(duration_expr)

    def _validate_sleep_duration(self, expr: ast.AST) -> None:
        """Validate that the sleep duration expression is valid.

        We allow any expression that can be evaluated at runtime (variables, literals,
        arithmetic expressions, etc.). We reject negative literals at compile time.
        """
        # Check for negative literal: -10 is UnaryOp(USub, Constant(10))
        if isinstance(expr, ast.UnaryOp) and isinstance(expr.op, ast.USub):
            if isinstance(expr.operand, ast.Constant) and isinstance(
                expr.operand.value, (int, float)
            ):
                raise ValueError("sleep duration must be non-negative")
        # Check for negative constant
        if isinstance(expr, ast.Constant) and isinstance(expr.value, (int, float)):
            if expr.value < 0:
                raise ValueError("sleep duration must be non-negative")

    def _append_sleep_node(self, sleep_duration_expr: str) -> None:
        """Append a sleep node to the DAG."""
        node_id = self._new_node_id()
        sleep_node = DagNode(
            id=node_id,
            action="sleep",
            kwargs={},
            module="rappel.workflow_runtime",
            depends_on=[],
            produces=[],
            sleep_duration_expr=sleep_duration_expr,
        )
        self._append_node(sleep_node)

    def _handle_gather(self, targets: List[ast.expr], call: ast.Call) -> bool:
        """Handle asyncio.gather() call. Returns True if handled, False if caller should fallback."""
        flat_targets = self._flatten_targets(targets)

        # Handle starred list comprehension pattern: asyncio.gather(*[action(x) for x in items])
        if (
            len(call.args) == 1
            and isinstance(call.args[0], ast.Starred)
            and isinstance(call.args[0].value, ast.ListComp)
        ):
            listcomp = call.args[0].value
            if len(flat_targets) == 1 and flat_targets[0]:
                target_name = flat_targets[0]
                if self._handle_gather_starred_listcomp(target_name, listcomp):
                    return True
            # If we can't unroll the starred gather, return False to fall through to python_block
            return False

        if len(flat_targets) == 1 and flat_targets[0]:
            target_name = flat_targets[0]
            item_vars: List[str] = []
            for idx, arg in enumerate(call.args):
                action_call = self._extract_action_call(arg)
                if action_call is None:
                    continue
                item_var = self._collection_item_name(target_name, idx)
                dag_node = self._build_node(action_call, item_var)
                self._record_variable_producer(item_var, dag_node.id)
                item_vars.append(item_var)
                self._append_node(dag_node)
            # Only create collection node if we successfully extracted at least one action
            # Otherwise, this gather pattern isn't supported for DAG unrolling
            if item_vars:
                self._collections[target_name] = list(item_vars)
                self._append_collection_node(target_name, item_vars)
                return True
            # Fall through - gather will be handled as python_block by caller
            return False
        handled = False
        for idx, arg in enumerate(call.args):
            action_call = self._extract_action_call(arg)
            if action_call is None:
                continue
            target_name = flat_targets[idx] if idx < len(flat_targets) else None
            dag_node = self._build_node(action_call, target_name)
            if target_name:
                self._record_variable_producer(target_name, dag_node.id)
                self._collections.pop(target_name, None)
            self._append_node(dag_node)
            handled = True
        return handled

    def _handle_gather_starred_listcomp(self, target: str, listcomp: ast.ListComp) -> bool:
        """Handle asyncio.gather(*[action(x) for x in items]) pattern."""
        if len(listcomp.generators) != 1:
            return False
        generator = listcomp.generators[0]
        if generator.ifs or generator.is_async or not isinstance(generator.target, ast.Name):
            return False
        iter_name = self._resolve_collection_name(generator.iter)
        if iter_name is None:
            return False
        sources = self._collections.get(iter_name)
        if not sources:
            return False
        action_call = self._extract_action_call(listcomp.elt)
        if action_call is None:
            return False
        loop_var = generator.target.id
        produced: List[str] = []
        for idx, source in enumerate(sources):
            substituted_call = self._substitute_call(action_call.call, {loop_var: source})
            parsed = ParsedActionCall(call=substituted_call, config=action_call.config)
            item_var = self._collection_item_name(target, idx)
            dag_node = self._build_node(parsed, item_var)
            self._record_variable_producer(item_var, dag_node.id)
            produced.append(item_var)
            self._append_node(dag_node)
        self._collections[target] = list(produced)
        self._append_collection_node(target, produced)
        return True

    def _is_complex_block(self, expr: ast.AST) -> bool:
        return (
            isinstance(expr, ast.Call)
            and isinstance(expr.func, ast.Name)
            and expr.func.id in {"list", "dict"}
        )

    def _is_side_effect_expr(self, expr: ast.AST) -> bool:
        """Check if an expression has side effects that need to be captured.

        This includes method calls on objects (like list.append, dict.update, etc.)
        that modify state but don't return a meaningful value.
        """
        if isinstance(expr, ast.Call) and isinstance(expr.func, ast.Attribute):
            # Method call like obj.method() - these often have side effects
            return True
        return False

    def _capture_block(self, node: ast.AST) -> None:
        override_snippet = vars(node).get("_rappel_snippet_override")
        snippet = (
            override_snippet
            if override_snippet is not None
            else ast.get_source_segment(self._source, node)
        )
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
            self._record_variable_producer(name, block_id)
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
        # Resolve module references for imports and definitions
        references = self._resolve_module_references(parsed)
        imports, definitions = self._module_index.resolve(references)
        imports_text = "\n".join(imports)
        definitions_text = "\n".join(definitions)
        block_node = DagNode(
            id=block_id,
            action="python_block",
            kwargs={"code": snippet, "imports": imports_text, "definitions": definitions_text},
            depends_on=sorted(set(deps)),
            produces=list(produces),
        )
        for name in produces:
            self._record_variable_producer(name, block_id)
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
            self._record_variable_producer(item_var, dag_node.id)
            produced.append(item_var)
            self._append_node(dag_node)
        self._collections[target] = list(produced)
        self._append_collection_node(target, produced)
        return True

    def _resolve_collection_name(self, expr: ast.AST) -> Optional[str]:
        if isinstance(expr, ast.Name):
            return expr.id
        return None

    def _substitute_node(self, node: ast.stmt, replacements: Dict[str, str]) -> ast.stmt:
        mapping: Dict[str, ast.AST] = {
            name: ast.Name(id=expr, ctx=ast.Load()) for name, expr in replacements.items()
        }
        substituter = _NameSubstituter(mapping)
        return cast(ast.stmt, substituter.visit(copy.deepcopy(node)))

    def _attach_snippet_override(self, node: ast.AST) -> None:
        if not hasattr(node, "_rappel_snippet_override"):
            cast(Any, node)._rappel_snippet_override = ast.unparse(node)

    def _substitute_call(self, call: ast.Call, replacements: Dict[str, str]) -> ast.Call:
        mapping: Dict[str, ast.AST] = {}
        for name, expr in replacements.items():
            mapping[name] = ast.Name(id=expr, ctx=ast.Load())
        substituter = _NameSubstituter(mapping)
        return substituter.visit(copy.deepcopy(call))

    def _parse_run_action_metadata(self, keywords: List[ast.keyword]) -> Optional[RunActionConfig]:
        from .workflow import BackoffPolicy

        timeout_seconds: Optional[int] = None
        retry_limit: Optional[int] = None
        timeout_retry_limit: Optional[int] = None
        backoff: Optional[BackoffPolicy] = None
        for kw in keywords:
            name = kw.arg
            if name is None:
                raise ValueError("run_action does not accept **kwargs")
            if name == "timeout":
                timeout_seconds = self._evaluate_timeout_literal(kw.value)
            elif name == "retry":
                retry_value = self._evaluate_retry_literal(kw.value)
                retry_limit = retry_value
                timeout_retry_limit = retry_value
            elif name == "backoff":
                backoff = self._evaluate_backoff_literal(kw.value)
            else:
                raise ValueError(f"unsupported run_action keyword argument '{name}'")
        if (
            timeout_seconds is None
            and retry_limit is None
            and timeout_retry_limit is None
            and backoff is None
        ):
            return None
        return RunActionConfig(
            timeout_seconds=timeout_seconds,
            max_retries=retry_limit,
            timeout_retry_limit=timeout_retry_limit,
            backoff=backoff,
        )

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
        if isinstance(expr, ast.Constant) and expr.value is None:
            return None
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

    def _evaluate_backoff_literal(self, expr: ast.AST) -> Optional["BackoffPolicy"]:
        if isinstance(expr, ast.Constant) and expr.value is None:
            return None
        if isinstance(expr, ast.Call):
            func_name = self._get_backoff_constructor_name(expr.func)
            if func_name == "LinearBackoff":
                return self._parse_linear_backoff(expr)
            elif func_name == "ExponentialBackoff":
                return self._parse_exponential_backoff(expr)
            elif func_name == "BackoffPolicy":
                raise ValueError(
                    "BackoffPolicy is abstract; use LinearBackoff or ExponentialBackoff"
                )
        raise ValueError("backoff must be LinearBackoff(...), ExponentialBackoff(...), or None")

    def _get_backoff_constructor_name(self, func: ast.AST) -> Optional[str]:
        if isinstance(func, ast.Name):
            return func.id
        if isinstance(func, ast.Attribute):
            return func.attr
        return None

    def _parse_linear_backoff(self, call: ast.Call) -> "LinearBackoff":
        from .workflow import LinearBackoff

        base_delay_ms = 1000  # default
        for kw in call.keywords:
            if kw.arg == "base_delay_ms":
                value = self._literal_number(kw.value)
                if value is None:
                    raise ValueError("LinearBackoff base_delay_ms must be a numeric literal")
                base_delay_ms = int(value)
            else:
                raise ValueError(f"LinearBackoff received unsupported argument '{kw.arg}'")
        if call.args:
            value = self._literal_number(call.args[0])
            if value is None:
                raise ValueError("LinearBackoff base_delay_ms must be a numeric literal")
            base_delay_ms = int(value)
        return LinearBackoff(base_delay_ms=base_delay_ms)

    def _parse_exponential_backoff(self, call: ast.Call) -> "ExponentialBackoff":
        from .workflow import ExponentialBackoff

        base_delay_ms = 1000  # default
        multiplier = 2.0  # default
        for kw in call.keywords:
            if kw.arg == "base_delay_ms":
                value = self._literal_number(kw.value)
                if value is None:
                    raise ValueError("ExponentialBackoff base_delay_ms must be a numeric literal")
                base_delay_ms = int(value)
            elif kw.arg == "multiplier":
                value = self._literal_number(kw.value)
                if value is None:
                    raise ValueError("ExponentialBackoff multiplier must be a numeric literal")
                multiplier = float(value)
            else:
                raise ValueError(f"ExponentialBackoff received unsupported argument '{kw.arg}'")
        # Handle positional args: first is base_delay_ms, second is multiplier
        if len(call.args) >= 1:
            value = self._literal_number(call.args[0])
            if value is None:
                raise ValueError("ExponentialBackoff base_delay_ms must be a numeric literal")
            base_delay_ms = int(value)
        if len(call.args) >= 2:
            value = self._literal_number(call.args[1])
            if value is None:
                raise ValueError("ExponentialBackoff multiplier must be a numeric literal")
            multiplier = float(value)
        return ExponentialBackoff(base_delay_ms=base_delay_ms, multiplier=multiplier)

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
            if parsed_call.config.timeout_retry_limit is not None:
                dag_node.timeout_retry_limit = parsed_call.config.timeout_retry_limit
            if parsed_call.config.backoff is not None:
                dag_node.backoff = parsed_call.config.backoff
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

    def _new_loop_id(self) -> str:
        loop_id = f"loop_{self._loop_counter}"
        self._loop_counter += 1
        return loop_id

    def _add_edge(self, from_node: str, to_node: str, edge_type: str = EdgeType.FORWARD) -> None:
        """Add an explicit edge to the DAG."""
        self.edges.append(DagEdge(from_node=from_node, to_node=to_node, edge_type=edge_type))

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
        """Return node IDs that produce variables referenced in `node`.

        When a variable has multiple producers (e.g., from different branches of
        try/except or if/else), all producers are included as dependencies to
        ensure proper ordering regardless of which branch executed.
        """
        deps: List[str] = []
        for sub in ast.walk(node):
            if isinstance(sub, ast.Name) and isinstance(sub.ctx, ast.Load):
                var_name = sub.id
                # Include ALL producers of this variable, not just the latest
                if var_name in self._var_all_producers:
                    deps.extend(self._var_all_producers[var_name])
                elif var_name in self._var_to_node:
                    deps.append(self._var_to_node[var_name])
        return deps

    def _record_variable_producer(self, var_name: str, node_id: str) -> None:
        """Record that `node_id` produces variable `var_name`.

        This tracks all producers to handle convergent branches properly.
        """
        self._var_to_node[var_name] = node_id
        if var_name not in self._var_all_producers:
            self._var_all_producers[var_name] = []
        self._var_all_producers[var_name].append(node_id)

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
