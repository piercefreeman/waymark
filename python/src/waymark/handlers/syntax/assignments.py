"""Assignment validation, expression binding, and comprehension expansion."""

import ast
import copy
from collections.abc import Iterable

from waymark.compiler_ast import make_span, rewrite_load_names
from waymark.compiler_context import CompilerContext
from waymark.compiler_diagnostics import reject
from waymark.compiler_errors import UnsupportedPatternError
from waymark.compiler_operators import binary_operator
from waymark.compiler_targets import assignment_targets
from waymark.language_syntax import StatementHandler
from waymark.proto import ast_pb2 as ir


class AssignmentHandler(StatementHandler[ast.Assign | ast.AnnAssign | ast.AugAssign]):
    node_types = (ast.Assign, ast.AnnAssign, ast.AugAssign)

    def validate(
        self, node: ast.Assign | ast.AnnAssign | ast.AugAssign, context: CompilerContext
    ) -> None:
        """Reject lossy writes and verify ownership before copy-based augmented updates."""
        if isinstance(node, ast.AugAssign):
            self._validate_augmented(node, context)
            return
        if isinstance(node, ast.Assign):
            if len(node.targets) != 1:
                reject(node, "Chained assignments are unsupported")
            target = node.targets[0]
            if (
                isinstance(target, (ast.Tuple, ast.List))
                and isinstance(node.value, (ast.Tuple, ast.List))
                and len(target.elts) != len(node.value.elts)
            ):
                reject(node, "Unpacking target count does not match result count")
        else:
            target = node.target
        if not (
            isinstance(target, ast.Name)
            or isinstance(target, (ast.Tuple, ast.List))
            and all(isinstance(element, ast.Name) for element in target.elts)
        ):
            reject(
                target,
                "Assignment requires a name or flat unpacking target; attribute and subscript writes are unsupported",
            )

    def children(self, node: ast.Assign | ast.AnnAssign | ast.AugAssign) -> Iterable[ast.AST]:
        """Local annotations describe types and must not be validated as executable operators."""
        if isinstance(node, ast.AnnAssign):
            return [node.value] if node.value is not None else []
        return ast.iter_child_nodes(node)

    def lower(
        self, node: ast.Assign | ast.AnnAssign | ast.AugAssign, context: CompilerContext
    ) -> list[ir.Statement]:
        """Lower ordinary, annotated, augmented, and comprehension assignments."""
        if isinstance(node, ast.AugAssign):
            return self._lower_augmented(node, context)
        if isinstance(node, ast.AnnAssign):
            if node.value is None:
                return []
            node = ast.fix_missing_locations(
                ast.copy_location(ast.Assign(targets=[node.target], value=node.value), node)
            )
        expanded = self._expand_comprehension(node, context)
        if expanded is not None:
            return expanded
        assignment = self._lower_assignment(node, context)
        return [assignment] if assignment is not None else []

    def _lower_assignment(self, node: ast.Assign, context: CompilerContext) -> ir.Statement | None:
        targets = assignment_targets(node.targets)
        action = context.action(node.value)
        value = (
            ir.Expr(action_call=action, span=make_span(node))
            if action is not None
            else context.expression(node.value)
        )
        if value is None:
            return None
        return ir.Statement(
            assignment=ir.Assignment(targets=targets, value=value), span=make_span(node)
        )

    def _validate_augmented(self, node: ast.AugAssign, context: CompilerContext) -> None:
        """Check this construct against the shared source facts."""
        if not isinstance(node.target, ast.Name):
            reject(
                node.target,
                "Augmented assignment requires a name; subscript and attribute writes are unsupported",
            )
        assert isinstance(node.target, ast.Name)
        kind = context.analysis.kind(node.target)
        if (
            kind not in {"int", "float", "str"}
            and context.analysis.kind(node.value) not in {"int", "float"}
            and not (
                kind == "list"
                and node.target.id in context.analysis.fresh_lists - context.analysis.aliased
            )
        ):
            reject(
                node, "Augmented assignment may mutate an aliased value; use explicit assignment"
            )
        context.validate_syntax(
            ast.copy_location(ast.BinOp(left=node.target, op=node.op, right=node.value), node)
        )

    def _lower_augmented(self, node: ast.AugAssign, context: CompilerContext) -> list[ir.Statement]:
        """Convert augmented assignment (+=, -=, etc.) to IR."""
        # For now, we can represent this as a regular assignment with binary op
        # target op= value  ->  target = target op value
        stmt = ir.Statement(span=make_span(node))

        targets: list[str] = []
        if isinstance(node.target, ast.Name):
            targets.append(node.target.id)

        left = context.expression(node.target)
        right = context.expression(node.value)
        if right and right.HasField("function_call"):
            tmp_var = context.next_name("aug_tmp")

            assign_tmp = ir.Statement(span=make_span(node))
            assign_tmp.assignment.CopyFrom(
                ir.Assignment(
                    targets=[tmp_var],
                    value=ir.Expr(function_call=right.function_call, span=make_span(node)),
                )
            )

            if left:
                op = binary_operator(node.op)
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
            op = binary_operator(node.op)
            if op:
                binary = ir.BinaryOp(left=left, op=op, right=right)
                value = ir.Expr(binary_op=binary)
                assign = ir.Assignment(targets=targets, value=value)
                stmt.assignment.CopyFrom(assign)
                return [stmt]

        return []

    def _expand_comprehension(
        self, node: ast.Assign, context: CompilerContext
    ) -> list[ir.Statement] | None:
        """Expand a list comprehension assignment into loop-based statements.

        Example:
            active_users = [user for user in users if user.active]

        Becomes:
            source = users
            result = []
            for item in source:
                if item.active:
                    result = result + [item]
            active_users = result
        """
        if not isinstance(node.value, ast.ListComp):
            return None

        if len(node.targets) != 1 or not isinstance(node.targets[0], ast.Name):
            line = node.lineno
            col = node.col_offset
            raise UnsupportedPatternError(
                "List comprehension assignments must target a single variable",
                "Assign the comprehension to a simple variable like `results = [x for x in items]`",
                line=line,
                col=col,
            )

        listcomp = copy.deepcopy(node.value)
        if len(listcomp.generators) != 1:
            line = listcomp.lineno
            col = listcomp.col_offset
            raise UnsupportedPatternError(
                "List comprehensions with multiple generators are not supported",
                "Use nested for loops instead of combining multiple generators in one comprehension",
                line=line,
                col=col,
            )

        gen = listcomp.generators[0]
        if gen.is_async:
            line = listcomp.lineno
            col = listcomp.col_offset
            raise UnsupportedPatternError(
                "Async list comprehensions are not supported",
                "Rewrite using an explicit async for loop",
                line=line,
                col=col,
            )

        replacements = {
            name: ast.Name(id=context.next_name("comprehension_item"), ctx=ast.Load())
            for name in assignment_targets([gen.target])
        }
        for target in ast.walk(gen.target):
            if isinstance(target, ast.Name):
                replacement = replacements[target.id]
                assert isinstance(replacement, ast.Name)
                target.id = replacement.id
        rewritten_element = rewrite_load_names(listcomp.elt, replacements)
        assert isinstance(rewritten_element, ast.expr)
        listcomp.elt = rewritten_element
        rewritten_filters = [rewrite_load_names(condition, replacements) for condition in gen.ifs]
        assert all(isinstance(condition, ast.expr) for condition in rewritten_filters)
        gen.ifs = [condition for condition in rewritten_filters if isinstance(condition, ast.expr)]
        iterable_name = context.next_name("comprehension_input")
        iterable_assignment = ast.fix_missing_locations(
            ast.copy_location(
                ast.Assign(targets=[ast.Name(id=iterable_name, ctx=ast.Store())], value=gen.iter),
                node,
            )
        )
        gen.iter = ast.copy_location(ast.Name(id=iterable_name, ctx=ast.Load()), gen.iter)

        target_name = context.next_name("comprehension_result")

        # Publish only after the entire comprehension succeeds.
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

        append_statements: list[ast.stmt] = []
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

        loop_body: list[ast.stmt] = append_statements
        for condition in reversed(gen.ifs):
            if_stmt = ast.If(test=copy.deepcopy(condition), body=loop_body, orelse=[])
            ast.copy_location(if_stmt, condition)
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

        statements = context.statement(iterable_assignment)
        init_stmt = self._lower_assignment(init_assign_ast, context)
        if init_stmt:
            statements.append(init_stmt)
        statements.extend(context.statement(loop_ast))
        final_assignment = ast.fix_missing_locations(
            ast.copy_location(
                ast.Assign(
                    targets=copy.deepcopy(node.targets),
                    value=ast.Name(id=target_name, ctx=ast.Load()),
                ),
                node,
            )
        )
        statements.extend(context.statement(final_assignment))

        return statements
