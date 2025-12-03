"""
Rappel Language - A DSL with immutable variables, explicit I/O, and first-class actions.

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

from .tokens import Token, TokenType
from .ir import (
    # Source location
    SourceLocation,
    # Values
    RappelValue,
    RappelString,
    RappelNumber,
    RappelBoolean,
    RappelList,
    RappelDict,
    # Expressions
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
    RappelExpr,
    # Statements
    RappelAssignment,
    RappelMultiAssignment,
    RappelReturn,
    RappelExprStatement,
    RappelPythonBlock,
    RappelFunctionDef,
    RappelForLoop,
    RappelIfStatement,
    RappelSpreadAction,
    RappelStatement,
    # Program
    RappelProgram,
)
from .lexer import RappelLexer
from .parser import RappelParser, RappelSyntaxError, parse
from .printer import RappelPrettyPrinter
from .dag import (
    EdgeType,
    DAGNode,
    DAGEdge,
    DAG,
    DAGConverter,
    convert_to_dag,
)
from .examples import (
    EXAMPLE_IMMUTABLE_VARS,
    EXAMPLE_LIST_OPERATIONS,
    EXAMPLE_DICT_OPERATIONS,
    EXAMPLE_PYTHON_BLOCK,
    EXAMPLE_FUNCTION_DEF,
    EXAMPLE_ACTION_CALL,
    EXAMPLE_FOR_LOOP,
    EXAMPLE_SPREAD_OPERATOR,
    EXAMPLE_CONDITIONALS,
    EXAMPLE_COMPLEX_WORKFLOW,
    EXAMPLE_ACTION_SPREAD_LOOP,
)
from .runner import (
    ActionStatus,
    ActionType,
    RunnableAction,
    RunnableActionData,
    ActionQueue,
    DAGRunner,
    ThreadedDAGRunner,
)
from .db import (
    DBStats,
    Table,
    InMemoryDB,
    get_db,
    reset_db,
)

__all__ = [
    # Tokens
    "Token",
    "TokenType",
    # IR
    "SourceLocation",
    "RappelValue",
    "RappelString",
    "RappelNumber",
    "RappelBoolean",
    "RappelList",
    "RappelDict",
    "RappelLiteral",
    "RappelVariable",
    "RappelListExpr",
    "RappelDictExpr",
    "RappelBinaryOp",
    "RappelUnaryOp",
    "RappelIndexAccess",
    "RappelDotAccess",
    "RappelSpread",
    "RappelCall",
    "RappelActionCall",
    "RappelExpr",
    "RappelAssignment",
    "RappelMultiAssignment",
    "RappelReturn",
    "RappelExprStatement",
    "RappelPythonBlock",
    "RappelFunctionDef",
    "RappelForLoop",
    "RappelIfStatement",
    "RappelSpreadAction",
    "RappelStatement",
    "RappelProgram",
    # Lexer
    "RappelLexer",
    # Parser
    "RappelParser",
    "RappelSyntaxError",
    "parse",
    # Pretty Printer
    "RappelPrettyPrinter",
    # DAG
    "EdgeType",
    "DAGNode",
    "DAGEdge",
    "DAG",
    "DAGConverter",
    "convert_to_dag",
    # Examples
    "EXAMPLE_IMMUTABLE_VARS",
    "EXAMPLE_LIST_OPERATIONS",
    "EXAMPLE_DICT_OPERATIONS",
    "EXAMPLE_PYTHON_BLOCK",
    "EXAMPLE_FUNCTION_DEF",
    "EXAMPLE_ACTION_CALL",
    "EXAMPLE_FOR_LOOP",
    "EXAMPLE_SPREAD_OPERATOR",
    "EXAMPLE_CONDITIONALS",
    "EXAMPLE_COMPLEX_WORKFLOW",
    "EXAMPLE_ACTION_SPREAD_LOOP",
    # Runner
    "ActionStatus",
    "ActionType",
    "RunnableAction",
    "RunnableActionData",
    "ActionQueue",
    "DAGRunner",
    "ThreadedDAGRunner",
    # Database
    "DBStats",
    "Table",
    "InMemoryDB",
    "get_db",
    "reset_db",
]
