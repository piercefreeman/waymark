"""Rewrite guidance for unsupported workflow syntax."""

from waymark.compiler_context import GLOBAL_FUNCTIONS

# The builtins a workflow body may call, as the help text lists them;
# `isexception` is what `isinstance` lowers to, not a name users write.
_CALLABLE_BUILTINS = sorted(name for name in GLOBAL_FUNCTIONS if name != "isexception")
_CALLABLE_BUILTINS_TEXT = (
    ", ".join(f"{name}()" for name in _CALLABLE_BUILTINS[:-1]) + f" and {_CALLABLE_BUILTINS[-1]}()"
)

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
    "sleep_assignment": (
        "Assigning the result of asyncio.sleep() is not supported.\n"
        "sleep() always returns None and should be used as a standalone statement.\n\n"
        "Use:\n\n"
        "    await asyncio.sleep(1)\n"
    ),
    "sync_function_call": (
        "Workflow code can only call actions (with await), the workflow's own methods, "
        "awaited asyncio.sleep() and asyncio.gather(), pydantic models and dataclasses as "
        f"constructors, and the builtins {_CALLABLE_BUILTINS_TEXT} - len() anywhere, "
        "range() and enumerate() as a for loop's iterable, and range() assigned to a name "
        "or returned.\n"
        "Move other calls into an @action:\n\n"
        "    @action\n"
        "    async def compute(x: int) -> int:\n"
        "        return some_function(x)"
    ),
    "policy_literal": (
        "Every retry and timeout field must be a literal in the workflow body: the\n"
        "compiler reads them from the source, not at runtime.\n\n"
        "Use:\n\n"
        '    retry=RetryPolicy(attempts=3, exception_types=["ConnectionError"], backoff_seconds=5)\n'
        "    timeout=timedelta(minutes=2)\n\n"
        "or assign a literal to a self attribute once, at the top level of __init__, and\n"
        "pass self.<attribute>."
    ),
    "run_action_keywords": (
        "run_action() takes the awaitable plus retry= and timeout= only.\n\n"
        "Use:\n\n"
        "    await self.run_action(action(), retry=RetryPolicy(attempts=3), timeout=60)"
    ),
    "duration_range": (
        "A timeout is more than zero seconds and a backoff is zero or more; fractions\n"
        "are fine, the runtime sleeps to its poll granularity.\n\n"
        "Use:\n\n"
        "    timeout=timedelta(seconds=30)\n"
        "    backoff_seconds=0.5"
    ),
    "except_type": (
        "An except clause lists exception classes by name, or dotted through a module, "
        "which resolves to the class: the VM matches by the class's own name alone, so an "
        "alias names the same class. The name must resolve, in the workflow module's "
        "scope, to an exception class, and a dotted name's root to a module or a class; a "
        "value, a class that is not an exception, a tuple alias or an undefined name "
        "would compile into a class that never matches, so it is rejected.\n\n"
        "Use:\n\n"
        "    except ValueError:\n"
        "    except (KeyError, waymark.ActionTimeout):\n"
    ),
    "except_empty_tuple": (
        "`except ():` catches nothing, so the handler is dead code.\n"
        "Drop the handler, or list the classes to catch:\n\n"
        "    except (KeyError, ValueError):\n"
    ),
    "retry_exception_types": (
        "RetryPolicy exception_types takes class names as strings; the VM matches by "
        "class name alone, so a module path never matches. A string is the name the VM "
        "sees, so it must already be bare, unlike an except clause, which resolves a "
        "dotted spelling to its class name.\n\n"
        "Use:\n\n"
        '    retry=RetryPolicy(attempts=3, exception_types=["ConnectError", "ActionTimeout"])\n'
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
        "Generator expressions are only supported when used directly in asyncio.gather(...).\n"
        "For other cases, use a list, a for loop, or an @action instead."
    ),
    "walrus": (
        "The walrus operator (:=) is not supported in workflow code.\n"
        "Use separate assignment statements instead."
    ),
    "match": (
        "Match statements are not supported in workflow code.\nUse if/elif/else chains instead."
    ),
    "for_loop_append_pattern": (
        "Building a task list in a for loop then spreading in asyncio.gather() is not "
        "supported.\n"
        "Use a list comprehension directly in gather:\n\n"
        "    # Instead of:\n"
        "    tasks = []\n"
        "    for i in range(count):\n"
        "        tasks.append(process(value=i))\n"
        "    results = await asyncio.gather(*tasks, return_exceptions=False)\n\n"
        "    # Use:\n"
        "    results = await asyncio.gather(*[process(value=i) for i in range(count)], "
        "return_exceptions=False)"
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
    "unsupported_statement": (
        "This statement type is not supported in workflow code.\n"
        "Move the logic into an @action or rewrite using supported statements."
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
