import pytest

from waymark.__tests__.compiler_test_helpers import compile_source
from waymark.compiler.errors import UnsupportedPatternError


def test_fresh_local_append_emits_an_explicit_update() -> None:
    function = compile_source("values = []\nvalues.append(1)\nreturn values")
    assignment = function.body.statements[1].assignment
    assert list(assignment.targets) == ["values"]
    assert assignment.value.binary_op.right.list.elements[0].literal.int_value == 1


@pytest.mark.parametrize(
    "body",
    [
        "values.append(1)",
        "values = []\nalias = values\nvalues.append(1)",
        "values = []\ncontainer = [values]\nvalues.append(1)",
        "values = []\nvalues.append(1, 2)",
        "values = []\nvalues.append(object=1)",
        "values += [1]",
    ],
)
def test_append_and_inplace_add_reject_unproven_or_aliased_receivers(body: str) -> None:
    with pytest.raises(UnsupportedPatternError):
        compile_source(body, "values: list")


@pytest.mark.parametrize(
    "body",
    [
        "values.append(1)\nvalues = []",
        "if True:\n    values = []\nvalues.append(1)",
        "values = []\nfor values in [[1]]:\n    values.append(2)",
    ],
)
def test_a_later_or_conditional_initializer_does_not_prove_exclusive_ownership(body: str) -> None:
    with pytest.raises(UnsupportedPatternError):
        compile_source(body, "values: list")


def test_alias_returned_through_an_awaited_call_prevents_mutation() -> None:
    """Regression: sleep's result can expose a list before a later update."""
    with pytest.raises(UnsupportedPatternError):
        compile_source(
            "values = []\nalias = await asyncio.sleep(0, result=values)\nvalues.append(1)\nreturn alias"
        )


def test_storing_a_list_through_append_loses_exclusive_ownership() -> None:
    """Regression: a container must observe updates to a list stored through append."""
    with pytest.raises(UnsupportedPatternError):
        compile_source(
            "values = []\ncontainer = []\ncontainer.append(values)\nvalues.append(1)\nreturn container"
        )


@pytest.mark.parametrize("mutation", ["values.append(1)", "values += [1]"])
def test_storing_a_list_in_a_comprehension_loses_exclusive_ownership(mutation: str) -> None:
    """Regression: comprehension results can retain a list that is mutated later."""
    with pytest.raises(UnsupportedPatternError):
        compile_source(
            f"values = []\ncontainer = [values for index in [1]]\n{mutation}\nreturn container"
        )


def test_escape_after_mutation_in_a_loop_prevents_the_next_iteration_from_mutating() -> None:
    """Regression: source order alone cannot prove ownership across a loop back edge."""
    with pytest.raises(UnsupportedPatternError):
        compile_source(
            "values = []\ncontainer = []\nfor item in [1, 2]:\n"
            "    values.append(item)\n    container.append(values)\nreturn container"
        )


@pytest.mark.parametrize("mutation", ["values.append(values)", "values += [values]"])
def test_cyclic_list_updates_are_rejected(mutation: str) -> None:
    """Regression: copy-based lowering cannot preserve a list containing itself."""
    with pytest.raises(UnsupportedPatternError):
        compile_source(f"values = []\n{mutation}\nreturn values")


def test_ownership_rejection_comes_from_the_append_handler() -> None:
    """Known lists resolve to append even when its ownership policy rejects them."""
    with pytest.raises(UnsupportedPatternError, match="list.append.*fresh, unaliased"):
        compile_source("values = []\nalias = values\nvalues.append(1)")
