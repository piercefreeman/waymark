import pytest

from waymark.__tests__.compiler_test_helpers import compile_source
from waymark.compiler.errors import UnsupportedPatternError


@pytest.mark.parametrize("arguments", ["3", "1, 3", "1, 5, 2"])
def test_range_accepts_supported_bounds(arguments: str) -> None:
    function = compile_source(
        f"for item in sequence({arguments}):\n    await task(item)", namespace={"sequence": range}
    )
    assert function.body.statements[0].for_loop.iterable.function_call.name == "range"


@pytest.mark.parametrize("arguments", ["", "1,2,3,4", "stop=4", "*values", "2.5", "True", "1,5,0"])
def test_range_rejects_unsupported_bounds(arguments: str) -> None:
    with pytest.raises(UnsupportedPatternError):
        compile_source(f"for item in range({arguments}):\n    pass", "values: list")


@pytest.mark.parametrize(
    "body", ["return len(range(3))", "return range(3)[0]", "for item in range(-1.5):\n    pass"]
)
def test_range_rejects_unsupported_placement_and_negative_float_bounds(body: str) -> None:
    with pytest.raises(UnsupportedPatternError):
        compile_source(body)
