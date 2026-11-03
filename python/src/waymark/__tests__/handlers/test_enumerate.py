import pytest

from waymark.__tests__.compiler_test_helpers import compile_source
from waymark.compiler_errors import UnsupportedPatternError


def test_enumerate_accepts_explicit_zero_start_and_alias() -> None:
    function = compile_source(
        "for index, item in indexed([4], start=0):\n    await task(item)",
        namespace={"indexed": enumerate},
    )
    assert list(function.body.statements[0].for_loop.loop_vars) == ["index", "item"]


@pytest.mark.parametrize(
    "body",
    [
        "return enumerate([1])",
        "for pair in enumerate([1], 2):\n    pass",
        "for pair in enumerate([1], start=False):\n    pass",
        "for pair in enumerate({'x': 1}):\n    pass",
    ],
)
def test_enumerate_rejects_unsupported_forms(body: str) -> None:
    with pytest.raises(UnsupportedPatternError):
        compile_source(body)
