"""Length validates its signature and avoids the VM's byte-count string semantics."""

import pytest

from waymark.__tests__.compiler_test_helpers import compile_source
from waymark.compiler.errors import UnsupportedPatternError


def test_literal_unicode_length_is_folded_using_python_semantics() -> None:
    function = compile_source("return length('é🙂')", namespace={"length": len})
    assert function.body.statements[0].return_stmt.value.literal.int_value == 2


@pytest.mark.parametrize(
    "body",
    [
        "return len()",
        "return len([1], [2])",
        "return len(obj=[1])",
        "text = 'é'\nreturn len(text)",
        "return len(text)",
    ],
)
def test_length_rejects_unsupported_forms(body: str) -> None:
    with pytest.raises(UnsupportedPatternError):
        compile_source(body, "text: str")
