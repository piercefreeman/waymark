import pytest

from waymark.__tests__.compiler_test_helpers import compile_source
from waymark.compiler_errors import UnsupportedPatternError


@pytest.mark.parametrize(
    "arguments",
    [
        "value",
        "value, ValueError",
        "value, (ValueError, TypeError)",
        "value, str",
        "value, errors.Missing",
    ],
)
def test_type_checks_are_rejected_until_the_vm_supports_them(arguments: str) -> None:
    with pytest.raises(UnsupportedPatternError) as error:
        compile_source(f"return check({arguments})", "value", {"check": isinstance})
    assert isinstance(error.value, UnsupportedPatternError)
    assert "inside an action" in error.value.recommendation
