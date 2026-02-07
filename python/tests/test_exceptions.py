from waymark.exceptions import ExhaustedRetries


def test_exhausted_retries_inherits_exception() -> None:
    error = ExhaustedRetries("custom message")
    assert isinstance(error, Exception)
    assert str(error) == "custom message"
