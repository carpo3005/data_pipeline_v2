import pytest

from pipeline.errors import DataValidationError, PipelineError, step


def test_step_wraps_exception_with_step_and_context():
    with pytest.raises(PipelineError) as info:
        with step("read", entity="customer"):
            1 / 0

    err = info.value
    assert str(err) == "Step 'read' failed (entity=customer): ZeroDivisionError: division by zero"
    assert err.step == "read"
    assert err.context == {"entity": "customer"}
    assert isinstance(err.__cause__, ZeroDivisionError)

def test_step_uses_only_first_line_of_cause():
    with pytest.raises(PipelineError, match=r"ValueError: first line$"):
        with step("write"):
            raise ValueError("first line\nlong JVM stack trace")

def test_step_passes_pipeline_errors_through_unchanged():
    original = DataValidationError("Missing columns: id")
    with pytest.raises(DataValidationError) as info:
        with step("validate"):
            raise original
    assert info.value is original

def test_step_does_nothing_on_success():
    with step("noop"):
        result = 1
    assert result == 1
