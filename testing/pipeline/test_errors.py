import pytest

from pipeline import events
from pipeline.errors import DataValidationError, PipelineError, step
from pipeline.events import EventBus


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


def test_step_publishes_started_and_completed():
    bus = EventBus()
    seen = []
    bus.subscribe(seen.append)
    with step("read", bus=bus, entity="customer"):
        pass
    assert [e.name for e in seen] == [events.STEP_STARTED, events.STEP_COMPLETED]
    assert seen[1].payload["step"] == "read"
    assert seen[1].payload["context"] == {"entity": "customer"}
    assert seen[1].payload["duration_seconds"] >= 0


def test_step_publishes_failed_with_error_and_still_raises():
    bus = EventBus()
    seen = []
    bus.subscribe(seen.append, names=[events.STEP_FAILED])
    with pytest.raises(PipelineError, match="ZeroDivisionError"):
        with step("read", bus=bus):
            1 / 0
    assert seen[0].payload["error"] == "ZeroDivisionError: division by zero"
