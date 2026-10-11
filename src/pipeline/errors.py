import time
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Iterator

from pipeline import events

if TYPE_CHECKING:
    from pipeline.events import EventBus


class PipelineError(Exception):
    """A pipeline failure with a readable message. The original exception is kept as __cause__."""

    def __init__(self, message: str, step: str | None = None, context: dict[str, Any] | None = None):
        super().__init__(message)
        self.step = step
        self.context = context or {}


class DataValidationError(PipelineError, ValueError):
    """The data does not meet an expectation (missing columns, null keys, schema mismatch)."""


def _summarise(exc: BaseException) -> str:
    lines = str(exc).strip().splitlines()
    summary = lines[0] if lines else ""
    return f"{type(exc).__name__}: {summary}" if summary else type(exc).__name__


@contextmanager
def step(name: str, bus: EventBus | None = None, **context: Any) -> Iterator[None]:
    """Wrap a block so any failure is raised as a PipelineError naming the step and its context.

    If bus is given, publishes step_started, then step_completed or step_failed.
    """
    if bus is not None:
        bus.publish(events.STEP_STARTED, step=name, context=context)
    started = time.perf_counter()
    try:
        yield
    except PipelineError as exc:
        if bus is not None:
            bus.publish(events.STEP_FAILED, step=name, context=context, error=_summarise(exc))
        raise
    except Exception as exc:
        details = ", ".join(f"{k}={v}" for k, v in context.items())
        where = f" ({details})" if details else ""
        summary = _summarise(exc)
        if bus is not None:
            bus.publish(events.STEP_FAILED, step=name, context=context, error=summary)
        raise PipelineError(
            f"Step '{name}' failed{where}: {summary}",
            step=name,
            context=context,
        ) from exc
    if bus is not None:
        bus.publish(
            events.STEP_COMPLETED,
            step=name,
            context=context,
            duration_seconds=time.perf_counter() - started,
        )
