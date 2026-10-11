from contextlib import contextmanager
from typing import Any, Iterator


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
def step(name: str, **context: Any) -> Iterator[None]:
    """Wrap a block so any failure is raised as a PipelineError naming the step and its context."""
    try:
        yield
    except PipelineError:
        raise
    except Exception as exc:
        details = ", ".join(f"{k}={v}" for k, v in context.items())
        where = f" ({details})" if details else ""
        raise PipelineError(
            f"Step '{name}' failed{where}: {_summarise(exc)}",
            step=name,
            context=context,
        ) from exc
