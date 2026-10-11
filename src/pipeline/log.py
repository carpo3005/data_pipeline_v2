from __future__ import annotations

import logging

from pipeline import events
from pipeline.events import Event, Handler


def _describe_run(event: Event) -> str:
    record = event.payload["record"]
    message = f"{record.stage} {record.entity} run {record.run_id}"
    if event.name == events.RUN_FAILED:
        message += f" failed: {record.error_message}"
    elif event.name == events.RUN_SUCCEEDED:
        message += f" succeeded: {record.metrics}"
    else:
        message += " started"
    return message


def _describe_step(event: Event) -> str:
    message = f"step '{event.payload['step']}'"
    context = event.payload.get("context") or {}
    if context:
        message += " (" + ", ".join(f"{k}={v}" for k, v in context.items()) + ")"
    if event.name == events.STEP_COMPLETED:
        message += f" completed in {event.payload['duration_seconds']:.2f}s"
    elif event.name == events.STEP_FAILED:
        message += f" failed: {event.payload['error']}"
    else:
        message += " started"
    return message


_LEVELS = {
    events.STEP_STARTED: logging.DEBUG,
    events.STEP_COMPLETED: logging.INFO,
    events.STEP_FAILED: logging.ERROR,
    events.RUN_STARTED: logging.INFO,
    events.RUN_SUCCEEDED: logging.INFO,
    events.RUN_FAILED: logging.ERROR,
}


def make_log_subscriber(logger: logging.Logger) -> Handler:
    """Return a bus handler that writes each pipeline event to logger.

    Does not configure logging: handlers and levels belong to the entry point.
    """
    def handler(event: Event) -> None:
        level = _LEVELS.get(event.name, logging.INFO)
        if event.name.startswith("run_"):
            message = _describe_run(event)
        else:
            message = _describe_step(event)
        logger.log(level, message)
    return handler
