from __future__ import annotations

import itertools
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Iterable

STEP_STARTED = "step_started"
STEP_COMPLETED = "step_completed"
STEP_FAILED = "step_failed"
RUN_STARTED = "run_started"
RUN_SUCCEEDED = "run_succeeded"
RUN_FAILED = "run_failed"

Handler = Callable[["Event"], None]


def _now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class Event:
    name: str
    timestamp: datetime
    payload: dict[str, Any] = field(default_factory=dict)


class EventBus:
    """Synchronous, in-process publish/subscribe. Handler failures never propagate."""

    def __init__(self) -> None:
        self._subscriptions: dict[int, tuple[Handler, frozenset[str] | None]] = {}
        self._ids = itertools.count(1)

    def subscribe(self, handler: Handler, names: Iterable[str] | None = None) -> int:
        """Call handler for events named in names, or for every event when names is None.

        Returns a subscription ID to pass to unsubscribe().
        """
        sub_id = next(self._ids)
        self._subscriptions[sub_id] = (handler, frozenset(names) if names is not None else None)
        return sub_id

    def unsubscribe(self, sub_id: int) -> None:
        """Remove a subscription. Unknown IDs are ignored."""
        self._subscriptions.pop(sub_id, None)

    def publish(self, name: str, **payload: Any) -> list[tuple[Handler, Exception]]:
        """Deliver an event to matching handlers in subscription order.

        Returns (handler, exception) for each handler that failed.
        """
        event = Event(name=name, timestamp=_now(), payload=payload)
        failures = []
        for handler, names in list(self._subscriptions.values()):
            if names is not None and name not in names:
                continue
            try:
                handler(event)
            except Exception as exc:
                failures.append((handler, exc))
        return failures
