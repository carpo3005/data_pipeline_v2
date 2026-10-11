from pipeline import events
from pipeline.events import EventBus


def test_handlers_receive_events_in_subscription_order():
    bus = EventBus()
    seen = []
    bus.subscribe(lambda e: seen.append(("first", e.name)))
    bus.subscribe(lambda e: seen.append(("second", e.name)))
    bus.publish(events.STEP_STARTED, step="read")
    assert seen == [("first", events.STEP_STARTED), ("second", events.STEP_STARTED)]


def test_names_filter_limits_delivery():
    bus = EventBus()
    seen = []
    bus.subscribe(seen.append, names=[events.RUN_FAILED])
    bus.publish(events.RUN_SUCCEEDED)
    bus.publish(events.RUN_FAILED)
    assert [e.name for e in seen] == [events.RUN_FAILED]


def test_event_carries_payload_and_timestamp():
    bus = EventBus()
    seen = []
    bus.subscribe(seen.append)
    bus.publish(events.STEP_FAILED, step="read", error="boom")
    event = seen[0]
    assert event.payload == {"step": "read", "error": "boom"}
    assert event.timestamp.tzinfo is not None


def test_failing_handler_does_not_stop_others_or_raise():
    bus = EventBus()
    seen = []

    def broken(event):
        raise RuntimeError("observer down")

    bus.subscribe(broken)
    bus.subscribe(seen.append)
    failures = bus.publish(events.RUN_STARTED)
    assert len(seen) == 1
    assert len(failures) == 1
    assert isinstance(failures[0][1], RuntimeError)


def test_unsubscribe_stops_delivery():
    bus = EventBus()
    seen = []
    sub_id = bus.subscribe(seen.append)
    bus.publish(events.RUN_STARTED)
    bus.unsubscribe(sub_id)
    bus.publish(events.RUN_STARTED)
    assert len(seen) == 1


def test_unsubscribe_unknown_id_is_ignored():
    EventBus().unsubscribe(999)


def test_publish_with_no_subscribers_returns_no_failures():
    assert EventBus().publish(events.RUN_STARTED) == []
