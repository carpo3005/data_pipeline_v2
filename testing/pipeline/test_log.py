import logging

from pipeline import events
from pipeline.audit import AuditRun
from pipeline.errors import step
from pipeline.events import EventBus
from pipeline.log import make_log_subscriber


def _bus_with_logging(caplog):
    logger = logging.getLogger("test_log")
    bus = EventBus()
    bus.subscribe(make_log_subscriber(logger))
    caplog.set_level(logging.DEBUG, logger="test_log")
    return bus


def test_step_events_logged_at_expected_levels(caplog):
    bus = _bus_with_logging(caplog)
    with step("read", bus=bus, entity="customer"):
        pass
    levels = [r.levelno for r in caplog.records]
    assert levels == [logging.DEBUG, logging.INFO]
    assert "step 'read' (entity=customer) completed" in caplog.records[1].getMessage()


def test_failed_step_logged_as_error(caplog):
    bus = _bus_with_logging(caplog)
    try:
        with step("read", bus=bus, entity="customer"):
            1 / 0
    except Exception:
        pass
    failed = caplog.records[-1]
    assert failed.levelno == logging.ERROR
    assert "step 'read'" in failed.getMessage()
    assert "ZeroDivisionError: division by zero" in failed.getMessage()


def test_run_events_logged(caplog):
    bus = _bus_with_logging(caplog)
    with AuditRun("bronze", "customer", "t", sink=lambda r: None, bus=bus) as run:
        run.add("source_row_count", 5)
    messages = [r.getMessage() for r in caplog.records]
    assert messages[0].endswith("started")
    assert "succeeded" in messages[1] and "source_row_count" in messages[1]


def test_failed_run_logged_as_error(caplog):
    bus = _bus_with_logging(caplog)
    try:
        with AuditRun("bronze", "customer", "t", sink=lambda r: None, bus=bus):
            raise ValueError("boom")
    except ValueError:
        pass
    assert caplog.records[-1].levelno == logging.ERROR
    assert "failed: boom" in caplog.records[-1].getMessage()


def test_module_does_not_add_handlers_to_logger():
    logger = logging.getLogger("test_log_no_handlers")
    make_log_subscriber(logger)
    assert logger.handlers == []
