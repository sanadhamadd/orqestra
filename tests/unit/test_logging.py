import json
import logging

import pytest

from orqestra.logging import JsonFormatter, configure_logging

pytestmark = pytest.mark.unit


def _record(**extra: object) -> logging.LogRecord:
    record = logging.LogRecord(
        "orqestra.test", logging.INFO, __file__, 1, "hello %s", ("world",), None
    )
    for key, value in extra.items():
        setattr(record, key, value)
    return record


def test_formats_single_line_json() -> None:
    line = JsonFormatter().format(_record())
    assert "\n" not in line
    payload = json.loads(line)
    assert payload["message"] == "hello world"
    assert payload["level"] == "INFO"
    assert payload["logger"] == "orqestra.test"
    assert payload["ts"].endswith("+00:00")


def test_context_fields_emitted_only_when_present() -> None:
    without = json.loads(JsonFormatter().format(_record()))
    assert "job_id" not in without
    with_ids = json.loads(JsonFormatter().format(_record(job_id="j1", task_id="t1")))
    assert with_ids["job_id"] == "j1"
    assert with_ids["task_id"] == "t1"
    assert "attempt_id" not in with_ids


@pytest.mark.parametrize(
    "event",
    [
        "usage_result_unrepresentable",
        "usage_write_retry",
        "usage_write_rejected",
        "usage_write_ambiguous",
        "usage_intent_withdrawn",
    ],
)
def test_call_seq_is_kept_in_the_json_line(event: str) -> None:
    record = _record(attempt_id="a1", event=event, call_seq="3")
    payload = json.loads(JsonFormatter().format(record))
    assert (payload["event"], payload["attempt_id"], payload["call_seq"]) == (event, "a1", "3")
    assert "call_seq" not in json.loads(JsonFormatter().format(_record(event=event)))


def test_configure_logging_is_idempotent() -> None:
    configure_logging("DEBUG")
    configure_logging("WARNING")
    root = logging.getLogger()
    assert len(root.handlers) == 1
    assert isinstance(root.handlers[0].formatter, JsonFormatter)
    assert root.level == logging.WARNING


def test_exception_info_is_included() -> None:
    try:
        raise ValueError("boom")
    except ValueError:
        import sys

        record = _record()
        record.exc_info = sys.exc_info()
    payload = json.loads(JsonFormatter().format(record))
    assert "ValueError: boom" in payload["exception"]
