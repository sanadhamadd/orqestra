"""An extract result's ``extraction`` block may carry ``documents_processed`` (DEC-073)."""

from typing import Any

import pytest

from benchmarks.result import validate_result
from tests.unit.test_bench_report import real_record

pytestmark = [pytest.mark.unit, pytest.mark.usefixtures("no_network")]


def record(**block: Any) -> dict[str, Any]:
    value = real_record()
    value["extraction"].update(block)
    return value


def test_a_block_written_before_documents_processed_stays_valid() -> None:
    old = record()
    assert "documents_processed" not in old["extraction"]
    validate_result(old)


@pytest.mark.parametrize("processed", [0, 25, 40])
def test_a_block_may_count_its_processed_documents(processed: int) -> None:
    validate_result(record(documents_processed=processed))


@pytest.mark.parametrize("processed", [-1, 41, True, 2.5, "40", None])
def test_a_processed_count_outside_the_documents_is_refused(processed: object) -> None:
    with pytest.raises(ValueError, match="documents_processed"):
        validate_result(record(documents_processed=processed))


def test_any_other_extra_key_is_still_refused() -> None:
    with pytest.raises(ValueError, match="extraction block must hold exactly"):
        validate_result(record(processed_documents=40))
