"""Deterministic task partitioning (DEC-052): pure, so every case runs without a database."""

import copy
import math
from typing import Any, cast

import pytest

from orqestra.domain.jobs import JobSubmission, JsonValue, TaskSpec
from orqestra.domain.partitioning import partition

pytestmark = pytest.mark.unit


def envelopes(tasks: tuple[TaskSpec, ...]) -> list[dict[str, Any]]:
    """Each task's payload, read as the ``{"offset", "items"}`` envelope it must be."""
    payloads = [task.payload for task in tasks]
    assert all(isinstance(p, dict) and set(p) == {"offset", "items"} for p in payloads)
    return cast(list[dict[str, Any]], payloads)


def test_every_partition_keeps_the_items_in_order_in_full_batches() -> None:
    # Every length from 1 to 200, with every batch size from 1 to n + 2.
    for n in range(1, 201):
        items: list[JsonValue] = [i % 7 for i in range(n)]  # repeated values throughout
        for size in range(1, n + 3):
            tasks = partition(items, batch_size=size, max_attempts=3)
            batches = envelopes(tasks)
            assert len(tasks) == math.ceil(n / size)
            assert [x for b in batches for x in b["items"]] == items
            assert [b["offset"] for b in batches] == [i * size for i in range(len(tasks))]
            assert all(len(b["items"]) == size for b in batches[:-1])
            assert 1 <= len(batches[-1]["items"]) <= size
            keys = [task.task_key for task in tasks]
            assert len(set(keys)) == len(keys)
            assert sorted(keys) == keys  # code-point order is batch order (DEC-027)
            assert {task.max_attempts for task in tasks} == {3}


@pytest.mark.parametrize(
    ("n", "size", "keys"),
    [
        (1, 1, ["batch-0"]),
        (6, 3, ["batch-0", "batch-1"]),  # an exact multiple
        (7, 3, ["batch-0", "batch-1", "batch-2"]),  # a shorter last batch
        (10, 1, [f"batch-{i}" for i in range(10)]),  # ten batches: indices 0-9, one digit
        (11, 1, [f"batch-{i:02d}" for i in range(11)]),  # eleven: padded to two digits
        (101, 1, [f"batch-{i:03d}" for i in range(101)]),
        (5, 100, ["batch-0"]),  # a batch larger than the input
    ],
)
def test_keys_are_zero_padded_to_the_last_index(n: int, size: int, keys: list[str]) -> None:
    assert [t.task_key for t in partition(list(range(n)), batch_size=size, max_attempts=1)] == keys


def test_batches_are_contiguous_and_keep_repeats() -> None:
    assert partition(["a", "b", "a", "c", "a"], batch_size=2, max_attempts=3) == (
        TaskSpec("batch-0", {"offset": 0, "items": ["a", "b"]}, 3),
        TaskSpec("batch-1", {"offset": 2, "items": ["a", "c"]}, 3),
        TaskSpec("batch-2", {"offset": 4, "items": ["a"]}, 3),
    )


def test_the_same_input_always_gives_the_same_tasks_and_is_left_unchanged() -> None:
    items: list[JsonValue] = [{"id": i, "lines": [i, None, "x"]} for i in range(13)]
    before = copy.deepcopy(items)
    first = partition(items, batch_size=4, max_attempts=2, key_prefix="invoices")
    second = partition(
        tuple(copy.deepcopy(items)), batch_size=4, max_attempts=2, key_prefix="invoices"
    )
    assert first == second
    assert items == before  # the caller's sequence is not changed
    assert [t.task_key for t in first] == ["invoices-0", "invoices-1", "invoices-2", "invoices-3"]


def test_different_batch_sizes_that_make_the_same_tasks_make_equal_tasks() -> None:
    items: list[JsonValue] = [1, 2, 3]
    assert partition(items, batch_size=3, max_attempts=3) == partition(
        items, batch_size=8, max_attempts=3
    )


@pytest.mark.parametrize("max_attempts", [1, 3, 100])
def test_max_attempts_is_passed_through(max_attempts: int) -> None:
    tasks = partition([1, 2, 3], batch_size=2, max_attempts=max_attempts)
    assert {task.max_attempts for task in tasks} == {max_attempts}


def test_the_tasks_form_a_valid_submission() -> None:
    tasks = partition(list(range(25)), batch_size=2, max_attempts=3)
    assert JobSubmission("invoice_verification", tasks).tasks == tasks  # unique keys, not empty


@pytest.mark.parametrize("items", [[], ()], ids=["list", "tuple"])
def test_there_must_be_items(items: Any) -> None:
    with pytest.raises(ValueError, match="no items to partition"):
        partition(items, batch_size=1, max_attempts=3)


@pytest.mark.parametrize(
    "items", ["abc", b"abc", bytearray(b"abc"), {1, 2}, (i for i in range(3)), {"a": 1}, 3]
)
def test_items_must_be_a_sequence_of_values(items: Any) -> None:
    with pytest.raises(TypeError, match="items must be a sequence"):
        partition(items, batch_size=1, max_attempts=3)


@pytest.mark.parametrize(
    ("batch_size", "error"),
    [
        (0, ValueError),
        (-1, ValueError),
        (True, TypeError),
        (False, TypeError),
        (1.5, TypeError),
        (2.0, TypeError),
        ("2", TypeError),
        (None, TypeError),
    ],
)
def test_batch_size_must_be_a_positive_integer(batch_size: Any, error: type[Exception]) -> None:
    with pytest.raises(error, match="batch_size"):
        partition([1, 2, 3], batch_size=batch_size, max_attempts=3)


@pytest.mark.parametrize(
    ("key_prefix", "error"),
    [
        ("", ValueError),
        ("   ", ValueError),
        ("a\x00b", ValueError),
        (5, TypeError),
        (None, TypeError),
    ],
)
def test_the_key_prefix_must_make_a_storable_key(key_prefix: Any, error: type[Exception]) -> None:
    with pytest.raises(error, match="key_prefix"):
        partition([1, 2, 3], batch_size=2, max_attempts=3, key_prefix=key_prefix)
