"""Deterministic task partitioning: an ordered list of items into fixed-size batches (DEC-052).

A pure function, no I/O. ``partition`` splits the items, in order and with
any repeats kept, into contiguous batches of ``batch_size`` (the last may be
shorter), one ``TaskSpec`` each:

- ``task_key`` is ``"<key_prefix>-<index>"``, the batch index zero-padded to
  the width of the last index, so code-point order (how ``list_tasks`` sorts,
  DEC-027) is batch order: ``batch-00`` … ``batch-10``;
- ``payload`` is ``{"offset": <index of its first item>, "items": [...]}``;
- ``max_attempts`` is passed through; the database enforces its range (DEC-026).

The same items and settings always give equal ``TaskSpec`` values, so a
submission built from them replays idempotently (DEC-025). PostgreSQL compares
the resulting tasks, not these settings: a different ``batch_size`` that
yields the same tasks is still a replay.

The caller's sequence is never changed, but the items themselves are not
copied: like any payload, treat them as read-only. There is no limit on the
number of items or the batch size yet; production values have not been
measured (BS-05).
"""

from collections.abc import Sequence

from orqestra.domain.jobs import JsonValue, TaskSpec


def partition(
    items: Sequence[JsonValue],
    *,
    batch_size: int,
    max_attempts: int,
    key_prefix: str = "batch",
) -> tuple[TaskSpec, ...]:
    """Split ``items`` into contiguous batches of ``batch_size``, one task each.

    Raises ``TypeError`` for a ``str`` or ``bytes`` (not a list of items), a
    non-integer ``batch_size`` (``bool`` included) or a non-``str``
    ``key_prefix``; ``ValueError`` for no items, a ``batch_size`` below 1, or a
    blank or NUL-containing ``key_prefix``.
    """
    if isinstance(items, str | bytes | bytearray) or not isinstance(items, Sequence):
        msg = f"items must be a sequence of JSON values, got {type(items).__name__}"
        raise TypeError(msg)
    if isinstance(batch_size, bool) or not isinstance(batch_size, int):
        msg = f"batch_size must be an integer, got {type(batch_size).__name__}"
        raise TypeError(msg)
    if batch_size < 1:
        msg = f"batch_size must be at least 1, got {batch_size}"
        raise ValueError(msg)
    _check_key_prefix(key_prefix)
    values = list(items)  # one read of the caller's sequence, never changed
    if not values:
        msg = "there are no items to partition"
        raise ValueError(msg)
    starts = range(0, len(values), batch_size)
    width = len(str(len(starts) - 1))
    return tuple(
        TaskSpec(
            f"{key_prefix}-{index:0{width}d}",
            {"offset": start, "items": values[start : start + batch_size]},
            max_attempts,
        )
        for index, start in enumerate(starts)
    )


def _check_key_prefix(key_prefix: object) -> None:
    if not isinstance(key_prefix, str):
        msg = f"key_prefix must be a str, got {type(key_prefix).__name__}"
        raise TypeError(msg)
    if not key_prefix.strip(" "):  # the rule of tasks_task_key_not_blank: btrim(key) <> ''
        msg = "key_prefix must not be blank"
        raise ValueError(msg)
    if "\x00" in key_prefix:  # PostgreSQL text cannot hold NUL
        msg = "key_prefix must not contain a NUL character"
        raise ValueError(msg)
