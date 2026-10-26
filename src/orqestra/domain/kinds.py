"""Which job kinds a claimant or a handler registry may name.

A pure check, no I/O. It mirrors the database's rule for stored job kinds,
``jobs_kind_not_blank`` (``btrim(kind) <> ''``, where ``btrim`` trims spaces
only), so every kind a job can have can be named, and nothing that no job can
have is accepted: a kind is a non-blank ``str`` without a NUL character, which
PostgreSQL text cannot hold. Kinds match exactly; nothing is case-folded or
trimmed.
"""

from collections.abc import Collection


def check_job_kinds(kinds: object) -> list[str]:
    """Return the distinct kinds in ``kinds``, sorted; raise for anything else.

    ``kinds`` must be a non-empty collection of kinds. A bare ``str`` or
    ``bytes`` is refused, since it would be read as its characters.
    """
    if isinstance(kinds, str | bytes) or not isinstance(kinds, Collection):
        msg = f"kinds must be a collection of job kinds, got {type(kinds).__name__}"
        raise TypeError(msg)
    if not kinds:
        msg = "kinds must name at least one supported job kind"
        raise ValueError(msg)
    checked: set[str] = set()
    for kind in kinds:
        if not isinstance(kind, str):
            msg = f"each kind must be a str, got {type(kind).__name__}"
            raise TypeError(msg)
        if not kind.strip(" "):  # the rule of jobs_kind_not_blank: btrim(kind) <> ''
            msg = "a kind must not be blank"
            raise ValueError(msg)
        if "\x00" in kind:
            msg = "a kind must not contain a NUL character"
            raise ValueError(msg)
        checked.add(kind)
    return sorted(checked)
