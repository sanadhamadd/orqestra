"""``docs/architecture.md`` against the code it describes.

The task and attempt state diagrams, and the transition tables under them, must
list exactly the moves of ``TASK_TRANSITIONS`` and ``ATTEMPT_TRANSITIONS``: a move
added to or removed from either side fails here. Every relative link must
resolve, anchors included, and the page keeps to the repository's wording.
"""

import re
from collections.abc import Mapping
from enum import StrEnum
from pathlib import Path

import pytest

from orqestra.domain.states import (
    ATTEMPT_TRANSITIONS,
    TASK_TRANSITIONS,
    TERMINAL_ATTEMPT_STATES,
    TERMINAL_TASK_STATES,
    AttemptState,
    TaskState,
)

pytestmark = [pytest.mark.unit, pytest.mark.usefixtures("no_network")]

ROOT = Path(__file__).resolve().parents[2]
DOC = ROOT / "docs" / "architecture.md"

Edge = tuple[str, str]

_FENCE = re.compile(r"^```(\S*)\n(.*?)^```$", re.MULTILINE | re.DOTALL)
_EDGE = re.compile(r"^\s*(\S+)\s*-->\s*([^\s:]+)")
_ROW = re.compile(r"^\|\s*([a-z_]+)\s*\|\s*([a-z_]+)\s*\|")
_HEADING = re.compile(r"^(#{1,6})\s+(.*?)\s*$", re.MULTILINE)
_LINK = re.compile(r"\[[^\]]*\]\(([^)\s]+)\)")
_SCHEDULE_LABEL = re.compile(r"\bday ?\d+[a-z]?\b", re.IGNORECASE)
_OLD_TERM = re.compile(r"\bai\b", re.IGNORECASE)


def doc_text() -> str:
    return DOC.read_text(encoding="utf-8")


def mermaid_blocks(text: str) -> list[str]:
    return [body for language, body in _FENCE.findall(text) if language == "mermaid"]


def state_block(text: str, marker: str) -> str:
    """The one Mermaid block that carries the comment ``%% <marker>``."""
    found = [body for body in mermaid_blocks(text) if f"%% {marker}" in body.splitlines()[1]]
    assert len(found) == 1, f"expected one state diagram marked {marker!r}"
    return found[0]


def diagram_edges(block: str) -> tuple[set[Edge], set[str], set[str]]:
    """The block's moves between states, its initial states and its final states."""
    edges: set[Edge] = set()
    initial: set[str] = set()
    final: set[str] = set()
    for line in block.splitlines():
        match = _EDGE.match(line)
        if match is None:
            continue
        source, target = match.groups()
        if source == "[*]":
            initial.add(target)
        elif target == "[*]":
            final.add(source)
        else:
            edges.add((source, target))
    return edges, initial, final


def section(text: str, heading: str) -> str:
    """The text under ``heading`` up to the next heading of any level."""
    start = text.index(f"\n{heading}\n") + len(heading) + 2
    following = _HEADING.search(text, start)
    return text[start : following.start() if following else len(text)]


def table_edges(text: str) -> set[Edge]:
    """The ``From | To`` pairs of the transition table in ``text``, outside code fences."""
    outside = _FENCE.sub("", text)
    rows = [_ROW.match(line) for line in outside.splitlines()]
    return {(m.group(1), m.group(2)) for m in rows if m is not None}


def table_of[S: StrEnum](transitions: Mapping[S, frozenset[S]]) -> set[Edge]:
    return {
        (source.value, target.value)
        for source, targets in transitions.items()
        for target in targets
    }


def slug(heading: str) -> str:
    """The anchor GitHub gives a heading: lower case, punctuation dropped, spaces as hyphens."""
    return re.sub(r"[^\w\- ]", "", heading.strip().lower()).replace(" ", "-")


def anchors(path: Path) -> set[str]:
    outside = _FENCE.sub("", path.read_text(encoding="utf-8"))
    return {slug(match.group(2)) for match in _HEADING.finditer(outside)}


# --- the state diagrams and tables against domain/states.py ----------------------


TASK = ("task states", TASK_TRANSITIONS, TaskState.QUEUED, TERMINAL_TASK_STATES)
ATTEMPT = (
    "attempt states",
    ATTEMPT_TRANSITIONS,
    AttemptState.RUNNING,
    TERMINAL_ATTEMPT_STATES,
)


@pytest.mark.parametrize(
    ("marker", "transitions", "start", "terminal"),
    [pytest.param(*TASK, id="task"), pytest.param(*ATTEMPT, id="attempt")],
)
def test_the_state_diagram_has_exactly_the_transition_table(
    marker: str,
    transitions: Mapping[StrEnum, frozenset[StrEnum]],
    start: StrEnum,
    terminal: frozenset[StrEnum],
) -> None:
    edges, initial, final = diagram_edges(state_block(doc_text(), marker))

    assert edges == table_of(transitions)
    assert initial == {start.value}
    assert final == {state.value for state in terminal}


@pytest.mark.parametrize(
    ("heading", "transitions"),
    [
        pytest.param("### Task", TASK_TRANSITIONS, id="task"),
        pytest.param("### Attempt", ATTEMPT_TRANSITIONS, id="attempt"),
    ],
)
def test_the_transition_table_has_exactly_the_code_table(
    heading: str, transitions: Mapping[StrEnum, frozenset[StrEnum]]
) -> None:
    assert table_edges(section(doc_text(), heading)) == table_of(transitions)


def test_an_added_or_removed_diagram_edge_is_noticed() -> None:
    block = state_block(doc_text(), "task states")
    expected = table_of(TASK_TRANSITIONS)

    added, _, _ = diagram_edges(f"{block}    failed --> queued: never\n")
    removed, _, _ = diagram_edges(
        "\n".join(line for line in block.splitlines() if "running --> queued" not in line)
    )

    assert added == expected | {("failed", "queued")}
    assert removed == expected - {("running", "queued")}


def test_an_added_or_removed_table_row_is_noticed() -> None:
    text = section(doc_text(), "### Attempt")
    expected = table_of(ATTEMPT_TRANSITIONS)

    added = table_edges(f"{text}\n| revoked | running | never |\n")
    removed = table_edges(
        "\n".join(line for line in text.splitlines() if not line.startswith("| running | revoked"))
    )

    assert added == expected | {("revoked", "running")}
    assert removed == expected - {("running", "revoked")}


def test_a_missing_move_in_the_code_would_be_noticed() -> None:
    # The comparison runs both ways: a move the code gains or loses is a difference too.
    edges, _, _ = diagram_edges(state_block(doc_text(), "attempt states"))
    fewer: dict[AttemptState, frozenset[AttemptState]] = {s: frozenset() for s in AttemptState}
    assert edges != table_of(fewer)
    assert edges != table_of(
        {**ATTEMPT_TRANSITIONS, AttemptState.REVOKED: frozenset({AttemptState.RUNNING})}
    )


# --- the three diagrams ------------------------------------------------------------


def test_the_page_has_the_data_path_the_two_state_diagrams_and_the_ownership_sequence() -> None:
    kinds = [block.splitlines()[0].split()[0] for block in mermaid_blocks(doc_text())]
    assert kinds == ["flowchart", "stateDiagram-v2", "stateDiagram-v2", "sequenceDiagram"]


# --- links and wording -------------------------------------------------------------


def test_every_relative_link_resolves() -> None:
    targets = _LINK.findall(_FENCE.sub("", doc_text()))
    assert targets, "the page links to the decision log, the ADRs and the README"
    for target in targets:
        assert not target.startswith(("http:", "https:", "mailto:")), target
        path_part, _, anchor = target.partition("#")
        path = (DOC.parent / path_part).resolve() if path_part else DOC
        assert path.exists(), f"{target}: no such file"
        assert path.is_relative_to(ROOT), f"{target}: outside the repository"
        if anchor:
            assert anchor in anchors(path), f"{target}: no such heading"


def test_the_slug_rule_matches_the_headings_it_is_used_for() -> None:
    assert slug("Known limitations") == "known-limitations"
    assert slug("Worker runtime (first slice)") == "worker-runtime-first-slice"
    assert "known-limitations" in anchors(ROOT / "README.md")


def test_the_page_keeps_to_the_repository_wording() -> None:
    text = doc_text()
    assert _SCHEDULE_LABEL.search(text) is None
    assert _OLD_TERM.search(text) is None
