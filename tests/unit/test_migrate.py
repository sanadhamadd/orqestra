import hashlib
from pathlib import Path

import pytest

from orqestra.persistence.migrate import (
    LOCK_KEY,
    Migration,
    MigrationError,
    checksum,
    load_migrations,
    migrate,
)

pytestmark = pytest.mark.unit


def _write(directory: Path, files: dict[str, str | bytes]) -> Path:
    for name, content in files.items():
        path = directory / name
        if isinstance(content, bytes):
            path.write_bytes(content)
        else:
            path.write_text(content, encoding="utf-8")
    return directory


def test_packaged_migrations_load() -> None:
    migrations = load_migrations()
    assert [migration.label for migration in migrations] == ["0001_initial_schema"]
    initial = migrations[0]
    assert initial.version == 1
    assert "CREATE TABLE tasks" in initial.sql
    assert initial.checksum == hashlib.sha256(initial.sql.encode("utf-8")).hexdigest()


def test_migrations_are_ordered_by_version(tmp_path: Path) -> None:
    _write(
        tmp_path,
        {
            "0003_third.sql": "SELECT 3;",
            "0001_first.sql": "SELECT 1;",
            "0002_second.sql": "SELECT 2;",
        },
    )
    assert [m.label for m in load_migrations(tmp_path)] == [
        "0001_first",
        "0002_second",
        "0003_third",
    ]


def test_empty_directory_has_no_migrations(tmp_path: Path) -> None:
    assert load_migrations(tmp_path) == ()


@pytest.mark.parametrize(
    "files",
    [
        {"0001_first.sql": "SELECT 1;", "0003_third.sql": "SELECT 3;"},
        {"0002_second.sql": "SELECT 2;"},
        {"0001_first.sql": "SELECT 1;", "0001_again.sql": "SELECT 1;"},
    ],
    ids=["gap", "does-not-start-at-0001", "duplicate-version"],
)
def test_versions_must_be_unique_and_contiguous(tmp_path: Path, files: dict[str, str]) -> None:
    _write(tmp_path, dict(files))
    with pytest.raises(MigrationError, match="unique and contiguous"):
        load_migrations(tmp_path)


@pytest.mark.parametrize(
    "name",
    [
        "1_init.sql",
        "0001-init.sql",
        "0001_Init.sql",
        "0001_init.SQL",
        "0001_.sql",
        "0001_init.sql.bak",
        "notes.txt",
    ],
)
def test_unexpected_files_are_rejected(tmp_path: Path, name: str) -> None:
    _write(tmp_path, {name: "SELECT 1;"})
    with pytest.raises(MigrationError, match="unexpected entry"):
        load_migrations(tmp_path)


def test_directories_are_rejected(tmp_path: Path) -> None:
    (tmp_path / "0001_init.sql").mkdir()
    with pytest.raises(MigrationError, match="unexpected entry"):
        load_migrations(tmp_path)


def test_hidden_files_are_ignored(tmp_path: Path) -> None:
    _write(tmp_path, {".DS_Store": b"\x00\x01", "0001_init.sql": "SELECT 1;"})
    assert [m.label for m in load_migrations(tmp_path)] == ["0001_init"]


@pytest.mark.parametrize("content", ["", "   \n\t\n"])
def test_empty_migrations_are_rejected(tmp_path: Path, content: str) -> None:
    _write(tmp_path, {"0001_init.sql": content})
    with pytest.raises(MigrationError, match="empty"):
        load_migrations(tmp_path)


def test_non_utf8_migrations_are_rejected(tmp_path: Path) -> None:
    _write(tmp_path, {"0001_init.sql": b"SELECT '\xff';"})
    with pytest.raises(MigrationError, match="UTF-8"):
        load_migrations(tmp_path)


def test_line_endings_do_not_change_checksum(tmp_path: Path) -> None:
    lf = tmp_path / "lf"
    crlf = tmp_path / "crlf"
    lf.mkdir()
    crlf.mkdir()
    _write(lf, {"0001_init.sql": b"CREATE TABLE a (id int);\nSELECT 1;\n"})
    _write(crlf, {"0001_init.sql": b"CREATE TABLE a (id int);\r\nSELECT 1;\r\n"})
    (from_lf,) = load_migrations(lf)
    (from_crlf,) = load_migrations(crlf)
    assert from_lf.sql == from_crlf.sql
    assert from_lf.checksum == from_crlf.checksum


def test_checksum_is_sha256_of_text_and_detects_any_change() -> None:
    assert checksum("SELECT 1;\n") == hashlib.sha256(b"SELECT 1;\n").hexdigest()
    assert checksum("SELECT 1;\n") != checksum("SELECT 1; \n")


def test_lock_key_is_a_stable_signed_64_bit_value() -> None:
    assert -(2**63) <= LOCK_KEY < 2**63
    expected = int.from_bytes(
        hashlib.sha256(b"orqestra.schema_migrations").digest()[:8], "big", signed=True
    )
    assert expected == LOCK_KEY


def test_migrate_validates_the_sequence_before_connecting() -> None:
    gap = [Migration(2, "second", "SELECT 2;", checksum("SELECT 2;"))]
    # An unreachable target proves no connection is attempted: the sequence
    # error is raised first.
    with pytest.raises(MigrationError, match="contiguous"):
        migrate("host=/nonexistent dbname=never", gap)
