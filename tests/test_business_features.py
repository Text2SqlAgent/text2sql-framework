"""Tests for business-oriented additions: SQLResult export and read-only verification."""

from __future__ import annotations

import csv
import os
import tempfile

import pytest
from sqlalchemy import create_engine, text

from text2sql.connection import Database
from text2sql.generate import SQLResult


@pytest.fixture
def writable_sqlite():
    """Plain SQLite — writable. Engine is disposed before unlink to avoid
    Windows file-handle issues."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    engine = create_engine(f"sqlite:///{path}")
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE t (x INTEGER)"))
        conn.commit()
    engine.dispose()

    db = Database(f"sqlite:///{path}")
    yield db
    db.engine.dispose()
    try:
        os.unlink(path)
    except (PermissionError, OSError):
        pass


class TestSQLResultExport:
    def test_to_dict_list_returns_rows(self):
        r = SQLResult(
            question="q",
            sql="SELECT 1",
            data=[{"a": 1}, {"a": 2}],
        )
        assert r.to_dict_list() == [{"a": 1}, {"a": 2}]

    def test_to_csv_writes_header_and_rows(self, tmp_path):
        r = SQLResult(
            question="q",
            sql="SELECT * FROM x",
            data=[
                {"customer": "Acme", "owed": 1000.5},
                {"customer": "Beta", "owed": 250.0},
            ],
        )
        out = tmp_path / "report.csv"
        path = r.to_csv(str(out))
        assert path == str(out)

        with open(out, encoding="utf-8") as f:
            reader = list(csv.DictReader(f))
        assert reader == [
            {"customer": "Acme", "owed": "1000.5"},
            {"customer": "Beta", "owed": "250.0"},
        ]

    def test_to_csv_creates_parent_dirs(self, tmp_path):
        r = SQLResult(question="q", sql="SELECT 1", data=[{"a": 1}])
        out = tmp_path / "nested" / "deep" / "out.csv"
        r.to_csv(str(out))
        assert out.exists()

    def test_to_csv_empty_data_creates_empty_file(self, tmp_path):
        r = SQLResult(question="q", sql="SELECT 1", data=[])
        out = tmp_path / "empty.csv"
        r.to_csv(str(out))
        assert out.exists()
        assert out.read_text() == ""


class TestReadOnlyVerification:
    def test_writable_connection_returns_false(self, writable_sqlite):
        # SQLite has no role system — the connection is fully writable.
        assert writable_sqlite.verify_read_only() is False

    def test_writable_connection_raises_when_strict(self, writable_sqlite):
        with pytest.raises(PermissionError):
            writable_sqlite.verify_read_only(raise_on_writable=True)
