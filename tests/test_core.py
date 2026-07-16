"""Tests for core functionality (no LLM required)."""

import os
import tempfile

import pytest
from sqlalchemy import create_engine, text

from text2sql.connection import Database
from text2sql.tools import execute_tool, _is_read_only
from text2sql.dialects import get_dialect_guide


@pytest.fixture
def sample_db():
    """Create a temporary SQLite database with sample data."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    engine = create_engine("sqlite:///{}".format(path))

    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE customers (
                customer_id INTEGER PRIMARY KEY,
                first_name TEXT NOT NULL,
                last_name TEXT NOT NULL,
                email TEXT,
                country TEXT
            )
        """))
        conn.execute(text("""
            CREATE TABLE orders (
                order_id INTEGER PRIMARY KEY,
                customer_id INTEGER NOT NULL,
                order_date TEXT NOT NULL,
                total REAL NOT NULL,
                FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
            )
        """))
        conn.execute(text("""
            INSERT INTO customers VALUES
            (1, 'Alice', 'Smith', 'alice@example.com', 'USA'),
            (2, 'Bob', 'Jones', 'bob@example.com', 'Canada'),
            (3, 'Charlie', 'Brown', 'charlie@example.com', 'UK')
        """))
        conn.execute(text("""
            INSERT INTO orders VALUES
            (1, 1, '2024-01-15', 100.00),
            (2, 1, '2024-02-20', 250.00),
            (3, 2, '2024-01-10', 75.00)
        """))
        conn.commit()

    db = Database("sqlite:///{}".format(path))
    yield db
    db.engine.dispose()
    try:
        os.unlink(path)
    except PermissionError:
        pass  # Windows holds SQLite files briefly after dispose; OS cleans up on exit


class TestDatabase:
    def test_connection(self, sample_db):
        assert sample_db.test_connection()

    def test_execute(self, sample_db):
        rows = sample_db.execute("SELECT * FROM customers ORDER BY customer_id")
        assert len(rows) == 3
        assert rows[0]["first_name"] == "Alice"

    def test_dialect(self, sample_db):
        assert sample_db.dialect == "sqlite"


class TestExecuteSql:
    def test_select(self, sample_db):
        result = execute_tool(
            "execute_sql",
            {"sql": "SELECT first_name FROM customers ORDER BY customer_id LIMIT 1"},
            db=sample_db,
        )
        assert "Alice" in result

    def test_pragma(self, sample_db):
        result = execute_tool(
            "execute_sql",
            {"sql": "PRAGMA table_info('customers')"},
            db=sample_db,
        )
        assert "first_name" in result

    def test_error_returns_message(self, sample_db):
        result = execute_tool(
            "execute_sql",
            {"sql": "SELECT * FROM nonexistent_table"},
            db=sample_db,
        )
        assert "SQL Error" in result

    def test_blocks_drop(self, sample_db):
        result = execute_tool("execute_sql", {"sql": "DROP TABLE customers"}, db=sample_db)
        assert "Blocked" in result

    def test_blocks_delete(self, sample_db):
        result = execute_tool("execute_sql", {"sql": "DELETE FROM customers"}, db=sample_db)
        assert "Blocked" in result

    def test_blocks_update(self, sample_db):
        result = execute_tool("execute_sql", {"sql": "UPDATE customers SET first_name='x'"}, db=sample_db)
        assert "Blocked" in result

    def test_blocks_insert(self, sample_db):
        result = execute_tool("execute_sql", {"sql": "INSERT INTO customers VALUES (99,'X','Y','z','US')"}, db=sample_db)
        assert "Blocked" in result

    def test_blocks_truncate(self, sample_db):
        result = execute_tool("execute_sql", {"sql": "TRUNCATE TABLE customers"}, db=sample_db)
        assert "Blocked" in result

    def test_allows_with_cte(self, sample_db):
        result = execute_tool(
            "execute_sql",
            {"sql": "WITH cte AS (SELECT 1 as n) SELECT * FROM cte"},
            db=sample_db,
        )
        assert "Blocked" not in result

    def test_empty_sql(self, sample_db):
        result = execute_tool("execute_sql", {"sql": ""}, db=sample_db)
        assert "Empty" in result

    def test_no_db(self):
        result = execute_tool("execute_sql", {"sql": "SELECT 1"})
        assert "not available" in result


class TestReadOnly:
    def test_select_allowed(self):
        assert _is_read_only("SELECT * FROM foo")

    def test_with_allowed(self):
        assert _is_read_only("WITH cte AS (SELECT 1) SELECT * FROM cte")

    def test_explain_allowed(self):
        assert _is_read_only("EXPLAIN SELECT * FROM foo")

    def test_show_allowed(self):
        assert _is_read_only("SHOW TABLES")

    def test_pragma_allowed(self):
        assert _is_read_only("PRAGMA table_info('foo')")

    def test_drop_blocked(self):
        assert not _is_read_only("DROP TABLE foo")

    def test_insert_blocked(self):
        assert not _is_read_only("INSERT INTO foo VALUES (1)")

    def test_delete_blocked(self):
        assert not _is_read_only("DELETE FROM foo")

    def test_update_blocked(self):
        assert not _is_read_only("UPDATE foo SET x=1")


class TestExamples:
    def test_load_and_lookup_exact(self, tmp_path):
        md = tmp_path / "examples.md"
        md.write_text("## net revenue\nRevenue minus refunds.\n\n## active customers\nOrdered in last 12 months.\n")
        from text2sql.examples import ExampleStore
        store = ExampleStore(str(md))
        assert "refunds" in store.lookup("net revenue").lower()

    def test_lookup_keyword_match(self, tmp_path):
        md = tmp_path / "examples.md"
        md.write_text("## customer home address\nJoin customers to regions.\n\n## order status\nCompleted, shipped, pending.\n")
        from text2sql.examples import ExampleStore
        store = ExampleStore(str(md))
        result = store.lookup("home address")
        assert "regions" in result.lower()

    def test_lookup_no_match(self, tmp_path):
        md = tmp_path / "examples.md"
        md.write_text("## net revenue\nStuff.\n")
        from text2sql.examples import ExampleStore
        store = ExampleStore(str(md))
        result = store.lookup("something totally unrelated xyz")
        assert "No matching example" in result

    def test_list_scenarios(self, tmp_path):
        md = tmp_path / "examples.md"
        md.write_text("## alpha\nA.\n\n## beta\nB.\n\n## gamma\nC.\n")
        from text2sql.examples import ExampleStore
        store = ExampleStore(str(md))
        assert store.list_scenarios() == ["alpha", "beta", "gamma"]

    def test_lookup_tool(self, tmp_path):
        md = tmp_path / "examples.md"
        md.write_text("## net revenue\nGross minus refunds.\n")
        from text2sql.examples import ExampleStore
        store = ExampleStore(str(md))
        result = execute_tool("lookup_example", {"scenario": "net revenue"}, example_store=store)
        assert "refunds" in result.lower()

    def test_lookup_tool_no_store(self):
        result = execute_tool("lookup_example", {"scenario": "anything"})
        assert "No example scenarios" in result


class TestReadOnlyBypassPrevention:
    """Regression tests for security bypasses identified in the attack-surface analysis."""

    def test_blocks_writable_cte(self, sample_db):
        # PostgreSQL writable CTE: WITH d AS (DELETE ...) SELECT *
        result = execute_tool(
            "execute_sql",
            {"sql": "WITH d AS (DELETE FROM customers RETURNING *) SELECT * FROM d"},
            db=sample_db,
        )
        assert "Blocked" in result

    def test_blocks_subquery_dml(self, sample_db):
        # DML inside a subquery — DELETE not at line start
        result = execute_tool(
            "execute_sql",
            {"sql": "SELECT * FROM (DELETE FROM customers RETURNING customer_id) AS d"},
            db=sample_db,
        )
        assert "Blocked" in result

    def test_blocks_multistatement(self, sample_db):
        # Two statements on one line — SELECT first, then DELETE
        result = execute_tool(
            "execute_sql",
            {"sql": "SELECT 1; DELETE FROM customers"},
            db=sample_db,
        )
        assert "Blocked" in result

    def test_final_sql_guard_blocks_destructive(self, sample_db):
        # Critical fix: _parse_result must apply _is_read_only to the final SQL
        # even if the LLM somehow emits a destructive statement in its response.
        from unittest.mock import MagicMock
        from text2sql.generate import SQLGenerator

        gen = SQLGenerator.__new__(SQLGenerator)
        gen.db = sample_db
        gen.tracer = None

        mock_msg = MagicMock()
        mock_msg.content = "```sql\nDELETE FROM customers\n```"
        mock_msg.tool_calls = []
        mock_msg.type = "ai"

        result = gen._parse_result("delete all customers", [mock_msg])
        assert result.error is not None
        assert "Blocked" in result.error
        # Confirm no rows were actually deleted
        rows = sample_db.execute("SELECT COUNT(*) AS n FROM customers")
        assert rows[0]["n"] == 3


class TestDialects:
    def test_known_dialects(self):
        for d in ["postgresql", "mysql", "sqlite", "mssql", "snowflake", "bigquery"]:
            guide = get_dialect_guide(d)
            assert len(guide) > 50
            assert "table" in guide.lower()

    def test_aliases(self):
        assert "PostgreSQL" in get_dialect_guide("postgres")
        assert "MySQL" in get_dialect_guide("mariadb")

    def test_unknown_fallback(self):
        guide = get_dialect_guide("some_exotic_db")
        assert "information_schema" in guide
