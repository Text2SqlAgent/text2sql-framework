from sqlalchemy import create_engine, text

from text2sql.sandbox import run_python
from text2sql.tools import make_tools


def _query(sql):
    engine = create_engine("sqlite:///:memory:")
    with engine.connect() as conn:
        return [dict(row._mapping) for row in conn.execute(text(sql))]


def test_sandbox_queries_and_transforms_rows():
    output = run_python(
        "rows = sql('SELECT 2 AS value UNION ALL SELECT 3 AS value')\n"
        "result = sum(row['value'] for row in rows)",
        _query,
    )
    assert output == "5"


def test_sandbox_blocks_imports_and_writes():
    assert "Unsupported Python syntax: Import" in run_python("import os", _query)
    assert "read-only SQL" in run_python("sql('DELETE FROM users')", _query)


def test_sandbox_support_is_opt_in():
    db = type("Db", (), {"execute": lambda self, sql: []})()
    assert "run_python" not in [tool.__name__ for tool in make_tools(db)]
    assert "run_python" in [tool.__name__ for tool in make_tools(db, enable_python_sandbox=True)]
