from sqlalchemy import create_engine, text

from text2sql.connection import Database
from text2sql.state import SQLiteStateStore
from text2sql.workspace import PythonWorkspace


def _workspace(tmp_path, tracer=None, allow_self_modification=False):
    db_path = tmp_path / "analytics.db"
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE IF NOT EXISTS sales (amount INTEGER)"))
        conn.execute(text("INSERT INTO sales VALUES (2), (3)"))
    return PythonWorkspace(
        Database(f"sqlite:///{db_path}"),
        SQLiteStateStore(tmp_path / "state.db"),
        tracer=tracer,
        allow_self_modification=allow_self_modification,
    )


def test_workspace_is_persistent_and_queries_through_db(tmp_path):
    workspace = _workspace(tmp_path)
    assert workspace.execute("rows = db.query('SELECT amount FROM sales')\ntotal = sum(r['amount'] for r in rows)\nresult = total") == "5"
    assert workspace.execute("result = total * 2") == "10"
    assert workspace.execute("result = db.list_tables()") == '["sales"]'
    assert "amount" in workspace.execute("result = db.describe('sales')")


def test_workspace_blocks_imports_writes_and_capability_replacement(tmp_path):
    workspace = _workspace(tmp_path)
    assert "Unsupported Python syntax: Import" in workspace.execute("import os")
    assert "only permits read-only SQL" in workspace.execute("db.query('DELETE FROM sales')")
    assert "protected workspace capability" in workspace.execute("db = 3")
    assert "cannot be replaced" in workspace.execute("db.query = print")
    assert "writes are disabled" in workspace.execute("skills.write('bad', 'poison')")
    assert "has no attribute 'append'" in workspace.execute("db.query_history.append('SELECT 99')")
    assert "limited to 100,000" in workspace.execute("result = sum(range(1000000))")
    assert "protected workspace capability" in workspace.execute("sum = 7")


def test_skills_and_prompt_persist_in_state(tmp_path):
    workspace = _workspace(tmp_path, allow_self_modification=True)
    assert "Saved skill" in workspace.execute("result = skills.write('revenue', 'Use sales.amount')")
    assert workspace.execute("result = skills.list()") == '["revenue"]'
    assert "sales.amount" in workspace.execute("result = skills.read('revenue')")
    workspace.execute("result = prompt.write('Always use fiscal years')")

    other = _workspace(tmp_path)
    assert other.prompt_addendum() == "Always use fiscal years"
    assert other.skill_names() == ["revenue"]


def test_python_agent_exposes_only_run_python(tmp_path):
    from unittest.mock import patch
    from text2sql.generate import SQLGenerator
    from text2sql.state import MemoryStateStore

    db_path = tmp_path / "mode.db"
    db = Database(f"sqlite:///{db_path}")
    fake_agent = type("Agent", (), {"system_prompt": ""})()
    with patch("text2sql.agent.create_deep_agent", return_value=fake_agent):
        generator = SQLGenerator(
            db,
            model="anthropic:test",
            agent_mode="python",
            state_store=MemoryStateStore(),
        )
    assert [tool.__name__ for tool in generator.tools] == ["run_python"]
    assert "no standalone SQL tool" in generator.system_prompt
    assert "execute_sql" not in generator.system_prompt


def test_workspace_can_inspect_local_traces_after_restart(tmp_path):
    from text2sql.tracing import Tracer

    path = tmp_path / "traces.jsonl"
    first = Tracer(output_path=str(path))
    first.start_query("total sales")
    first.end_query("SELECT SUM(amount) FROM sales", True)

    workspace = _workspace(tmp_path, tracer=Tracer(output_path=str(path)))
    output = workspace.execute("result = traces.search('total sales')")
    assert "SELECT SUM(amount) FROM sales" in output


def test_python_mode_rejects_an_untested_final_query(tmp_path):
    from unittest.mock import MagicMock, patch
    from text2sql.generate import SQLGenerator
    from text2sql.state import MemoryStateStore

    db = Database(f"sqlite:///{tmp_path / 'verify.db'}")
    fake_agent = type("Agent", (), {"system_prompt": ""})()
    with patch("text2sql.agent.create_deep_agent", return_value=fake_agent):
        generator = SQLGenerator(
            db, model="anthropic:test", agent_mode="python",
            state_store=MemoryStateStore(),
        )
    generator._query_history_mark = 0
    message = MagicMock(content="```sql\nSELECT 2\n```", tool_calls=[], type="ai")
    result = generator._parse_result("q", [message])
    assert "not the last query tested" in result.error

    generator.workspace.db.query("SELECT 2")
    result = generator._parse_result("q", [message])
    assert result.success

    generator.workspace.db.query("SELECT 'ABC  X' AS value")
    changed = MagicMock(content="```sql\nSELECT 'abc x' AS value\n```", tool_calls=[], type="ai")
    assert "not the last query tested" in generator._parse_result("q", [changed]).error
