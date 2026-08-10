import json

from sqlalchemy import create_engine, text

from text2sql.subagent import ExternalAgentSession


def _database(tmp_path):
    path = tmp_path / "analytics.db"
    engine = create_engine(f"sqlite:///{path}")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE orders (id INTEGER, amount REAL)"))
        conn.execute(text("INSERT INTO orders VALUES (1, 10.0), (2, 25.0), (3, 5.0)"))
    engine.dispose()
    return f"sqlite:///{path}", path


def test_keyless_external_session_writes_local_trace(tmp_path, monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    url, _ = _database(tmp_path)
    trace_file = tmp_path / "agent" / "traces.jsonl"
    session = ExternalAgentSession(
        url, workspace_dir=tmp_path / "agent", trace_file=trace_file
    )

    context = session.start_query("What is total revenue?")
    assert context["dialect"] == "sqlite"
    sql = "SELECT SUM(amount) AS total FROM orders"
    assert '40.0' in session.run_python(f"result = db.query({sql!r})")
    result = session.finish_query(sql)

    assert result == {"sql": sql, "data": [{"total": 40.0}], "error": None, "row_count": 1}
    lines = trace_file.read_text().splitlines()
    assert len(lines) == 1
    trace = json.loads(lines[0])
    assert trace["question"] == "What is total revenue?"
    assert trace["success"] is True
    assert [call["name"] for call in trace["tool_calls"]] == ["execute_sql", "run_python"]
    assert trace["tool_calls"][1]["execution_ms"] >= trace["tool_calls"][0]["execution_ms"]


def test_finish_requires_exact_query_tested_since_start(tmp_path):
    url, _ = _database(tmp_path)
    session = ExternalAgentSession(url, workspace_dir=tmp_path / "state", trace_mode="off")
    session.start_query("List orders")
    tested = "SELECT id FROM orders ORDER BY id"
    session.run_python(f"result = db.query({tested!r})")

    rejected = session.finish_query("SELECT amount FROM orders")
    assert "exactly match" in rejected["error"]
    assert session.active
    accepted = session.finish_query(tested + ";", max_rows=2)
    assert accepted["error"] is None
    assert len(accepted["data"]) == 2
    assert not session.active


def test_abort_and_superseded_queries_are_persisted(tmp_path):
    url, _ = _database(tmp_path)
    trace = tmp_path / "traces.jsonl"
    session = ExternalAgentSession(url, workspace_dir=tmp_path / "state", trace_file=trace)
    session.start_query("first")
    session.start_query("second")
    session.abort_query("could not determine semantics")

    records = [json.loads(line) for line in trace.read_text().splitlines()]
    assert [record["question"] for record in records] == ["first", "second"]
    assert all(not record["success"] for record in records)
    assert "Superseded" in records[0]["error"]


def test_database_trace_mode_writes_framework_tables(tmp_path):
    url, path = _database(tmp_path)
    session = ExternalAgentSession(
        url, workspace_dir=tmp_path / "state", trace_mode="database"
    )
    sql = "SELECT COUNT(*) AS count FROM orders"
    session.start_query("How many orders?")
    session.run_python(f"result = db.query({sql!r})")
    assert session.finish_query(sql)["error"] is None

    engine = create_engine(f"sqlite:///{path}")
    with engine.connect() as conn:
        row = conn.execute(text("SELECT question, success FROM text2sql_traces")).one()
        calls = conn.execute(text("SELECT COUNT(*) FROM text2sql_tool_calls")).scalar_one()
    assert row == ("How many orders?", 1)
    assert calls == 2
    stored = session.recent_traces(1)
    assert stored[0]["question"] == "How many orders?"
    assert [call["name"] for call in stored[0]["tool_calls"]] == ["execute_sql", "run_python"]
    assert stored[0]["tool_calls"][0]["arguments"]["sql"] == sql


def test_run_python_requires_started_query(tmp_path):
    url, _ = _database(tmp_path)
    session = ExternalAgentSession(url, workspace_dir=None, trace_mode="off")
    assert "Call start_query" in session.run_python("result = db.list_tables()")


def test_trace_reader_skips_malformed_lines_and_honors_zero_limit(tmp_path, caplog):
    url, _ = _database(tmp_path)
    trace = tmp_path / "traces.jsonl"
    session = ExternalAgentSession(url, workspace_dir=tmp_path / "state", trace_file=trace)
    session.start_query("one")
    session.abort_query("expected")
    with trace.open("a") as handle:
        handle.write("not-json\n")
    session.start_query("two")
    session.abort_query("expected")

    assert session.recent_traces(0) == []
    records = session.recent_traces(10)
    assert [record["question"] for record in records] == ["one", "two"]
    assert "Skipping malformed trace line" in caplog.text


def test_local_trace_sink_rejects_symlink_without_breaking_query(tmp_path, caplog):
    url, _ = _database(tmp_path)
    target = tmp_path / "target.jsonl"
    target.touch()
    trace = tmp_path / "traces.jsonl"
    try:
        trace.symlink_to(target)
    except OSError:
        import pytest
        pytest.skip("symlinks unavailable")
    session = ExternalAgentSession(url, workspace_dir=tmp_path / "state", trace_file=trace)
    session.start_query("still answer")
    result = session.abort_query("done")
    assert result["aborted"] is True
    assert target.read_text() == ""
    assert "Local trace writing disabled" in caplog.text


def test_databricks_rejects_source_database_trace_sink(tmp_path):
    import pytest
    with pytest.raises(ValueError, match="separate Postgres"):
        ExternalAgentSession(
            "databricks://token:test@example.invalid?http_path=/sql/warehouse",
            workspace_dir=tmp_path,
            trace_mode="database",
        )
