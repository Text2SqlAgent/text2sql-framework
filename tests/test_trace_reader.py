import json

from click.testing import CliRunner

from text2sql.cli import main
from text2sql.connection import Database
from text2sql.trace_reader import TraceReader
from text2sql.tracing import Tracer


def _write_trace(tracer, question, success=True, tokens=(100, 20)):
    tracer.start_query(question)
    tracer.record_token_usage(*tokens)
    tracer.record_tool_call(
        "execute_sql",
        {"sql": "SELECT 1"},
        "1" if success else "SQL Error: unresolved column",
    )
    return tracer.end_query("SELECT 1", success=success)


def test_same_trace_id_is_persisted_to_jsonl_and_database(tmp_path):
    database = Database(f"sqlite:///{tmp_path / 'traces.db'}")
    trace_file = tmp_path / "traces.jsonl"
    tracer = Tracer(output_path=str(trace_file), db=database)

    trace = _write_trace(tracer, "Count records")

    local = TraceReader(jsonl_path=trace_file).recent()
    stored = TraceReader(database=database).recent()
    assert local[0]["trace_id"] == trace.trace_id
    assert stored[0]["trace_id"] == trace.trace_id
    assert stored[0]["tool_calls"][0]["arguments"] == {"sql": "SELECT 1"}


def test_jsonl_reader_skips_malformed_lines_and_summarizes(tmp_path, caplog):
    trace_file = tmp_path / "traces.jsonl"
    tracer = Tracer(output_path=str(trace_file))
    _write_trace(tracer, "Successful", tokens=(80, 20))
    _write_trace(tracer, "Failed", success=False, tokens=(40, 10))
    with trace_file.open("a", encoding="utf-8") as handle:
        handle.write("not-json\n")

    reader = TraceReader(jsonl_path=trace_file)
    summary = reader.summary()

    assert summary["total_queries"] == 2
    assert summary["success_rate"] == 0.5
    assert summary["total_tokens"] == 150
    assert summary["avg_tokens_per_success"] == 100
    assert [item["question"] for item in reader.recent(failures_only=True)] == ["Failed"]
    assert "Skipping malformed trace line" in caplog.text


def test_legacy_jsonl_trace_gets_a_stable_derived_id(tmp_path):
    trace_file = tmp_path / "legacy.jsonl"
    trace_file.write_text(
        json.dumps({"question": "legacy", "final_sql": "SELECT 1", "success": True})
        + "\n",
        encoding="utf-8",
    )

    reader = TraceReader(jsonl_path=trace_file)
    first = reader.recent()[0]["trace_id"]
    second = reader.recent()[0]["trace_id"]
    assert first == second


def test_trace_source_does_not_expose_database_url(tmp_path):
    database = Database(f"sqlite:///{tmp_path / 'private.db'}")
    source = TraceReader(database=database).source()

    assert source["tables"] == ["text2sql_traces", "text2sql_tool_calls"]
    assert "private.db" not in json.dumps(source)


def test_install_improvement_skill(tmp_path):
    result = CliRunner().invoke(main, ["install-improvement-skill", str(tmp_path)])

    assert result.exit_code == 0
    skill = tmp_path / "improve-text2sql" / "SKILL.md"
    content = skill.read_text(encoding="utf-8")
    assert "Do not enumerate the full schema" in content
    assert "Text2SQL-Evidence" in content

    duplicate = CliRunner().invoke(main, ["install-improvement-skill", str(tmp_path)])
    assert duplicate.exit_code != 0
    assert "--force" in duplicate.output
