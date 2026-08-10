import json

from click.testing import CliRunner

from text2sql.cli import main
from text2sql.scaffold import AGENT_MARKDOWN


def test_init_scaffolds_claude_code_without_secret(tmp_path, monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    result = CliRunner().invoke(main, ["init", "--target", str(tmp_path)])
    assert result.exit_code == 0, result.output

    config = json.loads((tmp_path / ".mcp.json").read_text())
    server = config["mcpServers"]["text2sql"]
    assert server["command"] == "uvx"
    assert server["args"] == ["--from", "text2sql-mcp>=0.2.0", "text2sql-mcp"]
    assert server["env"]["TEXT2SQL_DATABASE_URL"] == "${TEXT2SQL_DATABASE_URL}"
    assert "API_KEY" not in json.dumps(config)
    assert (tmp_path / ".claude/agents/text2sql.md").read_text() == AGENT_MARKDOWN
    assert (tmp_path / ".text2sql/.gitignore").exists()


def test_init_merges_and_is_idempotent(tmp_path):
    original = {"other": {"keep": True}, "mcpServers": {"existing": {"command": "x"}}}
    (tmp_path / ".mcp.json").write_text(json.dumps(original))
    runner = CliRunner()
    first = runner.invoke(main, ["init", "--target", str(tmp_path), "--trace-mode", "off"])
    assert first.exit_code == 0, first.output
    snapshot = (tmp_path / ".mcp.json").read_bytes()
    second = runner.invoke(main, ["init", "--target", str(tmp_path), "--trace-mode", "off"])
    assert second.exit_code == 0, second.output
    assert (tmp_path / ".mcp.json").read_bytes() == snapshot
    merged = json.loads(snapshot)
    assert merged["other"] == original["other"]
    assert merged["mcpServers"]["existing"] == original["mcpServers"]["existing"]


def test_init_refuses_conflicts_without_force(tmp_path):
    (tmp_path / ".mcp.json").write_text(json.dumps({
        "mcpServers": {"text2sql": {"command": "custom"}}
    }))
    agent = tmp_path / ".claude/agents/text2sql.md"
    agent.parent.mkdir(parents=True)
    agent.write_text("custom prompt")

    result = CliRunner().invoke(main, ["init", "--target", str(tmp_path)])
    assert result.exit_code != 0
    assert json.loads((tmp_path / ".mcp.json").read_text())["mcpServers"]["text2sql"]["command"] == "custom"
    assert agent.read_text() == "custom prompt"


def test_init_preserves_unrelated_config_when_forced(tmp_path):
    (tmp_path / ".mcp.json").write_text(json.dumps({
        "theme": "dark", "mcpServers": {"other": {"command": "other"}, "text2sql": {"command": "old"}}
    }))
    result = CliRunner().invoke(main, ["init", "--target", str(tmp_path), "--force"])
    assert result.exit_code == 0, result.output
    config = json.loads((tmp_path / ".mcp.json").read_text())
    assert config["theme"] == "dark"
    assert config["mcpServers"]["other"] == {"command": "other"}
    assert config["mcpServers"]["text2sql"]["command"] == "uvx"


def test_init_selects_database_driver_extra(tmp_path):
    result = CliRunner().invoke(main, [
        "init", "--target", str(tmp_path), "--database-type", "postgres"
    ])
    assert result.exit_code == 0, result.output
    server = json.loads((tmp_path / ".mcp.json").read_text())["mcpServers"]["text2sql"]
    assert server["args"] == ["--from", "text2sql-mcp[postgres]>=0.2.0", "text2sql-mcp"]


def test_init_selects_databricks_driver_extra(tmp_path):
    result = CliRunner().invoke(main, [
        "init", "--target", str(tmp_path), "--database-type", "databricks"
    ])
    assert result.exit_code == 0, result.output
    server = json.loads((tmp_path / ".mcp.json").read_text())["mcpServers"]["text2sql"]
    assert server["args"] == [
        "--from", "text2sql-mcp[databricks]>=0.2.0", "text2sql-mcp"
    ]


def test_databricks_database_tracing_requires_separate_postgres(tmp_path):
    result = CliRunner().invoke(main, [
        "init", "--target", str(tmp_path), "--database-type", "databricks",
        "--trace-mode", "database",
    ])
    assert result.exit_code != 0
    assert "--trace-database-type postgres" in result.output
    assert not (tmp_path / ".mcp.json").exists()


def test_databricks_with_postgres_trace_database(tmp_path):
    result = CliRunner().invoke(main, [
        "init", "--target", str(tmp_path), "--database-type", "databricks",
        "--trace-mode", "database", "--trace-database-type", "postgres",
    ])
    assert result.exit_code == 0, result.output
    server = json.loads((tmp_path / ".mcp.json").read_text())["mcpServers"]["text2sql"]
    assert server["args"] == [
        "--from", "text2sql-mcp[databricks,postgres]>=0.2.0", "text2sql-mcp"
    ]
    assert server["env"]["TEXT2SQL_TRACE_DATABASE_URL"] == "${TEXT2SQL_TRACE_DATABASE_URL}"
    assert "TEXT2SQL_TRACE_DATABASE_URL" in result.output
