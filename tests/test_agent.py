"""Tests for the native agent loop with mocked provider clients.

These exercise the real backend payload conversion and response parsing (only the
provider HTTP client is faked), plus the loop mechanics, final-answer parsing, and
usage/timestamp metadata propagation into traces.
"""

import os
import tempfile
import types
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, text as sql_text

from text2sql import agent as agent_mod
from text2sql.agent import (
    AIMessage,
    HumanMessage,
    NativeAgent,
    ToolMessage,
    _function_to_schema,
    create_deep_agent,
)


# ---------------------------------------------------------------------------
# Fake Anthropic client
# ---------------------------------------------------------------------------


def _text_block(text):
    return types.SimpleNamespace(type="text", text=text)


def _tool_use_block(id, name, input):
    return types.SimpleNamespace(type="tool_use", id=id, name=name, input=input)


class _FakeAnthropicResponse:
    def __init__(self, content, input_tokens, output_tokens):
        self.content = content
        self.usage = types.SimpleNamespace(
            input_tokens=input_tokens, output_tokens=output_tokens
        )


class _FakeAnthropicMessages:
    def __init__(self, scripted):
        self._scripted = list(scripted)
        self.calls = []  # captured kwargs per create() call

    def create(self, **kwargs):
        self.calls.append(kwargs)
        return self._scripted.pop(0)


class _FakeAnthropicClient:
    def __init__(self, scripted):
        self.messages = _FakeAnthropicMessages(scripted)


def _patch_anthropic(scripted):
    """Patch anthropic.Anthropic so the backend picks up a scripted fake client."""
    holder = {}

    def factory(*args, **kwargs):
        client = _FakeAnthropicClient(scripted)
        holder["client"] = client
        return client

    return patch("anthropic.Anthropic", factory), holder


# ---------------------------------------------------------------------------
# Fake OpenAI client
# ---------------------------------------------------------------------------


class _FakeOAToolCall:
    def __init__(self, id, name, arguments):
        self.id = id
        self.type = "function"
        self.function = types.SimpleNamespace(name=name, arguments=arguments)


class _FakeOAChoice:
    def __init__(self, content, tool_calls):
        self.message = types.SimpleNamespace(content=content, tool_calls=tool_calls)


class _FakeOAResponse:
    def __init__(self, content, tool_calls, prompt_tokens, completion_tokens):
        self.choices = [_FakeOAChoice(content, tool_calls)]
        self.usage = types.SimpleNamespace(
            prompt_tokens=prompt_tokens, completion_tokens=completion_tokens
        )


class _FakeOACompletions:
    def __init__(self, scripted):
        self._scripted = list(scripted)
        self.calls = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        return self._scripted.pop(0)


class _FakeOAClient:
    def __init__(self, scripted, **kwargs):
        self.init_kwargs = kwargs
        self.chat = types.SimpleNamespace(completions=_FakeOACompletions(scripted))


# ---------------------------------------------------------------------------
# Simple in-memory tools
# ---------------------------------------------------------------------------


def _make_echo_tools(log):
    def run_query(sql: str) -> str:
        """Run a query and return a result. Only used for tests."""
        log.append(("run_query", sql))
        return "col\n---\nvalue"

    return [run_query]


# ---------------------------------------------------------------------------
# Schema derivation
# ---------------------------------------------------------------------------


class TestSchema:
    def test_single_required_string(self):
        def f(sql: str) -> str:
            """Docstring here."""
            return sql

        schema = _function_to_schema(f)
        assert schema["name"] == "f"
        assert schema["description"] == "Docstring here."
        assert schema["parameters"] == {
            "type": "object",
            "properties": {"sql": {"type": "string"}},
            "required": ["sql"],
        }

    def test_types_and_defaults(self):
        def g(a: int, b: bool = False, c: float = 1.0) -> str:
            """G."""
            return ""

        params = _function_to_schema(g)["parameters"]
        assert params["properties"] == {
            "a": {"type": "integer"},
            "b": {"type": "boolean"},
            "c": {"type": "number"},
        }
        assert params["required"] == ["a"]


# ---------------------------------------------------------------------------
# Message shapes match what generate._parse_result reads
# ---------------------------------------------------------------------------


class TestMessageShapes:
    def test_toolmessage_detected_by_name_and_id_only(self):
        ai = AIMessage(content="hi", tool_calls=[{"id": "1", "name": "x", "args": {}}])
        tool = ToolMessage(content="out", name="x", tool_call_id="1")
        human = HumanMessage(content="q")
        # Only ToolMessage exposes both name and tool_call_id.
        assert hasattr(tool, "name") and hasattr(tool, "tool_call_id")
        assert not hasattr(ai, "name") and not hasattr(ai, "tool_call_id")
        assert not hasattr(human, "name") and not hasattr(human, "tool_call_id")
        # Only AIMessage exposes tool_calls.
        assert hasattr(ai, "tool_calls")
        assert not hasattr(tool, "tool_calls")
        assert not hasattr(human, "tool_calls")
        # response_metadata present on all three.
        assert ai.response_metadata == {} and tool.response_metadata == {}


# ---------------------------------------------------------------------------
# Anthropic backend: tool-call round trip + final answer
# ---------------------------------------------------------------------------


class TestAnthropicLoop:
    def test_tool_round_trip(self):
        log = []
        tools = _make_echo_tools(log)
        scripted = [
            _FakeAnthropicResponse(
                content=[
                    _text_block("Let me run a query."),
                    _tool_use_block("toolu_1", "run_query", {"sql": "SELECT 1"}),
                ],
                input_tokens=100,
                output_tokens=20,
            ),
            _FakeAnthropicResponse(
                content=[_text_block("```sql\nSELECT 1\n```")],
                input_tokens=130,
                output_tokens=10,
            ),
        ]
        patcher, holder = _patch_anthropic(scripted)
        with patcher:
            agent = create_deep_agent(
                model="anthropic:claude-sonnet-4-6",
                tools=tools,
                system_prompt="SYS",
            )
            result = agent.invoke({"messages": [{"role": "user", "content": "count"}]})

        msgs = result["messages"]
        # Human, AI(tool_call), Tool, AI(final)
        assert [m.type for m in msgs] == ["human", "ai", "tool", "ai"]
        # Tool executed once with the given args.
        assert log == [("run_query", "SELECT 1")]
        # Tool message wired correctly.
        tool_msg = msgs[2]
        assert tool_msg.name == "run_query"
        assert tool_msg.tool_call_id == "toolu_1"
        assert "value" in tool_msg.content
        # Final message carries the SQL text.
        assert "SELECT 1" in msgs[-1].content
        # Usage + timestamp metadata on both AI messages.
        for ai in (msgs[1], msgs[3]):
            assert ai.response_metadata["usage"]["input_tokens"] > 0
            assert ai.response_metadata["usage"]["output_tokens"] > 0
            assert ai.response_metadata["timestamp"] > 0

    def test_second_call_includes_tool_result_and_cache_control(self):
        log = []
        tools = _make_echo_tools(log)
        scripted = [
            _FakeAnthropicResponse(
                content=[_tool_use_block("toolu_9", "run_query", {"sql": "SELECT 2"})],
                input_tokens=50,
                output_tokens=5,
            ),
            _FakeAnthropicResponse(
                content=[_text_block("done")],
                input_tokens=60,
                output_tokens=5,
            ),
        ]
        patcher, holder = _patch_anthropic(scripted)
        with patcher:
            agent = create_deep_agent("anthropic:m", tools, "SYS")
            agent.invoke({"messages": [{"role": "user", "content": "go"}]})

        calls = holder["client"].messages.calls
        assert len(calls) == 2
        # System prompt carries prompt-caching cache_control.
        sys_blocks = calls[0]["system"]
        assert sys_blocks[0]["cache_control"] == {"type": "ephemeral"}
        # Tool schema derived and passed.
        assert calls[0]["tools"][0]["name"] == "run_query"
        assert "input_schema" in calls[0]["tools"][0]
        # Second call feeds the tool result back as a user/tool_result turn.
        second_msgs = calls[1]["messages"]
        tool_result_turns = [
            m
            for m in second_msgs
            if m["role"] == "user"
            and isinstance(m["content"], list)
            and any(b.get("type") == "tool_result" for b in m["content"])
        ]
        assert tool_result_turns, "tool_result not fed back to the model"
        block = tool_result_turns[0]["content"][0]
        assert block["tool_use_id"] == "toolu_9"

    def test_final_answer_without_tools(self):
        tools = _make_echo_tools([])
        scripted = [
            _FakeAnthropicResponse(
                content=[_text_block("```sql\nSELECT 42\n```")],
                input_tokens=10,
                output_tokens=3,
            )
        ]
        patcher, _ = _patch_anthropic(scripted)
        with patcher:
            agent = create_deep_agent("anthropic:m", tools, "SYS")
            result = agent.invoke({"messages": [{"role": "user", "content": "q"}]})
        assert [m.type for m in result["messages"]] == ["human", "ai"]
        assert "SELECT 42" in result["messages"][-1].content


# ---------------------------------------------------------------------------
# OpenAI backend: json-encoded args + base_url + usage mapping
# ---------------------------------------------------------------------------


class TestOpenAILoop:
    def test_tool_round_trip_and_usage_mapping(self):
        log = []
        tools = _make_echo_tools(log)
        scripted = [
            _FakeOAResponse(
                content=None,
                tool_calls=[_FakeOAToolCall("call_1", "run_query", '{"sql": "SELECT 3"}')],
                prompt_tokens=200,
                completion_tokens=15,
            ),
            _FakeOAResponse(
                content="```sql\nSELECT 3\n```",
                tool_calls=None,
                prompt_tokens=220,
                completion_tokens=8,
            ),
        ]

        def factory(*args, **kwargs):
            return _FakeOAClient(scripted, **kwargs)

        with patch("openai.OpenAI", factory):
            agent = create_deep_agent("openai:gpt-4o", tools, "SYS")
            result = agent.invoke({"messages": [{"role": "user", "content": "q"}]})

        assert log == [("run_query", "SELECT 3")]  # json args parsed
        msgs = result["messages"]
        assert [m.type for m in msgs] == ["human", "ai", "tool", "ai"]
        # OpenAI prompt/completion tokens mapped to input/output.
        assert msgs[1].response_metadata["usage"] == {
            "input_tokens": 200,
            "output_tokens": 15,
        }
        assert "SELECT 3" in msgs[-1].content

    def test_base_url_honored_for_openai_compatible_endpoints(self):
        tools = _make_echo_tools([])
        scripted = [_FakeOAResponse("done", None, 1, 1)]
        captured = {}

        def factory(*args, **kwargs):
            client = _FakeOAClient(scripted, **kwargs)
            captured["client"] = client
            return client

        with patch.dict(os.environ, {"OPENAI_BASE_URL": "http://localhost:11434/v1"}):
            with patch("openai.OpenAI", factory):
                agent = create_deep_agent("openai:llama3", tools, "SYS")
                agent.invoke({"messages": [{"role": "user", "content": "q"}]})

        assert captured["client"].init_kwargs.get("base_url") == "http://localhost:11434/v1"


# ---------------------------------------------------------------------------
# Unknown provider + lazy-import error message
# ---------------------------------------------------------------------------


class TestBackendSelection:
    def test_unknown_provider_raises(self):
        with pytest.raises(ValueError, match="Unsupported provider"):
            create_deep_agent("cohere:model", _make_echo_tools([]), "SYS")

    def test_default_provider_is_anthropic(self):
        scripted = [_FakeAnthropicResponse([_text_block("hi")], 1, 1)]
        patcher, _ = _patch_anthropic(scripted)
        with patcher:
            agent = create_deep_agent("just-a-model", _make_echo_tools([]), "SYS")
        assert isinstance(agent.backend, agent_mod._AnthropicBackend)
        assert agent.backend.model_name == "just-a-model"


# ---------------------------------------------------------------------------
# Context guard
# ---------------------------------------------------------------------------


class TestContextGuard:
    def test_truncates_oldest_tool_output(self):
        big = "x" * 4000
        messages = [
            HumanMessage(content="q"),
            AIMessage(content="", tool_calls=[{"id": "1", "name": "t", "args": {}}]),
            ToolMessage(content=big, name="t", tool_call_id="1"),
            AIMessage(content="", tool_calls=[{"id": "2", "name": "t", "args": {}}]),
            ToolMessage(content=big, name="t", tool_call_id="2"),
        ]
        agent_mod._apply_context_guard(messages, token_limit=500)
        # Oldest tool output truncated; the last two messages preserved.
        assert messages[2].content == agent_mod._TRUNCATED_STUB
        assert messages[4].content == big

    def test_no_op_under_budget(self):
        messages = [HumanMessage(content="hi"), ToolMessage(content="small", name="t", tool_call_id="1")]
        agent_mod._apply_context_guard(messages, token_limit=1_000_000)
        assert messages[1].content == "small"


# ---------------------------------------------------------------------------
# End-to-end through SQLGenerator with tracing (mocked provider, real DB)
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_db_path():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    engine = create_engine("sqlite:///{}".format(path))
    with engine.connect() as conn:
        conn.execute(sql_text("CREATE TABLE customers (id INTEGER, name TEXT, revenue REAL)"))
        conn.execute(sql_text("INSERT INTO customers VALUES (1,'Alice',1000),(2,'Bob',1500)"))
        conn.commit()
    yield path
    os.unlink(path)


class TestTracePropagation:
    def test_usage_and_tool_calls_flow_into_trace(self, sample_db_path):
        from text2sql import TextSQL

        scripted = [
            _FakeAnthropicResponse(
                content=[
                    _text_block("Exploring."),
                    _tool_use_block(
                        "toolu_a",
                        "execute_sql",
                        {"sql": "SELECT name FROM customers ORDER BY revenue DESC LIMIT 1"},
                    ),
                ],
                input_tokens=500,
                output_tokens=40,
            ),
            _FakeAnthropicResponse(
                content=[
                    _text_block(
                        "The top customer is Bob.\n"
                        "```sql\nSELECT name FROM customers ORDER BY revenue DESC LIMIT 1\n```"
                    )
                ],
                input_tokens=560,
                output_tokens=25,
            ),
        ]
        patcher, _ = _patch_anthropic(scripted)
        with patcher:
            engine = TextSQL(
                "sqlite:///{}".format(sample_db_path),
                model="anthropic:claude-sonnet-4-6",
                trace_file=None,
            )
            # Force tracing on for this assertion.
            from text2sql.tracing import Tracer

            engine.generator.tracer = Tracer()
            result = engine.ask("Top customer by revenue?")

        assert result.success
        assert result.sql.startswith("SELECT")
        assert result.data == [{"name": "Bob"}]
        # Token usage accumulated from both AI turns (500+560 / 40+25).
        assert result.input_tokens == 1060
        assert result.output_tokens == 65
        # Trace recorded exactly one execute_sql tool call.
        trace = engine.generator.tracer.traces[-1]
        assert trace.total_tool_calls == 1
        assert trace.sql_attempts == 1
        assert trace.input_tokens == 1060
        assert trace.output_tokens == 65


# ---------------------------------------------------------------------------
# trace_to_db=True — traces persisted into the user's own database
# ---------------------------------------------------------------------------


def _scripted_two_tool_run():
    """Two execute_sql calls (one schema query, one answer) then a final answer."""
    return [
        _FakeAnthropicResponse(
            content=[
                _text_block("Let me look at the schema."),
                _tool_use_block(
                    "toolu_1",
                    "execute_sql",
                    {"sql": "SELECT name FROM sqlite_master WHERE type='table'"},
                ),
            ],
            input_tokens=400,
            output_tokens=30,
        ),
        _FakeAnthropicResponse(
            content=[
                _tool_use_block(
                    "toolu_2",
                    "execute_sql",
                    {"sql": "SELECT name FROM customers ORDER BY revenue DESC LIMIT 1"},
                ),
            ],
            input_tokens=450,
            output_tokens=35,
        ),
        _FakeAnthropicResponse(
            content=[
                _text_block(
                    "Bob.\n"
                    "```sql\nSELECT name FROM customers ORDER BY revenue DESC LIMIT 1\n```"
                )
            ],
            input_tokens=500,
            output_tokens=20,
        ),
    ]


class TestDatabaseTracing:
    def test_traces_and_tool_calls_written_to_db(self, sample_db_path):
        import json as _json

        from text2sql import TextSQL

        patcher, _ = _patch_anthropic(_scripted_two_tool_run())
        with patcher:
            engine = TextSQL(
                "sqlite:///{}".format(sample_db_path),
                model="anthropic:claude-sonnet-4-6",
                trace_to_db=True,
            )
            # Opting in must turn tracing on and hand the tracer the same Database.
            assert engine.tracer is not None
            assert engine.tracer._db is engine.db
            result = engine.ask("Top customer by revenue?")

        assert result.success

        # --- query-level summary row ---
        traces = engine.db.execute("SELECT * FROM text2sql_traces")
        assert len(traces) == 1
        row = traces[0]
        assert row["question"] == "Top customer by revenue?"
        assert row["final_sql"].startswith("SELECT name FROM customers")
        assert row["success"] == 1
        assert row["error"] is None
        assert row["total_tool_calls"] == 2
        assert row["sql_attempts"] == 2
        assert row["sql_errors"] == 0
        assert row["schema_queries"] == 1
        assert row["input_tokens"] == 1350
        assert row["output_tokens"] == 85
        assert row["duration_seconds"] >= 0
        assert row["created_at"]
        trace_id = row["id"]
        assert trace_id

        # --- one row per individual tool call, in order ---
        calls = engine.db.execute(
            "SELECT * FROM text2sql_tool_calls ORDER BY sequence"
        )
        assert len(calls) == 2
        assert [c["trace_id"] for c in calls] == [trace_id, trace_id]
        assert [c["sequence"] for c in calls] == [0, 1]
        assert [c["name"] for c in calls] == ["execute_sql", "execute_sql"]
        assert all(c["id"] for c in calls)

        # arguments are the tool's input, JSON-serialized
        args = [_json.loads(c["arguments"])["sql"] for c in calls]
        assert args == [
            "SELECT name FROM sqlite_master WHERE type='table'",
            "SELECT name FROM customers ORDER BY revenue DESC LIMIT 1",
        ]

        # result is the tool's actual output
        assert "customers" in calls[0]["result"]
        assert "Bob" in calls[1]["result"]

    def test_tables_created_lazily_and_reused_across_engines(self, sample_db_path):
        """CREATE TABLE IF NOT EXISTS is idempotent; a second run appends."""
        from text2sql import TextSQL

        from text2sql.connection import Database

        # No trace tables exist until a query actually completes.
        probe_db = Database("sqlite:///{}".format(sample_db_path))
        before = {
            r["name"]
            for r in probe_db.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        assert not any(n.startswith("text2sql_") for n in before)
        probe_db.engine.dispose()

        for question in ("Top customer by revenue?", "And again?"):
            patcher, _ = _patch_anthropic(_scripted_two_tool_run())
            with patcher:
                engine = TextSQL(
                    "sqlite:///{}".format(sample_db_path),
                    model="anthropic:claude-sonnet-4-6",
                    trace_to_db=True,
                )
                # Lazy: nothing written at construction time.
                assert not engine.tracer._db_tables_ready
                engine.ask(question)
                assert engine.tracer._db_tables_ready

        assert len(engine.db.execute("SELECT * FROM text2sql_traces")) == 2
        assert len(engine.db.execute("SELECT * FROM text2sql_tool_calls")) == 4
        # Two distinct parent traces, two tool calls each.
        rows = engine.db.execute(
            "SELECT trace_id, COUNT(*) AS n FROM text2sql_tool_calls GROUP BY trace_id"
        )
        assert len(rows) == 2
        assert all(r["n"] == 2 for r in rows)

    def test_disabled_by_default(self, sample_db_path):
        from text2sql import TextSQL

        patcher, _ = _patch_anthropic([])
        with patcher:
            engine = TextSQL("sqlite:///{}".format(sample_db_path))
        assert engine.tracer is None
        names = engine.db.execute("SELECT name FROM sqlite_master WHERE type='table'")
        assert not any(n["name"].startswith("text2sql_") for n in names)

    def test_write_failure_disables_db_tracing_without_raising(self, sample_db_path, caplog):
        """A read-only role (or any DB error) must degrade, never break .ask()."""
        import logging

        from text2sql.connection import Database
        from text2sql.tracing import Tracer

        class _BoomEngine:
            def begin(self):
                raise RuntimeError("permission denied for table text2sql_traces")

        db = Database("sqlite:///{}".format(sample_db_path))
        db.engine.dispose()
        db.engine = _BoomEngine()
        tracer = Tracer(db=db)

        with caplog.at_level(logging.WARNING, logger="text2sql.tracing"):
            tracer.start_query("q")
            tracer.record_tool_call("execute_sql", {"sql": "SELECT 1"}, "1")
            trace = tracer.end_query("SELECT 1", success=True)  # must not raise

            assert trace.success
            assert tracer._db_enabled is False
            assert "Database tracing disabled" in caplog.text

            # Permanently off: no retry, no second warning, no exception.
            caplog.clear()
            tracer.start_query("q2")
            tracer.end_query("SELECT 2", success=True)

        assert tracer._db_enabled is False
        assert "Database tracing disabled" not in caplog.text
        # The in-memory sink is unaffected.
        assert len(tracer.traces) == 2
