"""Tests for the LangChain middleware adapter.

Skipped automatically when langchain v1 isn't installed (the optional `[langchain]` extra).
"""

import os
import tempfile
import uuid

import pytest
from sqlalchemy import create_engine, text as sql_text

langchain_agents = pytest.importorskip("langchain.agents")
langchain_middleware = pytest.importorskip("langchain.agents.middleware")

from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage, SystemMessage, ToolMessage

from text2sql import Text2SqlMiddleware


class _ToolCapableFakeModel(GenericFakeChatModel):
    """Fake chat model that pretends to support bind_tools."""

    def bind_tools(self, tools, **kwargs):
        return self


@pytest.fixture
def db_path():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    engine = create_engine("sqlite:///{}".format(path))
    with engine.connect() as conn:
        conn.execute(sql_text(
            "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, revenue REAL)"
        ))
        conn.execute(sql_text(
            "INSERT INTO customers VALUES (1,'Alice',1000),(2,'Bob',1500)"
        ))
        conn.commit()
    yield path
    os.unlink(path)


@pytest.fixture
def scenarios_md(tmp_path):
    p = tmp_path / "scenarios.md"
    p.write_text("## net revenue\nRevenue minus refunds.\n\n## active customers\nOrdered last 90 days.\n")
    return str(p)


class TestConstruction:
    def test_basic(self, db_path):
        t2s = Text2SqlMiddleware(db_url="sqlite:///{}".format(db_path))
        names = [t.name for t in t2s.tools]
        assert "execute_sql" in names
        assert "lookup_example" not in names

    def test_with_examples(self, db_path, scenarios_md):
        t2s = Text2SqlMiddleware(db_url="sqlite:///{}".format(db_path), examples=scenarios_md)
        names = [t.name for t in t2s.tools]
        assert "execute_sql" in names
        assert "lookup_example" in names
        assert "net revenue" in t2s._prompt_fragment
        assert "active customers" in t2s._prompt_fragment

    def test_with_instructions(self, db_path):
        t2s = Text2SqlMiddleware(
            db_url="sqlite:///{}".format(db_path),
            instructions="Revenue is net of refunds.",
        )
        assert "Revenue is net of refunds." in t2s._prompt_fragment

    def test_dialect_in_prompt(self, db_path):
        t2s = Text2SqlMiddleware(db_url="sqlite:///{}".format(db_path))
        assert "sqlite" in t2s._prompt_fragment.lower()


class TestPromptInjection:
    """Confirms wrap_model_call appends the SQL guidance to the agent's system message."""

    def _spy(self, db_path, **mw_kwargs):
        captured = {}

        class SpyMW(Text2SqlMiddleware):
            def wrap_model_call(inner_self, request, handler):
                def wrapped(req):
                    captured["system"] = req.system_message
                    return handler(req)
                return super().wrap_model_call(request, wrapped)

        return SpyMW(db_url="sqlite:///{}".format(db_path), **mw_kwargs), captured

    def test_no_system_prompt(self, db_path):
        t2s, captured = self._spy(db_path)
        fake = _ToolCapableFakeModel(messages=iter([AIMessage(content="done")]))
        agent = langchain_agents.create_agent(model=fake, tools=t2s.tools, middleware=[t2s])
        agent.invoke({"messages": [{"role": "user", "content": "hi"}]})

        sys_msg = captured["system"]
        assert isinstance(sys_msg, SystemMessage)
        assert len(sys_msg.content) == 1
        assert "sqlite" in sys_msg.content[0]["text"].lower()

    def test_preserves_user_system_prompt(self, db_path):
        t2s, captured = self._spy(db_path)
        fake = _ToolCapableFakeModel(messages=iter([AIMessage(content="done")]))
        agent = langchain_agents.create_agent(
            model=fake,
            tools=t2s.tools,
            middleware=[t2s],
            system_prompt="You are a SQL analyst.",
        )
        agent.invoke({"messages": [{"role": "user", "content": "hi"}]})

        sys_msg = captured["system"]
        assert isinstance(sys_msg, SystemMessage)
        assert len(sys_msg.content) == 2
        assert sys_msg.content[0]["text"] == "You are a SQL analyst."
        assert "sqlite" in sys_msg.content[-1]["text"].lower()


class TestAgentExecution:
    """End-to-end agent loop with a scripted fake model."""

    def test_execute_sql_runs(self, db_path):
        t2s = Text2SqlMiddleware(db_url="sqlite:///{}".format(db_path))
        tool_call_msg = AIMessage(
            content="",
            tool_calls=[{
                "name": "execute_sql",
                "args": {"sql": "SELECT name FROM customers ORDER BY revenue DESC LIMIT 1"},
                "id": str(uuid.uuid4()),
            }],
        )
        final_msg = AIMessage(content="Bob")

        fake = _ToolCapableFakeModel(messages=iter([tool_call_msg, final_msg]))
        agent = langchain_agents.create_agent(model=fake, tools=t2s.tools, middleware=[t2s])
        result = agent.invoke({"messages": [{"role": "user", "content": "Top customer?"}]})

        tool_msgs = [m for m in result["messages"] if isinstance(m, ToolMessage)]
        assert len(tool_msgs) == 1
        assert "Bob" in tool_msgs[0].content

    def test_lookup_example_runs(self, db_path, scenarios_md):
        t2s = Text2SqlMiddleware(db_url="sqlite:///{}".format(db_path), examples=scenarios_md)
        tool_call_msg = AIMessage(
            content="",
            tool_calls=[{
                "name": "lookup_example",
                "args": {"scenario": "net revenue"},
                "id": str(uuid.uuid4()),
            }],
        )
        final_msg = AIMessage(content="Got it.")

        fake = _ToolCapableFakeModel(messages=iter([tool_call_msg, final_msg]))
        agent = langchain_agents.create_agent(model=fake, tools=t2s.tools, middleware=[t2s])
        result = agent.invoke({"messages": [{"role": "user", "content": "what is net revenue?"}]})

        tool_msgs = [m for m in result["messages"] if isinstance(m, ToolMessage)]
        assert len(tool_msgs) == 1
        assert "refunds" in tool_msgs[0].content.lower()
