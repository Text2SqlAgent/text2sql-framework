"""LangChain middleware adapter for text2sql.

Plugs text2sql's SQL execution and schema-exploration capabilities into any
LangChain agent built with `create_agent`, via the middleware interface.

Requires `langchain>=1.0`. Install with: `pip install "text2sql-agent[langchain]"`
"""

from __future__ import annotations

from typing import Callable

from langchain.agents.middleware import (
    AgentMiddleware,
    ModelRequest,
    ModelResponse,
)
from langchain_core.messages import SystemMessage

from text2sql.connection import Database
from text2sql.dialects import get_dialect_guide
from text2sql.examples import ExampleStore
from text2sql.tools import make_tools


class Text2SqlMiddleware(AgentMiddleware):
    """Adds text-to-SQL capability to a LangChain agent.

    Usage:
        from langchain.agents import create_agent
        from text2sql import Text2SqlMiddleware

        t2s = Text2SqlMiddleware(db_url="sqlite:///mydb.db")
        agent = create_agent(
            model="anthropic:claude-sonnet-4-6",
            tools=t2s.tools,
            middleware=[t2s],
        )
        result = agent.invoke({
            "messages": [{"role": "user", "content": "Top 5 customers by revenue?"}],
        })

    Args:
        db_url: A SQLAlchemy connection string for the target database.
            Examples: "sqlite:///mydb.db", "postgresql://user:pw@host/db".
        examples: Optional path to a markdown file of business-concept ->
            SQL-guidance scenarios. When set, the agent gets a `lookup_example`
            tool to pull in domain knowledge on demand.
        instructions: Optional free-form text appended to the system prompt
            (e.g. "Revenue is net of refunds").
    """

    def __init__(
        self,
        db_url: str,
        examples: str | None = None,
        instructions: str | None = None,
    ):
        super().__init__()
        self.db = Database(db_url)
        self.example_store = ExampleStore(examples) if examples else None
        self.instructions = instructions

        self.tools = make_tools(self.db, self.example_store)
        self._prompt_fragment = self._build_prompt_fragment()

    def _build_prompt_fragment(self) -> str:
        dialect = self.db.dialect
        guide = get_dialect_guide(dialect)
        parts = [
            f"\n\n## SQL database access ({dialect})",
            "You have access to `execute_sql` for any read-only query.",
            "Always explore the schema before writing your final query — never guess table or column names.",
            guide,
        ]
        if self.example_store:
            names = self.example_store.list_scenarios()
            if names:
                parts.append(
                    "Use `lookup_example` when the question involves a business "
                    "concept you're unsure about. Available scenarios: "
                    + ", ".join(names)
                )
        if self.instructions:
            parts.append(self.instructions)
        return "\n".join(parts)

    def wrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], ModelResponse],
    ) -> ModelResponse:
        existing_blocks = (
            list(request.system_message.content_blocks)
            if request.system_message is not None
            else []
        )
        new_blocks = existing_blocks + [{"type": "text", "text": self._prompt_fragment}]
        new_system = SystemMessage(content=new_blocks)
        return handler(request.override(system_message=new_system))
