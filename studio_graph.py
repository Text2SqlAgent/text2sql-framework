"""Studio entrypoint — exposes the Penicor-wired text2sql agent graph for LangGraph Studio.

LangGraph Studio (`langgraph dev`) loads this module, finds the `graph`
object, and gives you a visual playground to chat with the agent and
inspect every tool call / LLM message in the loop.

IMPORTANT — what Studio sees vs what `engine.ask()` sees:
  - Studio invokes the underlying LangGraph directly. That means
    canonical-query matching is BYPASSED here — every question goes
    through the agent path, including ones that would normally hit a
    canonical. This is by design: Studio is for *debugging the agent
    loop*, not the full TextSQL pipeline.
  - text2sql's own JSONL tracing (Tracer) is also bypassed. LangSmith
    instrumentation still works since it hooks at the LangChain layer.

Configured via env vars (loaded from .env by langgraph.json):
  - PENICOR_DB_URL
  - OPENROUTER_API_KEY (or ANTHROPIC_API_KEY / OPENAI_API_KEY)
  - LANGSMITH_* (optional, for cloud traces)
"""

from __future__ import annotations

import os
from pathlib import Path

from text2sql import TextSQL

REPO_ROOT = Path(__file__).resolve().parent

_DEFAULT_CONN = "postgresql+psycopg://penicor:penicor@localhost:5433/penicor?options=-csearch_path%3Dgold"


def _resolve_model() -> str:
    """Pick provider/model based on which API key is available — same logic as penicor_demo.py.
    Default = cheap-tier per provider (Haiku / 4o-mini). Override with PENICOR_MODEL in .env
    using the full 'provider:model' form, e.g. 'openrouter:anthropic/claude-sonnet-4.6'.
    """
    if (override := os.environ.get("PENICOR_MODEL_FULL")):
        return override
    env_model = os.environ.get("PENICOR_MODEL")
    if os.environ.get("OPENROUTER_API_KEY"):
        return f"openrouter:{env_model or 'anthropic/claude-haiku-4.5'}"
    if os.environ.get("ANTHROPIC_API_KEY"):
        return f"anthropic:{env_model or 'claude-haiku-4-5'}"
    if os.environ.get("OPENAI_API_KEY"):
        return f"openai:{env_model or 'gpt-4o-mini'}"
    raise RuntimeError(
        "No LLM provider API key set. Add OPENROUTER_API_KEY (or ANTHROPIC_API_KEY) to .env."
    )


_engine = TextSQL(
    connection_string=os.environ.get("PENICOR_DB_URL", _DEFAULT_CONN),
    model=_resolve_model(),
    canonical_queries=str(REPO_ROOT / "examples" / "penicor_canonical.md"),
    trace_file=str(REPO_ROOT / "traces" / "penicor.jsonl"),
)

# The compiled LangGraph that Studio drives. Path:
#   TextSQL.generator (SQLGenerator) -> .agent (DeepAgent wrapper) -> .agent (CompiledGraph)
graph = _engine.generator.agent.agent
