"""Optional LangChain/deepagents backend (preserved for existing forks).

This is the original agent implementation: it wraps LangChain's Deep Agents
harness (langchain-ai/deepagents) with a minimal middleware stack — summarization
(context compaction) and Anthropic prompt caching. Filesystem, todo, and
sub-agent middleware are disabled.

It is kept importable so existing deepagents-based deployments aren't broken.
Select it explicitly with ``TextSQL(..., agent_backend="langchain")`` (or
``SQLGenerator(..., agent_backend="langchain")``). Requires the ``langchain``
extra:

    pip install "text2sql-framework[langchain]"

The default backend is the dependency-free native loop in ``text2sql.agent``.
"""

from __future__ import annotations

from text2sql.tools import to_langchain_tools


def _get_chat_model(model_str: str):
    """Parse 'provider:model_name' and return a LangChain chat model."""
    if ":" in model_str:
        provider, model_name = model_str.split(":", 1)
    else:
        provider, model_name = "anthropic", model_str

    provider = provider.lower()
    if provider == "anthropic":
        from langchain_anthropic import ChatAnthropic

        return ChatAnthropic(model=model_name, max_tokens=4096)
    elif provider == "openai":
        from langchain_openai import ChatOpenAI

        return ChatOpenAI(model=model_name)
    else:
        raise ValueError(f"Unsupported provider: {provider}")


class DeepAgent:
    """LangChain Deep Agents harness with text2sql tools and system prompt."""

    def __init__(
        self,
        model_str: str,
        tools: list,
        system_prompt: str,
    ):
        try:
            from deepagents import create_deep_agent as _deepagents_create
        except ImportError as exc:  # pragma: no cover - exercised via message
            raise ImportError(
                "The 'langchain' agent backend requires deepagents + langchain. "
                "Install it with: pip install 'text2sql-framework[langchain]'"
            ) from exc

        self.llm = _get_chat_model(model_str)
        self.system_prompt = system_prompt

        # generate.py hands us plain tool functions; deepagents needs LangChain tools.
        lc_tools = to_langchain_tools(tools)

        self.agent = _deepagents_create(
            model=self.llm,
            tools=lc_tools,
            system_prompt=system_prompt,
            subagents=[],
        )

    def invoke(self, input_dict: dict) -> dict:
        """Run the agent. Input: {"messages": [{"role": "user", "content": "..."}]}"""
        from langchain_core.messages import HumanMessage

        messages = []
        for msg in input_dict.get("messages", []):
            if msg["role"] == "user":
                messages.append(HumanMessage(content=msg["content"]))

        result = self.agent.invoke(
            {"messages": messages},
            config={"recursion_limit": 50},
        )

        return {"messages": result["messages"]}


def create_deep_agent(
    model: str,
    tools: list,
    system_prompt: str,
    token_limit: int = 75_000,  # kept for backward compatibility
) -> DeepAgent:
    """Create a Deep Agent with tools and a system prompt (LangChain backend)."""
    return DeepAgent(
        model_str=model,
        tools=tools,
        system_prompt=system_prompt,
    )
