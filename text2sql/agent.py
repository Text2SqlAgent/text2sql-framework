"""Deep Agent — wraps LangGraph's create_react_agent for agentic tool-calling loops.

Uses LangGraph's built-in agent infrastructure which handles:
- Multi-turn tool calling loop
- Message management
- Configurable recursion limits
- Context compaction when tool calls exceed 75k tokens
"""

from __future__ import annotations

from langchain_core.messages import HumanMessage, SystemMessage, trim_messages
from langgraph.prebuilt import create_react_agent

TOKEN_LIMIT = 75_000


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


def _make_compaction_hook(token_limit: int):
    """Create a pre_model_hook that trims messages when they exceed the token limit."""

    def compact_messages(state: dict) -> dict:
        messages = state.get("messages", [])
        trimmed = trim_messages(
            messages,
            max_tokens=token_limit,
            token_counter="approximate",
            strategy="last",
            include_system=True,
            start_on="human",
        )
        return {"messages": trimmed}

    return compact_messages


class DeepAgent:
    """A LangGraph-backed ReAct agent with tool calling and context compaction."""

    def __init__(
        self,
        model_str: str,
        tools: list,
        system_prompt: str,
        token_limit: int = TOKEN_LIMIT,
    ):
        self.llm = _get_chat_model(model_str)
        self.system_prompt = system_prompt

        self.agent = create_react_agent(
            model=self.llm,
            tools=tools,
            prompt=SystemMessage(content=system_prompt),
            pre_model_hook=_make_compaction_hook(token_limit),
        )

    def invoke(self, input_dict: dict) -> dict:
        """Run the agent. Input: {"messages": [{"role": "user", "content": "..."}]}"""
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
    token_limit: int = TOKEN_LIMIT,
) -> DeepAgent:
    """Create a Deep Agent with tools and a system prompt."""
    return DeepAgent(
        model_str=model,
        tools=tools,
        system_prompt=system_prompt,
        token_limit=token_limit,
    )
