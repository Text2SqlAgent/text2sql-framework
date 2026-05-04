"""Deep Agent — wraps LangChain's Deep Agents harness for agentic tool-calling loops.

Single-tier mode (default): one model handles the full agent loop.

Two-tier mode: when `model_heavy_str` is supplied, registers an `analyst`
subagent for narrative/interpretation rendering. The supervisor delegates to
it via the auto-generated `task` tool when a question demands business
commentary on top of raw SQL results.

A third tier (a Light schema_explorer subagent) was prototyped but removed
on 2026-05-04: cheap models reliably hallucinated schemas instead of running
the metadata queries we delegated to them, and the redundant exploration
ballooned latency and tokens. Schema discovery is fast enough at the
supervisor's tier; reintroduce a Light subagent only with a model that
actually tool-uses reliably under our prompts.

Anthropic prompt caching: when the user runs on direct Anthropic
(provider='anthropic'), this module registers a provider-wide HarnessProfile
that overrides deepagents' default 5-minute cache TTL with 1 hour. The
extended cache writes are 2x normal input cost but cache reads are 90% off,
so the break-even is ~2 hits per 1h window — comfortably reached by any
real session. Anthropic silently dropped the default TTL from 1h to 5m on
2026-03-06; explicit configuration is now required for the longer window.
For OpenRouter-routed Anthropic models, the deepagents middleware no-ops
(it only applies to ChatAnthropic instances), so this caching benefit
requires the direct provider — set ANTHROPIC_API_KEY and use
TEXT2SQL_MODEL=anthropic:claude-haiku-4.5.
"""

from __future__ import annotations

from deepagents import (
    HarnessProfile,
    create_deep_agent as _deepagents_create,
    register_harness_profile,
)
from langchain_anthropic.middleware import AnthropicPromptCachingMiddleware
from langchain_core.messages import HumanMessage

from text2sql.subagents import make_analyst


# ---------------------------------------------------------------------------
# Anthropic prompt-caching: register provider-wide HarnessProfile with TTL=1h
# ---------------------------------------------------------------------------
# deepagents auto-includes AnthropicPromptCachingMiddleware in the tail of
# every agent's middleware stack. Its default TTL is 5 minutes; we override
# to 1h here, which is the right setting for human-paced sessions where
# requests can be minutes apart. Registration is global / process-wide, so
# every TextSQL instance in this process picks it up automatically.
def _register_anthropic_extended_cache() -> None:
    profile = HarnessProfile(
        excluded_middleware={"AnthropicPromptCachingMiddleware"},
        extra_middleware=[AnthropicPromptCachingMiddleware(ttl="1h")],
    )
    register_harness_profile("anthropic", profile)


_register_anthropic_extended_cache()


def _get_chat_model(model_str: str):
    """Parse 'provider:model_name' and return a LangChain chat model.

    For OpenRouter, reasoning-token passthrough is disabled by default so
    chain-of-thought blobs from reasoning-tuned models (Grok 4.1 Fast, GPT-5,
    etc.) don't accumulate as opaque payload across tool-call iterations.
    Each call still gets the model's full capability — we just don't ship
    its private scratchpad between calls.
    """
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
    elif provider == "openrouter":
        from langchain_openrouter import ChatOpenRouter
        return ChatOpenRouter(
            model=model_name,
            reasoning={"enabled": False},
        )
    else:
        raise ValueError(f"Unsupported provider: {provider}")


class DeepAgent:
    """LangChain Deep Agents harness with text2sql tools and system prompt.

    Single-tier mode (default): one model handles the full agent loop.

    Two-tier mode: when `model_heavy_str` is supplied, registers the `analyst`
    subagent (no tools, pure reasoning) the supervisor can delegate to via
    the auto-generated `task` tool for narrative/interpretation rendering.

    The `model_light_str` parameter is accepted for backward compatibility
    but currently unused — the schema_explorer subagent it once configured
    was removed on 2026-05-04 (see module docstring).
    """

    def __init__(
        self,
        model_str: str,
        tools: list,
        system_prompt: str,
        model_light_str: str | None = None,  # deprecated; unused
        model_heavy_str: str | None = None,
    ):
        self.llm = _get_chat_model(model_str)
        self.system_prompt = system_prompt

        subagents: list[dict] = []
        if model_heavy_str:
            subagents.append(
                make_analyst(model=_get_chat_model(model_heavy_str))
            )

        self.subagent_names = [s["name"] for s in subagents]
        self.agent = _deepagents_create(
            model=self.llm,
            tools=tools,
            system_prompt=system_prompt,
            subagents=subagents,
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
    token_limit: int = 75_000,  # kept for backward compatibility
    model_light: str | None = None,  # deprecated; unused
    model_heavy: str | None = None,
) -> DeepAgent:
    """Create a Deep Agent with tools, a system prompt, and an optional analyst subagent."""
    return DeepAgent(
        model_str=model,
        tools=tools,
        system_prompt=system_prompt,
        model_light_str=model_light,
        model_heavy_str=model_heavy,
    )
