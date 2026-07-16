"""Native agentic tool-calling loop on raw provider SDKs.

This replaces the old LangChain/deepagents harness with a small, dependency-free
agent loop that talks directly to provider SDKs. Two backends are selected via
the existing ``"provider:model"`` string convention:

- ``anthropic:<model>`` → the raw ``anthropic`` SDK (with prompt caching on the
  system prompt, matching what deepagents provided before).
- ``openai:<model>``    → the raw ``openai`` SDK, honoring ``OPENAI_BASE_URL`` so
  any OpenAI-compatible endpoint works (OpenRouter, Ollama, vLLM, …).

Provider SDKs are imported lazily; a missing SDK raises a clear error naming the
extra to install. The old deepagents path is preserved in ``agent_langchain`` and
selectable via the ``langchain`` extra.

The loop returns a list of lightweight message objects (`HumanMessage`,
`AIMessage`, `ToolMessage`) whose attribute surface matches exactly what
``generate._parse_result`` reads duck-typed from LangChain messages
(``tool_calls``, ``content``, ``type``, ``response_metadata`` with ``timestamp``
and ``usage``; plus ``name``/``tool_call_id`` on tool messages), so the rest of
the framework is unchanged.
"""

from __future__ import annotations

import inspect
import json
import os
import time
from dataclasses import dataclass, field
from typing import Any, Callable, get_type_hints

# Matches the old deepagents recursion_limit.
MAX_ITERATIONS = 50


# ---------------------------------------------------------------------------
# Internal message representation
#
# Three separate dataclasses so that _parse_result's duck-typed detection works
# unchanged: only ToolMessage carries `name`/`tool_call_id`, only AIMessage
# carries `tool_calls`, and `type` distinguishes them.
# ---------------------------------------------------------------------------


@dataclass
class HumanMessage:
    content: str
    type: str = "human"
    response_metadata: dict = field(default_factory=dict)


@dataclass
class AIMessage:
    content: Any = ""
    tool_calls: list = field(default_factory=list)  # [{"id","name","args"}]
    type: str = "ai"
    response_metadata: dict = field(default_factory=dict)  # {"timestamp","usage"}


@dataclass
class ToolMessage:
    content: str
    name: str
    tool_call_id: str
    type: str = "tool"
    response_metadata: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Tool schema derivation from plain functions
# ---------------------------------------------------------------------------

_PY_TO_JSON = {
    str: "string",
    int: "integer",
    float: "number",
    bool: "boolean",
    list: "array",
    dict: "object",
}


def _function_to_schema(func: Callable) -> dict:
    """Derive a neutral JSON tool schema from a plain function's signature + docstring.

    Returns {"name", "description", "parameters"} where parameters is a JSON
    Schema object. Provider backends adapt this to their own tool format.
    """
    sig = inspect.signature(func)
    try:
        hints = get_type_hints(func)
    except Exception:
        hints = {}

    properties: dict = {}
    required: list = []
    for name, param in sig.parameters.items():
        if name in ("self", "cls"):
            continue
        if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
            continue
        annotation = hints.get(name, param.annotation)
        json_type = _PY_TO_JSON.get(annotation, "string")
        properties[name] = {"type": json_type}
        if param.default is inspect.Parameter.empty:
            required.append(name)

    return {
        "name": func.__name__,
        "description": (inspect.getdoc(func) or "").strip(),
        "parameters": {
            "type": "object",
            "properties": properties,
            "required": required,
        },
    }


# ---------------------------------------------------------------------------
# Context guard (replaces deepagents' summarization middleware)
# ---------------------------------------------------------------------------

_TRUNCATED_STUB = "[Earlier tool output truncated to fit the context window.]"


def _estimate_tokens(text: str) -> int:
    return len(text) // 4


def _apply_context_guard(messages: list, token_limit: int) -> None:
    """Keep the transcript under a rough token budget.

    A simple guard: if the estimated size exceeds ``token_limit``, truncate the
    oldest tool-result contents first (keeping the two most recent messages
    intact). Mutates messages in place. This is deliberately basic — deepagents'
    summarization middleware is gone; for very large schemas this trades some
    trace fidelity for staying under the window.
    """
    if not token_limit or token_limit <= 0:
        return

    def total() -> int:
        return sum(
            _estimate_tokens(m.content if isinstance(m.content, str) else str(m.content))
            for m in messages
        )

    if total() <= token_limit:
        return

    for msg in messages[:-2]:
        if (
            getattr(msg, "type", None) == "tool"
            and isinstance(msg.content, str)
            and msg.content != _TRUNCATED_STUB
            and len(msg.content) > len(_TRUNCATED_STUB)
        ):
            msg.content = _TRUNCATED_STUB
            if total() <= token_limit:
                return


# ---------------------------------------------------------------------------
# Provider backends
# ---------------------------------------------------------------------------


class _Backend:
    """Base backend. Subclasses convert the internal transcript to provider
    format, call the API, and return (text, tool_calls, usage)."""

    def __init__(self, model_name: str):
        self.model_name = model_name

    def generate(self, system_prompt: str, tool_schemas: list, messages: list):
        raise NotImplementedError


class _AnthropicBackend(_Backend):
    def __init__(self, model_name: str, max_tokens: int = 4096):
        super().__init__(model_name)
        try:
            import anthropic
        except ImportError as exc:  # pragma: no cover - exercised via message
            raise ImportError(
                "The 'anthropic' provider requires the anthropic SDK. "
                "Install it with: pip install 'text2sql-framework[anthropic]'"
            ) from exc
        self._anthropic = anthropic
        # Honor ANTHROPIC_BASE_URL if set (for gateways/proxies); otherwise default.
        self._client = anthropic.Anthropic()
        self.max_tokens = max_tokens

    def _tools_payload(self, tool_schemas: list) -> list:
        return [
            {
                "name": s["name"],
                "description": s["description"],
                "input_schema": s["parameters"],
            }
            for s in tool_schemas
        ]

    def _messages_payload(self, messages: list) -> list:
        payload: list = []
        pending_tool_results: list = []

        def _flush():
            nonlocal pending_tool_results
            if pending_tool_results:
                payload.append({"role": "user", "content": pending_tool_results})
                pending_tool_results = []

        for msg in messages:
            if msg.type == "tool":
                pending_tool_results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": msg.tool_call_id,
                        "content": msg.content,
                    }
                )
                continue

            _flush()
            if msg.type == "human":
                payload.append({"role": "user", "content": msg.content})
            elif msg.type == "ai":
                blocks: list = []
                text = _content_to_text(msg.content)
                if text.strip():
                    blocks.append({"type": "text", "text": text})
                for tc in msg.tool_calls:
                    blocks.append(
                        {
                            "type": "tool_use",
                            "id": tc["id"],
                            "name": tc["name"],
                            "input": tc["args"],
                        }
                    )
                if not blocks:
                    blocks.append({"type": "text", "text": "(no output)"})
                payload.append({"role": "assistant", "content": blocks})

        _flush()
        return payload

    def generate(self, system_prompt: str, tool_schemas: list, messages: list):
        system_blocks = [
            {
                "type": "text",
                "text": system_prompt,
                "cache_control": {"type": "ephemeral"},
            }
        ]
        resp = self._client.messages.create(
            model=self.model_name,
            max_tokens=self.max_tokens,
            system=system_blocks,
            messages=self._messages_payload(messages),
            tools=self._tools_payload(tool_schemas),
        )

        text_parts: list = []
        tool_calls: list = []
        for block in resp.content:
            if block.type == "text":
                text_parts.append(block.text)
            elif block.type == "tool_use":
                tool_calls.append(
                    {"id": block.id, "name": block.name, "args": dict(block.input)}
                )

        usage = {
            "input_tokens": getattr(resp.usage, "input_tokens", 0) or 0,
            "output_tokens": getattr(resp.usage, "output_tokens", 0) or 0,
        }
        return "".join(text_parts), tool_calls, usage


class _OpenAIBackend(_Backend):
    def __init__(self, model_name: str):
        super().__init__(model_name)
        try:
            import openai
        except ImportError as exc:  # pragma: no cover - exercised via message
            raise ImportError(
                "The 'openai' provider requires the openai SDK. "
                "Install it with: pip install 'text2sql-framework[openai]'"
            ) from exc
        self._openai = openai
        # Honor OPENAI_BASE_URL so OpenRouter / Ollama / vLLM / any OpenAI-compatible
        # endpoint works. The SDK also reads this env var itself, but we pass it
        # explicitly for clarity.
        base_url = os.environ.get("OPENAI_BASE_URL")
        self._client = openai.OpenAI(base_url=base_url) if base_url else openai.OpenAI()

    def _tools_payload(self, tool_schemas: list) -> list:
        return [
            {
                "type": "function",
                "function": {
                    "name": s["name"],
                    "description": s["description"],
                    "parameters": s["parameters"],
                },
            }
            for s in tool_schemas
        ]

    def _messages_payload(self, system_prompt: str, messages: list) -> list:
        payload: list = [{"role": "system", "content": system_prompt}]
        for msg in messages:
            if msg.type == "human":
                payload.append({"role": "user", "content": msg.content})
            elif msg.type == "ai":
                text = _content_to_text(msg.content)
                entry: dict = {"role": "assistant", "content": text or None}
                if msg.tool_calls:
                    entry["tool_calls"] = [
                        {
                            "id": tc["id"],
                            "type": "function",
                            "function": {
                                "name": tc["name"],
                                "arguments": json.dumps(tc["args"]),
                            },
                        }
                        for tc in msg.tool_calls
                    ]
                payload.append(entry)
            elif msg.type == "tool":
                payload.append(
                    {
                        "role": "tool",
                        "tool_call_id": msg.tool_call_id,
                        "content": msg.content,
                    }
                )
        return payload

    def generate(self, system_prompt: str, tool_schemas: list, messages: list):
        tools_payload = self._tools_payload(tool_schemas)
        resp = self._client.chat.completions.create(
            model=self.model_name,
            messages=self._messages_payload(system_prompt, messages),
            tools=tools_payload or None,
        )
        choice = resp.choices[0]
        message = choice.message
        text = message.content or ""

        tool_calls: list = []
        for tc in getattr(message, "tool_calls", None) or []:
            raw_args = getattr(tc.function, "arguments", "") or ""
            try:
                args = json.loads(raw_args) if raw_args else {}
            except (json.JSONDecodeError, TypeError):
                args = {}
            tool_calls.append({"id": tc.id, "name": tc.function.name, "args": args})

        usage = {}
        if getattr(resp, "usage", None):
            usage = {
                "input_tokens": getattr(resp.usage, "prompt_tokens", 0) or 0,
                "output_tokens": getattr(resp.usage, "completion_tokens", 0) or 0,
            }
        return text, tool_calls, usage


def _content_to_text(content: Any) -> str:
    """Coerce a message content (str or list of text blocks) to a plain string."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "".join(
            b.get("text", "") for b in content if isinstance(b, dict) and b.get("type") == "text"
        )
    return str(content)


def _get_backend(model_str: str) -> _Backend:
    """Parse 'provider:model_name' and return the matching native backend."""
    if ":" in model_str:
        provider, model_name = model_str.split(":", 1)
    else:
        provider, model_name = "anthropic", model_str

    provider = provider.lower()
    if provider == "anthropic":
        return _AnthropicBackend(model_name)
    if provider == "openai":
        return _OpenAIBackend(model_name)
    raise ValueError(
        f"Unsupported provider: {provider!r}. "
        "Native backends are 'anthropic' and 'openai'. For OpenAI-compatible "
        "endpoints (OpenRouter, Ollama, vLLM, ...), use 'openai:<model>' and set "
        "OPENAI_BASE_URL. To use the legacy deepagents backend, install the "
        "'langchain' extra and pass agent_backend='langchain'."
    )


# ---------------------------------------------------------------------------
# Agent
# ---------------------------------------------------------------------------


class NativeAgent:
    """A native tool-calling agent over the text2sql tools and system prompt.

    Mirrors the old DeepAgent interface: ``invoke({"messages": [...]})`` returns
    ``{"messages": [<message objects>]}``.
    """

    def __init__(
        self,
        model_str: str,
        tools: list,
        system_prompt: str,
        token_limit: int = 75_000,
    ):
        self.backend = _get_backend(model_str)
        self.tools = list(tools)
        self._tool_map = {t.__name__: t for t in self.tools}
        self._tool_schemas = [_function_to_schema(t) for t in self.tools]
        self.system_prompt = system_prompt
        self.token_limit = token_limit

    def _execute_tool(self, name: str, args: dict) -> str:
        func = self._tool_map.get(name)
        if func is None:
            return f"Unknown tool: {name}"
        try:
            return func(**(args or {}))
        except Exception as exc:  # surface tool errors to the model, don't crash
            return f"Tool error: {exc}"

    def invoke(self, input_dict: dict) -> dict:
        messages: list = []
        for msg in input_dict.get("messages", []):
            if msg.get("role") == "user":
                messages.append(HumanMessage(content=msg["content"]))
        if not messages:
            messages.append(HumanMessage(content=""))

        for _ in range(MAX_ITERATIONS):
            _apply_context_guard(messages, self.token_limit)
            text, tool_calls, usage = self.backend.generate(
                self.system_prompt, self._tool_schemas, messages
            )
            messages.append(
                AIMessage(
                    content=text,
                    tool_calls=tool_calls,
                    response_metadata={"timestamp": time.time(), "usage": usage},
                )
            )
            if not tool_calls:
                break
            for tc in tool_calls:
                output = self._execute_tool(tc["name"], tc.get("args", {}))
                messages.append(
                    ToolMessage(
                        content=output,
                        name=tc["name"],
                        tool_call_id=tc["id"],
                    )
                )

        return {"messages": messages}


def create_deep_agent(
    model: str,
    tools: list,
    system_prompt: str,
    token_limit: int = 75_000,
) -> NativeAgent:
    """Create the native agent. Signature kept for backward compatibility with the
    old deepagents factory (``token_limit`` now drives the context guard)."""
    return NativeAgent(
        model_str=model,
        tools=tools,
        system_prompt=system_prompt,
        token_limit=token_limit,
    )
