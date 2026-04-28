"""penicor_demo.py — quick smoke test for text2sql against Penicor's local DB.

Prereqs:
  - docker compose up -d postgres   (Postgres reachable)
  - bronze + silver + gold loaded   (extract -> bronze -> silver -> gold)
  - .env has OPENROUTER_API_KEY (or ANTHROPIC_API_KEY) set
  - text2sql installed with the matching provider extra (managed via uv):
      uv pip install -e ".[etl,openrouter]"  # OpenRouter (recommended)
      uv pip install -e ".[etl,anthropic]"   # direct Anthropic
      uv pip install -e ".[etl,openai]"      # direct OpenAI
      uv pip install -e ".[all]"             # everything

Usage:
  python examples/penicor_demo.py
  python examples/penicor_demo.py --question "¿Cuánto nos deben?"
  python examples/penicor_demo.py --model anthropic/claude-haiku-4.5

LLM provider routing
--------------------
Defaults to provider=openrouter when OPENROUTER_API_KEY is set, otherwise
provider=anthropic. Override with --provider {openrouter|anthropic|openai}.
Model names follow each provider's catalog:
  - openrouter:  anthropic/claude-sonnet-4.6, openai/gpt-4o, …
  - anthropic:   claude-sonnet-4-6, claude-haiku-4-5, …
  - openai:      gpt-4o, gpt-4o-mini, …

The default questions exercise both the canonical-query intercept (fast,
$0, deterministic SQL) and the agent path (longer, costs API tokens, but
handles novel questions).
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

DEFAULT_QUESTIONS = [
    # Should hit canonicals
    "¿Cuánto nos deben?",
    "Top 10 clientes este año",
    "Productos más vendidos",
    # Should fall through to the agent
    "¿Cuántos clientes activos tenemos en Montevideo?",
]


def main() -> int:
    load_env()

    # Default provider follows whichever API key is present in env.
    # Default model = the cheapest sane choice per provider (good for POC iteration);
    # override with --model or set PENICOR_MODEL in .env for a session-wide override.
    env_model = os.environ.get("PENICOR_MODEL")
    if os.environ.get("OPENROUTER_API_KEY"):
        default_provider = "openrouter"
        default_model = env_model or "anthropic/claude-haiku-4.5"
    elif os.environ.get("ANTHROPIC_API_KEY"):
        default_provider = "anthropic"
        default_model = env_model or "claude-haiku-4-5"
    elif os.environ.get("OPENAI_API_KEY"):
        default_provider = "openai"
        default_model = env_model or "gpt-4o-mini"
    else:
        default_provider = "openrouter"
        default_model = env_model or "anthropic/claude-haiku-4.5"

    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--question", "-q", action="append",
                    help="Ask a specific question (can be repeated). Default: a 4-question smoke test.")
    ap.add_argument("--model", default=default_model,
                    help=f"Model name in the chosen provider's catalog (default: {default_model!r}).")
    ap.add_argument("--provider", default=default_provider,
                    choices=("openrouter", "anthropic", "openai"),
                    help=f"LLM provider passed to text2sql (default: {default_provider!r})")
    ap.add_argument("--connection-string", "-c",
                    default=os.environ.get("PENICOR_DB_URL"),
                    help="Postgres URL (default: $PENICOR_DB_URL)")
    ap.add_argument("--canonical", type=Path,
                    default=REPO_ROOT / "examples" / "penicor_canonical.md",
                    help="Path to canonical queries file")
    ap.add_argument("--trace-file", type=Path,
                    default=REPO_ROOT / "traces" / "penicor.jsonl",
                    help="Where to write the trace JSONL")
    args = ap.parse_args()

    required_env = {
        "openrouter": "OPENROUTER_API_KEY",
        "anthropic":  "ANTHROPIC_API_KEY",
        "openai":     "OPENAI_API_KEY",
    }[args.provider]
    if not args.connection_string:
        print("[demo] PENICOR_DB_URL not set in .env (or pass --connection-string)")
        return 1
    if not os.environ.get(required_env):
        print(f"[demo] {required_env} not set in .env (required for provider={args.provider!r})")
        return 1

    try:
        from text2sql import TextSQL  # type: ignore
    except ImportError as e:
        print(f"[demo] text2sql not installed: {e}")
        print('Install (pick the matching extra):')
        print('  uv pip install -e ".[etl,openrouter]"   # OpenRouter')
        print('  uv pip install -e ".[etl,anthropic]"    # direct Anthropic')
        print('  uv pip install -e ".[etl,openai]"       # direct OpenAI')
        print('  uv pip install -e ".[all]"              # everything')
        return 2

    args.trace_file.parent.mkdir(parents=True, exist_ok=True)

    print(f"[demo] connecting via {redact(args.connection_string)}")
    print(f"[demo] model    = {args.provider}:{args.model}")
    print(f"[demo] canonical = {args.canonical.relative_to(REPO_ROOT)}")
    print()

    engine = TextSQL(
        connection_string=args.connection_string,
        model=f"{args.provider}:{args.model}",
        canonical_queries=str(args.canonical),
        trace_file=str(args.trace_file),
    )

    questions = args.question or DEFAULT_QUESTIONS
    for i, q in enumerate(questions, 1):
        print(f"=== Q{i}: {q}")
        try:
            result = engine.ask(
                q,
                user_id="rodrigo@penicor",
                user_role="admin",
                metadata={"tenant": "penicor", "session": "demo"},
            )
        except Exception as e:  # noqa: BLE001
            print(f"    !! ERROR: {e}")
            print()
            continue

        print(f"--- SQL")
        print(textwrap_indent(result.sql, "    "))
        if result.commentary:
            print(f"--- commentary: {result.commentary}")
        print(f"--- rows: {len(result.data)}")
        for row in result.data[:5]:
            print(f"    {row}")
        if len(result.data) > 5:
            print(f"    ... and {len(result.data) - 5} more")
        print()

    print(f"[demo] traces -> {args.trace_file.relative_to(REPO_ROOT)}")
    return 0


def load_env() -> None:
    try:
        from dotenv import load_dotenv  # type: ignore
        env_path = REPO_ROOT / ".env"
        if env_path.exists():
            load_dotenv(env_path)
    except ImportError:
        pass


def redact(url: str) -> str:
    if "://" not in url or "@" not in url:
        return url
    scheme, rest = url.split("://", 1)
    if "@" not in rest:
        return url
    creds, host = rest.rsplit("@", 1)
    if ":" not in creds:
        return url
    user, _ = creds.split(":", 1)
    return f"{scheme}://{user}:***@{host}"


def textwrap_indent(text: str, prefix: str) -> str:
    return "\n".join(prefix + line for line in text.splitlines())


if __name__ == "__main__":
    sys.exit(main())
