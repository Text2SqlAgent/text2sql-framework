# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

**Install (dev):**
```bash
uv pip install -e ".[all,dev]"
```

**Tests:**
```bash
uv run pytest                                          # all tests
uv run pytest tests/test_core.py                       # single file
uv run pytest tests/test_core.py::TestDatabase         # single class
uv run pytest tests/test_core.py::TestDatabase::test_connection  # single test
```

Most tests in `tests/test_core.py` run without an LLM. Tests in `tests/test_canonical.py`, `tests/test_business_features.py`, etc. may require `ANTHROPIC_API_KEY` or `OPENROUTER_API_KEY`.

**CLI:**
```bash
uv run text2sql ask "sqlite:///mydb.db"                # interactive
uv run text2sql query "sqlite:///mydb.db" "Top 5 customers"
```

**FastAPI server:**
```bash
TEXT2SQL_DB="postgresql://..." uvicorn api.main:app --reload --port 8000
```
Full env var list is in the docstring at the top of [api/main.py](api/main.py).

**Local Postgres (ETL dev):**
```bash
docker compose up -d postgres    # starts on localhost:5433
docker compose down              # stop (data persists)
docker compose down -v           # stop + wipe
```

**LangGraph Studio** (visual agent debugger):
```bash
langgraph dev   # reads langgraph.json + studio_graph.py; opens browser UI
```
Requires `pip install "text2sql[studio]"`. Studio bypasses canonical-query routing and JSONL tracing — it drives the raw LangGraph directly for debugging the agent loop.

## Architecture

### Two query paths

`TextSQL.ask()` in [text2sql/core.py](text2sql/core.py) routes every question through one of two paths:

1. **Canonical path** — if `canonical_queries=` is set and a question's keywords match a vetted SQL template in that file above the threshold (default 0.6), the template runs directly with no LLM. Fast, deterministic, zero token cost. Matching is token-overlap against query name + aliases (see [text2sql/canonical.py](text2sql/canonical.py)).

2. **Agent path** — the Deep Agents harness gives the LLM two tools (`execute_sql`, `lookup_example`) and lets it explore the schema, write SQL, test it, and self-correct in a loop. No pre-computed schema context is injected — the agent discovers what it needs. This is the core design philosophy: remove constraints, get capability back.

### Core modules

| Module | Responsibility |
|---|---|
| [text2sql/core.py](text2sql/core.py) | `TextSQL` — public API, wires everything together |
| [text2sql/generate.py](text2sql/generate.py) | `SQLGenerator` — builds the system prompt, runs the agent, parses the final SQL from the response |
| [text2sql/agent.py](text2sql/agent.py) | `DeepAgent` — thin wrapper around `deepagents.create_deep_agent`; parses `provider:model` strings |
| [text2sql/tools.py](text2sql/tools.py) | `execute_sql` + `lookup_example` LangChain tools, bound to a `Database` instance |
| [text2sql/connection.py](text2sql/connection.py) | `Database` — SQLAlchemy wrapper, dialect detection, read-only enforcement |
| [text2sql/dialects.py](text2sql/dialects.py) | Per-dialect system-prompt guidance for schema exploration (information_schema vs PRAGMA vs sys.tables) |
| [text2sql/examples.py](text2sql/examples.py) | `ExampleStore` — loads `scenarios.md`; keyword-overlap lookup for `lookup_example` tool |
| [text2sql/canonical.py](text2sql/canonical.py) | `CanonicalQueryStore` — loads `canonical.md`; token-overlap matcher with diacritic normalization |
| [text2sql/tracing.py](text2sql/tracing.py) | `Tracer` — records every tool call, token usage, timing, and SQL errors to JSONL |
| [text2sql/analyze.py](text2sql/analyze.py) | `AnalysisEngine` — deterministic trace analysis; produces schema rename + scenario suggestions |

### System prompt variants

`SQLGenerator._build_system_prompt()` in [text2sql/generate.py](text2sql/generate.py) produces two different prompts:
- **Default** (`SYSTEM_PROMPT`): agent explores schema dynamically — includes dialect-specific metadata query guidance.
- **Fixed schema** (`SYSTEM_PROMPT_FIXED_SCHEMA`): caller passes the schema as a string; agent skips exploration, jumps straight to writing SQL. Activated by passing `schema=` to `TextSQL()`.

### Scenarios vs canonical queries

Both are markdown files with `## heading` sections, but they serve different purposes:

- **`scenarios.md`** (`examples=` param): business-concept guidance the agent retrieves *during* its loop via `lookup_example`. The agent decides when to call it. Each heading is a domain concept (e.g., "net revenue", "customer home address"). See [examples/scenarios.md](examples/scenarios.md).

- **`canonical.md`** (`canonical_queries=` param): vetted SQL templates that *short-circuit* the agent entirely. Each heading is a query name with `aliases:`, optional `description:`, and a ```sql block. See [examples/canonical.md](examples/canonical.md).

### Model strings

Format: `provider:model_name`. Examples: `anthropic:claude-sonnet-4-6`, `openai:gpt-4o`, `openrouter:anthropic/claude-haiku-4.5`. Default is `anthropic:claude-sonnet-4-6`.

### ETL pipeline (`etl/`)

Customer-specific pipeline: AltoControl (Azure SQL Server) → bronze (raw copy) → silver (cleaned) → gold (views the text2sql agent reads). Requires `pip install "text2sql[etl]"` and credentials in `.env` (see [.env.example](.env.example)).

### FastAPI layer (`api/`)

HTTP wrapper around `TextSQL.ask()`. All configuration is via environment variables — no config files. Endpoints: `POST /ask` (JSON), `POST /ask/csv` (file download), `GET /traces`, `GET /canonical`, `GET /health`.

### Read-only enforcement

`execute_sql` blocks destructive SQL via a regex allowlist (`SELECT`, `WITH`, `EXPLAIN`, `SHOW`, `DESCRIBE`, `PRAGMA`). For production, also connect with a DB-level read-only user and pass `enforce_read_only=True` to `TextSQL()` to fail fast on writable connections.
