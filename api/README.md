# API

FastAPI wrapper around the `TextSQL` engine, with conversation history, data ingestion, and API key auth.

## Structure

```
api/
  engine.py          — TextSQL singleton shared across all routers
  auth.py            — API key dependency (X-API-Key header)
  db.py              — SQLAlchemy ORM models + session factory
  main.py            — App wiring: middleware, routers, lifespan
  jobs/
    ingest.py        — Pipeline execution + DB recording
  routers/
    health.py        — /health  /traces  /canonical
    ask.py           — /ask  /ask/csv
    conversations.py — /conversations
    ingest.py        — /ingest
  migrations/
    env.py
    versions/001_initial.py
```

## Install

```bash
uv pip install -e ".[api,anthropic,etl]"
```

## Configure

All configuration is via environment variables. Copy `.env.example` to `.env` and fill in:

| Variable | Required | Description |
|---|---|---|
| `TEXT2SQL_DB` | Yes | Read-only Postgres URL for the agent (gold schema) |
| `APP_DB_URL` | Yes | Full-access Postgres URL for app tables (conversations, pipeline_runs). Falls back to `TEXT2SQL_DB`. |
| `API_KEY` | Yes | Shared secret — clients pass this as `X-API-Key` header |
| `TEXT2SQL_MODEL` | No | Model string, e.g. `openrouter:anthropic/claude-haiku-4.5` (default: `anthropic:claude-sonnet-4-6`) |
| `TEXT2SQL_CANONICAL` | No | Path to `canonical.md` |
| `TEXT2SQL_SCENARIOS` | No | Path to `scenarios.md` |
| `TEXT2SQL_TRACES` | No | Path to traces JSONL output file |
| `TEXT2SQL_METADATA_HINT` | No | Free-form hint injected into the agent system prompt |
| `TEXT2SQL_ENFORCE_RO` | No | `true` to fail fast if the DB connection is writable (default: `true`) |
| `TEXT2SQL_CORS_ORIGINS` | No | Comma-separated allowed origins (default: `*`) |
| `ANTHROPIC_API_KEY` | — | Required when using Anthropic models directly |
| `OPENROUTER_API_KEY` | — | Required when using OpenRouter models |

### Minimal `.env` for local dev

```bash
TEXT2SQL_DB=postgresql+psycopg://penicor:penicor@localhost:5433/penicor?options=-csearch_path%3Dgold
APP_DB_URL=postgresql+psycopg://penicor:penicor@localhost:5433/penicor
API_KEY=changeme
TEXT2SQL_MODEL=openrouter:anthropic/claude-haiku-4.5
OPENROUTER_API_KEY=sk-or-...
```

## Run

```bash
# 1. Start local Postgres
docker compose up -d postgres

# 2. Run migrations (once, or after schema changes)
uv run alembic upgrade head

# 3. Start the API
uv run uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

| URL | Description |
|---|---|
| `http://localhost:8000/docs` | Swagger UI — interactive, try endpoints directly |
| `http://localhost:8000/redoc` | ReDoc — clean read-only reference |
| `http://localhost:8000/openapi.json` | Raw OpenAPI schema |

---

## Endpoints

All endpoints except `/health`, `/traces`, and `/canonical` require the header:
```
X-API-Key: <API_KEY>
```

### Utility (no auth)

#### `GET /health`
```json
{"status": "ok", "db": "ok", "canonical_count": 7, "tracer_enabled": true}
```

#### `GET /traces?limit=20`
Recent query traces, newest first. Only populated when `TEXT2SQL_TRACES` is set.

#### `GET /canonical`
List configured canonical queries — useful for "Suggested questions" in the UI.

---

### Ask

#### `POST /ask`

Ask a natural-language question. Returns SQL, result rows, and metadata.
Optionally continues an existing conversation by passing `conversation_id`.

**Request**
```json
{
  "question": "Top 5 customers by revenue this month",
  "conversation_id": "uuid-or-null",
  "user_id": "alice@acme.com",
  "user_role": "finance_manager",
  "max_rows": 200
}
```

**Response**
```json
{
  "conversation_id": "3fa85f64-...",
  "message_id": "7cb12a00-...",
  "question": "Top 5 customers by revenue this month",
  "sql": "SELECT customer_name, SUM(net_revenue) ...",
  "columns": ["customer_name", "net_revenue"],
  "data": [{"customer_name": "Acme", "net_revenue": 14302.50}],
  "row_count": 5,
  "truncated": false,
  "error": null,
  "commentary": "...",
  "canonical_query": null,
  "canonical_score": null,
  "tool_calls_made": 4,
  "iterations": 4,
  "duration_seconds": 12.3,
  "input_tokens": 3200,
  "output_tokens": 410
}
```

When a canonical query matches, `tool_calls_made` is `0` and `duration_seconds` is under `0.1s`.

#### `POST /ask/csv`
Same input as `/ask`, returns a CSV file download instead of JSON.

---

### Conversations

#### `POST /conversations`
Create a new conversation explicitly (optional — `/ask` creates one automatically).
```json
{"user_id": "alice@acme.com", "user_role": "finance_manager", "title": "Monthly review"}
```

#### `GET /conversations?user_id=alice@acme.com&limit=50`
List conversations, newest first. Filter by `user_id` to scope to one user.

#### `GET /conversations/{conversation_id}`
Full conversation with all messages in chronological order.

---

### Ingest

Runs the bronze → silver → gold ETL pipeline for a given date window.

#### `POST /ingest`
```json
{
  "start_date": "2025-01-01",
  "end_date":   "2025-01-31",
  "layers": ["bronze", "silver", "gold"]
}
```

**Response**
```json
{
  "run_id": "...",
  "status": "success",
  "bronze_rows": 4821,
  "silver_rows": 12043,
  "gold_views": 15,
  "duration_seconds": 38.2,
  "errors": []
}
```

#### `GET /ingest?limit=20`
List recent pipeline runs, newest first.

#### `GET /ingest/{run_id}`
Get a single run's status and row counts.
