# Chat API

FastAPI wrapper around the `TextSQL` engine. Backend for the chat UI.

## Install

```bash
pip install -e ".[api,anthropic]"
```

## Configure

Set environment variables (or use a `.env`):

| Variable | Purpose | Example |
|---|---|---|
| `TEXT2SQL_DB` | Connection string. Use `agent_reader` for production. | `postgresql://agent_reader:pwd@localhost/customer_demo?options=-csearch_path%3Dgold` |
| `TEXT2SQL_MODEL` | LangChain model spec | `anthropic:claude-sonnet-4-6` (default) |
| `TEXT2SQL_CANONICAL` | Canonical queries markdown | `examples/canonical.md` |
| `TEXT2SQL_SCENARIOS` | Scenarios markdown | `examples/scenarios.md` |
| `TEXT2SQL_TRACES` | JSONL output for traces | `traces/api.jsonl` |
| `TEXT2SQL_METADATA_HINT` | Free-form context for the agent | `Base currency PEN; fiscal year starts Jan; tz America/Lima` |
| `TEXT2SQL_ENFORCE_RO` | Fail if DB role can write | `true` (default) |
| `TEXT2SQL_CORS_ORIGINS` | Comma-separated CORS origins | `http://localhost:3000` |
| `ANTHROPIC_API_KEY` | For Anthropic models | `sk-ant-...` |

## Run

```bash
uvicorn api.main:app --reload --port 8000
```

OpenAPI docs at `http://localhost:8000/docs` once running.

## Endpoints

### `POST /ask`

Ask a natural-language question and get the rendered SQL + data back.

**Request**
```json
{
  "question": "How much are we owed?",
  "user_id": "alice@acme.com",
  "user_role": "finance_manager",
  "session_id": "sess-abc-123",
  "max_rows": 200
}
```

**Response**
```json
{
  "question": "How much are we owed?",
  "sql": "SELECT * FROM gold.v_ar_aging ORDER BY total_owed DESC",
  "columns": ["customer_name","not_yet_due","overdue_1_30","overdue_31_60","overdue_60_plus","total_owed","currency_code"],
  "data": [
    {"customer_name":"Acme Logistics","total_owed":14302.50, "...":"..."}
  ],
  "row_count": 18,
  "truncated": false,
  "error": null,
  "commentary": "[canonical:ar_aging score=1.00] AR aging by customer with 30/60/90+ buckets.",
  "canonical_query": "ar_aging",
  "canonical_score": 1.0,
  "tool_calls_made": 0,
  "iterations": 0,
  "duration_seconds": 0.04,
  "input_tokens": 0,
  "output_tokens": 0
}
```

When the agent runs (canonical miss), `canonical_query` is null,
`tool_calls_made` is the LLM's tool-call count, and `duration_seconds`
includes the agent loop (typically 5–30s on cold cache).

### `POST /ask/csv`

Same input as `/ask`, but returns a CSV file for download. Useful for
"generate an expense report" flows where the user wants an artifact.

### `GET /traces?limit=20`

Recent query traces (most recent first). For an admin/audit view in the
UI. Only populated when `TEXT2SQL_TRACES` is set.

### `GET /canonical`

List configured canonical queries — useful for showing "Suggested
questions" in the UI.

### `GET /health`

```json
{"status": "ok", "db": "ok", "canonical_count": 7, "tracer_enabled": true}
```

## Testing without a database

For UI development before Postgres is set up, the design brief
(`UI_DESIGN_BRIEF.md`) includes mock JSON responses you can wire to a
mocked fetch on the frontend.
