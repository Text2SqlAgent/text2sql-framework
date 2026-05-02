# Technical Architecture Plan

## Overview

This plan extends the existing FastAPI application with three integrated systems. All database state lives in the same **PostgreSQL instance** the ETL already uses (or a dedicated app DB alongside it). Migrations are managed by **Alembic**. The three systems share a single SQLAlchemy async session factory.

---

## System 1 — Conversation Management

### Database Schema

```sql
-- migrations/versions/001_conversations.py

CREATE TABLE conversations (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    title       TEXT,                    -- auto-set from first question
    user_id     TEXT,                    -- optional, mirrors existing user_id field
    user_role   TEXT,
    metadata    JSONB NOT NULL DEFAULT '{}'
);

CREATE TABLE messages (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    conversation_id   UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    role              TEXT NOT NULL CHECK (role IN ('user', 'assistant')),
    content           TEXT NOT NULL,     -- user question or assistant commentary
    sql_generated     TEXT,             -- null for user messages
    row_count         INT,
    tool_calls_made   INT,
    iterations        INT,
    input_tokens      INT,
    output_tokens     INT,
    duration_seconds  FLOAT,
    is_canonical      BOOLEAN DEFAULT FALSE,
    error             TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_messages_conversation_id ON messages(conversation_id);
CREATE INDEX idx_conversations_user_id ON conversations(user_id);
```

### Migration Strategy

Use **Alembic** in `api/migrations/`. One revision per logical change. The `env.py` reads `DATABASE_URL` from the environment — same var used by the app. The `alembic upgrade head` command runs at container startup before uvicorn. Never use `autogenerate` in production; always review the generated script before committing.

```
api/
  migrations/
    env.py
    versions/
      001_conversations.py
      002_api_keys.py
```

### API Endpoints

**Create a conversation**
```
POST /conversations
Response: { id, created_at, title }
```

**List conversations** (for sidebar)
```
GET /conversations?user_id=&limit=50&offset=0
Response: [{ id, title, created_at, message_count }]
```

**Get conversation with messages**
```
GET /conversations/{conversation_id}
Response: { id, title, messages: [...] }
```

**Ask within a conversation** — the key endpoint change:
```
POST /ask
Body: {
  question: str,
  conversation_id: str | null,   # null = start new conversation
  user_id: str | null,
  user_role: str | null,
  max_rows: int = 200
}
Response: {
  conversation_id: str,          # always returned, even for new ones
  message_id: str,
  ...existing AskResponse fields
}
```

When `conversation_id` is provided, the handler:
1. Loads the last N messages from that conversation (cap at ~10 pairs to stay within context).
2. Serializes them as `[{"role": "user", "content": q}, {"role": "assistant", "content": sql + commentary}]`.
3. Passes the history list into `TextSQL.ask()` (requires a small extension — see below).
4. Persists both the user message and assistant message rows.

### Required Core Extension

`TextSQL.ask()` needs one new parameter:

```python
def ask(
    self,
    question: str,
    *,
    message_history: list[dict] | None = None,   # NEW
    user_id: str | None = None,
    user_role: str | None = None,
) -> SQLResult: ...
```

`SQLGenerator` threads `message_history` into the Deep Agents call. The Deep Agents SDK accepts prior messages for multi-turn context. If it doesn't natively, prepend history as a formatted block at the top of the question string — deterministic, no SDK changes needed.

---

## System 2 — API Key Authentication

### Database Schema

```sql
-- migrations/versions/002_api_keys.py

CREATE TABLE api_keys (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name         TEXT NOT NULL,                -- human label, e.g. "frontend-prod"
    key_hash     TEXT NOT NULL UNIQUE,         -- SHA-256 hex of the raw key
    key_prefix   TEXT NOT NULL,                -- first 8 chars of raw key, for UI listing
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used_at TIMESTAMPTZ,
    expires_at   TIMESTAMPTZ,                  -- null = no expiry
    is_active    BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX idx_api_keys_hash ON api_keys(key_hash);
```

### Key Design

- Raw key format: `t2s_` + 32 random URL-safe bytes (base64url). Example: `t2s_abcDEF123...`
- The prefix `t2s_` makes keys greppable in logs and easy to identify in secrets scanners.
- Only `key_hash = SHA-256(raw_key)` is stored. The raw key is shown **once** at creation time and never stored.
- `key_prefix` (first 8 chars of the raw key) is stored for display in key-management UI without exposing the full secret.

### Admin Endpoints

These endpoints are protected by a separate `ADMIN_SECRET` env var (simple bearer token), not by the API key table itself. This bootstraps the system without a chicken-and-egg problem.

```
POST /admin/keys
Body:  { name: str, expires_at: datetime | null }
Response: { id, name, key_prefix, raw_key, expires_at }
# raw_key shown only here, never again

GET /admin/keys
Response: [{ id, name, key_prefix, created_at, last_used_at, expires_at, is_active }]

DELETE /admin/keys/{id}
# sets is_active = false (soft delete); hard delete after rotation confirmed
```

### Authentication Middleware

FastAPI dependency injected into all protected routes:

```python
# api/auth.py

async def require_api_key(
    x_api_key: str = Header(..., alias="X-API-Key"),
    db: AsyncSession = Depends(get_db),
) -> ApiKey:
    key_hash = hashlib.sha256(x_api_key.encode()).hexdigest()
    row = await db.scalar(
        select(ApiKey).where(
            ApiKey.key_hash == key_hash,
            ApiKey.is_active == True,
            or_(ApiKey.expires_at.is_(None), ApiKey.expires_at > func.now()),
        )
    )
    if row is None:
        raise HTTPException(status_code=401, detail="Invalid or expired API key")
    # fire-and-forget last_used_at update (best-effort, no await needed)
    await db.execute(
        update(ApiKey).where(ApiKey.id == row.id).values(last_used_at=func.now())
    )
    return row
```

Apply to routes via:
```python
router = APIRouter(dependencies=[Depends(require_api_key)])
```

### Key Rotation Flow

1. Call `POST /admin/keys` with a new name — get `raw_key`.
2. Update the secret in the consuming service.
3. Verify the new key works (check `last_used_at` updates).
4. Call `DELETE /admin/keys/{old_id}` to deactivate the old key.

No downtime, no hard delete until confirmed. Keep old keys in the table for audit history (mark `is_active=false`).

---

## System 3 — Parameterized Data Ingestion Pipeline

### Design Philosophy

A single entry point `run_pipeline(start_date, end_date)` orchestrates all three layers sequentially. Gold layer views are always fully refreshed (they're stateless SQL views, not incremental). Bronze and silver are date-range filtered.

### Pipeline Module

```
etl/
  pipeline.py          # NEW: single-entry orchestrator
  extract.py           # renamed from extract_altocontrol.py (or kept as-is)
  load_bronze.py       # renamed from load_altocontrol_bronze.py
  transform_silver.py  # renamed from transform_altocontrol_silver.py
  apply_gold.py        # renamed from apply_altocontrol_gold.py
```

`etl/pipeline.py` core signature:

```python
@dataclass
class PipelineResult:
    bronze_rows:   int
    silver_rows:   int
    gold_views:    int
    started_at:    datetime
    finished_at:   datetime
    errors:        list[str]

def run_pipeline(
    start_date: date,
    end_date:   date,
    layers:     list[Literal["bronze", "silver", "gold"]] = ["bronze", "silver", "gold"],
    source_conn_str: str | None = None,    # overrides env var
    target_conn_str: str | None = None,
) -> PipelineResult:
    result = PipelineResult(...)

    if "bronze" in layers:
        # 1. Extract from source (Azure SQL Server) filtered by date range
        # 2. Upsert into raw_* tables by source_record_id
        result.bronze_rows = load_bronze(start_date, end_date, ...)

    if "silver" in layers:
        # 3. Transform bronze rows created/updated in this window into silver
        result.silver_rows = transform_silver(start_date, end_date, ...)

    if "gold" in layers:
        # 4. DROP + CREATE gold views (idempotent, no date filter needed)
        result.gold_views = apply_gold(...)

    return result
```

Each layer function receives explicit `start_date`/`end_date` so they can scope their queries. The date filter is applied consistently:
- **Bronze extract**: `WHERE modified_date >= start_date AND modified_date < end_date` on the source system.
- **Silver transform**: `WHERE ingested_at >= start_date AND ingested_at < end_date + 1 day` on bronze tables (catches records just ingested).
- **Gold**: no filter — views recalculate on every query, so a full refresh is just re-running the `CREATE OR REPLACE VIEW` statements.

### Pipeline Tracking Table

```sql
CREATE TABLE pipeline_runs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    started_at      TIMESTAMPTZ NOT NULL,
    finished_at     TIMESTAMPTZ,
    start_date      DATE NOT NULL,
    end_date        DATE NOT NULL,
    layers          TEXT[] NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    bronze_rows     INT,
    silver_rows     INT,
    gold_views      INT,
    error_detail    TEXT,
    triggered_by    TEXT           -- 'api', 'cli', 'cron'
);
```

### API Endpoint

```
POST /ingest
Headers: X-API-Key, X-Admin-Secret (either accepted — ingest is privileged)
Body: {
  start_date: "2025-01-01",    # ISO date string
  end_date:   "2025-01-31",
  layers:     ["bronze", "silver", "gold"]  # optional, defaults to all three
}
Response (sync, runs to completion): {
  run_id:           str,
  status:           "success" | "failed",
  bronze_rows:      int,
  silver_rows:      int,
  gold_views:       int,
  duration_seconds: float,
  errors:           []
}
```

For long-running ingestions, consider making this **async** (return `run_id` immediately, poll `GET /ingest/{run_id}`). For the first implementation, synchronous with a 600-second uvicorn timeout is fine — add async later if needed.

```
GET /ingest/{run_id}
Response: pipeline_runs row as JSON

GET /ingest?limit=20
Response: recent pipeline_runs, newest first
```

---

## Integration Points

### Shared Infrastructure

```
api/
  db.py          # async SQLAlchemy engine + session factory (shared by all routers)
  auth.py        # require_api_key dependency
  routers/
    ask.py       # /ask, /ask/csv (existing, extended with conversation_id)
    conversations.py
    admin.py     # /admin/keys
    ingest.py    # /ingest
    traces.py    # existing /traces
    canonical.py # existing /canonical
  migrations/
    env.py
    versions/
```

### Startup Sequence

```python
# api/main.py lifespan
@asynccontextmanager
async def lifespan(app: FastAPI):
    await run_migrations()          # alembic upgrade head
    await init_text2sql_engine()    # existing engine init
    yield
    await close_db()
```

### Environment Variables (complete set)

```
# Existing
TEXT2SQL_DB          # target DB (also used by app DB if same Postgres)
TEXT2SQL_MODEL
TEXT2SQL_CANONICAL
TEXT2SQL_EXAMPLES
TEXT2SQL_TRACES_DIR
TEXT2SQL_MAX_ROWS

# New
APP_DB_URL           # SQLAlchemy async URL for conversations/keys/pipeline_runs
                     # e.g. postgresql+asyncpg://user:pass@host:5432/appdb
ADMIN_SECRET         # bearer token for /admin/* routes
SOURCE_DB_URL        # Azure SQL Server DSN for ETL extract (existing, in .env)
```

If `APP_DB_URL` is not set, fall back to `TEXT2SQL_DB` (same database, separate schema or just separate tables — fine for most deployments).

---

## Implementation Order

1. **Alembic setup** — `alembic init api/migrations`, wire `env.py` to `APP_DB_URL`, write revision 001 (conversations + messages) and 002 (api_keys + pipeline_runs).
2. **Auth** — `api/auth.py` + `api/routers/admin.py` with key CRUD. Test with curl before touching any other route.
3. **Conversation persistence** — add `conversation_id` to `AskRequest`, extend `TextSQL.ask()` with `message_history`, write `api/routers/conversations.py`.
4. **ETL parameterization** — refactor existing scripts into `etl/pipeline.py` with the unified signature, add `api/routers/ingest.py`.
5. **Wire auth** to `/ask` and `/ingest` routes last, after everything else passes tests.

---

## Key Decisions & Tradeoffs

| Decision | Choice | Alternative considered |
|---|---|---|
| Conversation history delivery | Prepend serialized history to question if SDK doesn't support multi-turn natively | Modify Deep Agents SDK |
| Key storage | SHA-256 hash only | bcrypt — overkill for 32-byte random keys, adds latency |
| ETL sync vs async endpoint | Sync first, promote to async later | Background tasks from day one — adds complexity before proven need |
| Single `APP_DB_URL` for all app tables | Yes — simpler ops | Separate DBs per concern — unnecessary for this scale |
| Gold layer parameterization | No date filter (views are stateless) | Materialized views with incremental refresh — significant added complexity |
