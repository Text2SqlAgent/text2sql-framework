# Ideal database design for the NL-to-SQL service

This document specifies what the consolidated database we build for each
customer should look like, so the text2sql + canonical-query layer on top
performs well, is auditable, and is safe.

It is opinionated. The service should default to this shape and only
deviate per-customer when there's a concrete reason.

---

## 1. Who this is for

We sell to mid-market logistics, distribution, and operations companies.
For each customer we:

1. Pull data from their ERP, scattered file shares, OCR'd physical docs.
2. Land it in a database **we own and operate** (per-customer instance).
3. Refresh on a schedule (daily for transactional, hourly for inventory).
4. Expose a chat UI on top, backed by text2sql (this fork).

The DB is the contract between our ingestion pipeline and our query layer.
If we get it right, the chat layer is largely "plug and play": the same
text2sql engine, the same canonical queries, just pointed at a new
connection string.

---

## 2. Design goals (in priority order)

| # | Goal | Why |
|---|---|---|
| 1 | **Read-only by default for the agent** | Hard privilege boundary at the DB layer (defense in depth on top of `enforce_read_only=True` in this fork). |
| 2 | **Schema reads like English** | The LLM gets dramatically better answers when columns are named `customer_name` not `cstm_nm_001` and have comments. |
| 3 | **One canonical entity per concept** | Customers, products, orders should each live in *one* place — not three. The agent picks the wrong table when there are two `singer` tables (literally happens in upstream's Spider trace). |
| 4 | **Pre-computed views for hard joins** | If "revenue" requires joining 5 tables, expose `v_revenue_monthly` and let canonical/agent SQL hit that. |
| 5 | **Stable grain on every fact table** | "One row = one ____" must be unambiguous and documented. |
| 6 | **Auditability** | Every row knows where it came from, when, and the source-system ID. |
| 7 | **Refresh is incremental and idempotent** | Re-running yesterday's load doesn't double-count. |
| 8 | **Currency and units are normalized** | `amount_cents` + `currency_code`, never floats; SI units; UTC timestamps. |

These goals come from how the text2sql agent actually behaves: it explores
schema, picks tables by name match, joins by guessing FKs, and gives up if
the query takes more than ~10 tool calls. Schema clarity is the single
biggest lever we have on accuracy.

---

## 3. Three-layer architecture (medallion-style)

We separate raw landing data from the polished layer the agent sees:

```
┌─────────────────────────────────────────────────────────────────────┐
│                  EXTERNAL SOURCES (per customer)                    │
│  ERP DB · CSV exports · Excel · scanned PDFs (post-OCR) · APIs      │
└─────────────────────────────────┬───────────────────────────────────┘
                                  │  ETL workers
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│  LAYER 1 — bronze.*   (raw, append-only, schema-on-read)            │
│  - One table per source system per object: bronze.sap_invoices, …   │
│  - No transformations. Source columns preserved. + ingestion_ts.    │
│  - Used by us only — never exposed to the agent.                    │
└─────────────────────────────────┬───────────────────────────────────┘
                                  │  dbt-style transforms
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│  LAYER 2 — silver.*   (cleaned, deduped, type-cast, conformed keys) │
│  - One table per logical entity, drawn from one or more bronze: …   │
│    silver.invoices, silver.customers, silver.shipments              │
│  - Units normalized, dates UTC, currency standardized.              │
│  - Used by us and downstream layer 3.                               │
└─────────────────────────────────┬───────────────────────────────────┘
                                  │  views + materialized views
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│  LAYER 3 — gold.*     (semantic layer — what the agent sees)        │
│  - Star/snowflake fact + dim tables.                                │
│  - Pre-built business views: gold.v_ar_aging, gold.v_revenue_month  │
│  - This is the ONLY schema the read-only agent role can SELECT from.│
└─────────────────────────────────────────────────────────────────────┘
```

**Why three layers, not two:**
- Bronze gives us recoverability. If silver gets corrupted, we re-derive.
- Silver gives us a clean canonical entity layer for our own internal use
  (reports, exports, our ops dashboards).
- Gold gives the LLM the smallest, cleanest possible surface to reason
  over. Per ezinsights.ai's text-to-SQL agent guide, **exposing only
  curated read-only views is the single biggest accuracy and security win**.

The agent never sees bronze or silver. The bronze schema can be ugly; gold
must read like a textbook.

---

## 4. Engine choice: PostgreSQL 16+

| Why | Detail |
|---|---|
| First-class column comments | `COMMENT ON COLUMN ...` — directly readable by the agent through `pg_description`. The dialect guide in this fork's `dialects.py` already uses it. |
| Schema-level permissions | `GRANT SELECT ON ALL TABLES IN SCHEMA gold TO agent_role` — enforces our read-only boundary. |
| Materialized views with `REFRESH CONCURRENTLY` | Pre-aggregated metrics without blocking reads. |
| Partitioning | `PARTITION BY RANGE (event_date)` keeps fact tables fast as they grow. |
| `JSONB` columns | For source-system payloads we don't model out yet, queryable later. |
| Row Level Security (RLS) | If we ever do per-user scopes (warehouse manager only sees their warehouse), it's built-in — we don't have to inject WHERE clauses. |
| Mature replication / backups | Customer data, business-critical, we don't compromise here. |

We will deploy one Postgres instance per customer (managed: AWS RDS or
similar). Cross-customer isolation by separate instance, not shared schema
— makes deletion, audit, and incident scope trivial.

---

## 5. Dimensional model: facts and dimensions

The gold layer follows Kimball-style dimensional modeling — well-supported
by Postgres, and the only style most LLMs handle reliably (per
motherduck.com and snowflake.com guides).

### Fact tables (what happened)

One row = one business event. The grain is documented in the table comment.

| Fact table | Grain | Key measures |
|---|---|---|
| `gold.fact_orders` | one customer order header | order_total, line_count |
| `gold.fact_order_items` | one line on one order | quantity, unit_price, line_total |
| `gold.fact_invoices` | one invoice header | amount_billed, amount_paid, amount_due |
| `gold.fact_payments` | one payment receipt | amount_received |
| `gold.fact_shipments` | one shipment | weight_kg, distance_km, freight_cost |
| `gold.fact_shipment_items` | one item on one shipment | units_shipped |
| `gold.fact_inventory_movements` | one stock movement | quantity_delta |
| `gold.fact_expenses` | one expense entry | amount, tax_amount |

### Dimension tables (the context)

One row = one entity, slowly changing.

| Dimension | What |
|---|---|
| `gold.dim_customer` | customer master (name, segment, billing terms) |
| `gold.dim_product` | product master (SKU, category, weight, dimensions) |
| `gold.dim_warehouse` | warehouses, locations, regions |
| `gold.dim_vehicle` | trucks/vans for logistics |
| `gold.dim_driver` | drivers (if relevant per customer) |
| `gold.dim_supplier` | vendors |
| `gold.dim_employee` | for expense ownership |
| `gold.dim_date` | every date with fiscal year/quarter/month/week |
| `gold.dim_currency` | currency codes + reference rates by date |

### Slowly Changing Dimensions (SCD Type 2) — when needed

For dimensions where history matters (a product's price changed, a customer
moved billing address), use SCD Type 2: add `valid_from`, `valid_to`,
`is_current` columns. Most queries filter `WHERE is_current = true`;
historical reports use the date range.

**Rule of thumb:** `dim_customer`, `dim_product`, `dim_warehouse` get SCD2.
Static reference data (`dim_date`, `dim_currency`) does not.

---

## 6. LLM-friendly conventions

These are not optional. They are how we get the agent's success rate up
from "OK in demos" to "good enough to bill for."

### Naming

- `snake_case`, lowercase, English words.
- Tables: singular noun (`customer`, not `customers`) — both upstream
  Spider and the agent's prompts handle either, but pick one and stick.
  *Convention here: dimensions singular (`dim_customer`), facts plural
  (`fact_orders`) — matches Kimball convention.*
- Columns: noun, no prefix, no abbreviation unless universal (`id`, `qty`).
  - Bad: `cstm_nm_001`, `c_addr_l1`, `dt_crt`.
  - Good: `customer_name`, `address_line_1`, `created_at`.
- Foreign keys: `<referenced_table>_id`. So `fact_orders.customer_id`
  → `dim_customer.customer_id`. The agent will guess the join correctly
  every time.
- Booleans: `is_active`, `is_paid`, `has_been_returned`. Always
  `is_/has_/can_` prefix.
- Money: `amount_<unit>` where unit is the currency or `amount_cents`.
  Pair with `currency_code` if multi-currency.
- Dates/times: `<event>_at` for timestamps (UTC), `<event>_date` for dates.

### Column comments — non-negotiable

Every gold-layer column has a `COMMENT ON COLUMN`. The agent reads these
through Postgres's `pg_description` (the dialect guide already does this).

```sql
COMMENT ON COLUMN gold.fact_invoices.amount_due IS
  'Outstanding balance on this invoice in invoice currency. '
  'Equal to amount_billed - amount_paid. Zero when fully paid.';
```

This is the single highest-ROI thing we can do. A 3-line comment on a
finance column changes the agent's success on questions about that column
from a coin flip to near-certain.

### Table comments

Every table gets a `COMMENT ON TABLE` that documents:
- What one row represents (the **grain**).
- Important filters callers usually want (e.g. "filter `is_void = false`
  for normal reporting").
- Cross-references to related tables.

```sql
COMMENT ON TABLE gold.fact_invoices IS
  'One row per invoice header. Filter is_void=false for normal reporting. '
  'Line-level detail in fact_invoice_items. Joined to fact_payments via '
  'invoice_id.';
```

### Single canonical entity

If the source ERP has `customers`, `customer_master`, `cust_legacy`, our
silver layer dedups them into one `silver.customers`, and the gold layer
exposes one `gold.dim_customer`. The agent must never see two tables
that *might* be the customer table.

### No surprises

- Don't expose helper/staging tables in `gold`.
- Don't have one column that means different things in different rows
  (no "polymorphic" columns). Split into separate columns or separate
  tables.
- Don't use generic columns like `extra`, `notes`, `data` unless they're
  truly free-text and the agent never needs them.

---

## 7. Currency, units, time — get these right once

### Currency

- Store every monetary amount as **integer minor units** (cents) in a
  column named `amount_cents` (or `<role>_amount_cents`).
- Never use floats for money.
- Pair with `currency_code` (ISO 4217: `'USD'`, `'EUR'`, `'PEN'`).
- Provide a view `gold.v_<fact>_in_base_currency` that converts to the
  customer's reporting currency using `dim_currency` rates by date.
- Document in column comments which views convert and which don't.

### Units

- Mass: `_kg`. Volume: `_m3` or `_l`. Distance: `_km`. Time: `_seconds` or
  `_minutes` (be explicit). Always SI, always in the column name.
- Bad: `weight`. Good: `weight_kg`.

### Time

- All timestamps in `TIMESTAMPTZ` (Postgres) and stored UTC.
- All dates in `DATE`.
- Build `gold.dim_date` once, joined to every fact via `event_date_key`.
  Lets the agent answer fiscal-quarter questions without doing date math.

```sql
CREATE TABLE gold.dim_date (
  date_key       DATE PRIMARY KEY,
  day            INT, month INT, year INT,
  quarter        INT,            -- 1..4
  fiscal_year    INT,            -- per customer fiscal calendar
  fiscal_quarter INT,            -- 1..4
  fiscal_month   INT,            -- 1..12
  is_weekend     BOOLEAN,
  is_holiday     BOOLEAN,
  iso_week       INT
);
COMMENT ON TABLE gold.dim_date IS
  'One row per calendar date. Use fiscal_quarter/fiscal_year for finance '
  'questions ("this quarter", "last fiscal year"). Customer fiscal year '
  'starts in the month documented in tenant_config.fiscal_year_start_month.';
```

---

## 8. Audit and lineage columns

Every gold table gets these, populated by the ETL:

| Column | Type | Purpose |
|---|---|---|
| `record_loaded_at` | `TIMESTAMPTZ` | When our ETL inserted this row. |
| `record_updated_at` | `TIMESTAMPTZ` | When our ETL last updated this row. |
| `source_system` | `TEXT` | `'sap'`, `'odoo'`, `'csv:warehouse_dump'`, `'ocr:scanned_invoice'`. |
| `source_record_id` | `TEXT` | The natural ID in the source system. |
| `etl_batch_id` | `UUID` | Which load batch produced/updated this. |

These are **not** for the agent — exclude them from view definitions.
They're for our incident response, drift detection, and "where did this
number come from" debugging.

---

## 9. Permissions

Three Postgres roles per customer DB:

| Role | Privileges | Used by |
|---|---|---|
| `etl_writer` | `INSERT/UPDATE/DELETE` on `bronze.*`, `silver.*`, `gold.*` | Our ingestion workers |
| `service_admin` | `SELECT` on everything | Our internal ops dashboards |
| `agent_reader` | `SELECT` on `gold.*` only | The text2sql connection string |

Connection string the chat UI uses:
```
postgresql://agent_reader:<pwd>@host:5432/customer_db?options=-csearch_path%3Dgold
```

With `text2sql.TextSQL(..., enforce_read_only=True)` (this fork), we get a
hard error at startup if the role accidentally has write access. Defense
in depth: the regex in `tools.py` is the second line, the DB role is the
first.

---

## 10. The semantic layer — pre-built views

This is where canonical queries (in this fork) and the agent meet. Most
business questions should be one-line SELECTs against a curated view.

### Tier 1 — entity views (denormalized for readability)

Wrap each fact table with its dimension joins so the agent doesn't have
to perform them.

```sql
CREATE VIEW gold.v_invoices AS
SELECT
  i.invoice_id,
  i.invoice_number,
  i.invoice_date,
  i.due_date,
  i.status,
  i.amount_billed_cents,
  i.amount_paid_cents,
  i.amount_due_cents,
  i.currency_code,
  c.customer_id,
  c.customer_name,
  c.customer_segment,
  c.country_code
FROM gold.fact_invoices i
JOIN gold.dim_customer c
  ON c.customer_id = i.customer_id AND c.is_current = true
WHERE i.is_void = false;

COMMENT ON VIEW gold.v_invoices IS
  'Denormalized invoice view: one row per non-void invoice with current '
  'customer context. The default view for any invoice / AR question.';
```

Build one of these per fact table. This dropps the join-finding burden
from the agent for 80% of questions.

### Tier 2 — metric views (pre-aggregated)

For the metrics finance asks about constantly:

```sql
CREATE MATERIALIZED VIEW gold.v_revenue_monthly AS
SELECT
  DATE_TRUNC('month', i.invoice_date)::DATE AS month,
  SUM(i.amount_billed_cents) / 100.0 AS revenue,
  i.currency_code,
  COUNT(*) AS invoice_count
FROM gold.fact_invoices i
WHERE i.is_void = false AND i.status IN ('paid','partial','unpaid')
GROUP BY 1, i.currency_code;
CREATE UNIQUE INDEX ON gold.v_revenue_monthly (month, currency_code);

COMMENT ON MATERIALIZED VIEW gold.v_revenue_monthly IS
  'Monthly billed revenue. Refreshed nightly. Use this for "revenue by '
  'month" questions instead of summing fact_invoices directly.';
```

Materialized → `REFRESH MATERIALIZED VIEW CONCURRENTLY ...` on the same
schedule as ingestion.

### Tier 3 — diagnostic views (for canonical queries)

Things like AR aging buckets, late-shipment counts, low-stock alerts —
each one designed to back exactly one canonical query.

```sql
CREATE VIEW gold.v_ar_aging AS
SELECT
  customer_id, customer_name,
  SUM(amount_due_cents) FILTER (WHERE due_date >= CURRENT_DATE) / 100.0
    AS not_yet_due,
  SUM(amount_due_cents) FILTER (WHERE CURRENT_DATE - due_date BETWEEN 1 AND 30) / 100.0
    AS overdue_1_30,
  SUM(amount_due_cents) FILTER (WHERE CURRENT_DATE - due_date BETWEEN 31 AND 60) / 100.0
    AS overdue_31_60,
  SUM(amount_due_cents) FILTER (WHERE CURRENT_DATE - due_date > 60) / 100.0
    AS overdue_60_plus,
  SUM(amount_due_cents) / 100.0 AS total_owed
FROM gold.v_invoices
WHERE status = 'unpaid'
GROUP BY customer_id, customer_name;
```

Then in `canonical.md`:
```markdown
## ar_aging
aliases: ar aging, accounts receivable aging, overdue breakdown
```sql
SELECT * FROM gold.v_ar_aging ORDER BY total_owed DESC
```
```

That's the architectural punchline: **the gold-layer view does the work,
the canonical query is one line, the agent never runs.**

---

## 11. Concrete starter schema (logistics + finance customer)

A minimal-but-complete sketch of what `gold.*` looks like for a
distribution company. DDL excerpts only — full migration script is a
separate component.

### Dimensions

```sql
CREATE TABLE gold.dim_customer (
  customer_id        BIGINT      PRIMARY KEY,
  customer_code      TEXT        NOT NULL,    -- source-system code
  customer_name      TEXT        NOT NULL,
  customer_segment   TEXT,                    -- 'enterprise', 'smb', ...
  payment_terms_days INT,                     -- 30, 60, 90
  country_code       TEXT,                    -- ISO 3166-1 alpha-2
  region             TEXT,
  is_active          BOOLEAN     NOT NULL DEFAULT true,
  -- SCD2
  valid_from         TIMESTAMPTZ NOT NULL,
  valid_to           TIMESTAMPTZ,             -- NULL = current
  is_current         BOOLEAN     NOT NULL,
  -- audit
  record_loaded_at   TIMESTAMPTZ NOT NULL,
  source_system      TEXT NOT NULL
);

CREATE TABLE gold.dim_product (
  product_id         BIGINT PRIMARY KEY,
  sku                TEXT NOT NULL,
  product_name       TEXT NOT NULL,
  product_category   TEXT,
  unit_weight_kg     NUMERIC(10,3),
  unit_volume_m3     NUMERIC(10,4),
  reorder_threshold  INT,
  is_active          BOOLEAN NOT NULL DEFAULT true,
  valid_from         TIMESTAMPTZ NOT NULL,
  valid_to           TIMESTAMPTZ,
  is_current         BOOLEAN NOT NULL,
  record_loaded_at   TIMESTAMPTZ NOT NULL,
  source_system      TEXT NOT NULL
);

CREATE TABLE gold.dim_warehouse (
  warehouse_id   BIGINT PRIMARY KEY,
  warehouse_code TEXT NOT NULL,
  warehouse_name TEXT NOT NULL,
  city           TEXT,
  country_code   TEXT,
  capacity_m3    NUMERIC(12,2),
  is_active      BOOLEAN NOT NULL DEFAULT true,
  -- (timestamps as above)
);
```

### Facts

```sql
CREATE TABLE gold.fact_orders (
  order_id          BIGINT PRIMARY KEY,
  order_number      TEXT NOT NULL,
  customer_id       BIGINT NOT NULL REFERENCES gold.dim_customer,
  order_date        DATE NOT NULL,
  status            TEXT NOT NULL,             -- 'open','fulfilled','canceled'
  total_amount_cents BIGINT NOT NULL,
  currency_code     TEXT NOT NULL,
  warehouse_id      BIGINT REFERENCES gold.dim_warehouse,
  -- audit
  record_loaded_at  TIMESTAMPTZ NOT NULL,
  source_system     TEXT NOT NULL,
  source_record_id  TEXT NOT NULL
) PARTITION BY RANGE (order_date);

-- monthly partitions
CREATE TABLE gold.fact_orders_2026_04 PARTITION OF gold.fact_orders
  FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');

CREATE TABLE gold.fact_invoices (
  invoice_id          BIGINT PRIMARY KEY,
  invoice_number      TEXT NOT NULL,
  customer_id         BIGINT NOT NULL REFERENCES gold.dim_customer,
  order_id            BIGINT REFERENCES gold.fact_orders,
  invoice_date        DATE NOT NULL,
  due_date            DATE NOT NULL,
  status              TEXT NOT NULL,            -- 'paid','partial','unpaid'
  amount_billed_cents BIGINT NOT NULL,
  amount_paid_cents   BIGINT NOT NULL DEFAULT 0,
  amount_due_cents    BIGINT GENERATED ALWAYS AS (amount_billed_cents - amount_paid_cents) STORED,
  currency_code       TEXT NOT NULL,
  is_void             BOOLEAN NOT NULL DEFAULT false,
  -- audit ...
);

CREATE TABLE gold.fact_shipments (
  shipment_id       BIGINT PRIMARY KEY,
  order_id          BIGINT NOT NULL REFERENCES gold.fact_orders,
  warehouse_id      BIGINT NOT NULL REFERENCES gold.dim_warehouse,
  vehicle_id        BIGINT REFERENCES gold.dim_vehicle,
  ship_date         DATE NOT NULL,
  delivered_date    DATE,
  weight_kg         NUMERIC(10,3),
  distance_km       NUMERIC(10,2),
  freight_cost_cents BIGINT,
  currency_code     TEXT,
  status            TEXT NOT NULL,             -- 'in_transit','delivered','returned'
  -- audit ...
);

CREATE TABLE gold.fact_inventory_movements (
  movement_id       BIGSERIAL PRIMARY KEY,
  product_id        BIGINT NOT NULL REFERENCES gold.dim_product,
  warehouse_id      BIGINT NOT NULL REFERENCES gold.dim_warehouse,
  event_at          TIMESTAMPTZ NOT NULL,
  movement_type     TEXT NOT NULL,             -- 'receipt','shipment','adjustment'
  quantity_delta    INT NOT NULL,
  reference_table   TEXT,                      -- e.g. 'fact_shipments'
  reference_id      BIGINT,
  -- audit ...
);
```

Add column comments on every business column. Build entity views
(`v_orders`, `v_invoices`, `v_shipments`) and metric views (`v_revenue_monthly`,
`v_ar_aging`, `v_late_deliveries`) per the patterns in §10.

---

## 12. Documents — first-class part of the product

Customers don't only have an ERP. They also have business-relevant
information scattered across PDFs, Excel files, scanned paper docs, and
loose Word files on staff machines. The chatbot must be able to answer
questions whose evidence sits in those files. Documents are therefore
**in scope for this product**, not a separate service.

We split documents into three buckets and route each to the handling
that fits its shape:

| Bucket | Examples | Handling | Where the answer lives |
|---|---|---|---|
| **Templated business docs** | invoices, POs, bills of lading, expense receipts, packing slips | Structured extraction at ingestion (Textract / Azure Document Intelligence / Mistral OCR / etc.) — extract fields straight into the relational layer | Rows in `fact_invoices`, `fact_expenses`, `fact_shipments`, … — answerable via SQL |
| **Semi-structured spreadsheets** | warehouse Excel exports, manual ops reports, planning sheets | Parse to bronze, conform to silver, project into gold facts/dims | Rows in the relevant gold facts/dims — answerable via SQL |
| **Unstructured prose** | contracts, emails, meeting notes, scopes of work, free-text addenda | OCR if needed, chunk + embed into a vector store; keep the original in object storage; reference from `dim_document` | Object storage + vector index — answerable via a `search_documents` tool |

### dim_document — the registry every doc lands in

Every document the system ingests gets a row in `dim_document`,
regardless of which bucket it falls into. This is the join point between
the relational layer and the prose-document path.

```sql
CREATE TABLE gold.dim_document (
  document_id       BIGINT PRIMARY KEY,
  document_type     TEXT NOT NULL,           -- 'invoice','contract','bill_of_lading','email',…
  storage_url       TEXT NOT NULL,           -- s3://bucket/customer/...
  filename          TEXT,
  uploaded_at       TIMESTAMPTZ NOT NULL,
  page_count        INT,
  ocr_status        TEXT,                    -- 'pending','done','failed','not_required'
  ocr_text_url      TEXT,                    -- s3://... full text (when applicable)
  vector_indexed    BOOLEAN NOT NULL DEFAULT false,
  extraction_status TEXT                     -- 'pending','extracted','failed','not_applicable'
);
```

Any structured row that came from a document references it:

```sql
ALTER TABLE gold.fact_invoices
  ADD COLUMN source_document_id BIGINT REFERENCES gold.dim_document;
```

This means: a question about an invoice can return the numbers (from
`fact_invoices`) **and** link to the original PDF (via
`source_document_id` → `dim_document.storage_url`) in the same answer.

### Two tools, one agent

The chat agent gets two tools and decides per question:

- **`execute_sql`** (existing) — for numeric / structured questions. Hits
  the gold schema as before. Most finance/ops questions land here, both
  for ERP-sourced data and for fields extracted from templated docs.
- **`search_documents`** (new, to build) — vector search over the prose
  document store. For questions like "find the contract that mentioned
  90-day payment terms" or "which supplier emails reference the
  warehouse closure?" Returns ranked passages plus `dim_document` rows
  so structured and unstructured results stitch into one reply.

Some questions need both — e.g. "what's our exposure to suppliers with
late-delivery clauses?" wants `search_documents` to find clauses, then
`execute_sql` to aggregate the matching suppliers' open POs. The agent
can chain.

### Why this shape

- **Single product surface** — the user types one question; the agent
  picks the tool. No "structured vs documents" toggle in the UI.
- **No information loss** — answers grounded in extracted-fact rows are
  precise; answers grounded in passages cite the document. Either way
  the system can show its work.
- **No info we have goes unused** — if a fact lives only in a PDF, the
  structured-extraction pipeline pulls it into the relational layer; if
  it lives in prose, the RAG path finds it. The "no missed info"
  quality bar requires both.

---

## 13. ETL refresh patterns

Out of scope for this doc, but the database design must support:

- **Idempotent merges**: every silver/gold load is `INSERT … ON CONFLICT
  (source_system, source_record_id) DO UPDATE`. Re-running yesterday is
  always safe.
- **Watermark columns**: bronze tables track `max(source_updated_at)`
  per source so incremental pulls are cheap.
- **Materialized view refresh**: `REFRESH MATERIALIZED VIEW CONCURRENTLY
  v_revenue_monthly` after each silver→gold pass. No write contention
  with the chat UI's reads.
- **Partition rotation**: monthly cron creates next month's `fact_*`
  partitions; old partitions can be detached and archived to cold storage.

Recommended orchestrator: Prefect or Dagster (per-customer flow per
source system). Airflow is fine but heavier.

---

## 14. How this plugs into text2sql (this fork)

The whole point of this design is that the chat layer is plug-and-play:

1. **Connection string** for `agent_reader` — set once per customer.
2. **`canonical_queries`** — one `canonical.md` per customer; ~80% of
   the questions are the same across customers (AR/AP totals, top
   customers, monthly revenue) and live in a shared template; the rest
   are customer-specific. Each canonical SQL hits a `gold.v_*` view, so
   it's trivial.
3. **`scenarios.md`** — covers the long tail. Business jargon ("net
   revenue means X for this customer", "we count returns the following way")
   that the column comments alone can't convey.
4. **`enforce_read_only=True`** — guaranteed by the `agent_reader` role.
5. **`metadata_hint`** — pass the customer's fiscal calendar / base
   currency / timezone so the agent doesn't have to figure it out.
6. **`user_id` / `user_role`** on every `ask()` — populated from the chat
   session, lands in the trace JSONL for audit.

Concretely, our service code per customer is:

```python
engine = TextSQL(
    connection_string=f"postgresql://agent_reader:{pwd}@{host}/{db}?options=-csearch_path%3Dgold",
    model="anthropic:claude-sonnet-4-6",
    canonical_queries=f"./customers/{cust_id}/canonical.md",
    examples=f"./customers/{cust_id}/scenarios.md",
    metadata_hint=f"Fiscal year starts in {fiscal_month}. "
                  f"Base currency is {base_currency}. "
                  f"All timestamps are UTC; user is in timezone {tz}.",
    trace_file=f"./traces/{cust_id}.jsonl",
    enforce_read_only=True,
)

result = engine.ask(
    user_question,
    user_id=session.user_email,
    user_role=session.user_role,
    metadata={"session_id": session.id, "tenant": cust_id},
)
```

That's it. Onboarding a new customer = provision a Postgres instance,
run our schema migration, point the ETL workers at the source systems,
write the customer's `canonical.md` and `scenarios.md`, deploy.

---

## 15. Customer onboarding checklist

What needs to happen in order, per customer:

- [ ] Provision Postgres 16 instance (RDS), `gold`/`silver`/`bronze` schemas, three roles.
- [ ] Run schema migration: dims, facts, partitions, audit columns, comments.
- [ ] Build entity views (`v_*`) for every fact.
- [ ] Inventory source systems (ERP DB, file shares, paper docs by category).
- [ ] Build bronze loaders per source (Prefect flows).
- [ ] Build silver transforms (dedup, cleanse, conform).
- [ ] Build gold transforms (denormalize, populate dims/facts).
- [ ] Schedule daily/hourly refresh; verify idempotency.
- [ ] Populate `dim_date` and `dim_currency` (one-time + daily currency rates).
- [ ] Set fiscal calendar in `tenant_config`.
- [ ] Write column comments on every gold column. **No exceptions.**
- [ ] Build the customer's `canonical.md` (start from our template, add their top-20 questions).
- [ ] Write initial `scenarios.md` with their business jargon.
- [ ] Smoke test: 20 representative questions; manually verify SQL.
- [ ] Set up trace JSONL collection; pipe into our audit DB nightly.
- [ ] Hand the chat UI URL to the customer's first 3 users.
- [ ] After 2 weeks: review traces, promote frequent agent questions
      into canonical, fix scenarios where the agent struggled.

---

## 16. What this design deliberately does NOT solve

- **Per-row permissions** ("warehouse manager only sees their warehouse").
  Solvable later via Postgres Row Level Security policies on `gold.*`
  tables, keyed off a session var the chat layer sets. Add when needed,
  not before.
- **Real-time data** (sub-minute). The architecture assumes daily/hourly
  refresh. CDC streaming is a separate (much bigger) project.
- **Rolled-up cross-customer analytics** ("how does our customer's AR
  compare to the median?"). Requires a separate analytics warehouse with
  aggregated, anonymized data — outside the per-customer DB.
- **Conversation history / follow-ups** in the SDK. Belongs in the chat
  session store on top.
- **Insight synthesis as prose** ("AR is up 12% MoM driven by X and Y").
  Logged as a TODO — needs a second LLM pass over the SQL result, with
  citation guardrails so it can't invent numbers. Not in v1.

---

## 17. Components we still need to build (everything-now thread)

The DB design is the contract, but the plug-and-play vision needs:

| Component | Status | Notes |
|---|---|---|
| **text2sql engine** | done (this fork) | canonical + audit + read-only enforcement added in this branch |
| **DB schema migrations** | not yet | a `db/migrations/*.sql` pack we run per customer; derived from §11 |
| **`canonical.md` template** | partial | starter in `examples/canonical.md`; extend to logistics-specific entries |
| **`scenarios.md` template** | partial | upstream sample exists; needs logistics/finance flavor |
| **ETL flows (ERP → bronze/silver/gold)** | not yet | per-source, separate repo (e.g. `t2s-ingest`); Prefect-based |
| **Document ingestion — file discovery** | not yet | scrapers for the customer's file locations (Windows shares, OneDrive/SharePoint, per-laptop sync); land originals in S3 + register in `dim_document` |
| **Document ingestion — structured extraction** | not yet | OCR + field extraction for templated docs (invoices, POs, BoLs, receipts) → rows in the relational facts; Textract / Azure Document Intelligence / Mistral OCR |
| **Document ingestion — vector index** | not yet | chunk + embed prose docs (contracts, emails, notes) into a vector store; `vector_indexed=true` on `dim_document` |
| **`search_documents` agent tool** | not yet | second tool added to the text2sql agent; vector search against the prose store, returns ranked passages + `dim_document` rows |
| **Insight synthesis pass** | not yet (TODO) | optional second LLM call: turns `(sql, result, trace)` → prose insights with citation guardrails |
| **Chat UI** | not yet | thin web app; calls `engine.ask()` per turn, streams the trace; **no quick-pick chips / question gallery — single chat input only** |
| **Audit pipeline** | not yet | nightly job: trace JSONL → `audit.queries` table; queryable for compliance |
| **Customer admin tool** | not yet | provisioning, migration, role rotation, canonical/scenario editing |

Order of build matters: schema migrations next (so we can stand up a real
test DB), then a demo ETL into one of the dummy data sets, then the chat
UI on top. Everything else can wait until a real customer.

---

## Sources

- [LLM & AI Models for Text-to-SQL: Modern Frameworks](https://promethium.ai/guides/llm-ai-models-text-to-sql/)
- [Text to SQL Agent Design Best Practices for Enterprises (EzInsights)](https://ezinsights.ai/text-to-sql-agent/)
- [LLM Text-to-SQL Solutions: Challenges & Best Practices (EzInsights)](https://ezinsights.ai/llm-text-to-sql/)
- [Bridging Natural Language and Databases (Vi Q. Ha, Medium)](https://medium.com/@vi.ha.engr/bridging-natural-language-and-databases-best-practices-for-llm-generated-sql-fcba0449d4e5)
- [Star Schema Guide (MotherDuck)](https://motherduck.com/learn/star-schema-data-warehouse-guide/)
- [Star Schema Fundamentals (Snowflake)](https://www.snowflake.com/en/fundamentals/star-schema/)
- [Dimensional Data Modeling for Logistics (Saham Siddiqui, Medium)](https://medium.com/@sahamsiddiqui/dimensional-data-modeling-for-logistics-a-step-by-step-guide-f5a5e833517c)
- [Data Warehouse Schemas: Star, Snowflake & Galaxy (Exasol)](https://www.exasol.com/hub/data-warehouse/schemas/)
- [Techniques for improving text-to-SQL (Google Cloud Blog)](https://cloud.google.com/blog/products/databases/techniques-for-improving-text-to-sql)
