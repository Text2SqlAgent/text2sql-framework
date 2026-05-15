# Chat UI design brief

Self-contained brief to paste into Claude.ai (the web app, with artifacts
turned on) when you want to iterate on the UI visually. Everything Claude
needs to render a working mock — product context, visual style, components,
API contract, mock data — is in here.

> **How to use this**: open Claude.ai (web), make sure artifacts are
> enabled, paste the entire contents of this file, and ask "build this as
> a single React + Tailwind component artifact, using the mock data —
> don't call the API yet." Iterate on the visual until you're happy. Then
> bring the JSX back into this repo as the production frontend.

---

## Product context

**What it is**: a chat interface for finance and operations staff at a
mid-market logistics/distribution company ("Acme Logistics S.A.") to ask
natural-language questions of their consolidated data warehouse.

**Who uses it**: CFOs, finance managers, controllers, ops directors,
warehouse managers. Not engineers. They are comfortable with Excel and
Power BI but not SQL.

**Typical questions**:
- "How much are we owed?"
- "Top 10 customers by revenue this fiscal year"
- "Which products are running low on stock?"
- "Generate an expense report for Q3"
- "Which shipments to Mexico were late last month?"

**What happens behind the scenes** (the user does not need to see this
unless they expand it):
- The question is matched against a list of vetted SQL templates
  ("canonical queries"). If it matches, the SDK runs the template
  directly — millisecond response, deterministic, no LLM cost.
- If it doesn't match, an LLM agent explores the database schema,
  writes SQL, executes it, and self-corrects. 5–30 seconds, costs money,
  results vary slightly across runs.
- Either way, the user gets back: the SQL that ran, a result table, an
  optional commentary line, latency, and (for audit) trace metadata.

---

## Visual style

- **Tone**: clean, professional, finance-grade. NOT playful. Think
  Linear / Notion / Stripe Dashboard, not a consumer chat app.
- **Color palette**: light mode default. White or very pale gray
  background (`#FAFAFA` or similar). Primary accent: a single muted
  brand color (deep navy `#1E2A44` or forest green `#1F5F3F` —
  designer's choice). No rainbow charts, no gradients on buttons, no
  emoji.
- **Typography**: Inter for UI, JetBrains Mono / Fira Code for SQL and
  numeric tables. Comfortable density — these users read tables a lot.
- **Spacing**: generous on the chat thread, tight in tables. Tables are
  the actual product surface.
- **Iconography**: Lucide or Phosphor, single weight, monochrome.

What to avoid:
- Friendly mascots, chat bubbles with tails, animated typing indicators
  longer than ~1 line.
- Hiding numbers behind animations or scroll-triggered reveals.
- "AI" gradient pixie-dust styling — this is a serious tool for
  serious people.

---

## Layout

Single-page app, three regions:

```
┌──────────────────────────────────────────────────────────────────┐
│ Header   [Acme Logistics — text2sql]            [user pill]      │
├────────────┬─────────────────────────────────────────────────────┤
│            │                                                     │
│ Sidebar    │  Chat thread (scrollable)                           │
│ (left,     │                                                     │
│ optional)  │  • Suggested questions chips at top of empty state  │
│            │  • Each turn: question → response card              │
│  • Recent  │                                                     │
│    queries │                                                     │
│  • Pinned  │                                                     │
│  • Audit   │                                                     │
│            │                                                     │
│            ├─────────────────────────────────────────────────────┤
│            │  Input box (sticky bottom)                          │
└────────────┴─────────────────────────────────────────────────────┘
```

The sidebar is optional in the first cut — focus on the chat thread.

---

## Per-turn response card (the most important component)

Every response from the API gets rendered as a card with these
elements, top to bottom:

1. **The question** the user asked (small, muted).
2. **Status row** — small chips:
   - If `canonical_query` is set: a green "Verified" pill with the
     canonical name (e.g. `ar_aging`). Hover tooltip: "Ran a vetted SQL
     template — deterministic and fast."
   - If not: a blue "Agent" pill with the iteration count (e.g.
     "Agent · 4 tool calls").
   - Latency in ms or s.
   - Token cost (small, gray, only shown if non-zero).
3. **Result table** — the `data` array. Sticky header. Numbers
   right-aligned. Currency columns formatted (e.g. `S/14,302.50`).
   Date columns formatted ISO. Truncated cells with tooltip on hover.
   Below the table: row count, "Truncated to N rows" warning if
   `truncated: true`.
4. **Action row** under the table:
   - "Download CSV" button (calls `POST /ask/csv` with the same payload).
   - "Copy SQL" button.
   - "Show SQL" toggle that expands the code block below.
5. **Collapsible SQL block** (default collapsed) — monospace,
   syntax-highlighted. The actual SQL that ran.
6. **Commentary line** if `commentary` is non-empty — small italic gray
   text below the action row.
7. **Error state** — if `error` is set, show a red banner instead of the
   table, with the error message and a "Retry" button.

The card should feel scannable — the user should grasp "did I get my
answer, was it the right one, can I trust it" within 2 seconds.

---

## Empty state

When the chat is empty:
- A short hello: "Ask anything about Acme's operations and finance."
- A grid of 6–8 suggested-question chips, populated from the
  `GET /canonical` response. Click → fills the input and submits.
- A faint hint underneath: "Powered by text2sql · Read-only access ·
  Every query is logged".

---

## Input box

- Single textarea, expanding up to ~4 lines, then scrolls.
- Submit on Cmd/Ctrl+Enter; Enter alone inserts a newline.
- Disabled while a request is in flight; show a subtle inline spinner
  beside the submit button (NOT a giant overlay).
- Optional: small affordance to attach a "user role" selector (Finance
  Manager / Ops / Read-only) — this populates the `user_role` field in
  the API payload. For the demo, this can just be a stub.

---

## Audit / admin view

A separate route or modal showing recent traces from `GET /traces`. Each
row: question · who asked · when · canonical hit (yes/no/agent) · success
· duration · click to expand → see the SQL. Critical for finance
deployments. Make it dense — this is for ops/compliance, not end users.

---

## Out of scope for the first design pass

- Multi-customer / tenant switcher — assume one customer.
- Real-time streaming of agent reasoning. Just show a progress
  indicator; the response lands as a single payload.
- Editing or re-running historical queries.
- Saving/sharing/permalinks.
- Charts. (Tables only for v1; charts are a tempting rabbit hole.)
- Dark mode. (Light mode only for v1.)

---

## API contract (for the eventual fetch wiring)

Base URL: `http://localhost:8000` in dev; configurable in prod.

### `POST /ask`

```ts
type AskRequest = {
  question: string;
  user_id?: string;
  user_role?: string;
  session_id?: string;
  max_rows?: number;       // default 200
};

type AskResponse = {
  question: string;
  sql: string;
  data: Record<string, any>[];
  columns: string[];
  row_count: number;
  truncated: boolean;
  error: string | null;
  commentary: string;
  canonical_query: string | null;
  canonical_score: number | null;
  tool_calls_made: number;
  iterations: number;
  duration_seconds: number;
  input_tokens: number;
  output_tokens: number;
};
```

### `POST /ask/csv`

Same request body. Returns a `text/csv` body with
`Content-Disposition: attachment; filename="<slug>.csv"`.

### `GET /traces?limit=20`

Returns `TraceSummary[]` — see mock below.

### `GET /canonical`

Returns `{ queries: { name: string; aliases: string[]; description: string }[] }`.

### `GET /health`

`{ status: "ok", db: "ok", canonical_count: number, tracer_enabled: boolean }`

---

## Mock data (use this in the artifact)

Six representative responses covering canonical hit, agent run, error,
empty result, large result, and a CSV-target case. Wire the UI to pick
one based on the question text or just cycle through them.

### 1. Canonical hit — AR aging

```json
{
  "question": "How much are we owed?",
  "sql": "SELECT customer_id, customer_name, country_code, not_yet_due, overdue_1_30, overdue_31_60, overdue_61_90, overdue_90_plus, total_owed, currency_code FROM gold.v_ar_aging ORDER BY total_owed DESC",
  "columns": ["customer_name","country_code","not_yet_due","overdue_1_30","overdue_31_60","overdue_90_plus","total_owed","currency_code"],
  "data": [
    {"customer_name":"Polaris Distribution","country_code":"PE","not_yet_due":4520.0,"overdue_1_30":1240.0,"overdue_31_60":830.0,"overdue_90_plus":0.0,"total_owed":6590.0,"currency_code":"PEN"},
    {"customer_name":"Helios Trading","country_code":"PE","not_yet_due":2100.0,"overdue_1_30":3450.0,"overdue_31_60":0.0,"overdue_90_plus":0.0,"total_owed":5550.0,"currency_code":"PEN"},
    {"customer_name":"Vertex Holdings","country_code":"US","not_yet_due":0.0,"overdue_1_30":1200.0,"overdue_31_60":1900.0,"overdue_90_plus":1500.0,"total_owed":4600.0,"currency_code":"USD"},
    {"customer_name":"Atlas Logistics","country_code":"PE","not_yet_due":3200.0,"overdue_1_30":0.0,"overdue_31_60":0.0,"overdue_90_plus":0.0,"total_owed":3200.0,"currency_code":"PEN"},
    {"customer_name":"Quantum Group","country_code":"MX","not_yet_due":0.0,"overdue_1_30":0.0,"overdue_31_60":0.0,"overdue_90_plus":2800.0,"total_owed":2800.0,"currency_code":"MXN"}
  ],
  "row_count": 5,
  "truncated": false,
  "error": null,
  "commentary": "[canonical:ar_aging score=1.00] AR aging by customer with standard 30/60/90+ buckets.",
  "canonical_query": "ar_aging",
  "canonical_score": 1.0,
  "tool_calls_made": 0,
  "iterations": 0,
  "duration_seconds": 0.04,
  "input_tokens": 0,
  "output_tokens": 0
}
```

### 2. Agent run — non-canonical question

```json
{
  "question": "Which products had the highest sales growth last month vs. the prior month?",
  "sql": "WITH last_month AS (SELECT product_name, SUM(quantity) AS units FROM gold.v_order_items WHERE order_date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') AND order_date < DATE_TRUNC('month', CURRENT_DATE) GROUP BY product_name), prior AS (SELECT product_name, SUM(quantity) AS units FROM gold.v_order_items WHERE order_date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '2 months') AND order_date < DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') GROUP BY product_name) SELECT lm.product_name, lm.units AS last_month_units, COALESCE(p.units, 0) AS prior_month_units, ROUND(100.0 * (lm.units - COALESCE(p.units,0)) / NULLIF(COALESCE(p.units,0),0), 1) AS pct_growth FROM last_month lm LEFT JOIN prior p ON p.product_name = lm.product_name WHERE COALESCE(p.units,0) > 0 ORDER BY pct_growth DESC NULLS LAST LIMIT 10",
  "columns": ["product_name","last_month_units","prior_month_units","pct_growth"],
  "data": [
    {"product_name":"Granola #74","last_month_units":210,"prior_month_units":40,"pct_growth":425.0},
    {"product_name":"Disinfectant #18","last_month_units":180,"prior_month_units":62,"pct_growth":190.3},
    {"product_name":"Coffee #51","last_month_units":315,"prior_month_units":140,"pct_growth":125.0},
    {"product_name":"Cables #88","last_month_units":92,"prior_month_units":50,"pct_growth":84.0}
  ],
  "row_count": 4,
  "truncated": false,
  "error": null,
  "commentary": "Computed month-over-month growth using order_items. Excluded products with zero prior-month sales (would divide by zero).",
  "canonical_query": null,
  "canonical_score": null,
  "tool_calls_made": 6,
  "iterations": 6,
  "duration_seconds": 9.4,
  "input_tokens": 2840,
  "output_tokens": 312
}
```

### 3. Error — bad SQL the agent gave up on

```json
{
  "question": "What's the warranty terms on shipment 12345",
  "sql": "",
  "columns": [],
  "data": [],
  "row_count": 0,
  "truncated": false,
  "error": "No SQL produced. Response: I couldn't find a 'warranty_terms' column on the shipments table or any related table. This information may not be tracked in the database.",
  "commentary": "",
  "canonical_query": null,
  "canonical_score": null,
  "tool_calls_made": 4,
  "iterations": 4,
  "duration_seconds": 6.2,
  "input_tokens": 1820,
  "output_tokens": 184
}
```

### 4. Empty result

```json
{
  "question": "Show me orders from Antarctica",
  "sql": "SELECT * FROM gold.v_orders WHERE country_code = 'AQ'",
  "columns": ["order_id","order_number","order_date","ordered_at","status","total_amount","currency_code","customer_id","customer_name","customer_segment","country_code","warehouse_id","warehouse_code","warehouse_name"],
  "data": [],
  "row_count": 0,
  "truncated": false,
  "error": null,
  "commentary": "No orders found for that filter.",
  "canonical_query": null,
  "canonical_score": null,
  "tool_calls_made": 3,
  "iterations": 3,
  "duration_seconds": 4.1,
  "input_tokens": 1240,
  "output_tokens": 87
}
```

### 5. Large result — a top-products listing (showcase truncation indicator)

```json
{
  "question": "Top selling products last quarter",
  "sql": "SELECT * FROM gold.v_top_products_quarter LIMIT 200",
  "columns": ["product_name","product_category","units_sold","revenue","currency_code"],
  "data": [
    {"product_name":"Coffee #51","product_category":"Beverages","units_sold":315,"revenue":18900.0,"currency_code":"PEN"},
    {"product_name":"Soda #12","product_category":"Beverages","units_sold":280,"revenue":11200.0,"currency_code":"PEN"},
    {"product_name":"Disinfectant #18","product_category":"Cleaning","units_sold":180,"revenue":8910.0,"currency_code":"PEN"},
    {"product_name":"Granola #74","product_category":"Snacks","units_sold":210,"revenue":8400.0,"currency_code":"PEN"},
    {"product_name":"Pens Black #21","product_category":"Office Supplies","units_sold":1200,"revenue":7200.0,"currency_code":"PEN"}
  ],
  "row_count": 200,
  "truncated": true,
  "error": null,
  "commentary": "[canonical:top_product_last_quarter score=0.85]",
  "canonical_query": "top_product_last_quarter",
  "canonical_score": 0.85,
  "tool_calls_made": 0,
  "iterations": 0,
  "duration_seconds": 0.07,
  "input_tokens": 0,
  "output_tokens": 0
}
```

### 6. `GET /canonical` mock (for empty-state suggestion chips)

```json
{
  "queries": [
    {"name":"accounts_receivable_total","aliases":["how much are we owed","total receivables"],"description":"Sum of unpaid invoice amounts."},
    {"name":"ar_aging","aliases":["ar aging","accounts receivable aging","overdue breakdown"],"description":"AR aging by customer with 30/60/90+ buckets."},
    {"name":"top_customers_ytd","aliases":["top customers this year","biggest customers"],"description":"Top customers by fiscal-year-to-date revenue."},
    {"name":"top_product_last_quarter","aliases":["best selling product last quarter","top seller"],"description":"Single best-selling product in the last 3 months."},
    {"name":"monthly_expenses","aliases":["expense report","expenses by month"],"description":"Total expenses grouped by month and category."},
    {"name":"overdue_invoices","aliases":["overdue invoices","late invoices"],"description":"Unpaid invoices past their due date."},
    {"name":"low_stock","aliases":["low stock items","what to reorder"],"description":"Products at or below their reorder threshold."},
    {"name":"late_deliveries","aliases":["late deliveries","slow shipments"],"description":"Delivered shipments that took longer than 7 days."}
  ]
}
```

### 7. `GET /traces` mock (for the audit view)

```json
[
  {"question":"How much are we owed?","final_sql":"SELECT * FROM gold.v_ar_aging ...","success":true,"canonical_query":"ar_aging","duration_seconds":0.04,"user_id":"alice@acme.com","user_role":"finance_manager","started_at":1745800000.0},
  {"question":"Top selling products last quarter","final_sql":"SELECT * FROM gold.v_top_products_quarter LIMIT 200","success":true,"canonical_query":"top_product_last_quarter","duration_seconds":0.07,"user_id":"bob@acme.com","user_role":"ops","started_at":1745799980.0},
  {"question":"Which products had the highest sales growth ...","final_sql":"WITH last_month AS (...)","success":true,"canonical_query":null,"duration_seconds":9.4,"user_id":"alice@acme.com","user_role":"finance_manager","started_at":1745799900.0},
  {"question":"What's the warranty terms on shipment 12345","final_sql":"","success":false,"canonical_query":null,"duration_seconds":6.2,"user_id":"carla@acme.com","user_role":"ops","started_at":1745799820.0}
]
```

---

## What "done" looks like for the design phase

You'll know the artifact is good when:

1. An empty-state user can pick a suggested question and get to a
   well-rendered table in one click.
2. The visual difference between a canonical hit and an agent run is
   immediately obvious — the user can tell at a glance whether the
   answer is "verified" or "freshly generated".
3. Tables with currency, dates, and numeric columns format correctly
   without looking like a spreadsheet vomit.
4. Errors are clear and recoverable without panic.
5. The "Show SQL" reveal feels deliberate — power-users find it,
   non-technical users ignore it.
6. The whole layout looks at home next to a real Linear / Notion /
   Stripe Dashboard.

Once the artifact is at that bar, bring the JSX back into this repo
under `web/` (e.g. as a Vite + React + Tailwind app), wire up
`fetch('/ask')` to the FastAPI backend (CORS is open for `*` in dev),
and ship.
