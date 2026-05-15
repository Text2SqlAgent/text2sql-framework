# Business-oriented improvements

This branch adds four features tailored to the use case of running text2sql
as a service for companies (logistics, finance, operations) where the
consumers are management/finance staff querying a consolidated database
fed from ERPs, scattered files, and OCR'd physical documents.

The upstream library is excellent for the *ad-hoc* shape of NL→SQL — but for
a paying customer running the same finance questions repeatedly, it leaves
the following gaps:

| Gap | Why it matters in this business | Fix in this branch |
|---|---|---|
| Same question can produce slightly different SQL across runs | Finance leadership notices when "revenue Q3" returns 1,402,330 one day and 1,402,318 the next | **Canonical queries** |
| Latency: 5–30s of agent exploration per question | Bad chat UX, especially for repeat queries the model already "knows" how to answer | **Canonical queries** |
| Cost: every question = a frontier-model loop | Multiplies per customer, per dashboard load | **Canonical queries** |
| No record of *who* asked *what* | Finance/SOX-flavored audit needs identity on every query | **User context on `ask()`** |
| Result is `list[dict]` only | "Generate an expense report" implies a file deliverable | **CSV export on `SQLResult`** |
| Read-only protection is regex-based only | A misconfigured connection string for a customer = real risk | **DB-level read-only verification** |

Nothing here changes the upstream API surface — all additions are opt-in.

---

## 1. Canonical queries (`text2sql/canonical.py`)

A vetted SQL template store. Define your top ~20 business questions in a
markdown file; when a user asks one of them, the SDK runs the vetted SQL
directly instead of invoking the LLM agent.

**Why:** for the questions that matter ("how much are we owed", "top
product last quarter", "monthly expense report"), determinism, latency
(milliseconds vs. seconds), and cost ($0 vs. ~$0.05/query) all matter
more than ad-hoc flexibility. The agent should only fire for the long
tail of unusual questions.

### File format (`canonical.md`)

Same `## heading` style as `scenarios.md`:

```markdown
## accounts_receivable_total
aliases: how much are we owed, total receivables, ar balance, money owed to us
description: Sum of unpaid invoice amounts.

```sql
SELECT SUM(amount_due) AS total_owed
FROM invoices
WHERE status = 'unpaid'
```
```

Each entry has:
- A `name` (the `## heading`) — also used as a token source for matching.
- An optional `aliases:` line — comma-separated phrases the user might ask.
- An optional `description:` line — surfaced in `result.commentary`.
- A required ```sql code block — the vetted query that will be executed.

### Usage

```python
from text2sql import TextSQL

engine = TextSQL(
    "postgresql://localhost/acme",
    canonical_queries="canonical.md",       # opt-in
    canonical_threshold=0.6,                # tune for false-positive risk
    trace_file="traces.jsonl",
)

# This matches the AR canonical and runs the vetted SQL — no LLM.
result = engine.ask("How much are we owed?")
print(result.sql)         # the canonical SQL
print(result.commentary)  # "[canonical:accounts_receivable_total score=1.00] ..."

# This does NOT match — falls through to the agent as usual.
result = engine.ask("Which warehouse had the most damaged shipments last week?")
```

### Matcher

Deterministic and explainable. For each canonical query:

1. Tokenize `name + aliases`, drop stopwords (`the`, `how`, `much`, …).
2. Tokenize the user's question, drop stopwords.
3. `score = |overlap| / min(|question_tokens|, |canonical_tokens|)`.
4. Pick the highest-scoring query above `match_threshold`.

If no query clears the threshold, the agent runs as normal. There is no
LLM in the matching path — it is fast and cheap.

### Trace fields

Canonical runs are recorded in traces so you can audit them:

- `canonical_query` — the matched canonical name (or `None` for agent runs).
- `canonical_score` — the match confidence.

This lets you answer "what fraction of questions hit a canonical?" and
"which canonicals are actually used vs. dead weight?" with `engine.trace_summary()`.

---

## 2. User context on `ask()` (audit trail)

Every `ask()` now accepts `user_id`, `user_role`, and free-form `metadata`,
which propagate into the trace record:

```python
result = engine.ask(
    "Top customer this month",
    user_id="alice@acme.com",
    user_role="finance_manager",
    metadata={"session_id": "abc-123", "tenant": "acme"},
)
```

In the JSONL trace, you get `user_id`, `user_role`, `user_metadata` on
every record. Wire it to your audit pipeline however you want — most
likely a daily job that ingests the JSONL into your audit DB.

**Why:** for finance/regulated customers, "who asked what when, and what
SQL ran, and what was returned" must be answerable on demand. The library
already captured the SQL and result; now it captures who.

This is foundational for two things you will eventually need:
- **Permissions / row-level scopes** — enforced at the DB layer via separate
  roles + views per persona. The framework can record which scope was active.
- **Per-user analytics** — usage-based billing, abuse detection, "this
  manager asks for the same report 12× a day, let's pin it as a canonical".

---

## 3. Result export — `SQLResult.to_csv()` and `to_dict_list()`

```python
result = engine.ask("All Q1 expenses by category")
result.to_csv("/var/reports/q1_expenses.csv")
```

`to_csv(path)` writes a UTF-8 CSV with header, creates parent directories,
and returns the path written. Empty results write an empty file (so a
downstream "did the report run?" check just looks for the file's existence).

`to_dict_list()` is the explicit accessor for the `data` field — useful
when you want to be unambiguous in caller code (e.g. handing rows to a
templating engine for PDF generation).

**Why:** "generate an expense report with X" is a real business request
that ends in a file artifact, not a list of dicts in a Python REPL. This
is the smallest change that bridges "query result" → "deliverable".

For Excel output, layer `pandas` on top in your application code — kept
out of the dependency footprint here.

---

## 4. Read-only DB verification (`Database.verify_read_only`)

```python
engine = TextSQL(
    "postgresql://app_user@.../acme",
    enforce_read_only=True,   # raises PermissionError if user can write
)
```

`Database.verify_read_only()` probes the connection by attempting a
`CREATE TEMPORARY TABLE` inside a rolled-back transaction:

- If the write **fails**, the connection is read-only ✓.
- If the write **succeeds**, the connection is writable ✗ — the rollback
  reverts it, but the framework now knows.

Behavior:
- **Default (`enforce_read_only=False`)** — soft check. If the connection
  is writable, log a `UserWarning`. Useful in dev.
- **`enforce_read_only=True`** — hard check. Raise `PermissionError` at
  `TextSQL(...)` construction time if the connection is writable. Use this
  in production.

**Why:** the `execute_sql` tool blocks destructive SQL via a regex
allowlist. That is necessary but not sufficient — for a customer
deployment you want privilege boundaries enforced by the DB itself, with
the framework as defense-in-depth. A misconfigured connection string in
a deployment YAML should fail loudly, not silently.

Recommended customer setup:

1. Create a read-only DB user (`GRANT SELECT` on the analytics schema only).
2. Use that user's connection string in `TextSQL(...)`.
3. Pass `enforce_read_only=True`.
4. The regex in `tools.py` is then defense in depth, not the only barrier.

---

## What this branch does NOT do (deliberately)

These were considered and dropped because the right answer for each
lives outside the framework, in your service layer:

- **Row-level scopes / multi-tenant filtering.** Best handled by separate
  DB users and views, not by injecting `WHERE tenant_id = ?` into agent SQL
  (fragile, attackable, hard to verify).
- **Conversation history / follow-ups.** Belongs in your chat session
  store, not in the SDK.
- **Excel/PDF report templating.** Application concern; layer on top.
- **Web chat UI.** Out of scope for an SDK.
- **OCR / ingestion / scheduling.** Different products entirely — text2sql
  starts where the data is already in a SQLAlchemy-reachable DB.

Your moat as a business is in the ingestion + consolidation pipeline above
the DB and the report-delivery layer below it; text2sql is the middle
slice that turns NL into vetted SQL. This branch makes that middle slice
suitable for *production multi-customer use*, not just demos.

---

## Files changed

```
text2sql/canonical.py               (new)        canonical query store + matcher
text2sql/core.py                    (modified)   wire canonical + user context + read-only
text2sql/generate.py                (modified)   user context kwargs; to_csv/to_dict_list
text2sql/connection.py              (modified)   verify_read_only()
text2sql/tracing.py                 (modified)   user_id/user_role/canonical fields
text2sql/__init__.py                (modified)   export new symbols
tests/test_canonical.py             (new)        17 tests for matching + loading
tests/test_business_features.py     (new)        4 tests for export + read-only
examples/canonical.md               (new)        starter template
IMPROVEMENTS.md                     (new)        this file
```

## Test status

```
new tests:  21 passed
existing:   32 passed (13 errors are pre-existing Windows file-handle issues
            in the upstream test fixture, unrelated to this branch)
```
