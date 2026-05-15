# Demo ETL pipeline

End-to-end loader that produces a believable demo of the full system:
synthetic logistics data → bronze → silver → gold → materialized views,
runnable against the schema in `db/migrations/`.

Use this to:
- Smoke-test that the framework, schema, and data flow agree.
- Build the chat UI against a working backend before any real customer.
- Demo the canonical queries and agent behaviour to prospects.

## What gets loaded (defaults)

| Entity | Count |
|---|---|
| customers | 50 |
| products | 100 |
| warehouses | 5 |
| vehicles | 10 |
| employees | 20 |
| suppliers | 5 |
| orders | ~1,100 (across 24 months) |
| order items | ~4,000 |
| invoices | ~900 |
| payments | ~1,150 |
| shipments | ~850 |
| shipment items | ~2,900 |
| inventory movements | ~5,200 |
| expenses | ~480 |

Mix is realistic: most invoices are paid, some partial, some overdue;
~85% of fulfilled orders ship; 5% of shipments take >7 days (so
`v_late_deliveries` returns rows); inventory has receipts plus
shipment-driven outflows.

## Prerequisites

1. A Postgres 16+ database with `db/migrations/` already applied.
2. `gold.tenant_config` does NOT need to be pre-populated — the loader will
   insert a default row for "Acme Logistics S.A." if the table is empty.
3. Connect as `etl_writer` (or a superuser).

## Run

```bash
# from the repo root
python etl/run_demo.py "postgresql://etl_writer:pwd@host:5432/customer_demo"
```

Re-runnable: every invocation truncates bronze, silver, and the gold
fact + dim tables (preserving `dim_date`, `dim_currency`, and
`tenant_config`), then reloads from a deterministically generated
synthetic dataset.

Optional flags:
- `--seed <N>` — vary the synthetic data (default 42).
- `--months-back <N>` — date-range depth (default 24).

The loader prints per-step timing and a row-count verification at the end.

## Pipeline stages

1. **Apply bronze + silver schemas.** Idempotent `CREATE TABLE IF NOT EXISTS`.
2. **Ensure tenant_config + monthly partitions.** Auto-creates partitions
   covering the synth date range.
3. **Truncate.** Wipes bronze, silver, and gold facts/dims for re-runnability.
4. **Generate synth.** All entities produced as Python dicts — deterministic
   given the seed.
5. **Load bronze.** All values coerced to `TEXT` (mimics raw CSV/OCR ingest).
6. **Bronze → silver.** Type casts, normalization, dedup-by-natural-key.
7. **Silver → gold dims.** `dim_customer` / `dim_product` get SCD2 columns
   set with `is_current=true`. Other dims are simple inserts.
8. **Silver → gold facts.** Joins on natural keys to dim tables to resolve
   surrogate IDs. Date-partitioned facts insert into the right partition.
9. **Refresh materialized views.** `v_revenue_monthly`, `v_monthly_expenses`.
10. **Verify.** Print row counts for every gold table.

## After loading — try the agent

```python
from text2sql import TextSQL

engine = TextSQL(
    "postgresql://agent_reader:pwd@host:5432/customer_demo?options=-csearch_path%3Dgold",
    canonical_queries="examples/canonical.md",
    enforce_read_only=True,
    trace_file="traces/demo.jsonl",
)

result = engine.ask("How much are we owed?")
print(result.sql, result.data)
print(result.commentary)   # "[canonical:accounts_receivable_total ...]"

result = engine.ask("Top product last quarter")
print(result.sql, result.data)
```

(Both should hit canonical queries when the agent maps onto names/aliases
in `examples/canonical.md` — adapt the markdown to reference the demo
schema's view names like `gold.v_top_products_quarter`.)

## What's NOT in here

- **Currency rates.** `dim_currency_rate` is empty after the load. Demo
  views that report only in non-base currency just won't convert. For a
  real customer, ETL would pull daily rates from a provider.
- **Historical SCD2 changes.** Every customer/product gets exactly one
  version with `is_current=true`. Real ETL would detect changes and add
  new versions over time.
- **Document references.** `dim_document` is empty — no synthetic PDFs.
- **Holidays in `dim_date`.** All `is_holiday=false`.

## Tests

```bash
python -m pytest tests/test_synth.py -q
```

12 tests verify the synth output is deterministic and realistically
shaped. The orchestrator itself isn't unit-tested (needs a real
Postgres) — run it against a temp DB to smoke-test changes.
