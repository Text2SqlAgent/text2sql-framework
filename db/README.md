# Customer database — schema migration pack

This folder provisions the **gold-layer schema** described in
`IDEAL_DATABASE.md` for one customer. Run it once per customer, against an
empty Postgres database we own.

## What you get after running

- Three schemas: `bronze`, `silver`, `gold`.
- Three roles: `etl_writer`, `service_admin`, `agent_reader`.
- A singleton `gold.tenant_config` row (you insert it — see step 3 below).
- Reference dimensions: `dim_date` (10 years), `dim_currency` (25 codes),
  `dim_currency_rate` (empty — populated by ETL).
- SCD2 dimensions: `dim_customer`, `dim_product`. Plain dims:
  `dim_warehouse`, `dim_vehicle`, `dim_supplier`, `dim_employee`,
  `dim_document`.
- Partitioned facts (12 prior + 12 forward monthly partitions seeded):
  `fact_orders`, `fact_invoices`, `fact_shipments`. Plus
  `fact_order_items`, `fact_payments`, `fact_shipment_items`,
  `fact_inventory_movements`, `fact_expenses`.
- Entity views: `v_customers`, `v_products`, `v_orders`, `v_order_items`,
  `v_invoices`, `v_payments`, `v_shipments`, `v_inventory_status`,
  `v_expenses`.
- Metric views: `v_ar_aging`, `v_revenue_monthly` (mat),
  `v_top_customers_ytd`, `v_top_products_quarter`, `v_late_deliveries`,
  `v_low_stock`, `v_overdue_invoices`, `v_monthly_expenses` (mat).

The agent only ever sees `gold`. ETL fills `bronze` (raw landing) and
`silver` (cleaned), then transforms into `gold`.

## Prerequisites

- Postgres 16+ with `CREATE` rights as a superuser (RDS master role works).
- Python 3.10+ with `sqlalchemy` and `psycopg2-binary`.

## How to run

### 1. Provision an empty database

```sql
CREATE DATABASE customer_acme;
```

### 2. Run migrations 0001–0006 first

These create schemas, roles, dims, facts, and views — but `0007_seeds.sql`
needs `tenant_config` populated, so we run in two passes.

```bash
# from the repo root
python db/apply_migrations.py "postgresql://postgres:pwd@host:5432/customer_acme"
```

This will fail at `0007_seeds.sql` because `gold.tenant_config` is empty.
That's expected for a fresh DB — go to step 3.

### 3. Insert the tenant_config row

```sql
INSERT INTO gold.tenant_config (
  customer_legal_name,
  customer_short_name,
  base_currency_code,
  fiscal_year_start_month,
  reporting_timezone
) VALUES (
  'Acme Logistics S.A.',
  'Acme',
  'PEN',     -- whatever the customer reports in
  1,         -- fiscal year starts in January
  'America/Lima'
);
```

### 4. Re-run migrations (now 0007 + 0008 succeed)

```bash
python db/apply_migrations.py "postgresql://postgres:pwd@host:5432/customer_acme"
```

Migrations are idempotent — re-running is safe and skips already-applied
DDL.

### 5. Set passwords on the three roles

```sql
ALTER ROLE etl_writer    LOGIN PASSWORD '...';
ALTER ROLE service_admin LOGIN PASSWORD '...';
ALTER ROLE agent_reader  LOGIN PASSWORD '...';
```

Stash these in your secret manager. The chat UI uses `agent_reader` only.

### 6. Verify the read-only boundary

```python
from text2sql import TextSQL

engine = TextSQL(
    "postgresql://agent_reader:pwd@host:5432/customer_acme?options=-csearch_path%3Dgold",
    enforce_read_only=True,   # raises if the role can write — should pass
)
```

## What's NOT in here

- **bronze/silver tables**: per-source-system, built per customer when ETL
  is wired up. The schemas exist; populate as needed.
- **`fact_bills` / AP detail**: most logistics customers don't need it.
  Add when a customer ingests vendor bills with line detail.
- **Currency rate ingestion**: `dim_currency_rate` is empty after this
  pack runs. ETL pulls daily rates from a provider and upserts.
- **Holiday calendar**: `dim_date.is_holiday` defaults to `false`. Update
  per customer (admin tool will eventually do this).
- **Per-row permissions** (e.g. warehouse manager scopes): out of scope.
  Add Postgres RLS policies when needed.

## Refreshing materialized views

After every ETL pass, the orchestrator should run:

```sql
REFRESH MATERIALIZED VIEW CONCURRENTLY gold.v_revenue_monthly;
REFRESH MATERIALIZED VIEW CONCURRENTLY gold.v_monthly_expenses;
```

`CONCURRENTLY` requires the unique indexes that 0006 creates.

## Adding a new monthly partition

The pack seeds 12 months back and 12 forward. Once you're getting close
to running out:

```sql
SELECT gold.create_monthly_partition('fact_orders',    'order_date',   '2027-05-01');
SELECT gold.create_monthly_partition('fact_invoices',  'invoice_date', '2027-05-01');
SELECT gold.create_monthly_partition('fact_shipments', 'ship_date',    '2027-05-01');
```

Wire this into the ETL scheduler as a monthly cron.
