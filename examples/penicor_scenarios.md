## net revenue
Net revenue is `SUM(signed_net_cents)` from `gold.v_sales`. The `signed_*`
columns multiply by `affects_sale` (1 for sales, -1 for credit notes / returns),
so the SUM nets refunds out automatically. Do NOT use `SUM(net_cents)` —
that double-counts because credit notes appear with positive net_cents.

For total revenue including tax, use `SUM(signed_total_cents)`. For ex-tax
revenue, use `SUM(signed_net_cents)`. Spanish equivalents: ingresos netos,
ingresos brutos.

## active customers
A "canonical customer" (after dedup) is in `gold.v_customers`. There is
no built-in "active" flag based on recency. To filter by recent activity:
```
SELECT cm.canonical_customer_id, cm.customer_name
FROM gold.v_customers cm
WHERE cm.canonical_customer_id IN (
    SELECT DISTINCT canonical_customer_id FROM gold.v_sales
    WHERE fecha >= CURRENT_DATE - INTERVAL '90 days'
)
```
For "how many customers do we have" use simply `SELECT COUNT(*) FROM gold.v_customers`.

## top customers
Use `gold.v_revenue_by_customer` (already deduped by canonical_customer_id).
Order by `net_cents DESC` for revenue ranking. The column `source_branch_count`
tells how many physical source-system rows were rolled up into one canonical
entity (e.g., DEVOTO HNOS S.A. has source_branch_count = 23 because the
chain has 23 branches).

## customers, branches, and groups
Penicor's customer data has three grains:
- **Source branches** = `silver.clientes` rows, identified by `customer_branch_id`.
  One row per physical branch (e.g. "DEVOTO Nº1 - MALVIN").
- **Canonical customers** (legal entities) = `silver.customer_master` rows,
  identified by `canonical_customer_id`. One row per legal entity (e.g.
  "DEVOTO HNOS S.A." — a single row covering all 23 Devoto branches).
- **Groups** (optional) = `silver.customer_master` rows with `entity_kind='group'`
  and child rows pointing at them via `parent_id`. Currently unused for Penicor.

Default reporting grain is **legal entity** (canonical_customer_id). All `gold.v_*`
aggregations use this grain.

## ar / receivables
Open accounts receivable is `gold.v_open_ar` (one row per unpaid document) or
`gold.v_ar_total` (one number per currency). For aging buckets per customer
use `gold.v_ar_aging`. Aging buckets are exclusive: not_yet_due (due_date >= today),
1-30, 31-60, 60+ days past due_date. Spanish: deuda corriente, vencidos, antigüedad.

For "how much do customers owe us" use `gold.v_ar_total`. For "who owes us the
most" use `gold.v_ar_aging` ordered by `total_owed_cents DESC`.

## currency and money
All money columns are integer cents — divide by 100 for display.
Penicor's base currency is UYU (Uruguayan pesos). Some transactions are USD,
identified by `currency_code` on each row. The `exchange_rate` column (sourced
from AltoControl's cotizacion field) is the conversion rate to UYU at
transaction time. To convert a non-UYU amount to UYU equivalents, multiply
the cents column by exchange_rate.

For mixed-currency aggregations, GROUP BY currency_code so the user can see
each separately. Don't blindly sum across currencies.

## time periods
The data window currently loaded covers approximately 2026-01-02 through
2026-02-19 (Q1 2026). Q2 has no data yet. If the user asks for a period
outside the loaded window, return an empty result and explain the data
window rather than guessing.

For quarter filters, use `EXTRACT(QUARTER FROM fecha)`. For month, use
`DATE_TRUNC('month', fecha)`. For YTD, use `fecha >= DATE_TRUNC('year', CURRENT_DATE)`.

Spanish time terms: trimestre (quarter), mes (month), año (year), semana
(week), ayer (yesterday), hoy (today), última semana (last week).

## documents and invoices
Penicor uses Uruguayan e-fiscal documents (CFE — Comprobante Fiscal Electrónico).
Each invoice/credit-note row carries:
- `invoice_id` — the AltoControl-internal id
- `cfe_document` (on `gold.v_open_ar`) — the legal CFE number, e.g. "e-Ticket A 33891"
- `document_type` — text like 'Factura', 'Nota de Credito', etc.
- `affects_sale` (on v_sales) or `affects_ar` (on v_payments) — sign multiplier

When the user asks about a specific invoice, prefer searching by the legal CFE
number, not the internal invoice_id.

## salespeople and visits
Salesperson is identified by `canonical_salesperson_id` after dedup.
`gold.v_visits` tracks sales-rep visits to customers; this view may be empty
in early data slices because not all Penicor sales teams record visits.

For "top salesperson by revenue" use `gold.v_revenue_by_salesperson`.

## inventory and stock
`gold.v_inventory` is the current stock snapshot per (warehouse_id, canonical_product_id).
`gold.v_inventory_low` filters to items below their min_quantity threshold,
ordered by shortfall. `is_understocked` is a boolean from source data.

Note: Penicor's source system records min_quantity = 0 for many products,
which means `v_inventory_low` may show very few items. That's a source-data
limitation, not a query error.

## empresa scope (Penicor only)
The AltoControl Azure DB hosts data for 4 entities (Penicor, Indunet, plus
two others). All `silver.*` and `gold.*` data is filtered to `empresa = 'Penicor'`
during the silver transform. The agent should not see other entities. If
the user asks about Indunet or any non-Penicor company, return empty / explain
that scope is Penicor-only.
