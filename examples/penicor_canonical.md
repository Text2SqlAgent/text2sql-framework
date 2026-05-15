# Canonical queries — Penicor

Vetted SQL templates for the questions Penicor's management/finance staff
ask most often. Pass this file as `canonical_queries=` to `TextSQL(...)`.

All queries run against the `gold` schema (views over `silver`). Aliases
include both English and Spanish since end users speak Spanish.

## ar_total
aliases: how much are we owed, total receivables, ar balance, money owed to us, cuanto nos deben, total deuda, deuda total, deuda clientes
description: Total open AR across all customers, broken out by currency.

```sql
SELECT
  currency_code,
  ROUND(total_owed_cents / 100.0, 2) AS total_owed,
  open_doc_count,
  customers_with_balance
FROM gold.v_ar_total
ORDER BY total_owed DESC
```

## ar_aging
aliases: ar aging, accounts receivable aging, overdue breakdown, aging report, deuda por cliente, antiguedad de deuda, vencidos
description: Per-customer aging buckets — not yet due, 1-30 / 31-60 / 60+ days overdue.

```sql
SELECT
  customer_name,
  sales_zone,
  currency_code,
  ROUND(COALESCE(not_yet_due_cents, 0) / 100.0, 2)      AS not_yet_due,
  ROUND(COALESCE(overdue_1_30_cents, 0) / 100.0, 2)     AS overdue_1_30,
  ROUND(COALESCE(overdue_31_60_cents, 0) / 100.0, 2)    AS overdue_31_60,
  ROUND(COALESCE(overdue_60_plus_cents, 0) / 100.0, 2)  AS overdue_60_plus,
  ROUND(total_owed_cents / 100.0, 2)                    AS total_owed,
  open_doc_count,
  max_days_overdue
FROM gold.v_ar_aging
ORDER BY total_owed_cents DESC
LIMIT 50
```

## top_customers_by_revenue
aliases: top customers, biggest customers, best customers, top customers by revenue, mejores clientes, top clientes, principales clientes, clientes que mas compran, top clientes por ingresos
description: Top 10 customers by net revenue across the loaded period.

```sql
SELECT
  customer_name,
  ROUND(net_cents / 100.0, 2) AS net_revenue,
  invoice_count,
  units_sold,
  source_branch_count,
  last_sale_date
FROM gold.v_revenue_by_customer
ORDER BY net_cents DESC
LIMIT 10
```

## customer_count
aliases: how many customers, total customers, number of customers, count customers, cuantos clientes, total de clientes, numero de clientes
description: Total count of canonical (deduped) active customers.

```sql
SELECT COUNT(*) AS customer_count
FROM gold.v_customers
```

## top_products
aliases: top products, best sellers, best selling products, top sellers, mejores productos, productos mas vendidos, top articulos
description: Top 10 products by units sold across the loaded period.

```sql
SELECT
  product_name,
  product_family,
  product_brand,
  units_sold,
  cases_sold,
  ROUND(net_cents / 100.0, 2) AS net_revenue,
  distinct_customers,
  invoice_count
FROM gold.v_revenue_by_product
ORDER BY units_sold DESC
LIMIT 10
```

## monthly_revenue
aliases: monthly revenue, revenue trend, sales by month, ventas mensuales, ventas por mes, ingresos mensuales, evolucion ventas
description: Monthly net revenue trend by currency.

```sql
SELECT
  TO_CHAR(month, 'YYYY-MM') AS month,
  currency_code,
  ROUND(net_cents / 100.0, 2) AS net_revenue,
  invoice_count,
  active_customers
FROM gold.v_revenue_monthly
ORDER BY month DESC, currency_code
```

## salesperson_ranking
aliases: top salespeople, salesperson ranking, best reps, vendedores ranking, mejores vendedores, top vendedores, ranking vendedores
description: Salespeople ranked by net revenue across the loaded period.

```sql
SELECT
  salesperson_name,
  ROUND(net_cents / 100.0, 2) AS net_revenue,
  invoice_count,
  distinct_customers,
  first_sale_date,
  last_sale_date
FROM gold.v_revenue_by_salesperson
ORDER BY net_cents DESC
```

## monthly_collections
aliases: monthly collections, payments trend, collection trend, cobranzas mensuales, pagos por mes, recaudacion mensual
description: Monthly customer collections by currency.

```sql
SELECT
  TO_CHAR(month, 'YYYY-MM') AS month,
  currency_code,
  ROUND(amount_cents / 100.0, 2) AS amount_collected,
  payment_count,
  paying_customers
FROM gold.v_payments_monthly
ORDER BY month DESC, currency_code
```

## low_stock
aliases: low stock, understocked items, restock list, items running out, stock bajo, faltantes, productos faltantes, reposicion
description: Products currently below their minimum stock threshold.

```sql
SELECT
  warehouse_name,
  product_name,
  product_family,
  quantity,
  min_quantity,
  shortfall
FROM gold.v_inventory_low
ORDER BY shortfall DESC
LIMIT 50
```

## customer_count_by_zone
aliases: customers by zone, customer distribution, clientes por zona, distribucion clientes, clientes por barrio
description: Number of customers per sales zone.

```sql
SELECT
  COALESCE(sales_zone, 'N/A') AS sales_zone,
  COUNT(*) AS customer_count,
  COUNT(*) FILTER (WHERE NOT is_prospect) AS active_customers,
  COUNT(*) FILTER (WHERE is_prospect)     AS prospects
FROM gold.v_customers
GROUP BY sales_zone
ORDER BY customer_count DESC
```

## most_overdue_customers
aliases: most overdue, worst payers, who is most overdue, peores pagadores, clientes mas vencidos, clientes morosos
description: Customers with the largest overdue balances (60+ days past due).

```sql
SELECT
  customer_name,
  sales_zone,
  currency_code,
  ROUND(COALESCE(overdue_60_plus_cents, 0) / 100.0, 2)  AS overdue_60_plus,
  ROUND(total_owed_cents / 100.0, 2)                    AS total_owed,
  max_days_overdue
FROM gold.v_ar_aging
WHERE COALESCE(overdue_60_plus_cents, 0) > 0
ORDER BY overdue_60_plus_cents DESC
LIMIT 25
```
