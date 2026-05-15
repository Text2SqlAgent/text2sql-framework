-- ============================================================================
-- 0006_views_metrics.sql
--
-- Tier-2 / Tier-3 metric views — pre-aggregated business metrics. Each one
-- backs exactly one canonical query in canonical.md so that "how much
-- are we owed" → "SELECT * FROM v_ar_aging" with no agent reasoning.
--
-- Materialized for the heavy aggregations; refresh on the same schedule
-- as ETL via:
--     REFRESH MATERIALIZED VIEW CONCURRENTLY gold.v_revenue_monthly;
--
-- Idempotent (DROP + CREATE for materialized views, since CREATE OR REPLACE
-- isn't supported for them in Postgres).
-- ============================================================================


-- ---------- v_ar_aging (regular view — cheap to compute on demand) --------

CREATE OR REPLACE VIEW gold.v_ar_aging AS
SELECT
  i.customer_id,
  i.customer_name,
  i.country_code,
  SUM(i.amount_due_cents) FILTER (WHERE i.due_date >= CURRENT_DATE) / 100.0
    AS not_yet_due,
  SUM(i.amount_due_cents) FILTER (WHERE CURRENT_DATE - i.due_date BETWEEN 1 AND 30) / 100.0
    AS overdue_1_30,
  SUM(i.amount_due_cents) FILTER (WHERE CURRENT_DATE - i.due_date BETWEEN 31 AND 60) / 100.0
    AS overdue_31_60,
  SUM(i.amount_due_cents) FILTER (WHERE CURRENT_DATE - i.due_date BETWEEN 61 AND 90) / 100.0
    AS overdue_61_90,
  SUM(i.amount_due_cents) FILTER (WHERE CURRENT_DATE - i.due_date > 90) / 100.0
    AS overdue_90_plus,
  SUM(i.amount_due_cents) / 100.0 AS total_owed,
  i.currency_code
FROM gold.v_invoices i
WHERE i.status IN ('unpaid', 'partial') AND i.amount_due_cents > 0
GROUP BY i.customer_id, i.customer_name, i.country_code, i.currency_code;

COMMENT ON VIEW gold.v_ar_aging IS
  'AR aging by customer with standard 30/60/90+ buckets. The default '
  'answer to "what''s our AR aging" or "who owes us the most overdue".';


-- ---------- v_ap_aging (similar, for payables — minimal, expand per customer if relevant)

-- Skipped for now: most logistics customers don't ingest vendor bills with
-- the granularity needed for AP aging. Add a fact_bills table if needed.


-- ---------- v_revenue_monthly (materialized) ------------------------------

DROP MATERIALIZED VIEW IF EXISTS gold.v_revenue_monthly;
CREATE MATERIALIZED VIEW gold.v_revenue_monthly AS
SELECT
  DATE_TRUNC('month', i.invoice_date)::DATE AS month,
  i.currency_code,
  SUM(i.amount_billed_cents) / 100.0    AS revenue,
  SUM(i.amount_paid_cents)   / 100.0    AS revenue_collected,
  SUM(i.amount_due_cents)    / 100.0    AS revenue_outstanding,
  COUNT(*)                              AS invoice_count
FROM gold.fact_invoices i
WHERE i.is_void = false
GROUP BY 1, i.currency_code;

CREATE UNIQUE INDEX IF NOT EXISTS uq_v_revenue_monthly
  ON gold.v_revenue_monthly (month, currency_code);

COMMENT ON MATERIALIZED VIEW gold.v_revenue_monthly IS
  'Monthly billed/collected/outstanding revenue per currency. Refreshed '
  'after each ETL run via REFRESH MATERIALIZED VIEW CONCURRENTLY.';


-- ---------- v_top_customers_ytd (regular view, fiscal-year-aware) ---------

CREATE OR REPLACE VIEW gold.v_top_customers_ytd AS
WITH cfg AS (
  SELECT fiscal_year_start_month FROM gold.tenant_config LIMIT 1
),
fiscal_year_start AS (
  SELECT
    -- The most recent occurrence of (month=fiscal_year_start_month, day=1)
    -- that is on or before today.
    make_date(
      CASE
        WHEN EXTRACT(MONTH FROM CURRENT_DATE) >= (SELECT fiscal_year_start_month FROM cfg)
          THEN EXTRACT(YEAR FROM CURRENT_DATE)::INT
        ELSE EXTRACT(YEAR FROM CURRENT_DATE)::INT - 1
      END,
      (SELECT fiscal_year_start_month FROM cfg)::INT,
      1
    ) AS fy_start
)
SELECT
  i.customer_id,
  i.customer_name,
  i.country_code,
  SUM(i.amount_billed_cents) / 100.0 AS revenue_ytd,
  i.currency_code
FROM gold.v_invoices i
CROSS JOIN fiscal_year_start fys
WHERE i.invoice_date >= fys.fy_start
  AND i.status IN ('paid','partial','unpaid')
GROUP BY i.customer_id, i.customer_name, i.country_code, i.currency_code
ORDER BY revenue_ytd DESC;

COMMENT ON VIEW gold.v_top_customers_ytd IS
  'Customers ranked by fiscal-year-to-date billed revenue. Uses '
  'tenant_config.fiscal_year_start_month for the YTD cutoff.';


-- ---------- v_top_products_quarter (last 3 calendar months) ---------------

CREATE OR REPLACE VIEW gold.v_top_products_quarter AS
SELECT
  oi.product_id,
  oi.sku,
  oi.product_name,
  oi.product_category,
  SUM(oi.quantity)         AS units_sold,
  SUM(oi.line_total_cents) / 100.0 AS revenue,
  oi.currency_code
FROM gold.v_order_items oi
WHERE oi.order_date >= CURRENT_DATE - INTERVAL '3 months'
GROUP BY oi.product_id, oi.sku, oi.product_name, oi.product_category, oi.currency_code
ORDER BY units_sold DESC;

COMMENT ON VIEW gold.v_top_products_quarter IS
  'Products ranked by units sold in the last 3 calendar months. The '
  'default for "top product last quarter" type questions.';


-- ---------- v_late_deliveries ---------------------------------------------
-- Definition: a delivered shipment is "late" if delivered_at - shipped_at
-- exceeded the 95th percentile transit time for that origin→destination
-- pair OR took more than 7 days. Lightweight definition usable everywhere
-- without a customer-specific SLA table.

CREATE OR REPLACE VIEW gold.v_late_deliveries AS
SELECT
  s.shipment_id,
  s.shipment_number,
  s.ship_date,
  s.delivered_at,
  s.transit_hours,
  s.origin_city,
  s.destination_city,
  s.customer_name,
  s.warehouse_name
FROM gold.v_shipments s
WHERE s.status = 'delivered'
  AND s.delivered_at IS NOT NULL
  AND s.transit_hours > 24 * 7;   -- > 7 days

COMMENT ON VIEW gold.v_late_deliveries IS
  'Delivered shipments with transit time > 7 days. Replace this definition '
  'per customer (e.g. SLA-based) by overriding the view.';


-- ---------- v_low_stock ----------------------------------------------------

CREATE OR REPLACE VIEW gold.v_low_stock AS
SELECT
  product_id,
  sku,
  product_name,
  product_category,
  warehouse_id,
  warehouse_code,
  warehouse_name,
  units_on_hand,
  reorder_threshold
FROM gold.v_inventory_status
WHERE is_below_reorder_threshold = true
ORDER BY units_on_hand ASC;

COMMENT ON VIEW gold.v_low_stock IS
  'Items at or below their reorder threshold. The default for "what should '
  'we reorder" questions.';


-- ---------- v_overdue_invoices --------------------------------------------

CREATE OR REPLACE VIEW gold.v_overdue_invoices AS
SELECT
  invoice_id,
  invoice_number,
  customer_id,
  customer_name,
  invoice_date,
  due_date,
  CURRENT_DATE - due_date AS days_overdue,
  amount_due,
  currency_code
FROM gold.v_invoices
WHERE status IN ('unpaid', 'partial')
  AND amount_due_cents > 0
  AND due_date < CURRENT_DATE
ORDER BY days_overdue DESC;

COMMENT ON VIEW gold.v_overdue_invoices IS
  'Unpaid/partial invoices with due_date in the past, sorted by '
  'days_overdue DESC.';


-- ---------- v_monthly_expenses (materialized) ----------------------------

DROP MATERIALIZED VIEW IF EXISTS gold.v_monthly_expenses;
CREATE MATERIALIZED VIEW gold.v_monthly_expenses AS
SELECT
  DATE_TRUNC('month', expense_date)::DATE AS month,
  category,
  currency_code,
  SUM(amount_cents)         / 100.0 AS amount,
  SUM(tax_amount_cents)     / 100.0 AS tax_amount,
  SUM(amount_cents + tax_amount_cents) / 100.0 AS total_amount,
  COUNT(*)                          AS expense_count
FROM gold.fact_expenses
WHERE is_void = false
GROUP BY 1, category, currency_code;

CREATE UNIQUE INDEX IF NOT EXISTS uq_v_monthly_expenses
  ON gold.v_monthly_expenses (month, category, currency_code);

COMMENT ON MATERIALIZED VIEW gold.v_monthly_expenses IS
  'Monthly expenses grouped by category and currency. Refreshed after each '
  'ETL run.';
