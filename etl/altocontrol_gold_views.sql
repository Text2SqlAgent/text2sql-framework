-- gold.v_* — agent-facing views over silver for the Penicor POC.
--
-- Per IDEAL_DATABASE.md §3 + §10, the agent never sees bronze or silver
-- directly — only the gold layer. For this POC, gold is *just* views
-- (no real dim/fact tables yet). When we later invest in production
-- gold, these views become the contract: the SQL inside changes (point
-- to fact_* and dim_*), but the names + columns stay stable so canonical
-- queries don't break.
--
-- Conventions:
--   - All money columns named `<role>_cents` are integer cents in the
--     line currency. To convert to UYU: multiply by exchange_rate (the
--     `cotizacion` from source) where present.
--   - `signed_total_cents` (in v_sales/v_purchases) already applies the
--     sign carried by `affects_sale` so SUM correctly nets credit notes.
--   - Sales-rep visits are tracked but may be empty in the POC slice.
--
-- Idempotent: CREATE OR REPLACE.

CREATE SCHEMA IF NOT EXISTS gold;

-- ============================================================================
-- TIER 1 — entity views (silver joined, denormalized)
-- ============================================================================

CREATE OR REPLACE VIEW gold.v_sales AS
SELECT
    v.line_id,
    v.invoice_id,
    v.fecha,
    v.document_type,
    v.affects_sale,
    -- references
    v.customer_branch_id,
    v.customer_id,
    COALESCE(c.trade_name, c.legal_name)              AS customer_name,
    c.legal_name                                      AS customer_legal_name,
    c.sales_zone,
    c.delivery_zone,
    c.country_code,
    c.department                                      AS customer_department,
    v.product_id,
    a.product_name,
    a.family                                          AS product_family,
    a.brand                                           AS product_brand,
    a.category_1                                      AS product_category_1,
    v.salesperson_id,
    s.salesperson_name,
    -- price list / currency
    v.price_list,
    v.currency_code,
    v.exchange_rate,
    -- quantities
    v.measure_value,
    v.volume,
    v.quantity,
    v.cases,
    -- amounts (raw, in line currency cents)
    v.list_price_cents,
    v.unit_price_cents,
    v.subtotal_cents,
    v.discount_cents,
    v.net_cents,
    v.tax_cents,
    v.total_cents,
    -- amounts with sign applied (use these in SUMs to net credit notes)
    (v.subtotal_cents * v.affects_sale)               AS signed_subtotal_cents,
    (v.net_cents      * v.affects_sale)               AS signed_net_cents,
    (v.total_cents    * v.affects_sale)               AS signed_total_cents,
    (v.quantity       * v.affects_sale)               AS signed_quantity,
    (v.cases          * v.affects_sale)               AS signed_cases
FROM silver.ventas v
LEFT JOIN silver.clientes  c ON c.customer_branch_id = v.customer_branch_id
LEFT JOIN silver.articulos a ON a.product_id          = v.product_id
LEFT JOIN silver.vendedores s ON s.salesperson_id     = v.salesperson_id;

COMMENT ON VIEW gold.v_sales IS
  'Sales line items, denormalized with customer/product/salesperson context. '
  'One row per id_renglon. Includes both raw amounts and signed_* variants — '
  'use signed_* in SUM aggregations so credit notes (affects_sale = -1) cancel out.';

COMMENT ON COLUMN gold.v_sales.signed_total_cents IS
  'total_cents multiplied by affects_sale (-1 for credit notes, 1 for sales). '
  'SUM(signed_total_cents) gives net revenue across mixed sales+returns. Use this, '
  'not SUM(total_cents), unless you specifically want gross.';

COMMENT ON COLUMN gold.v_sales.exchange_rate IS
  'Source `cotizacion`. Multiply *_cents by exchange_rate to convert to UYU. '
  'For UYU-denominated transactions exchange_rate is 1.';


CREATE OR REPLACE VIEW gold.v_purchases AS
SELECT
    c.line_id,
    c.invoice_id,
    c.fecha,
    c.document_type,
    c.affects_sale,
    c.supplier_id,
    p.supplier_name,
    p.legal_name                                      AS supplier_legal_name,
    p.category                                        AS supplier_category,
    c.product_id,
    a.product_name,
    a.family                                          AS product_family,
    c.salesperson_id,
    s.salesperson_name,
    c.price_list,
    c.currency_code,
    c.exchange_rate,
    c.measure_value,
    c.volume,
    c.quantity,
    c.list_price_cents,
    c.unit_price_cents,
    c.subtotal_cents,
    c.discount_cents,
    c.net_cents,
    c.tax_cents,
    c.total_cents,
    (c.total_cents * c.affects_sale) AS signed_total_cents,
    (c.net_cents   * c.affects_sale) AS signed_net_cents,
    (c.quantity    * c.affects_sale) AS signed_quantity
FROM silver.compras c
LEFT JOIN silver.proveedores p ON p.supplier_id    = c.supplier_id
LEFT JOIN silver.articulos   a ON a.product_id     = c.product_id
LEFT JOIN silver.vendedores  s ON s.salesperson_id = c.salesperson_id;

COMMENT ON VIEW gold.v_purchases IS
  'Purchase line items denormalized. Penicor purchases from suppliers. '
  'Use signed_* in SUMs to net out purchase returns.';


CREATE OR REPLACE VIEW gold.v_payments AS
SELECT
    p.payment_id,
    p.fecha,
    p.document_type,
    p.affects_ar,
    p.customer_branch_id,
    p.customer_id,
    COALESCE(c.trade_name, c.legal_name) AS customer_name,
    c.legal_name                         AS customer_legal_name,
    c.sales_zone,
    p.salesperson_id,
    s.salesperson_name,
    p.currency_code,
    p.exchange_rate,
    p.amount_cents,
    (p.amount_cents * p.affects_ar) AS signed_amount_cents
FROM silver.pagos p
LEFT JOIN silver.clientes   c ON c.customer_branch_id = p.customer_branch_id
LEFT JOIN silver.vendedores s ON s.salesperson_id     = p.salesperson_id;

COMMENT ON VIEW gold.v_payments IS
  'Customer payments / AR collections, denormalized. amount_cents is the gross '
  'value; signed_amount_cents applies affects_ar so refunds/reversals net out.';


CREATE OR REPLACE VIEW gold.v_open_ar AS
SELECT
    d.customer_branch_id,
    d.document_id,
    d.document_date,
    d.due_date,
    d.document_type,
    d.cfe_document,
    d.salesperson_id,
    s.salesperson_name,
    COALESCE(c.trade_name, c.legal_name) AS customer_name,
    c.legal_name                         AS customer_legal_name,
    c.sales_zone,
    c.country_code,
    c.department                         AS customer_department,
    d.currency_code,
    d.exchange_rate,
    d.amount_cents,
    d.outstanding_cents,
    -- days overdue (positive = overdue, negative = future-due, 0 = due today)
    (CURRENT_DATE - d.due_date)::INT     AS days_overdue
FROM silver.deuda_por_cliente d
LEFT JOIN silver.clientes   c ON c.customer_branch_id = d.customer_branch_id
LEFT JOIN silver.vendedores s ON s.salesperson_id     = d.salesperson_id
WHERE d.outstanding_cents > 0;

COMMENT ON VIEW gold.v_open_ar IS
  'One row per open invoice/document still owed by a customer. The default for '
  'AR / "how much are we owed" / aging questions. Already filters '
  'outstanding_cents > 0 so closed documents are excluded.';

COMMENT ON COLUMN gold.v_open_ar.days_overdue IS
  'Positive = past due_date by N days. Zero = due today. Negative = not yet due.';


CREATE OR REPLACE VIEW gold.v_inventory AS
SELECT
    s.warehouse_id,
    s.warehouse_name,
    s.product_id,
    s.product_name,
    a.family       AS product_family,
    a.brand        AS product_brand,
    a.category_1   AS product_category_1,
    s.quantity,
    s.min_quantity,
    s.is_understocked,
    -- gap (positive if understocked)
    GREATEST(s.min_quantity - s.quantity, 0) AS shortfall
FROM silver.stock s
LEFT JOIN silver.articulos a ON a.product_id = s.product_id;

COMMENT ON VIEW gold.v_inventory IS
  'Current inventory snapshot per (warehouse, product), with product taxonomy '
  'joined. is_understocked is the source flag; shortfall = how many units below min.';


CREATE OR REPLACE VIEW gold.v_visits AS
SELECT
    v.visit_id,
    v.fecha,
    v.start_at,
    v.end_at,
    v.salesperson_id,
    s.salesperson_name,
    v.customer_branch_id,
    v.customer_id,
    COALESCE(c.trade_name, c.legal_name) AS customer_name,
    v.visit_type,
    v.notes,
    v.duration_minutes,
    v.latitude,
    v.longitude
FROM silver.visitas v
LEFT JOIN silver.vendedores s ON s.salesperson_id     = v.salesperson_id
LEFT JOIN silver.clientes   c ON c.customer_branch_id = v.customer_branch_id;

COMMENT ON VIEW gold.v_visits IS
  'Sales-rep visits to customers, denormalized with rep + customer names. '
  'Empty in the POC slice — verify with Penicor whether visits are tracked.';


CREATE OR REPLACE VIEW gold.v_customers AS
SELECT
    c.customer_branch_id,
    c.customer_id,
    c.branch_id,
    COALESCE(c.trade_name, c.legal_name) AS customer_name,
    c.legal_name,
    c.address,
    c.country_code,
    c.country_name,
    c.department,
    c.district,
    c.price_list,
    c.sales_zone,
    c.delivery_zone,
    c.category_1,
    c.category_2,
    c.status,
    c.latitude,
    c.longitude,
    c.rut,
    c.customer_type,
    c.is_prospect
FROM silver.clientes c;

COMMENT ON VIEW gold.v_customers IS
  'Customer master, light pass-through over silver.clientes. Use this for '
  '"how many customers in zone X" / customer-list questions.';


-- ============================================================================
-- TIER 2 — pre-aggregated metric views
-- ============================================================================

CREATE OR REPLACE VIEW gold.v_revenue_monthly AS
SELECT
    DATE_TRUNC('month', fecha)::DATE AS month,
    currency_code,
    SUM(signed_net_cents)            AS net_cents,
    SUM(signed_total_cents)          AS total_cents,
    COUNT(DISTINCT invoice_id)       AS invoice_count,
    COUNT(*)                         AS line_count,
    COUNT(DISTINCT customer_branch_id) AS active_customers
FROM gold.v_sales
GROUP BY 1, currency_code;

COMMENT ON VIEW gold.v_revenue_monthly IS
  'Monthly net revenue by currency. Uses signed_* so credit notes net out. '
  'For "revenue this month / last month / YTD" questions.';


CREATE OR REPLACE VIEW gold.v_revenue_by_customer AS
SELECT
    customer_branch_id,
    customer_id,
    customer_name,
    sales_zone,
    SUM(signed_net_cents)        AS net_cents,
    SUM(signed_total_cents)      AS total_cents,
    SUM(signed_quantity)         AS units_sold,
    COUNT(DISTINCT invoice_id)   AS invoice_count,
    MIN(fecha)                   AS first_sale_date,
    MAX(fecha)                   AS last_sale_date
FROM gold.v_sales
WHERE customer_branch_id IS NOT NULL
GROUP BY customer_branch_id, customer_id, customer_name, sales_zone;

COMMENT ON VIEW gold.v_revenue_by_customer IS
  'One row per customer-branch with lifetime totals (within the loaded date '
  'window). For "top customers", "biggest accounts" questions.';


CREATE OR REPLACE VIEW gold.v_revenue_by_product AS
SELECT
    product_id,
    product_name,
    product_family,
    product_brand,
    SUM(signed_quantity)         AS units_sold,
    SUM(signed_cases)            AS cases_sold,
    SUM(signed_net_cents)        AS net_cents,
    SUM(signed_total_cents)      AS total_cents,
    COUNT(DISTINCT invoice_id)   AS invoice_count,
    COUNT(DISTINCT customer_branch_id) AS distinct_customers
FROM gold.v_sales
WHERE product_id IS NOT NULL
GROUP BY product_id, product_name, product_family, product_brand;

COMMENT ON VIEW gold.v_revenue_by_product IS
  'One row per product with lifetime sales totals. For "top sellers", '
  '"slow movers", "best brand" questions.';


CREATE OR REPLACE VIEW gold.v_revenue_by_salesperson AS
SELECT
    salesperson_id,
    salesperson_name,
    SUM(signed_net_cents)              AS net_cents,
    SUM(signed_total_cents)            AS total_cents,
    COUNT(DISTINCT invoice_id)         AS invoice_count,
    COUNT(DISTINCT customer_branch_id) AS distinct_customers,
    MIN(fecha)                         AS first_sale_date,
    MAX(fecha)                         AS last_sale_date
FROM gold.v_sales
WHERE salesperson_id IS NOT NULL
GROUP BY salesperson_id, salesperson_name;

COMMENT ON VIEW gold.v_revenue_by_salesperson IS
  'Lifetime sales per rep. For "top salesperson", "rep ranking", '
  '"who sold X this month" questions.';


CREATE OR REPLACE VIEW gold.v_payments_monthly AS
SELECT
    DATE_TRUNC('month', fecha)::DATE AS month,
    currency_code,
    SUM(signed_amount_cents)         AS amount_cents,
    COUNT(*)                         AS payment_count,
    COUNT(DISTINCT customer_branch_id) AS paying_customers
FROM gold.v_payments
GROUP BY 1, currency_code;

COMMENT ON VIEW gold.v_payments_monthly IS
  'Monthly customer collections by currency. For "how much did we collect" / '
  'cashflow questions.';


-- ============================================================================
-- TIER 3 — diagnostic views (back canonical queries directly)
-- ============================================================================

CREATE OR REPLACE VIEW gold.v_ar_aging AS
SELECT
    customer_branch_id,
    customer_name,
    sales_zone,
    currency_code,
    SUM(outstanding_cents) FILTER (WHERE days_overdue <= 0)             AS not_yet_due_cents,
    SUM(outstanding_cents) FILTER (WHERE days_overdue BETWEEN 1 AND 30) AS overdue_1_30_cents,
    SUM(outstanding_cents) FILTER (WHERE days_overdue BETWEEN 31 AND 60) AS overdue_31_60_cents,
    SUM(outstanding_cents) FILTER (WHERE days_overdue > 60)              AS overdue_60_plus_cents,
    SUM(outstanding_cents)                                                AS total_owed_cents,
    COUNT(*)                                                              AS open_doc_count,
    MAX(days_overdue)                                                     AS max_days_overdue
FROM gold.v_open_ar
GROUP BY customer_branch_id, customer_name, sales_zone, currency_code;

COMMENT ON VIEW gold.v_ar_aging IS
  'Aging buckets per customer-branch and currency. For "AR aging report", '
  '"who owes us the most", "who is most overdue" questions. Buckets are exclusive: '
  'not_yet_due (due_date >= today), 1-30, 31-60, 60+ days past due_date.';


CREATE OR REPLACE VIEW gold.v_ar_total AS
SELECT
    currency_code,
    SUM(outstanding_cents) AS total_owed_cents,
    COUNT(*)               AS open_doc_count,
    COUNT(DISTINCT customer_branch_id) AS customers_with_balance
FROM gold.v_open_ar
GROUP BY currency_code;

COMMENT ON VIEW gold.v_ar_total IS
  'Single-row-per-currency rollup of total open AR. For the literal "how much '
  'are we owed?" question.';


CREATE OR REPLACE VIEW gold.v_inventory_low AS
SELECT
    warehouse_id,
    warehouse_name,
    product_id,
    product_name,
    product_family,
    quantity,
    min_quantity,
    shortfall
FROM gold.v_inventory
WHERE is_understocked = true
ORDER BY shortfall DESC;

COMMENT ON VIEW gold.v_inventory_low IS
  'Products currently below their minimum stock threshold, ordered by '
  'biggest shortfall first. For "what should we restock" questions.';
