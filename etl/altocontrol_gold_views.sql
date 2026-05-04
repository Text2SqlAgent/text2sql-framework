-- gold.v_* — agent-facing views over silver for the AltoControl ERP (currently Penicor).
--
-- Per IDEAL_DATABASE.md §3 + §10, the agent never sees bronze or silver
-- directly — only the gold layer. For this POC, gold is *just* views
-- (no real dim/fact tables yet).
--
-- Entity resolution
-- -----------------
-- All entity-grain references go through silver.<entity>_aliases ->
-- silver.<entity>_master. The agent therefore sees CANONICAL ids and
-- CANONICAL names — not raw source-system ids. Customer/product/salesperson/
-- supplier dedup is invisible at this layer; the agent just sees one
-- "MERCADOS DEVOTO S.A" instead of 12.
--
-- For traceability, line-grain views (v_sales, v_purchases, v_payments,
-- v_open_ar, v_visits) keep the source-system ids as `<entity>_branch_id`
-- / `<entity>_id` columns alongside the canonical ones. Aggregation views
-- (v_revenue_by_*, v_ar_aging, ...) use the canonical ids only.
--
-- Conventions:
--   - All money columns named `<role>_cents` are integer cents in the
--     line currency. To convert to UYU: multiply by exchange_rate (the
--     `cotizacion` from source) where present.
--   - `signed_total_cents` (in v_sales/v_purchases) already applies the
--     sign carried by `affects_sale` so SUM correctly nets credit notes.
--
-- Idempotent: drops and re-creates all views. Safe across schema evolution
-- (Postgres CREATE OR REPLACE VIEW can't rename or reorder columns).

CREATE SCHEMA IF NOT EXISTS gold;

DROP VIEW IF EXISTS
    gold.v_inventory_low,
    gold.v_ar_total,
    gold.v_ar_aging,
    gold.v_payments_monthly,
    gold.v_revenue_by_salesperson,
    gold.v_revenue_by_product,
    gold.v_revenue_by_customer,
    gold.v_revenue_monthly,
    gold.v_customers,
    gold.v_visits,
    gold.v_inventory,
    gold.v_open_ar,
    gold.v_payments,
    gold.v_purchases,
    gold.v_sales
CASCADE;

-- ============================================================================
-- TIER 1 — entity views (silver joined, denormalized, canonical-aware)
-- ============================================================================

CREATE OR REPLACE VIEW gold.v_sales AS
SELECT
    v.line_id,
    v.invoice_id,
    v.fecha,
    v.document_type,
    v.affects_sale,
    -- customer (canonical)
    cm.canonical_id                                   AS canonical_customer_id,
    cm.canonical_name                                 AS customer_name,
    cm.legal_name                                     AS customer_legal_name,
    -- customer (source — for tracing back to silver.clientes)
    v.customer_branch_id,
    v.customer_id,
    c.sales_zone,
    c.delivery_zone,
    c.country_code,
    c.department                                      AS customer_department,
    -- product (canonical)
    pm.canonical_id                                   AS canonical_product_id,
    pm.canonical_name                                 AS product_name,
    -- product (source attributes from silver.articulos)
    v.product_id,
    a.family                                          AS product_family,
    a.brand                                           AS product_brand,
    a.category_1                                      AS product_category_1,
    -- salesperson (canonical)
    sm.canonical_id                                   AS canonical_salesperson_id,
    sm.canonical_name                                 AS salesperson_name,
    v.salesperson_id,
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
LEFT JOIN silver.clientes c ON c.customer_branch_id = v.customer_branch_id
LEFT JOIN silver.customer_aliases cca ON cca.source_id = v.customer_branch_id
LEFT JOIN silver.customer_master  cm  ON cm.canonical_id = cca.canonical_id
LEFT JOIN silver.articulos a ON a.product_id = v.product_id
LEFT JOIN silver.product_aliases pa ON pa.source_id = v.product_id
LEFT JOIN silver.product_master  pm ON pm.canonical_id = pa.canonical_id
LEFT JOIN silver.vendedores s ON s.salesperson_id = v.salesperson_id
LEFT JOIN silver.salesperson_aliases sa ON sa.source_id = v.salesperson_id::TEXT
LEFT JOIN silver.salesperson_master  sm ON sm.canonical_id = sa.canonical_id;

COMMENT ON VIEW gold.v_sales IS
  'Sales line items, denormalized with canonical customer/product/salesperson context. '
  'One row per id_renglon. customer_name/product_name/salesperson_name are CANONICAL '
  '(deduped) names; customer_branch_id/product_id/salesperson_id are the source-system ids. '
  'Use signed_* in SUM aggregations so credit notes (affects_sale = -1) cancel out.';


CREATE OR REPLACE VIEW gold.v_purchases AS
SELECT
    c.line_id,
    c.invoice_id,
    c.fecha,
    c.document_type,
    c.affects_sale,
    -- supplier (canonical)
    spm.canonical_id     AS canonical_supplier_id,
    spm.canonical_name   AS supplier_name,
    spm.legal_name       AS supplier_legal_name,
    c.supplier_id,
    p.category           AS supplier_category,
    -- product (canonical)
    pm.canonical_id      AS canonical_product_id,
    pm.canonical_name    AS product_name,
    c.product_id,
    a.family             AS product_family,
    -- salesperson (canonical)
    sm.canonical_id      AS canonical_salesperson_id,
    sm.canonical_name    AS salesperson_name,
    c.salesperson_id,
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
LEFT JOIN silver.proveedores p ON p.supplier_id = c.supplier_id
LEFT JOIN silver.supplier_aliases spa ON spa.source_id = c.supplier_id::TEXT
LEFT JOIN silver.supplier_master  spm ON spm.canonical_id = spa.canonical_id
LEFT JOIN silver.articulos a ON a.product_id = c.product_id
LEFT JOIN silver.product_aliases pa ON pa.source_id = c.product_id
LEFT JOIN silver.product_master  pm ON pm.canonical_id = pa.canonical_id
LEFT JOIN silver.vendedores s ON s.salesperson_id = c.salesperson_id
LEFT JOIN silver.salesperson_aliases sa ON sa.source_id = c.salesperson_id::TEXT
LEFT JOIN silver.salesperson_master  sm ON sm.canonical_id = sa.canonical_id;

COMMENT ON VIEW gold.v_purchases IS
  'Purchase line items denormalized with canonical supplier/product/salesperson. '
  'Use signed_* in SUMs to net out purchase returns.';


CREATE OR REPLACE VIEW gold.v_payments AS
SELECT
    p.payment_id,
    p.fecha,
    p.document_type,
    p.affects_ar,
    -- customer (canonical)
    cm.canonical_id      AS canonical_customer_id,
    cm.canonical_name    AS customer_name,
    cm.legal_name        AS customer_legal_name,
    p.customer_branch_id,
    p.customer_id,
    c.sales_zone,
    -- salesperson (canonical)
    sm.canonical_id      AS canonical_salesperson_id,
    sm.canonical_name    AS salesperson_name,
    p.salesperson_id,
    p.currency_code,
    p.exchange_rate,
    p.amount_cents,
    (p.amount_cents * p.affects_ar) AS signed_amount_cents
FROM silver.pagos p
LEFT JOIN silver.clientes c ON c.customer_branch_id = p.customer_branch_id
LEFT JOIN silver.customer_aliases cca ON cca.source_id = p.customer_branch_id
LEFT JOIN silver.customer_master  cm  ON cm.canonical_id = cca.canonical_id
LEFT JOIN silver.vendedores s ON s.salesperson_id = p.salesperson_id
LEFT JOIN silver.salesperson_aliases sa ON sa.source_id = p.salesperson_id::TEXT
LEFT JOIN silver.salesperson_master  sm ON sm.canonical_id = sa.canonical_id;

COMMENT ON VIEW gold.v_payments IS
  'Customer payments / AR collections, denormalized with canonical customer + salesperson. '
  'amount_cents is the gross value; signed_amount_cents applies affects_ar so refunds net out.';


CREATE OR REPLACE VIEW gold.v_open_ar AS
SELECT
    -- customer (canonical)
    cm.canonical_id      AS canonical_customer_id,
    cm.canonical_name    AS customer_name,
    cm.legal_name        AS customer_legal_name,
    d.customer_branch_id,
    c.sales_zone,
    c.country_code,
    c.department         AS customer_department,
    -- document
    d.document_id,
    d.document_date,
    d.due_date,
    d.document_type,
    d.cfe_document,
    -- salesperson (canonical)
    sm.canonical_id      AS canonical_salesperson_id,
    sm.canonical_name    AS salesperson_name,
    d.salesperson_id,
    d.currency_code,
    d.exchange_rate,
    d.amount_cents,
    d.outstanding_cents,
    -- days overdue (positive = overdue, negative = future-due, 0 = due today)
    (CURRENT_DATE - d.due_date)::INT AS days_overdue
FROM silver.deuda_por_cliente d
LEFT JOIN silver.clientes c ON c.customer_branch_id = d.customer_branch_id
LEFT JOIN silver.customer_aliases cca ON cca.source_id = d.customer_branch_id
LEFT JOIN silver.customer_master  cm  ON cm.canonical_id = cca.canonical_id
LEFT JOIN silver.vendedores s ON s.salesperson_id = d.salesperson_id
LEFT JOIN silver.salesperson_aliases sa ON sa.source_id = d.salesperson_id::TEXT
LEFT JOIN silver.salesperson_master  sm ON sm.canonical_id = sa.canonical_id
WHERE d.outstanding_cents > 0;

COMMENT ON VIEW gold.v_open_ar IS
  'One row per open invoice/document still owed. customer_name is canonical (deduped); '
  'customer_branch_id is the source id for tracing. Already filters outstanding_cents > 0.';


CREATE OR REPLACE VIEW gold.v_inventory AS
SELECT
    s.warehouse_id,
    s.warehouse_name,
    -- product (canonical)
    pm.canonical_id      AS canonical_product_id,
    pm.canonical_name    AS product_name,
    s.product_id,
    a.family       AS product_family,
    a.brand        AS product_brand,
    a.category_1   AS product_category_1,
    s.quantity,
    s.min_quantity,
    s.is_understocked,
    GREATEST(s.min_quantity - s.quantity, 0) AS shortfall
FROM silver.stock s
LEFT JOIN silver.articulos a ON a.product_id = s.product_id
LEFT JOIN silver.product_aliases pa ON pa.source_id = s.product_id
LEFT JOIN silver.product_master  pm ON pm.canonical_id = pa.canonical_id;

COMMENT ON VIEW gold.v_inventory IS
  'Inventory snapshot per (warehouse, source product). product_name is canonical; '
  'aggregations across products should GROUP BY canonical_product_id. Warehouse is '
  'still source-grain (no warehouse_master populated for AltoControl).';


CREATE OR REPLACE VIEW gold.v_visits AS
SELECT
    v.visit_id,
    v.fecha,
    v.start_at,
    v.end_at,
    -- salesperson (canonical)
    sm.canonical_id      AS canonical_salesperson_id,
    sm.canonical_name    AS salesperson_name,
    v.salesperson_id,
    -- customer (canonical)
    cm.canonical_id      AS canonical_customer_id,
    cm.canonical_name    AS customer_name,
    v.customer_branch_id,
    v.customer_id,
    v.visit_type,
    v.notes,
    v.duration_minutes,
    v.latitude,
    v.longitude
FROM silver.visitas v
LEFT JOIN silver.vendedores s ON s.salesperson_id = v.salesperson_id
LEFT JOIN silver.salesperson_aliases sa ON sa.source_id = v.salesperson_id::TEXT
LEFT JOIN silver.salesperson_master  sm ON sm.canonical_id = sa.canonical_id
LEFT JOIN silver.clientes c ON c.customer_branch_id = v.customer_branch_id
LEFT JOIN silver.customer_aliases cca ON cca.source_id = v.customer_branch_id
LEFT JOIN silver.customer_master  cm  ON cm.canonical_id = cca.canonical_id;

COMMENT ON VIEW gold.v_visits IS
  'Sales-rep visits to customers, with canonical rep + customer names. '
  'Empty in the POC slice — verify with the customer whether visits are tracked.';


CREATE OR REPLACE VIEW gold.v_customers AS
SELECT DISTINCT ON (cm.canonical_id)
    cm.canonical_id      AS canonical_customer_id,
    cm.canonical_name    AS customer_name,
    cm.legal_name,
    cm.tax_id            AS rut,
    cm.entity_kind,
    cm.parent_id         AS canonical_parent_id,
    -- representative attributes from one of the underlying source rows
    -- (DISTINCT ON picks the row with smallest customer_branch_id deterministically)
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
    c.customer_type,
    c.is_prospect,
    -- count of source-system rows that map to this canonical (>=1)
    (SELECT COUNT(*) FROM silver.customer_aliases ca2 WHERE ca2.canonical_id = cm.canonical_id)
        AS source_branch_count
FROM silver.customer_master cm
LEFT JOIN silver.customer_aliases ca ON ca.canonical_id = cm.canonical_id
LEFT JOIN silver.clientes c ON c.customer_branch_id = ca.source_id
WHERE cm.is_active
ORDER BY cm.canonical_id, c.customer_branch_id;

COMMENT ON VIEW gold.v_customers IS
  'Canonical customer master — one row per real-world customer (after dedup). '
  'Attributes (address, sales_zone, etc.) come from a representative source row '
  'when a canonical entity has multiple aliases; source_branch_count tells how many. '
  'For "how many customers" / customer-list questions.';


-- ============================================================================
-- TIER 2 — pre-aggregated metric views (canonical grain)
-- ============================================================================

CREATE OR REPLACE VIEW gold.v_revenue_monthly AS
SELECT
    DATE_TRUNC('month', fecha)::DATE     AS month,
    currency_code,
    SUM(signed_net_cents)                AS net_cents,
    SUM(signed_total_cents)              AS total_cents,
    COUNT(DISTINCT invoice_id)           AS invoice_count,
    COUNT(*)                             AS line_count,
    COUNT(DISTINCT canonical_customer_id) AS active_customers
FROM gold.v_sales
GROUP BY 1, currency_code;

COMMENT ON VIEW gold.v_revenue_monthly IS
  'Monthly net revenue by currency. Uses signed_* so credit notes net out. '
  'active_customers counts CANONICAL customers, not source-branch ids.';


CREATE OR REPLACE VIEW gold.v_revenue_by_customer AS
SELECT
    canonical_customer_id,
    customer_name,
    SUM(signed_net_cents)                 AS net_cents,
    SUM(signed_total_cents)               AS total_cents,
    SUM(signed_quantity)                  AS units_sold,
    COUNT(DISTINCT invoice_id)            AS invoice_count,
    COUNT(DISTINCT customer_branch_id)    AS source_branch_count,
    MIN(fecha)                            AS first_sale_date,
    MAX(fecha)                            AS last_sale_date
FROM gold.v_sales
WHERE canonical_customer_id IS NOT NULL
GROUP BY canonical_customer_id, customer_name;

COMMENT ON VIEW gold.v_revenue_by_customer IS
  'One row per CANONICAL customer with lifetime totals. source_branch_count tells '
  'how many source customer_branch_id rows were rolled up into this canonical entity. '
  'For "top customers", "biggest accounts" questions — output is deduped.';


CREATE OR REPLACE VIEW gold.v_revenue_by_product AS
SELECT
    canonical_product_id,
    product_name,
    -- product attributes pulled from a representative source row
    MAX(product_family)                   AS product_family,
    MAX(product_brand)                    AS product_brand,
    SUM(signed_quantity)                  AS units_sold,
    SUM(signed_cases)                     AS cases_sold,
    SUM(signed_net_cents)                 AS net_cents,
    SUM(signed_total_cents)               AS total_cents,
    COUNT(DISTINCT invoice_id)            AS invoice_count,
    COUNT(DISTINCT canonical_customer_id) AS distinct_customers
FROM gold.v_sales
WHERE canonical_product_id IS NOT NULL
GROUP BY canonical_product_id, product_name;

COMMENT ON VIEW gold.v_revenue_by_product IS
  'One row per CANONICAL product with lifetime sales. For top-sellers, slow-movers, '
  'best-brand questions. distinct_customers counts canonical customers.';


CREATE OR REPLACE VIEW gold.v_revenue_by_salesperson AS
SELECT
    canonical_salesperson_id,
    salesperson_name,
    SUM(signed_net_cents)                 AS net_cents,
    SUM(signed_total_cents)               AS total_cents,
    COUNT(DISTINCT invoice_id)            AS invoice_count,
    COUNT(DISTINCT canonical_customer_id) AS distinct_customers,
    MIN(fecha)                            AS first_sale_date,
    MAX(fecha)                            AS last_sale_date
FROM gold.v_sales
WHERE canonical_salesperson_id IS NOT NULL
GROUP BY canonical_salesperson_id, salesperson_name;

COMMENT ON VIEW gold.v_revenue_by_salesperson IS
  'Lifetime sales per CANONICAL salesperson. For top-rep and rep-ranking questions.';


CREATE OR REPLACE VIEW gold.v_payments_monthly AS
SELECT
    DATE_TRUNC('month', fecha)::DATE      AS month,
    currency_code,
    SUM(signed_amount_cents)              AS amount_cents,
    COUNT(*)                              AS payment_count,
    COUNT(DISTINCT canonical_customer_id) AS paying_customers
FROM gold.v_payments
GROUP BY 1, currency_code;

COMMENT ON VIEW gold.v_payments_monthly IS
  'Monthly customer collections by currency. paying_customers counts canonicals.';


-- ============================================================================
-- TIER 3 — diagnostic views (back canonical queries directly, canonical grain)
-- ============================================================================

CREATE OR REPLACE VIEW gold.v_ar_aging AS
SELECT
    canonical_customer_id,
    customer_name,
    MAX(sales_zone)                                                       AS sales_zone,
    currency_code,
    SUM(outstanding_cents) FILTER (WHERE days_overdue <= 0)               AS not_yet_due_cents,
    SUM(outstanding_cents) FILTER (WHERE days_overdue BETWEEN 1 AND 30)   AS overdue_1_30_cents,
    SUM(outstanding_cents) FILTER (WHERE days_overdue BETWEEN 31 AND 60)  AS overdue_31_60_cents,
    SUM(outstanding_cents) FILTER (WHERE days_overdue > 60)               AS overdue_60_plus_cents,
    SUM(outstanding_cents)                                                AS total_owed_cents,
    COUNT(*)                                                              AS open_doc_count,
    MAX(days_overdue)                                                     AS max_days_overdue
FROM gold.v_open_ar
GROUP BY canonical_customer_id, customer_name, currency_code;

COMMENT ON VIEW gold.v_ar_aging IS
  'Aging buckets per CANONICAL customer + currency. Buckets are exclusive: '
  'not_yet_due (due_date >= today), 1-30, 31-60, 60+ days past due_date.';


CREATE OR REPLACE VIEW gold.v_ar_total AS
SELECT
    currency_code,
    SUM(outstanding_cents)                AS total_owed_cents,
    COUNT(*)                              AS open_doc_count,
    COUNT(DISTINCT canonical_customer_id) AS customers_with_balance
FROM gold.v_open_ar
GROUP BY currency_code;

COMMENT ON VIEW gold.v_ar_total IS
  'Single-row-per-currency rollup of total open AR. For the literal "how much '
  'are we owed?" question. customers_with_balance counts CANONICAL customers.';


CREATE OR REPLACE VIEW gold.v_inventory_low AS
SELECT
    warehouse_id,
    warehouse_name,
    canonical_product_id,
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
  'biggest shortfall first.';
