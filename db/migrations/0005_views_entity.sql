-- ============================================================================
-- 0005_views_entity.sql
--
-- Tier-1 entity views. Each one wraps a fact table with its standard dim
-- joins so the agent doesn't have to re-derive them per question. These are
-- the "default tables" the LLM should reach for — IDEAL_DATABASE.md §10.
--
-- Idempotent (CREATE OR REPLACE).
-- ============================================================================


-- ---------- v_customers ----------------------------------------------------

CREATE OR REPLACE VIEW gold.v_customers AS
SELECT
  customer_id,
  customer_code,
  customer_name,
  customer_segment,
  payment_terms_days,
  credit_limit_cents,
  default_currency_code,
  email,
  phone,
  city,
  region,
  country_code,
  is_active
FROM gold.dim_customer
WHERE is_current = true;

COMMENT ON VIEW gold.v_customers IS
  'Current customers (one row per customer, latest SCD2 version). The default '
  'table for any "customer" question.';


-- ---------- v_products -----------------------------------------------------

CREATE OR REPLACE VIEW gold.v_products AS
SELECT
  product_id,
  sku,
  product_name,
  product_category,
  product_subcategory,
  unit_weight_kg,
  unit_volume_m3,
  unit_cost_cents,
  unit_price_cents,
  reorder_threshold,
  is_active
FROM gold.dim_product
WHERE is_current = true;

COMMENT ON VIEW gold.v_products IS
  'Current products (latest SCD2 version). The default table for any '
  '"product" question.';


-- ---------- v_orders -------------------------------------------------------

CREATE OR REPLACE VIEW gold.v_orders AS
SELECT
  o.order_id,
  o.order_number,
  o.order_date,
  o.ordered_at,
  o.status,
  o.total_amount_cents,
  o.total_amount_cents / 100.0 AS total_amount,
  o.currency_code,
  o.line_count,

  o.customer_id,
  c.customer_code,
  c.customer_name,
  c.customer_segment,
  c.country_code,

  o.warehouse_id,
  w.warehouse_code,
  w.warehouse_name
FROM gold.fact_orders o
JOIN gold.dim_customer c
  ON c.customer_id = o.customer_id AND c.is_current = true
LEFT JOIN gold.dim_warehouse w
  ON w.warehouse_id = o.warehouse_id
WHERE o.is_void = false;

COMMENT ON VIEW gold.v_orders IS
  'Denormalized non-void orders with current customer and warehouse context. '
  'The default table for any "orders" question. total_amount is the '
  'human-readable decimal in currency_code.';


-- ---------- v_order_items --------------------------------------------------

CREATE OR REPLACE VIEW gold.v_order_items AS
SELECT
  oi.order_item_id,
  oi.order_id,
  oi.line_number,
  oi.quantity,
  oi.unit_price_cents,
  oi.line_total_cents,
  oi.line_total_cents / 100.0 AS line_total,
  oi.discount_cents,
  oi.currency_code,

  oi.order_date,
  o.customer_id,
  c.customer_name,

  oi.product_id,
  p.sku,
  p.product_name,
  p.product_category
FROM gold.fact_order_items oi
JOIN gold.fact_orders o     ON o.order_id = oi.order_id AND o.order_date = oi.order_date
JOIN gold.dim_customer c    ON c.customer_id = o.customer_id AND c.is_current = true
JOIN gold.dim_product p     ON p.product_id  = oi.product_id AND p.is_current  = true
WHERE o.is_void = false;

COMMENT ON VIEW gold.v_order_items IS
  'Order lines with current product and customer context. Sum line_total_cents '
  'grouped by product_category for "revenue by category" questions.';


-- ---------- v_invoices -----------------------------------------------------

CREATE OR REPLACE VIEW gold.v_invoices AS
SELECT
  i.invoice_id,
  i.invoice_number,
  i.invoice_date,
  i.due_date,
  i.status,
  i.amount_billed_cents,
  i.amount_paid_cents,
  i.amount_due_cents,
  i.amount_billed_cents / 100.0 AS amount_billed,
  i.amount_paid_cents   / 100.0 AS amount_paid,
  i.amount_due_cents    / 100.0 AS amount_due,
  i.currency_code,

  i.customer_id,
  c.customer_code,
  c.customer_name,
  c.customer_segment,
  c.country_code,
  c.payment_terms_days,

  i.order_id,
  i.source_document_id
FROM gold.fact_invoices i
JOIN gold.dim_customer c
  ON c.customer_id = i.customer_id AND c.is_current = true
WHERE i.is_void = false;

COMMENT ON VIEW gold.v_invoices IS
  'Denormalized non-void invoices with current customer context. The default '
  'table for any "invoice" or "AR" question. status=unpaid + amount_due_cents>0 '
  'is what counts as outstanding.';


-- ---------- v_payments -----------------------------------------------------

CREATE OR REPLACE VIEW gold.v_payments AS
SELECT
  p.payment_id,
  p.payment_date,
  p.amount_cents,
  p.amount_cents / 100.0 AS amount,
  p.currency_code,
  p.payment_method,
  p.reference_number,

  p.customer_id,
  c.customer_name,

  p.invoice_id,
  i.invoice_number
FROM gold.fact_payments p
JOIN gold.dim_customer c
  ON c.customer_id = p.customer_id AND c.is_current = true
LEFT JOIN gold.fact_invoices i
  ON i.invoice_id = p.invoice_id AND i.invoice_date = p.invoice_date;

COMMENT ON VIEW gold.v_payments IS
  'Payment receipts with customer and invoice number. SUM amount_cents '
  'grouped by payment_date for cash receipts reporting.';


-- ---------- v_shipments ----------------------------------------------------

CREATE OR REPLACE VIEW gold.v_shipments AS
SELECT
  s.shipment_id,
  s.shipment_number,
  s.ship_date,
  s.shipped_at,
  s.delivered_at,
  s.delivered_date,
  s.status,
  s.origin_city,
  s.destination_city,
  s.destination_country_code,
  s.distance_km,
  s.weight_kg,
  s.volume_m3,
  s.freight_cost_cents,
  s.freight_cost_cents / 100.0 AS freight_cost,
  s.currency_code,

  s.order_id,
  o.order_number,
  o.customer_id,
  c.customer_name,

  s.warehouse_id,
  w.warehouse_code,
  w.warehouse_name,

  s.vehicle_id,
  v.vehicle_code,
  v.license_plate,

  s.driver_employee_id,
  e.full_name AS driver_name,

  -- handy derived field
  CASE
    WHEN s.delivered_at IS NOT NULL AND s.shipped_at IS NOT NULL
      THEN EXTRACT(EPOCH FROM (s.delivered_at - s.shipped_at)) / 3600.0
    ELSE NULL
  END AS transit_hours
FROM gold.fact_shipments s
LEFT JOIN gold.fact_orders o     ON o.order_id = s.order_id
LEFT JOIN gold.dim_customer c    ON c.customer_id = o.customer_id AND c.is_current = true
JOIN gold.dim_warehouse w        ON w.warehouse_id = s.warehouse_id
LEFT JOIN gold.dim_vehicle v     ON v.vehicle_id = s.vehicle_id
LEFT JOIN gold.dim_employee e    ON e.employee_id = s.driver_employee_id;

COMMENT ON VIEW gold.v_shipments IS
  'Denormalized shipments with order, customer, warehouse, vehicle, and '
  'driver context. transit_hours is delivered_at minus shipped_at in hours '
  '(NULL until delivered). For late-delivery analysis use v_late_deliveries.';


-- ---------- v_inventory_status --------------------------------------------
-- Current on-hand by (product, warehouse), as of now. Computed by summing
-- all signed quantity_delta movements up to today. For point-in-time history,
-- query fact_inventory_movements directly with a date filter.

CREATE OR REPLACE VIEW gold.v_inventory_status AS
SELECT
  m.product_id,
  p.sku,
  p.product_name,
  p.product_category,
  p.reorder_threshold,
  m.warehouse_id,
  w.warehouse_code,
  w.warehouse_name,
  SUM(m.quantity_delta)::INT AS units_on_hand,
  CASE
    WHEN p.reorder_threshold IS NOT NULL AND SUM(m.quantity_delta) <= p.reorder_threshold
      THEN true ELSE false
  END AS is_below_reorder_threshold
FROM gold.fact_inventory_movements m
JOIN gold.dim_product p   ON p.product_id   = m.product_id   AND p.is_current = true
JOIN gold.dim_warehouse w ON w.warehouse_id = m.warehouse_id
GROUP BY m.product_id, p.sku, p.product_name, p.product_category,
         p.reorder_threshold, m.warehouse_id, w.warehouse_code, w.warehouse_name;

COMMENT ON VIEW gold.v_inventory_status IS
  'Current units on hand by (product, warehouse). Sum of all signed '
  'quantity_delta movements. is_below_reorder_threshold is TRUE when '
  'units_on_hand <= reorder_threshold. The default for inventory questions.';


-- ---------- v_expenses -----------------------------------------------------

CREATE OR REPLACE VIEW gold.v_expenses AS
SELECT
  e.expense_id,
  e.expense_date,
  e.category,
  e.description,
  e.amount_cents,
  e.tax_amount_cents,
  e.amount_cents / 100.0           AS amount,
  e.tax_amount_cents / 100.0       AS tax_amount,
  (e.amount_cents + e.tax_amount_cents) / 100.0 AS total_amount,
  e.currency_code,
  e.is_reimbursable,

  e.warehouse_id,    w.warehouse_name,
  e.vehicle_id,      v.vehicle_code,
  e.employee_id,     emp.full_name AS employee_name,
  e.supplier_id,     s.supplier_name,
  e.source_document_id
FROM gold.fact_expenses e
LEFT JOIN gold.dim_warehouse w  ON w.warehouse_id = e.warehouse_id
LEFT JOIN gold.dim_vehicle v    ON v.vehicle_id   = e.vehicle_id
LEFT JOIN gold.dim_employee emp ON emp.employee_id = e.employee_id
LEFT JOIN gold.dim_supplier s   ON s.supplier_id  = e.supplier_id
WHERE e.is_void = false;

COMMENT ON VIEW gold.v_expenses IS
  'Non-void expenses with related entity names. The default for expense '
  'reporting. total_amount = amount + tax_amount.';
