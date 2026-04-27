-- ============================================================================
-- bronze_schema.sql
--
-- Raw landing tables for the synthetic demo source. In production each
-- bronze table mirrors one source-system feed; here we have one synthetic
-- "source" that produces all entities. Columns deliberately use TEXT for
-- everything that isn't already structured, mimicking what you'd get from
-- a CSV import / OCR text / loose ERP export. Cleansing happens in silver.
--
-- Idempotent.
-- ============================================================================

CREATE TABLE IF NOT EXISTS bronze.raw_customers (
  ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_record_id    TEXT NOT NULL,
  customer_code       TEXT,
  customer_name       TEXT,
  customer_segment    TEXT,
  payment_terms_days  TEXT,                         -- TEXT on purpose
  credit_limit_cents  TEXT,
  default_currency_code TEXT,
  email               TEXT,
  phone               TEXT,
  address_line_1      TEXT,
  city                TEXT,
  country_code        TEXT,
  is_active           TEXT
);

CREATE TABLE IF NOT EXISTS bronze.raw_products (
  ingested_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_record_id   TEXT NOT NULL,
  sku                TEXT,
  product_name       TEXT,
  product_category   TEXT,
  product_subcategory TEXT,
  unit_weight_kg     TEXT,
  unit_volume_m3     TEXT,
  unit_cost_cents    TEXT,
  unit_price_cents   TEXT,
  reorder_threshold  TEXT,
  is_active          TEXT
);

CREATE TABLE IF NOT EXISTS bronze.raw_warehouses (
  ingested_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_record_id TEXT NOT NULL,
  warehouse_code   TEXT,
  warehouse_name   TEXT,
  city             TEXT,
  country_code     TEXT,
  capacity_m3      TEXT,
  is_active        TEXT
);

CREATE TABLE IF NOT EXISTS bronze.raw_vehicles (
  ingested_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_record_id TEXT NOT NULL,
  vehicle_code     TEXT,
  license_plate    TEXT,
  vehicle_type     TEXT,
  capacity_kg      TEXT,
  capacity_m3      TEXT,
  is_active        TEXT
);

CREATE TABLE IF NOT EXISTS bronze.raw_employees (
  ingested_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_record_id TEXT NOT NULL,
  employee_code    TEXT,
  full_name        TEXT,
  email            TEXT,
  department       TEXT,
  role_title       TEXT,
  hire_date        TEXT,
  is_active        TEXT
);

CREATE TABLE IF NOT EXISTS bronze.raw_suppliers (
  ingested_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_record_id   TEXT NOT NULL,
  supplier_code      TEXT,
  supplier_name      TEXT,
  contact_email      TEXT,
  country_code       TEXT,
  payment_terms_days TEXT,
  is_active          TEXT
);

CREATE TABLE IF NOT EXISTS bronze.raw_orders (
  ingested_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_record_id   TEXT NOT NULL,
  order_number       TEXT,
  customer_code      TEXT,
  warehouse_code     TEXT,
  order_date         TEXT,
  ordered_at         TEXT,
  status             TEXT,
  total_amount_cents TEXT,
  currency_code      TEXT,
  line_count         TEXT
);

CREATE TABLE IF NOT EXISTS bronze.raw_order_items (
  ingested_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_record_id TEXT NOT NULL,
  order_number     TEXT,
  order_date       TEXT,
  line_number      TEXT,
  sku              TEXT,
  quantity         TEXT,
  unit_price_cents TEXT,
  discount_cents   TEXT,
  line_total_cents TEXT,
  currency_code    TEXT
);

CREATE TABLE IF NOT EXISTS bronze.raw_invoices (
  ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_record_id    TEXT NOT NULL,
  invoice_number      TEXT,
  customer_code       TEXT,
  order_number        TEXT,
  invoice_date        TEXT,
  due_date            TEXT,
  status              TEXT,
  amount_billed_cents TEXT,
  amount_paid_cents   TEXT,
  currency_code       TEXT
);

CREATE TABLE IF NOT EXISTS bronze.raw_payments (
  ingested_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_record_id TEXT NOT NULL,
  invoice_number   TEXT,
  invoice_date     TEXT,
  customer_code    TEXT,
  payment_date     TEXT,
  amount_cents     TEXT,
  currency_code    TEXT,
  payment_method   TEXT,
  reference_number TEXT
);

CREATE TABLE IF NOT EXISTS bronze.raw_shipments (
  ingested_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_record_id         TEXT NOT NULL,
  shipment_number          TEXT,
  order_number             TEXT,
  warehouse_code           TEXT,
  vehicle_code             TEXT,
  driver_employee_code     TEXT,
  ship_date                TEXT,
  shipped_at               TEXT,
  delivered_at             TEXT,
  delivered_date           TEXT,
  status                   TEXT,
  origin_city              TEXT,
  destination_city         TEXT,
  destination_country_code TEXT,
  distance_km              TEXT,
  weight_kg                TEXT,
  volume_m3                TEXT,
  freight_cost_cents       TEXT,
  currency_code            TEXT
);

CREATE TABLE IF NOT EXISTS bronze.raw_shipment_items (
  ingested_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_record_id TEXT NOT NULL,
  shipment_number  TEXT,
  ship_date        TEXT,
  sku              TEXT,
  units_shipped    TEXT
);

CREATE TABLE IF NOT EXISTS bronze.raw_inventory_movements (
  ingested_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_record_id TEXT NOT NULL,
  sku              TEXT,
  warehouse_code   TEXT,
  event_at         TEXT,
  movement_type    TEXT,
  quantity_delta   TEXT,
  reference_table  TEXT,
  reference_id     TEXT,
  reason           TEXT
);

CREATE TABLE IF NOT EXISTS bronze.raw_expenses (
  ingested_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_record_id   TEXT NOT NULL,
  expense_id         TEXT,
  expense_date       TEXT,
  category           TEXT,
  description        TEXT,
  amount_cents       TEXT,
  tax_amount_cents   TEXT,
  currency_code      TEXT,
  warehouse_code     TEXT,
  vehicle_code       TEXT,
  employee_code      TEXT,
  supplier_code      TEXT,
  is_reimbursable    TEXT
);
