-- ============================================================================
-- silver_schema.sql
--
-- Cleaned, typed, deduplicated entity tables. The intermediate layer between
-- bronze (raw) and gold (dimensional). For real customers the silver→gold
-- transforms become non-trivial (SCD2, surrogate keys, conformed dims);
-- for the demo, silver and gold are largely 1:1 since the synth produces
-- already-clean data.
--
-- Idempotent.
-- ============================================================================

CREATE TABLE IF NOT EXISTS silver.customers (
  customer_code         TEXT PRIMARY KEY,
  customer_name         TEXT NOT NULL,
  customer_segment      TEXT,
  payment_terms_days    SMALLINT,
  credit_limit_cents    BIGINT,
  default_currency_code TEXT,
  email                 TEXT,
  phone                 TEXT,
  address_line_1        TEXT,
  city                  TEXT,
  country_code          TEXT,
  is_active             BOOLEAN NOT NULL DEFAULT true
);

CREATE TABLE IF NOT EXISTS silver.products (
  sku                  TEXT PRIMARY KEY,
  product_name         TEXT NOT NULL,
  product_category     TEXT,
  product_subcategory  TEXT,
  unit_weight_kg       NUMERIC(10,3),
  unit_volume_m3       NUMERIC(10,4),
  unit_cost_cents      BIGINT,
  unit_price_cents     BIGINT,
  reorder_threshold    INT,
  is_active            BOOLEAN NOT NULL DEFAULT true
);

CREATE TABLE IF NOT EXISTS silver.warehouses (
  warehouse_code TEXT PRIMARY KEY,
  warehouse_name TEXT NOT NULL,
  city           TEXT,
  country_code   TEXT,
  capacity_m3    NUMERIC(12,2),
  is_active      BOOLEAN NOT NULL DEFAULT true
);

CREATE TABLE IF NOT EXISTS silver.vehicles (
  vehicle_code   TEXT PRIMARY KEY,
  license_plate  TEXT,
  vehicle_type   TEXT,
  capacity_kg    NUMERIC(10,2),
  capacity_m3    NUMERIC(10,2),
  is_active      BOOLEAN NOT NULL DEFAULT true
);

CREATE TABLE IF NOT EXISTS silver.employees (
  employee_code  TEXT PRIMARY KEY,
  full_name      TEXT NOT NULL,
  email          TEXT,
  department     TEXT,
  role_title     TEXT,
  hire_date      DATE,
  is_active      BOOLEAN NOT NULL DEFAULT true
);

CREATE TABLE IF NOT EXISTS silver.suppliers (
  supplier_code      TEXT PRIMARY KEY,
  supplier_name      TEXT NOT NULL,
  contact_email      TEXT,
  country_code       TEXT,
  payment_terms_days SMALLINT,
  is_active          BOOLEAN NOT NULL DEFAULT true
);

CREATE TABLE IF NOT EXISTS silver.orders (
  order_number       TEXT PRIMARY KEY,
  customer_code      TEXT NOT NULL,
  warehouse_code     TEXT,
  order_date         DATE NOT NULL,
  ordered_at         TIMESTAMPTZ NOT NULL,
  status             TEXT NOT NULL,
  total_amount_cents BIGINT NOT NULL,
  line_count         INT NOT NULL,
  currency_code      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.order_items (
  order_number     TEXT NOT NULL,
  order_date       DATE NOT NULL,
  line_number      INT NOT NULL,
  sku              TEXT NOT NULL,
  quantity         INT NOT NULL,
  unit_price_cents BIGINT NOT NULL,
  discount_cents   BIGINT NOT NULL,
  line_total_cents BIGINT NOT NULL,
  currency_code    TEXT NOT NULL,
  PRIMARY KEY (order_number, line_number)
);

CREATE TABLE IF NOT EXISTS silver.invoices (
  invoice_number      TEXT PRIMARY KEY,
  customer_code       TEXT NOT NULL,
  order_number        TEXT,
  invoice_date        DATE NOT NULL,
  due_date            DATE NOT NULL,
  status              TEXT NOT NULL,
  amount_billed_cents BIGINT NOT NULL,
  amount_paid_cents   BIGINT NOT NULL,
  currency_code       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.payments (
  reference_number TEXT PRIMARY KEY,
  invoice_number   TEXT NOT NULL,
  invoice_date     DATE NOT NULL,
  customer_code    TEXT NOT NULL,
  payment_date     DATE NOT NULL,
  amount_cents     BIGINT NOT NULL,
  currency_code    TEXT NOT NULL,
  payment_method   TEXT
);

CREATE TABLE IF NOT EXISTS silver.shipments (
  shipment_number          TEXT PRIMARY KEY,
  order_number             TEXT,
  warehouse_code           TEXT NOT NULL,
  vehicle_code             TEXT,
  driver_employee_code     TEXT,
  ship_date                DATE NOT NULL,
  shipped_at               TIMESTAMPTZ NOT NULL,
  delivered_at             TIMESTAMPTZ,
  delivered_date           DATE,
  status                   TEXT NOT NULL,
  origin_city              TEXT,
  destination_city         TEXT,
  destination_country_code TEXT,
  distance_km              NUMERIC(10,2),
  weight_kg                NUMERIC(10,3),
  volume_m3                NUMERIC(10,3),
  freight_cost_cents       BIGINT,
  currency_code            TEXT
);

CREATE TABLE IF NOT EXISTS silver.shipment_items (
  shipment_number TEXT NOT NULL,
  ship_date       DATE NOT NULL,
  sku             TEXT NOT NULL,
  units_shipped   INT NOT NULL,
  PRIMARY KEY (shipment_number, sku)
);

CREATE TABLE IF NOT EXISTS silver.inventory_movements (
  movement_id      BIGSERIAL PRIMARY KEY,
  sku              TEXT NOT NULL,
  warehouse_code   TEXT NOT NULL,
  event_at         TIMESTAMPTZ NOT NULL,
  movement_type    TEXT NOT NULL,
  quantity_delta   INT NOT NULL,
  reference_table  TEXT,
  reference_id     TEXT,
  reason           TEXT
);

CREATE TABLE IF NOT EXISTS silver.expenses (
  expense_id       BIGINT PRIMARY KEY,
  expense_date     DATE NOT NULL,
  category         TEXT NOT NULL,
  description      TEXT,
  amount_cents     BIGINT NOT NULL,
  tax_amount_cents BIGINT NOT NULL,
  currency_code    TEXT NOT NULL,
  warehouse_code   TEXT,
  vehicle_code     TEXT,
  employee_code    TEXT,
  supplier_code    TEXT,
  is_reimbursable  BOOLEAN NOT NULL DEFAULT false
);
