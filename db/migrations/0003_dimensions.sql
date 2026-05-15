-- ============================================================================
-- 0003_dimensions.sql
--
-- All gold-layer dimension tables. Per IDEAL_DATABASE.md §5–6:
--   * SCD Type 2 (valid_from / valid_to / is_current) on entities where
--     history matters: customer, product, warehouse.
--   * Static reference dims (date, currency) are simple.
--   * Every business column gets a COMMENT — non-negotiable for agent
--     accuracy.
-- ============================================================================


-- ===========================================================================
-- dim_date — one row per calendar date, with fiscal calendar columns.
-- Populated by 0007_seeds.sql.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS gold.dim_date (
  date_key        DATE      PRIMARY KEY,
  day_of_month    SMALLINT  NOT NULL,
  day_of_week     SMALLINT  NOT NULL,                -- 0=Sun .. 6=Sat
  day_name        TEXT      NOT NULL,
  iso_week        SMALLINT  NOT NULL,                -- 1..53
  month_number    SMALLINT  NOT NULL,                -- 1..12
  month_name      TEXT      NOT NULL,
  quarter         SMALLINT  NOT NULL,                -- 1..4 calendar
  year            SMALLINT  NOT NULL,                -- calendar year
  fiscal_year     SMALLINT  NOT NULL,                -- per tenant_config
  fiscal_quarter  SMALLINT  NOT NULL,                -- 1..4 fiscal
  fiscal_month    SMALLINT  NOT NULL,                -- 1..12 fiscal (month-of-fiscal-year)
  is_weekend      BOOLEAN   NOT NULL,
  is_holiday      BOOLEAN   NOT NULL DEFAULT false
);

COMMENT ON TABLE gold.dim_date IS
  'One row per calendar date. JOIN any fact table on this dimension to '
  'answer fiscal questions ("this quarter", "last fiscal year") without '
  'date math. fiscal_* columns derive from tenant_config.fiscal_year_start_month.';

COMMENT ON COLUMN gold.dim_date.date_key       IS 'Calendar date (YYYY-MM-DD).';
COMMENT ON COLUMN gold.dim_date.day_of_week    IS '0 = Sunday … 6 = Saturday.';
COMMENT ON COLUMN gold.dim_date.fiscal_year    IS 'Fiscal year. If fiscal_year_start_month=4 (April), 2026-03-31 is fiscal_year=2025.';
COMMENT ON COLUMN gold.dim_date.fiscal_quarter IS 'Fiscal quarter, 1..4, relative to fiscal_year_start_month.';
COMMENT ON COLUMN gold.dim_date.fiscal_month   IS 'Month-of-fiscal-year, 1..12. Month 1 = fiscal_year_start_month.';
COMMENT ON COLUMN gold.dim_date.is_holiday     IS 'Customer-defined non-working days. Populated by ETL/admin tool, not the seed.';


-- ===========================================================================
-- dim_currency — ISO codes + display info.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS gold.dim_currency (
  currency_code   TEXT PRIMARY KEY,                  -- 'USD', 'EUR', 'PEN'
  currency_name   TEXT NOT NULL,
  symbol          TEXT,
  decimal_places  SMALLINT NOT NULL DEFAULT 2
);

COMMENT ON TABLE gold.dim_currency IS
  'ISO 4217 currency reference. The customer''s base currency is set in '
  'tenant_config.base_currency_code. Use dim_currency_rate to convert.';
COMMENT ON COLUMN gold.dim_currency.decimal_places IS
  'How many minor units. 2 for USD/EUR, 0 for JPY/CLP, 3 for KWD.';


-- ===========================================================================
-- dim_currency_rate — daily exchange rates, for converting non-base
-- currency facts to the reporting currency. ETL upserts daily.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS gold.dim_currency_rate (
  rate_date           DATE NOT NULL,
  from_currency_code  TEXT NOT NULL REFERENCES gold.dim_currency(currency_code),
  to_currency_code    TEXT NOT NULL REFERENCES gold.dim_currency(currency_code),
  rate                NUMERIC(18,8) NOT NULL,        -- units of `to` per 1 unit of `from`
  PRIMARY KEY (rate_date, from_currency_code, to_currency_code)
);

COMMENT ON TABLE gold.dim_currency_rate IS
  'Daily FX rates: rate units of to_currency per 1 unit of from_currency. '
  'Populated by ETL from a rate provider. Used by views that need to '
  'convert amounts to the customer''s base currency.';


-- ===========================================================================
-- dim_customer — SCD Type 2.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS gold.dim_customer (
  customer_id          BIGSERIAL    PRIMARY KEY,
  customer_code        TEXT         NOT NULL,         -- source-system natural key
  customer_name        TEXT         NOT NULL,
  customer_segment     TEXT,                          -- 'enterprise','smb','retail',...
  payment_terms_days   SMALLINT,                      -- 30, 60, 90, ...
  credit_limit_cents   BIGINT,                        -- in customer's invoicing currency
  default_currency_code TEXT REFERENCES gold.dim_currency(currency_code),

  email                TEXT,
  phone                TEXT,
  address_line_1       TEXT,
  address_line_2       TEXT,
  city                 TEXT,
  region               TEXT,
  postal_code          TEXT,
  country_code         TEXT,                          -- ISO 3166-1 alpha-2

  is_active            BOOLEAN     NOT NULL DEFAULT true,

  -- SCD2
  valid_from           TIMESTAMPTZ NOT NULL,
  valid_to             TIMESTAMPTZ,                   -- NULL = current version
  is_current           BOOLEAN     NOT NULL,

  -- audit
  record_loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  record_updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_system        TEXT        NOT NULL,
  source_record_id     TEXT        NOT NULL,
  etl_batch_id         UUID
);

CREATE INDEX IF NOT EXISTS ix_dim_customer_current
  ON gold.dim_customer(customer_code) WHERE is_current = true;
CREATE INDEX IF NOT EXISTS ix_dim_customer_active
  ON gold.dim_customer(is_active) WHERE is_current = true;

COMMENT ON TABLE gold.dim_customer IS
  'Customer master, slowly changing dimension type 2. For most queries, '
  'filter is_current=true to get the latest version of each customer. '
  'Joined to all customer-related fact tables via customer_id.';

COMMENT ON COLUMN gold.dim_customer.customer_id       IS 'Surrogate key. New customer_id assigned each SCD2 version.';
COMMENT ON COLUMN gold.dim_customer.customer_code     IS 'Stable natural key from source ERP — the same across SCD2 versions.';
COMMENT ON COLUMN gold.dim_customer.customer_segment  IS 'Business segment label. Free-text but typically "enterprise", "smb", "retail".';
COMMENT ON COLUMN gold.dim_customer.payment_terms_days IS 'Net payment terms, in days. NULL if cash-on-delivery / no terms.';
COMMENT ON COLUMN gold.dim_customer.credit_limit_cents IS 'Credit limit in customer''s invoicing currency, integer minor units (cents).';
COMMENT ON COLUMN gold.dim_customer.country_code      IS 'ISO 3166-1 alpha-2 (e.g. "US", "PE", "MX").';
COMMENT ON COLUMN gold.dim_customer.is_active         IS 'Whether the customer relationship is currently active.';
COMMENT ON COLUMN gold.dim_customer.valid_from        IS 'When this version of the row became current. SCD2 validity start.';
COMMENT ON COLUMN gold.dim_customer.valid_to          IS 'When this version was superseded. NULL means it is current.';
COMMENT ON COLUMN gold.dim_customer.is_current        IS 'TRUE on exactly one row per customer_code. The shortcut filter for normal queries.';


-- ===========================================================================
-- dim_product — SCD Type 2.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS gold.dim_product (
  product_id          BIGSERIAL    PRIMARY KEY,
  sku                 TEXT         NOT NULL,
  product_name        TEXT         NOT NULL,
  product_category    TEXT,
  product_subcategory TEXT,
  unit_weight_kg      NUMERIC(10,3),
  unit_volume_m3      NUMERIC(10,4),
  unit_cost_cents     BIGINT,                         -- in tenant base currency
  unit_price_cents    BIGINT,                         -- in tenant base currency
  reorder_threshold   INT,
  is_active           BOOLEAN     NOT NULL DEFAULT true,

  -- SCD2
  valid_from          TIMESTAMPTZ NOT NULL,
  valid_to            TIMESTAMPTZ,
  is_current          BOOLEAN     NOT NULL,

  -- audit
  record_loaded_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  record_updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_system       TEXT        NOT NULL,
  source_record_id    TEXT        NOT NULL,
  etl_batch_id        UUID
);

CREATE INDEX IF NOT EXISTS ix_dim_product_current
  ON gold.dim_product(sku) WHERE is_current = true;

COMMENT ON TABLE gold.dim_product IS
  'Product master, SCD2. Filter is_current=true for most queries. '
  'unit_cost_cents and unit_price_cents are in the tenant base currency.';
COMMENT ON COLUMN gold.dim_product.sku               IS 'Stock-keeping unit. Stable natural key across SCD2 versions.';
COMMENT ON COLUMN gold.dim_product.unit_weight_kg    IS 'Weight per single unit, kilograms.';
COMMENT ON COLUMN gold.dim_product.unit_volume_m3    IS 'Volume per single unit, cubic meters.';
COMMENT ON COLUMN gold.dim_product.unit_cost_cents   IS 'Internal unit cost in the tenant base currency, integer cents.';
COMMENT ON COLUMN gold.dim_product.unit_price_cents  IS 'List unit price in the tenant base currency, integer cents.';
COMMENT ON COLUMN gold.dim_product.reorder_threshold IS 'Stock level at or below which the item should be reordered.';


-- ===========================================================================
-- dim_warehouse — locations where inventory lives.
-- (No SCD2 — warehouses don't change often; if one is renamed, ETL updates in place.)
-- ===========================================================================

CREATE TABLE IF NOT EXISTS gold.dim_warehouse (
  warehouse_id      BIGSERIAL    PRIMARY KEY,
  warehouse_code    TEXT         NOT NULL UNIQUE,
  warehouse_name    TEXT         NOT NULL,
  city              TEXT,
  region            TEXT,
  country_code      TEXT,
  address_line_1    TEXT,
  postal_code       TEXT,
  capacity_m3       NUMERIC(12,2),
  is_active         BOOLEAN     NOT NULL DEFAULT true,

  record_loaded_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  record_updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_system     TEXT        NOT NULL,
  source_record_id  TEXT        NOT NULL
);

COMMENT ON TABLE gold.dim_warehouse IS
  'Warehouses / distribution centers. One row per active or historical warehouse.';
COMMENT ON COLUMN gold.dim_warehouse.warehouse_code IS 'Short code used on shipping documents. Unique.';
COMMENT ON COLUMN gold.dim_warehouse.capacity_m3    IS 'Total storage capacity in cubic meters.';


-- ===========================================================================
-- dim_vehicle — for logistics customers tracking their fleet.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS gold.dim_vehicle (
  vehicle_id          BIGSERIAL    PRIMARY KEY,
  vehicle_code        TEXT         NOT NULL UNIQUE,
  license_plate       TEXT,
  vehicle_type        TEXT,                            -- 'truck','van','semi-trailer'
  capacity_kg         NUMERIC(10,2),
  capacity_m3         NUMERIC(10,2),
  is_active           BOOLEAN     NOT NULL DEFAULT true,

  record_loaded_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  record_updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_system       TEXT,
  source_record_id    TEXT
);

COMMENT ON TABLE gold.dim_vehicle IS
  'Fleet vehicles used to fulfill shipments. Joined to fact_shipments via vehicle_id.';
COMMENT ON COLUMN gold.dim_vehicle.capacity_kg IS 'Maximum payload, kilograms.';
COMMENT ON COLUMN gold.dim_vehicle.capacity_m3 IS 'Maximum cargo volume, cubic meters.';


-- ===========================================================================
-- dim_supplier — vendors.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS gold.dim_supplier (
  supplier_id       BIGSERIAL    PRIMARY KEY,
  supplier_code     TEXT         NOT NULL UNIQUE,
  supplier_name     TEXT         NOT NULL,
  contact_email     TEXT,
  phone             TEXT,
  country_code      TEXT,
  payment_terms_days SMALLINT,
  is_active         BOOLEAN     NOT NULL DEFAULT true,

  record_loaded_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  record_updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_system     TEXT,
  source_record_id  TEXT
);

COMMENT ON TABLE gold.dim_supplier IS
  'Vendors / suppliers we buy from. Joined to fact_expenses (when vendor-related) and any fact_purchases tables added per-customer.';


-- ===========================================================================
-- dim_employee — for expense ownership and driver assignment.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS gold.dim_employee (
  employee_id        BIGSERIAL    PRIMARY KEY,
  employee_code      TEXT         NOT NULL UNIQUE,
  full_name          TEXT         NOT NULL,
  email              TEXT,
  department         TEXT,
  role_title         TEXT,
  hire_date          DATE,
  termination_date   DATE,
  is_active          BOOLEAN     NOT NULL DEFAULT true,

  record_loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  record_updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_system      TEXT,
  source_record_id   TEXT
);

COMMENT ON TABLE gold.dim_employee IS
  'Employees (and contractors). Used for expense ownership, driver references on shipments, and approver fields.';


-- ===========================================================================
-- dim_document — references to OCR'd PDFs, scanned invoices, etc.
-- The actual content lives in object storage (S3).
-- ===========================================================================

CREATE TABLE IF NOT EXISTS gold.dim_document (
  document_id        BIGSERIAL    PRIMARY KEY,
  document_type      TEXT         NOT NULL,           -- 'invoice','contract','bill_of_lading','expense_receipt'
  storage_url        TEXT         NOT NULL,           -- s3://bucket/customer/...
  filename           TEXT,
  page_count         INT,
  uploaded_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  ocr_status         TEXT NOT NULL DEFAULT 'pending', -- 'pending','done','failed'
  ocr_text_url       TEXT,                            -- s3://... extracted full text

  record_loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_system      TEXT,
  source_record_id   TEXT
);

COMMENT ON TABLE gold.dim_document IS
  'Pointers to source documents stored in object storage (PDFs, scanned '
  'invoices, contracts). Referenced by fact rows that came from a document. '
  'For unstructured-content questions, query the OCR text via the document '
  'service, then JOIN back here on document_id to surface metadata in the UI.';
COMMENT ON COLUMN gold.dim_document.storage_url IS 'Object-storage URL where the original file lives.';
COMMENT ON COLUMN gold.dim_document.ocr_text_url IS 'Object-storage URL where the extracted full text lives. NULL until ocr_status=done.';
