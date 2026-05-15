-- ============================================================================
-- 0004_facts.sql
--
-- Gold-layer fact tables. Per IDEAL_DATABASE.md §5, §11.
--   * Each fact has a documented grain.
--   * Money: BIGINT cents only — never floats.
--   * Time: TIMESTAMPTZ stored UTC; DATE for pure dates.
--   * High-volume facts (orders, invoices, shipments) are partitioned by
--     month on their primary date column.
--   * Audit columns: record_loaded_at, source_system, source_record_id,
--     etl_batch_id on every fact.
-- ============================================================================


-- ---------- Helper: create a monthly partition for a partitioned fact -----
-- Usage:
--     SELECT gold.create_monthly_partition('fact_orders', 'order_date', '2026-04-01');
-- Idempotent: skips if the partition already exists.

CREATE OR REPLACE FUNCTION gold.create_monthly_partition(
  parent_table TEXT,
  partition_col TEXT,    -- unused; kept for clarity
  month_start DATE
) RETURNS VOID
LANGUAGE plpgsql AS $$
DECLARE
  partition_name TEXT;
  next_month_start DATE;
BEGIN
  partition_name := parent_table || '_' || to_char(month_start, 'YYYY_MM');
  next_month_start := (month_start + INTERVAL '1 month')::DATE;

  IF NOT EXISTS (
    SELECT 1 FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'gold' AND c.relname = partition_name
  ) THEN
    EXECUTE format(
      'CREATE TABLE gold.%I PARTITION OF gold.%I FOR VALUES FROM (%L) TO (%L)',
      partition_name, parent_table, month_start, next_month_start
    );
  END IF;
END $$;

COMMENT ON FUNCTION gold.create_monthly_partition(TEXT, TEXT, DATE) IS
  'Helper to create a monthly RANGE partition on a parent fact table. '
  'Called by ETL/admin tooling when rolling forward to a new month.';


-- ===========================================================================
-- fact_orders — order header.
-- Grain: one row per customer order header.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS gold.fact_orders (
  order_id           BIGSERIAL,
  order_number       TEXT         NOT NULL,
  customer_id        BIGINT       NOT NULL REFERENCES gold.dim_customer,
  warehouse_id       BIGINT       REFERENCES gold.dim_warehouse,

  order_date         DATE         NOT NULL,
  ordered_at         TIMESTAMPTZ  NOT NULL,             -- precise time order was placed
  status             TEXT         NOT NULL,             -- 'open','fulfilled','canceled','returned'

  total_amount_cents BIGINT       NOT NULL DEFAULT 0,
  currency_code      TEXT         NOT NULL REFERENCES gold.dim_currency(currency_code),
  line_count         INT          NOT NULL DEFAULT 0,
  is_void            BOOLEAN      NOT NULL DEFAULT false,

  -- audit
  record_loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  record_updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_system      TEXT        NOT NULL,
  source_record_id   TEXT        NOT NULL,
  etl_batch_id       UUID,

  PRIMARY KEY (order_id, order_date)
) PARTITION BY RANGE (order_date);

COMMENT ON TABLE gold.fact_orders IS
  'One row per customer order header. Filter is_void=false for normal '
  'reporting. Line-level detail in fact_order_items. Joined to fact_invoices '
  'via order_id.';

COMMENT ON COLUMN gold.fact_orders.order_id           IS 'Surrogate key.';
COMMENT ON COLUMN gold.fact_orders.order_number       IS 'Human-readable order number from the source ERP.';
COMMENT ON COLUMN gold.fact_orders.order_date         IS 'Order date. Partition key.';
COMMENT ON COLUMN gold.fact_orders.ordered_at         IS 'Precise UTC timestamp the order was placed.';
COMMENT ON COLUMN gold.fact_orders.status             IS 'open | fulfilled | canceled | returned';
COMMENT ON COLUMN gold.fact_orders.total_amount_cents IS 'Order total in `currency_code`, integer cents.';
COMMENT ON COLUMN gold.fact_orders.line_count         IS 'Number of line items on the order. Denormalized for fast COUNT-style questions.';
COMMENT ON COLUMN gold.fact_orders.is_void            IS 'TRUE if the order was voided in source. Exclude from normal reporting.';

CREATE INDEX IF NOT EXISTS ix_fact_orders_customer ON gold.fact_orders(customer_id);
CREATE INDEX IF NOT EXISTS ix_fact_orders_status   ON gold.fact_orders(status) WHERE is_void = false;


-- Seed monthly partitions: previous 12, current, next 12 months.
DO $$
DECLARE
  m DATE := date_trunc('month', CURRENT_DATE)::DATE - INTERVAL '12 months';
  i INT;
BEGIN
  FOR i IN 0..24 LOOP
    PERFORM gold.create_monthly_partition('fact_orders', 'order_date', m::DATE);
    m := (m + INTERVAL '1 month')::DATE;
  END LOOP;
END $$;


-- ===========================================================================
-- fact_order_items — line-level order detail.
-- Grain: one row per (order, line).
-- ===========================================================================

CREATE TABLE IF NOT EXISTS gold.fact_order_items (
  order_item_id       BIGSERIAL    PRIMARY KEY,
  order_id            BIGINT       NOT NULL,
  order_date          DATE         NOT NULL,                -- denormalized for partitioning consistency
  product_id          BIGINT       NOT NULL REFERENCES gold.dim_product,
  line_number         INT          NOT NULL,

  quantity            INT          NOT NULL,
  unit_price_cents    BIGINT       NOT NULL,
  line_total_cents    BIGINT       NOT NULL,
  discount_cents      BIGINT       NOT NULL DEFAULT 0,
  currency_code       TEXT         NOT NULL REFERENCES gold.dim_currency(currency_code),

  -- audit
  record_loaded_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_system       TEXT        NOT NULL,
  source_record_id    TEXT        NOT NULL,
  etl_batch_id        UUID,

  UNIQUE (order_id, line_number)
);

COMMENT ON TABLE gold.fact_order_items IS
  'One row per line on an order. Sum line_total_cents for revenue by '
  'product/category. Joined to fact_orders by (order_id, order_date).';

COMMENT ON COLUMN gold.fact_order_items.quantity         IS 'Units on this line.';
COMMENT ON COLUMN gold.fact_order_items.unit_price_cents IS 'Per-unit price at time of sale, integer cents in currency_code.';
COMMENT ON COLUMN gold.fact_order_items.line_total_cents IS 'Final line total = quantity * unit_price - discount, integer cents.';
COMMENT ON COLUMN gold.fact_order_items.discount_cents   IS 'Line-level discount, integer cents. Already subtracted from line_total_cents.';

CREATE INDEX IF NOT EXISTS ix_fact_order_items_order   ON gold.fact_order_items(order_id);
CREATE INDEX IF NOT EXISTS ix_fact_order_items_product ON gold.fact_order_items(product_id);


-- ===========================================================================
-- fact_invoices — invoice header.
-- Grain: one row per invoice.
-- Partitioned by invoice_date.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS gold.fact_invoices (
  invoice_id           BIGSERIAL,
  invoice_number       TEXT         NOT NULL,
  customer_id          BIGINT       NOT NULL REFERENCES gold.dim_customer,
  order_id             BIGINT,
  source_document_id   BIGINT       REFERENCES gold.dim_document,

  invoice_date         DATE         NOT NULL,
  due_date             DATE         NOT NULL,
  status               TEXT         NOT NULL,                  -- 'paid','partial','unpaid','void'

  amount_billed_cents  BIGINT       NOT NULL,
  amount_paid_cents    BIGINT       NOT NULL DEFAULT 0,
  amount_due_cents     BIGINT       GENERATED ALWAYS AS (amount_billed_cents - amount_paid_cents) STORED,
  currency_code        TEXT         NOT NULL REFERENCES gold.dim_currency(currency_code),

  is_void              BOOLEAN     NOT NULL DEFAULT false,

  -- audit
  record_loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  record_updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_system        TEXT        NOT NULL,
  source_record_id     TEXT        NOT NULL,
  etl_batch_id         UUID,

  PRIMARY KEY (invoice_id, invoice_date)
) PARTITION BY RANGE (invoice_date);

COMMENT ON TABLE gold.fact_invoices IS
  'One row per invoice header. Filter is_void=false for normal reporting. '
  'amount_due_cents is generated (billed minus paid) — zero when fully paid. '
  'Use status to filter unpaid for AR questions. source_document_id links to '
  'the original PDF if invoice came from a scan.';

COMMENT ON COLUMN gold.fact_invoices.invoice_number     IS 'Human-readable invoice number from source.';
COMMENT ON COLUMN gold.fact_invoices.invoice_date       IS 'Date the invoice was issued. Partition key.';
COMMENT ON COLUMN gold.fact_invoices.due_date           IS 'Date payment is due (invoice_date + customer payment terms typically).';
COMMENT ON COLUMN gold.fact_invoices.status             IS 'paid | partial | unpaid | void';
COMMENT ON COLUMN gold.fact_invoices.amount_billed_cents IS 'Gross invoice total in `currency_code`, integer cents.';
COMMENT ON COLUMN gold.fact_invoices.amount_paid_cents   IS 'Sum of payments applied to this invoice, integer cents.';
COMMENT ON COLUMN gold.fact_invoices.amount_due_cents    IS 'Outstanding balance (generated). Zero when fully paid.';

CREATE INDEX IF NOT EXISTS ix_fact_invoices_customer ON gold.fact_invoices(customer_id);
CREATE INDEX IF NOT EXISTS ix_fact_invoices_status   ON gold.fact_invoices(status) WHERE is_void = false;
CREATE INDEX IF NOT EXISTS ix_fact_invoices_due      ON gold.fact_invoices(due_date) WHERE status = 'unpaid' AND is_void = false;

DO $$
DECLARE
  m DATE := date_trunc('month', CURRENT_DATE)::DATE - INTERVAL '12 months';
  i INT;
BEGIN
  FOR i IN 0..24 LOOP
    PERFORM gold.create_monthly_partition('fact_invoices', 'invoice_date', m::DATE);
    m := (m + INTERVAL '1 month')::DATE;
  END LOOP;
END $$;


-- ===========================================================================
-- fact_payments — payment receipts applied to invoices.
-- Grain: one row per payment application (one payment may split across invoices).
-- ===========================================================================

CREATE TABLE IF NOT EXISTS gold.fact_payments (
  payment_id          BIGSERIAL    PRIMARY KEY,
  invoice_id          BIGINT       NOT NULL,
  invoice_date        DATE         NOT NULL,                  -- for FK to partitioned fact_invoices
  customer_id         BIGINT       NOT NULL REFERENCES gold.dim_customer,
  payment_date        DATE         NOT NULL,
  amount_cents        BIGINT       NOT NULL,
  currency_code       TEXT         NOT NULL REFERENCES gold.dim_currency(currency_code),
  payment_method      TEXT,                                  -- 'wire','ach','check','card','cash'
  reference_number    TEXT,

  -- audit
  record_loaded_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_system       TEXT        NOT NULL,
  source_record_id    TEXT        NOT NULL,
  etl_batch_id        UUID,

  FOREIGN KEY (invoice_id, invoice_date) REFERENCES gold.fact_invoices(invoice_id, invoice_date)
);

COMMENT ON TABLE gold.fact_payments IS
  'Payment receipts applied to invoices. One payment might split across '
  'multiple invoices — each application is a row. SUM amount_cents grouped '
  'by payment_date for cash receipts; SUM grouped by invoice_id for total '
  'paid per invoice.';

COMMENT ON COLUMN gold.fact_payments.amount_cents   IS 'Amount applied to this invoice, integer cents in currency_code.';
COMMENT ON COLUMN gold.fact_payments.payment_method IS 'wire | ach | check | card | cash';

CREATE INDEX IF NOT EXISTS ix_fact_payments_invoice  ON gold.fact_payments(invoice_id);
CREATE INDEX IF NOT EXISTS ix_fact_payments_customer ON gold.fact_payments(customer_id);
CREATE INDEX IF NOT EXISTS ix_fact_payments_date     ON gold.fact_payments(payment_date);


-- ===========================================================================
-- fact_shipments — shipment header.
-- Grain: one row per shipment.
-- Partitioned by ship_date.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS gold.fact_shipments (
  shipment_id         BIGSERIAL,
  shipment_number     TEXT         NOT NULL,
  order_id            BIGINT,                                -- nullable: not all shipments tied to a customer order
  warehouse_id        BIGINT       NOT NULL REFERENCES gold.dim_warehouse,
  vehicle_id          BIGINT       REFERENCES gold.dim_vehicle,
  driver_employee_id  BIGINT       REFERENCES gold.dim_employee,

  ship_date           DATE         NOT NULL,
  shipped_at          TIMESTAMPTZ  NOT NULL,
  delivered_at        TIMESTAMPTZ,
  delivered_date      DATE,
  status              TEXT         NOT NULL,                  -- 'in_transit','delivered','returned','lost'

  origin_city         TEXT,
  destination_city    TEXT,
  destination_country_code TEXT,
  distance_km         NUMERIC(10,2),
  weight_kg           NUMERIC(10,3),
  volume_m3           NUMERIC(10,3),
  freight_cost_cents  BIGINT,
  currency_code       TEXT         REFERENCES gold.dim_currency(currency_code),

  -- audit
  record_loaded_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  record_updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_system       TEXT        NOT NULL,
  source_record_id    TEXT        NOT NULL,
  etl_batch_id        UUID,

  PRIMARY KEY (shipment_id, ship_date)
) PARTITION BY RANGE (ship_date);

COMMENT ON TABLE gold.fact_shipments IS
  'One row per shipment. status=delivered when delivered_at is set. '
  'For "late deliveries" questions: a shipment is late when delivered_at '
  'is after the order''s expected_ship_by date — see v_late_deliveries view. '
  'Joined to fact_orders by order_id (nullable: stock-transfer shipments '
  'have no customer order).';

COMMENT ON COLUMN gold.fact_shipments.shipment_number    IS 'Human-readable shipment number / waybill from source.';
COMMENT ON COLUMN gold.fact_shipments.ship_date          IS 'Date shipment left origin. Partition key.';
COMMENT ON COLUMN gold.fact_shipments.shipped_at         IS 'UTC timestamp shipment left origin.';
COMMENT ON COLUMN gold.fact_shipments.delivered_at       IS 'UTC timestamp delivered. NULL until delivery confirmed.';
COMMENT ON COLUMN gold.fact_shipments.distance_km        IS 'Total route distance in kilometers.';
COMMENT ON COLUMN gold.fact_shipments.weight_kg          IS 'Total payload weight in kilograms.';
COMMENT ON COLUMN gold.fact_shipments.volume_m3          IS 'Total cargo volume in cubic meters.';
COMMENT ON COLUMN gold.fact_shipments.freight_cost_cents IS 'Freight cost charged to customer (or absorbed), integer cents in currency_code.';

CREATE INDEX IF NOT EXISTS ix_fact_shipments_order     ON gold.fact_shipments(order_id);
CREATE INDEX IF NOT EXISTS ix_fact_shipments_warehouse ON gold.fact_shipments(warehouse_id);
CREATE INDEX IF NOT EXISTS ix_fact_shipments_status    ON gold.fact_shipments(status);

DO $$
DECLARE
  m DATE := date_trunc('month', CURRENT_DATE)::DATE - INTERVAL '12 months';
  i INT;
BEGIN
  FOR i IN 0..24 LOOP
    PERFORM gold.create_monthly_partition('fact_shipments', 'ship_date', m::DATE);
    m := (m + INTERVAL '1 month')::DATE;
  END LOOP;
END $$;


-- ===========================================================================
-- fact_shipment_items — line detail per shipment.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS gold.fact_shipment_items (
  shipment_item_id    BIGSERIAL    PRIMARY KEY,
  shipment_id         BIGINT       NOT NULL,
  ship_date           DATE         NOT NULL,
  product_id          BIGINT       NOT NULL REFERENCES gold.dim_product,
  units_shipped       INT          NOT NULL,

  record_loaded_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_system       TEXT        NOT NULL,
  source_record_id    TEXT        NOT NULL,
  etl_batch_id        UUID,

  FOREIGN KEY (shipment_id, ship_date) REFERENCES gold.fact_shipments(shipment_id, ship_date)
);

COMMENT ON TABLE gold.fact_shipment_items IS
  'One row per (shipment, product). Sum units_shipped for "units shipped by '
  'product/category" questions.';

CREATE INDEX IF NOT EXISTS ix_fact_shipment_items_shipment ON gold.fact_shipment_items(shipment_id);
CREATE INDEX IF NOT EXISTS ix_fact_shipment_items_product  ON gold.fact_shipment_items(product_id);


-- ===========================================================================
-- fact_inventory_movements — every stock change.
-- Grain: one row per movement event.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS gold.fact_inventory_movements (
  movement_id        BIGSERIAL    PRIMARY KEY,
  product_id         BIGINT       NOT NULL REFERENCES gold.dim_product,
  warehouse_id       BIGINT       NOT NULL REFERENCES gold.dim_warehouse,
  event_at           TIMESTAMPTZ  NOT NULL,
  event_date         DATE         GENERATED ALWAYS AS ((event_at AT TIME ZONE 'UTC')::DATE) STORED,
  movement_type      TEXT         NOT NULL,             -- 'receipt','shipment','adjustment','transfer','count'
  quantity_delta     INT          NOT NULL,             -- signed: +receipt, -shipment, +/- adjustment

  reference_table    TEXT,                              -- e.g. 'fact_shipments'
  reference_id       BIGINT,                            -- the ID in that table
  reason             TEXT,

  -- audit
  record_loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_system      TEXT        NOT NULL,
  source_record_id   TEXT        NOT NULL,
  etl_batch_id       UUID
);

COMMENT ON TABLE gold.fact_inventory_movements IS
  'Every stock change at every warehouse. Sum quantity_delta grouped by '
  '(product_id, warehouse_id) up to a date for current-on-hand. movement_type '
  'classifies the event; reference_table/id points to the source fact when '
  'applicable (e.g. a shipment that decremented stock).';

COMMENT ON COLUMN gold.fact_inventory_movements.quantity_delta IS
  'Signed integer: positive for inbound (receipts, returns), negative for outbound (shipments, sales), can be either for adjustments.';
COMMENT ON COLUMN gold.fact_inventory_movements.movement_type  IS
  'receipt | shipment | adjustment | transfer | count';
COMMENT ON COLUMN gold.fact_inventory_movements.event_date     IS
  'UTC date derived from event_at — convenience for date-grouping.';

CREATE INDEX IF NOT EXISTS ix_inv_mov_product   ON gold.fact_inventory_movements(product_id);
CREATE INDEX IF NOT EXISTS ix_inv_mov_warehouse ON gold.fact_inventory_movements(warehouse_id);
CREATE INDEX IF NOT EXISTS ix_inv_mov_event_at  ON gold.fact_inventory_movements(event_at);


-- ===========================================================================
-- fact_expenses — operating expenses (utilities, fuel, maintenance, T&E).
-- Grain: one row per expense entry / line.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS gold.fact_expenses (
  expense_id           BIGSERIAL   PRIMARY KEY,
  expense_date         DATE        NOT NULL,
  category             TEXT        NOT NULL,             -- 'fuel','rent','utilities','salaries','maintenance','t_and_e','other'
  description          TEXT,
  amount_cents         BIGINT      NOT NULL,
  tax_amount_cents     BIGINT      NOT NULL DEFAULT 0,
  currency_code        TEXT        NOT NULL REFERENCES gold.dim_currency(currency_code),

  warehouse_id         BIGINT      REFERENCES gold.dim_warehouse,
  vehicle_id           BIGINT      REFERENCES gold.dim_vehicle,
  employee_id          BIGINT      REFERENCES gold.dim_employee,
  supplier_id          BIGINT      REFERENCES gold.dim_supplier,
  source_document_id   BIGINT      REFERENCES gold.dim_document,

  is_reimbursable      BOOLEAN     NOT NULL DEFAULT false,
  is_void              BOOLEAN     NOT NULL DEFAULT false,

  -- audit
  record_loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  record_updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_system        TEXT        NOT NULL,
  source_record_id     TEXT        NOT NULL,
  etl_batch_id         UUID
);

COMMENT ON TABLE gold.fact_expenses IS
  'One row per expense line. Filter is_void=false for normal reporting. '
  'Categories are free-text but typically: fuel, rent, utilities, salaries, '
  'maintenance, t_and_e, other. source_document_id links to the underlying '
  'receipt PDF when the expense came from a scanned document.';

COMMENT ON COLUMN gold.fact_expenses.amount_cents     IS 'Expense amount excluding tax, integer cents in currency_code.';
COMMENT ON COLUMN gold.fact_expenses.tax_amount_cents IS 'Tax portion, integer cents. Total = amount_cents + tax_amount_cents.';
COMMENT ON COLUMN gold.fact_expenses.is_reimbursable  IS 'TRUE if the expense will be billed back to a customer or another party.';

CREATE INDEX IF NOT EXISTS ix_fact_expenses_date       ON gold.fact_expenses(expense_date);
CREATE INDEX IF NOT EXISTS ix_fact_expenses_category   ON gold.fact_expenses(category) WHERE is_void = false;
