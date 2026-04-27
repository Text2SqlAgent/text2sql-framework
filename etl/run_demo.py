"""End-to-end demo loader: synth → bronze → silver → gold → mat views.

Idempotent: truncates and reloads every time. Designed to be run after
db/migrations/ has been applied to a fresh customer Postgres database.

Usage:
    python etl/run_demo.py "postgresql://etl_writer:pwd@host:5432/customer_demo"

The connection string should authenticate as a role with full CRUD on all
three schemas (typically `etl_writer`, or a superuser for the first run).
"""

from __future__ import annotations

import argparse
import datetime as dt
import sys
import time
import uuid
from pathlib import Path
from typing import Iterable

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, Connection

from etl.synth import Synth, generate

ETL_DIR = Path(__file__).parent
BATCH_ID = str(uuid.uuid4())
SOURCE_SYSTEM = "demo_synth"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("connection_string", help="Postgres connection URL")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--months-back", type=int, default=24)
    args = ap.parse_args()

    engine = create_engine(args.connection_string, isolation_level="AUTOCOMMIT")

    t0 = time.time()
    print(f"[demo-etl] batch_id={BATCH_ID}")

    print("[demo-etl] applying bronze + silver schemas …")
    apply_schema_file(engine, ETL_DIR / "bronze_schema.sql")
    apply_schema_file(engine, ETL_DIR / "silver_schema.sql")

    print("[demo-etl] ensuring tenant_config …")
    ensure_tenant_config(engine)

    print(f"[demo-etl] generating synthetic data (seed={args.seed}, months_back={args.months_back}) …")
    synth = generate(seed=args.seed, months_back=args.months_back)
    print_synth_counts(synth)

    earliest, latest = synth_date_range(synth)
    print(f"[demo-etl] synth date range: {earliest} → {latest}")

    print("[demo-etl] truncating bronze + silver + gold (facts + dims) …")
    truncate_for_reload(engine)

    print("[demo-etl] ensuring monthly partitions cover synth date range …")
    ensure_partitions(engine, earliest, latest)

    print("[demo-etl] loading bronze …")
    load_bronze(engine, synth)

    print("[demo-etl] transforming bronze → silver …")
    transform_silver(engine)

    print("[demo-etl] transforming silver → gold (dims) …")
    load_gold_dims(engine)

    print("[demo-etl] transforming silver → gold (facts) …")
    load_gold_facts(engine)

    print("[demo-etl] refreshing materialized views …")
    refresh_materialized(engine)

    print(f"[demo-etl] verifying row counts …")
    verify(engine)

    engine.dispose()
    print(f"[demo-etl] done in {time.time() - t0:.1f}s")
    return 0


# ---------------------------------------------------------------------------
# Schema + setup
# ---------------------------------------------------------------------------

def apply_schema_file(engine: Engine, path: Path) -> None:
    sql = path.read_text(encoding="utf-8")
    with engine.connect() as conn:
        conn.exec_driver_sql(sql)


def ensure_tenant_config(engine: Engine) -> None:
    """Insert a default tenant_config row if missing. Demo customer = 'Acme Logistics'."""
    with engine.connect() as conn:
        existing = conn.execute(text("SELECT count(*) FROM gold.tenant_config")).scalar()
        if existing == 0:
            conn.execute(text("""
                INSERT INTO gold.tenant_config (
                    customer_legal_name, customer_short_name,
                    base_currency_code, fiscal_year_start_month,
                    reporting_timezone
                ) VALUES (
                    'Acme Logistics S.A.', 'Acme', 'PEN', 1, 'America/Lima'
                )
            """))


def synth_date_range(synth: Synth) -> tuple[dt.date, dt.date]:
    earliest = dt.date.fromisoformat(synth.config["earliest"])
    latest   = dt.date.fromisoformat(synth.config["today"])
    return earliest, latest


def ensure_partitions(engine: Engine, earliest: dt.date, latest: dt.date) -> None:
    """Make sure fact_orders/fact_invoices/fact_shipments have monthly
    partitions covering the synth date range."""
    months: list[dt.date] = []
    cur = earliest.replace(day=1)
    end = latest.replace(day=1)
    while cur <= end:
        months.append(cur)
        # advance one month
        if cur.month == 12:
            cur = cur.replace(year=cur.year + 1, month=1)
        else:
            cur = cur.replace(month=cur.month + 1)

    with engine.connect() as conn:
        for table, col in [
            ("fact_orders", "order_date"),
            ("fact_invoices", "invoice_date"),
            ("fact_shipments", "ship_date"),
        ]:
            for m in months:
                conn.execute(text(
                    "SELECT gold.create_monthly_partition(:t, :c, :d)"
                ), {"t": table, "c": col, "d": m})


def truncate_for_reload(engine: Engine) -> None:
    """Wipe bronze + silver + gold facts/dims (preserve dim_date, dim_currency, tenant_config)."""
    with engine.connect() as conn:
        conn.exec_driver_sql("""
            TRUNCATE
                bronze.raw_customers, bronze.raw_products, bronze.raw_warehouses,
                bronze.raw_vehicles, bronze.raw_employees, bronze.raw_suppliers,
                bronze.raw_orders, bronze.raw_order_items,
                bronze.raw_invoices, bronze.raw_payments,
                bronze.raw_shipments, bronze.raw_shipment_items,
                bronze.raw_inventory_movements, bronze.raw_expenses
            CASCADE;

            TRUNCATE
                silver.customers, silver.products, silver.warehouses,
                silver.vehicles, silver.employees, silver.suppliers,
                silver.orders, silver.order_items,
                silver.invoices, silver.payments,
                silver.shipments, silver.shipment_items,
                silver.inventory_movements, silver.expenses
            RESTART IDENTITY CASCADE;

            TRUNCATE
                gold.fact_expenses,
                gold.fact_inventory_movements,
                gold.fact_shipment_items,
                gold.fact_shipments,
                gold.fact_payments,
                gold.fact_invoices,
                gold.fact_order_items,
                gold.fact_orders
            RESTART IDENTITY CASCADE;

            TRUNCATE
                gold.dim_customer, gold.dim_product, gold.dim_warehouse,
                gold.dim_vehicle, gold.dim_supplier, gold.dim_employee,
                gold.dim_document
            RESTART IDENTITY CASCADE;
        """)


# ---------------------------------------------------------------------------
# Bronze load
# ---------------------------------------------------------------------------

def _stringify(rows: list[dict], extra: dict | None = None) -> list[dict]:
    """Bronze tables are TEXT — coerce every value to str (or None)."""
    out = []
    for r in rows:
        d = {k: (None if v is None else str(v)) for k, v in r.items()}
        if extra:
            d.update(extra)
        out.append(d)
    return out


def _bulk_insert(conn: Connection, table: str, columns: list[str], rows: list[dict]) -> None:
    if not rows:
        return
    placeholders = ", ".join(f":{c}" for c in columns)
    cols_sql = ", ".join(columns)
    stmt = text(f"INSERT INTO {table} ({cols_sql}) VALUES ({placeholders})")
    # Insert in chunks of 500 to keep parameter count reasonable
    CHUNK = 500
    for i in range(0, len(rows), CHUNK):
        conn.execute(stmt, rows[i:i + CHUNK])


def load_bronze(engine: Engine, s: Synth) -> None:
    with engine.connect() as conn:
        _load_bronze_table(conn, "bronze.raw_customers", s.customers,
            ["customer_code","customer_name","customer_segment","payment_terms_days",
             "credit_limit_cents","default_currency_code","email","phone",
             "address_line_1","city","country_code","is_active"])
        _load_bronze_table(conn, "bronze.raw_products", s.products,
            ["sku","product_name","product_category","product_subcategory",
             "unit_weight_kg","unit_volume_m3","unit_cost_cents","unit_price_cents",
             "reorder_threshold","is_active"])
        _load_bronze_table(conn, "bronze.raw_warehouses", s.warehouses,
            ["warehouse_code","warehouse_name","city","country_code","capacity_m3","is_active"])
        _load_bronze_table(conn, "bronze.raw_vehicles", s.vehicles,
            ["vehicle_code","license_plate","vehicle_type","capacity_kg","capacity_m3","is_active"])
        _load_bronze_table(conn, "bronze.raw_employees", s.employees,
            ["employee_code","full_name","email","department","role_title","hire_date","is_active"])
        _load_bronze_table(conn, "bronze.raw_suppliers", s.suppliers,
            ["supplier_code","supplier_name","contact_email","country_code","payment_terms_days","is_active"])
        _load_bronze_table(conn, "bronze.raw_orders", s.orders,
            ["order_number","customer_code","warehouse_code","order_date","ordered_at",
             "status","total_amount_cents","currency_code","line_count"])
        _load_bronze_table(conn, "bronze.raw_order_items", s.order_items,
            ["order_number","order_date","line_number","sku","quantity",
             "unit_price_cents","discount_cents","line_total_cents","currency_code"])
        _load_bronze_table(conn, "bronze.raw_invoices", s.invoices,
            ["invoice_number","customer_code","order_number","invoice_date","due_date",
             "status","amount_billed_cents","amount_paid_cents","currency_code"])
        _load_bronze_table(conn, "bronze.raw_payments", s.payments,
            ["invoice_number","invoice_date","customer_code","payment_date",
             "amount_cents","currency_code","payment_method","reference_number"])
        _load_bronze_table(conn, "bronze.raw_shipments", s.shipments,
            ["shipment_number","order_number","warehouse_code","vehicle_code","driver_employee_code",
             "ship_date","shipped_at","delivered_at","delivered_date","status",
             "origin_city","destination_city","destination_country_code",
             "distance_km","weight_kg","volume_m3","freight_cost_cents","currency_code"])
        _load_bronze_table(conn, "bronze.raw_shipment_items", s.shipment_items,
            ["shipment_number","ship_date","sku","units_shipped"])
        _load_bronze_table(conn, "bronze.raw_inventory_movements", s.inventory_movements,
            ["sku","warehouse_code","event_at","movement_type","quantity_delta",
             "reference_table","reference_id","reason"])
        _load_bronze_table(conn, "bronze.raw_expenses", s.expenses,
            ["expense_id","expense_date","category","description","amount_cents","tax_amount_cents",
             "currency_code","warehouse_code","vehicle_code","employee_code",
             "supplier_code","is_reimbursable"])


def _load_bronze_table(conn: Connection, table: str, rows: list[dict], cols: list[str]) -> None:
    """Generic bronze loader — adds source_record_id (synthetic) to every row."""
    if not rows:
        return
    enriched = []
    for i, r in enumerate(rows):
        d = {c: (None if r.get(c) is None else str(r[c])) for c in cols}
        d["source_record_id"] = f"{table.split('.')[-1]}_{i+1:08d}"
        enriched.append(d)
    full_cols = cols + ["source_record_id"]
    _bulk_insert(conn, table, full_cols, enriched)


# ---------------------------------------------------------------------------
# bronze → silver (cast types, dedup-by-natural-key)
# ---------------------------------------------------------------------------

def transform_silver(engine: Engine) -> None:
    with engine.connect() as conn:
        # Each silver table = SELECT FROM bronze with type casts. DISTINCT ON
        # pattern keeps only the latest ingest per natural key (here trivially
        # one per code since synth doesn't generate duplicates, but the
        # pattern is what real ETL would use).

        conn.exec_driver_sql("""
            INSERT INTO silver.customers
            SELECT DISTINCT ON (customer_code)
              customer_code, customer_name, customer_segment,
              NULLIF(payment_terms_days,'')::SMALLINT,
              NULLIF(credit_limit_cents,'')::BIGINT,
              default_currency_code, email, phone, address_line_1, city, country_code,
              (is_active = 'True')
            FROM bronze.raw_customers
            ORDER BY customer_code, ingested_at DESC;

            INSERT INTO silver.products
            SELECT DISTINCT ON (sku)
              sku, product_name, product_category, product_subcategory,
              NULLIF(unit_weight_kg,'')::NUMERIC,
              NULLIF(unit_volume_m3,'')::NUMERIC,
              NULLIF(unit_cost_cents,'')::BIGINT,
              NULLIF(unit_price_cents,'')::BIGINT,
              NULLIF(reorder_threshold,'')::INT,
              (is_active = 'True')
            FROM bronze.raw_products
            ORDER BY sku, ingested_at DESC;

            INSERT INTO silver.warehouses
            SELECT DISTINCT ON (warehouse_code)
              warehouse_code, warehouse_name, city, country_code,
              NULLIF(capacity_m3,'')::NUMERIC,
              (is_active = 'True')
            FROM bronze.raw_warehouses
            ORDER BY warehouse_code, ingested_at DESC;

            INSERT INTO silver.vehicles
            SELECT DISTINCT ON (vehicle_code)
              vehicle_code, license_plate, vehicle_type,
              NULLIF(capacity_kg,'')::NUMERIC,
              NULLIF(capacity_m3,'')::NUMERIC,
              (is_active = 'True')
            FROM bronze.raw_vehicles
            ORDER BY vehicle_code, ingested_at DESC;

            INSERT INTO silver.employees
            SELECT DISTINCT ON (employee_code)
              employee_code, full_name, email, department, role_title,
              NULLIF(hire_date,'')::DATE,
              (is_active = 'True')
            FROM bronze.raw_employees
            ORDER BY employee_code, ingested_at DESC;

            INSERT INTO silver.suppliers
            SELECT DISTINCT ON (supplier_code)
              supplier_code, supplier_name, contact_email, country_code,
              NULLIF(payment_terms_days,'')::SMALLINT,
              (is_active = 'True')
            FROM bronze.raw_suppliers
            ORDER BY supplier_code, ingested_at DESC;

            INSERT INTO silver.orders
            SELECT DISTINCT ON (order_number)
              order_number, customer_code, warehouse_code,
              order_date::DATE, ordered_at::TIMESTAMPTZ,
              status,
              total_amount_cents::BIGINT,
              line_count::INT,
              currency_code
            FROM bronze.raw_orders
            ORDER BY order_number, ingested_at DESC;

            INSERT INTO silver.order_items
            SELECT DISTINCT ON (order_number, line_number)
              order_number, order_date::DATE, line_number::INT,
              sku, quantity::INT,
              unit_price_cents::BIGINT, discount_cents::BIGINT, line_total_cents::BIGINT,
              currency_code
            FROM bronze.raw_order_items
            ORDER BY order_number, line_number, ingested_at DESC;

            INSERT INTO silver.invoices
            SELECT DISTINCT ON (invoice_number)
              invoice_number, customer_code, order_number,
              invoice_date::DATE, due_date::DATE, status,
              amount_billed_cents::BIGINT, amount_paid_cents::BIGINT,
              currency_code
            FROM bronze.raw_invoices
            ORDER BY invoice_number, ingested_at DESC;

            INSERT INTO silver.payments
            SELECT DISTINCT ON (reference_number)
              reference_number, invoice_number, invoice_date::DATE, customer_code,
              payment_date::DATE, amount_cents::BIGINT, currency_code, payment_method
            FROM bronze.raw_payments
            ORDER BY reference_number, ingested_at DESC;

            INSERT INTO silver.shipments
            SELECT DISTINCT ON (shipment_number)
              shipment_number, order_number, warehouse_code, vehicle_code, driver_employee_code,
              ship_date::DATE, shipped_at::TIMESTAMPTZ,
              NULLIF(delivered_at,'')::TIMESTAMPTZ,
              NULLIF(delivered_date,'')::DATE,
              status, origin_city, destination_city, destination_country_code,
              NULLIF(distance_km,'')::NUMERIC,
              NULLIF(weight_kg,'')::NUMERIC,
              NULLIF(volume_m3,'')::NUMERIC,
              NULLIF(freight_cost_cents,'')::BIGINT,
              currency_code
            FROM bronze.raw_shipments
            ORDER BY shipment_number, ingested_at DESC;

            INSERT INTO silver.shipment_items
            SELECT DISTINCT ON (shipment_number, sku)
              shipment_number, ship_date::DATE, sku, units_shipped::INT
            FROM bronze.raw_shipment_items
            ORDER BY shipment_number, sku, ingested_at DESC;

            INSERT INTO silver.inventory_movements (
              sku, warehouse_code, event_at, movement_type, quantity_delta, reference_table, reference_id, reason
            )
            SELECT
              sku, warehouse_code,
              event_at::TIMESTAMPTZ,
              movement_type,
              quantity_delta::INT,
              reference_table,
              reference_id,
              reason
            FROM bronze.raw_inventory_movements;

            -- expense_id from source is the silver PK
            INSERT INTO silver.expenses
            SELECT DISTINCT ON (expense_id)
              expense_id::BIGINT,
              expense_date::DATE, category, description,
              amount_cents::BIGINT, tax_amount_cents::BIGINT, currency_code,
              warehouse_code, vehicle_code, employee_code, supplier_code,
              (is_reimbursable = 'True')
            FROM bronze.raw_expenses
            ORDER BY expense_id, ingested_at DESC;
        """)


# ---------------------------------------------------------------------------
# silver → gold (dims first, then facts that join to dim surrogate keys)
# ---------------------------------------------------------------------------

def load_gold_dims(engine: Engine) -> None:
    """Insert dims with SCD2 columns set for initial load (is_current=true)."""
    with engine.connect() as conn:
        conn.exec_driver_sql(f"""
            INSERT INTO gold.dim_customer (
              customer_code, customer_name, customer_segment, payment_terms_days,
              credit_limit_cents, default_currency_code, email, phone, address_line_1,
              city, country_code, is_active,
              valid_from, valid_to, is_current,
              source_system, source_record_id, etl_batch_id
            )
            SELECT
              customer_code, customer_name, customer_segment, payment_terms_days,
              credit_limit_cents, default_currency_code, email, phone, address_line_1,
              city, country_code, is_active,
              now(), NULL, true,
              '{SOURCE_SYSTEM}', customer_code, '{BATCH_ID}'::uuid
            FROM silver.customers;

            INSERT INTO gold.dim_product (
              sku, product_name, product_category, product_subcategory,
              unit_weight_kg, unit_volume_m3, unit_cost_cents, unit_price_cents,
              reorder_threshold, is_active,
              valid_from, valid_to, is_current,
              source_system, source_record_id, etl_batch_id
            )
            SELECT
              sku, product_name, product_category, product_subcategory,
              unit_weight_kg, unit_volume_m3, unit_cost_cents, unit_price_cents,
              reorder_threshold, is_active,
              now(), NULL, true,
              '{SOURCE_SYSTEM}', sku, '{BATCH_ID}'::uuid
            FROM silver.products;

            INSERT INTO gold.dim_warehouse (
              warehouse_code, warehouse_name, city, country_code, capacity_m3, is_active,
              source_system, source_record_id
            )
            SELECT warehouse_code, warehouse_name, city, country_code, capacity_m3, is_active,
                   '{SOURCE_SYSTEM}', warehouse_code
            FROM silver.warehouses;

            INSERT INTO gold.dim_vehicle (
              vehicle_code, license_plate, vehicle_type, capacity_kg, capacity_m3, is_active,
              source_system, source_record_id
            )
            SELECT vehicle_code, license_plate, vehicle_type, capacity_kg, capacity_m3, is_active,
                   '{SOURCE_SYSTEM}', vehicle_code
            FROM silver.vehicles;

            INSERT INTO gold.dim_employee (
              employee_code, full_name, email, department, role_title, hire_date, is_active,
              source_system, source_record_id
            )
            SELECT employee_code, full_name, email, department, role_title, hire_date, is_active,
                   '{SOURCE_SYSTEM}', employee_code
            FROM silver.employees;

            INSERT INTO gold.dim_supplier (
              supplier_code, supplier_name, contact_email, country_code, payment_terms_days, is_active,
              source_system, source_record_id
            )
            SELECT supplier_code, supplier_name, contact_email, country_code, payment_terms_days, is_active,
                   '{SOURCE_SYSTEM}', supplier_code
            FROM silver.suppliers;
        """)


def load_gold_facts(engine: Engine) -> None:
    """Insert facts, joining to dim tables on natural codes to resolve surrogate IDs."""
    with engine.connect() as conn:
        conn.exec_driver_sql(f"""
            -- Orders
            INSERT INTO gold.fact_orders (
              order_number, customer_id, warehouse_id, order_date, ordered_at,
              status, total_amount_cents, currency_code, line_count, is_void,
              source_system, source_record_id, etl_batch_id
            )
            SELECT
              s.order_number, c.customer_id, w.warehouse_id,
              s.order_date, s.ordered_at,
              s.status, s.total_amount_cents, s.currency_code, s.line_count, false,
              '{SOURCE_SYSTEM}', s.order_number, '{BATCH_ID}'::uuid
            FROM silver.orders s
            JOIN gold.dim_customer  c ON c.customer_code  = s.customer_code  AND c.is_current = true
            LEFT JOIN gold.dim_warehouse w ON w.warehouse_code = s.warehouse_code;

            -- Order items: needs the order's surrogate id
            INSERT INTO gold.fact_order_items (
              order_id, order_date, product_id, line_number,
              quantity, unit_price_cents, line_total_cents, discount_cents, currency_code,
              source_system, source_record_id, etl_batch_id
            )
            SELECT
              o.order_id, oi.order_date, p.product_id, oi.line_number,
              oi.quantity, oi.unit_price_cents, oi.line_total_cents, oi.discount_cents, oi.currency_code,
              '{SOURCE_SYSTEM}',
              oi.order_number || '_' || oi.line_number::TEXT,
              '{BATCH_ID}'::uuid
            FROM silver.order_items oi
            JOIN gold.fact_orders o ON o.order_number = oi.order_number AND o.order_date = oi.order_date
            JOIN gold.dim_product p ON p.sku = oi.sku AND p.is_current = true;

            -- Invoices
            INSERT INTO gold.fact_invoices (
              invoice_number, customer_id, order_id, invoice_date, due_date,
              status, amount_billed_cents, amount_paid_cents, currency_code, is_void,
              source_system, source_record_id, etl_batch_id
            )
            SELECT
              s.invoice_number, c.customer_id,
              o.order_id,
              s.invoice_date, s.due_date,
              s.status, s.amount_billed_cents, s.amount_paid_cents, s.currency_code, false,
              '{SOURCE_SYSTEM}', s.invoice_number, '{BATCH_ID}'::uuid
            FROM silver.invoices s
            JOIN gold.dim_customer c ON c.customer_code = s.customer_code AND c.is_current = true
            LEFT JOIN gold.fact_orders o ON o.order_number = s.order_number;

            -- Payments
            INSERT INTO gold.fact_payments (
              invoice_id, invoice_date, customer_id, payment_date,
              amount_cents, currency_code, payment_method, reference_number,
              source_system, source_record_id, etl_batch_id
            )
            SELECT
              i.invoice_id, p.invoice_date, c.customer_id, p.payment_date,
              p.amount_cents, p.currency_code, p.payment_method, p.reference_number,
              '{SOURCE_SYSTEM}', p.reference_number, '{BATCH_ID}'::uuid
            FROM silver.payments p
            JOIN gold.fact_invoices i
              ON i.invoice_number = p.invoice_number AND i.invoice_date = p.invoice_date
            JOIN gold.dim_customer c
              ON c.customer_code = p.customer_code AND c.is_current = true;

            -- Shipments
            INSERT INTO gold.fact_shipments (
              shipment_number, order_id, warehouse_id, vehicle_id, driver_employee_id,
              ship_date, shipped_at, delivered_at, delivered_date, status,
              origin_city, destination_city, destination_country_code,
              distance_km, weight_kg, volume_m3, freight_cost_cents, currency_code,
              source_system, source_record_id, etl_batch_id
            )
            SELECT
              s.shipment_number,
              o.order_id, w.warehouse_id, v.vehicle_id, e.employee_id,
              s.ship_date, s.shipped_at, s.delivered_at, s.delivered_date, s.status,
              s.origin_city, s.destination_city, s.destination_country_code,
              s.distance_km, s.weight_kg, s.volume_m3, s.freight_cost_cents, s.currency_code,
              '{SOURCE_SYSTEM}', s.shipment_number, '{BATCH_ID}'::uuid
            FROM silver.shipments s
            LEFT JOIN gold.fact_orders   o ON o.order_number   = s.order_number
            JOIN gold.dim_warehouse w ON w.warehouse_code = s.warehouse_code
            LEFT JOIN gold.dim_vehicle  v ON v.vehicle_code  = s.vehicle_code
            LEFT JOIN gold.dim_employee e ON e.employee_code = s.driver_employee_code;

            -- Shipment items
            INSERT INTO gold.fact_shipment_items (
              shipment_id, ship_date, product_id, units_shipped,
              source_system, source_record_id, etl_batch_id
            )
            SELECT
              s.shipment_id, si.ship_date, p.product_id, si.units_shipped,
              '{SOURCE_SYSTEM}', si.shipment_number || '_' || si.sku, '{BATCH_ID}'::uuid
            FROM silver.shipment_items si
            JOIN gold.fact_shipments s
              ON s.shipment_number = si.shipment_number AND s.ship_date = si.ship_date
            JOIN gold.dim_product p
              ON p.sku = si.sku AND p.is_current = true;

            -- Inventory movements
            INSERT INTO gold.fact_inventory_movements (
              product_id, warehouse_id, event_at, movement_type, quantity_delta,
              reference_table, reference_id, reason,
              source_system, source_record_id, etl_batch_id
            )
            SELECT
              p.product_id, w.warehouse_id, m.event_at, m.movement_type, m.quantity_delta,
              m.reference_table,
              CASE WHEN m.reference_id IS NOT NULL THEN m.reference_id::BIGINT ELSE NULL END,
              m.reason,
              '{SOURCE_SYSTEM}', m.movement_id::TEXT, '{BATCH_ID}'::uuid
            FROM silver.inventory_movements m
            JOIN gold.dim_product   p ON p.sku            = m.sku            AND p.is_current = true
            JOIN gold.dim_warehouse w ON w.warehouse_code = m.warehouse_code;

            -- Expenses (expense_id is BIGSERIAL — let it default)
            INSERT INTO gold.fact_expenses (
              expense_date, category, description,
              amount_cents, tax_amount_cents, currency_code,
              warehouse_id, vehicle_id, employee_id, supplier_id,
              is_reimbursable, is_void,
              source_system, source_record_id, etl_batch_id
            )
            SELECT
              e.expense_date, e.category, e.description,
              e.amount_cents, e.tax_amount_cents, e.currency_code,
              w.warehouse_id, v.vehicle_id, emp.employee_id, sup.supplier_id,
              e.is_reimbursable, false,
              '{SOURCE_SYSTEM}', e.expense_id::TEXT, '{BATCH_ID}'::uuid
            FROM silver.expenses e
            LEFT JOIN gold.dim_warehouse w   ON w.warehouse_code = e.warehouse_code
            LEFT JOIN gold.dim_vehicle  v   ON v.vehicle_code   = e.vehicle_code
            LEFT JOIN gold.dim_employee emp ON emp.employee_code = e.employee_code
            LEFT JOIN gold.dim_supplier sup ON sup.supplier_code = e.supplier_code;
        """)


def refresh_materialized(engine: Engine) -> None:
    with engine.connect() as conn:
        # On a fresh DB the unique index just got created with no data; first
        # refresh has to be non-CONCURRENT.
        conn.exec_driver_sql("REFRESH MATERIALIZED VIEW gold.v_revenue_monthly;")
        conn.exec_driver_sql("REFRESH MATERIALIZED VIEW gold.v_monthly_expenses;")


# ---------------------------------------------------------------------------
# Misc
# ---------------------------------------------------------------------------

def print_synth_counts(s: Synth) -> None:
    for k in [
        "customers","products","warehouses","vehicles","employees","suppliers",
        "orders","order_items","invoices","payments","shipments","shipment_items",
        "inventory_movements","expenses",
    ]:
        print(f"    {k:24s} {len(getattr(s, k)):6d}")


def verify(engine: Engine) -> None:
    with engine.connect() as conn:
        for table in [
            "gold.dim_customer","gold.dim_product","gold.dim_warehouse",
            "gold.dim_vehicle","gold.dim_employee","gold.dim_supplier",
            "gold.fact_orders","gold.fact_order_items","gold.fact_invoices",
            "gold.fact_payments","gold.fact_shipments","gold.fact_shipment_items",
            "gold.fact_inventory_movements","gold.fact_expenses",
        ]:
            n = conn.execute(text(f"SELECT count(*) FROM {table}")).scalar()
            print(f"    {table:36s} {n:7d}")


if __name__ == "__main__":
    sys.exit(main())
