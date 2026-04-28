"""transform_altocontrol_silver.py — bronze.ac_*  ->  silver.* for Penicor.

Applies the silver schema (idempotent), TRUNCATEs each silver table, and
runs INSERT...SELECT statements that:
  - TRIM the heavy CHAR padding on every text column
  - Cast types (TEXT -> INT, NUMERIC, DATE, BIGINT cents, BOOLEAN)
  - Filter to TRIM(empresa) = 'Penicor' on tables that have an empresa column
  - Dedup by natural key (DISTINCT ON)

Run after load_altocontrol_bronze.py (which loads the CSVs into bronze).
The two scripts compose: extract -> bronze -> silver.

USAGE
-----
    python etl/transform_altocontrol_silver.py
    python etl/transform_altocontrol_silver.py --schema-only
    python etl/transform_altocontrol_silver.py --connection-string "postgresql+psycopg://..."
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
ETL_DIR = REPO_ROOT / "etl"
DEFAULT_SCHEMA_FILE = ETL_DIR / "altocontrol_silver_schema.sql"
DEFAULT_CONN = "postgresql+psycopg://penicor:penicor@localhost:5433/penicor"


# =============================================================================
# Transform SQL — runs as a single AUTOCOMMIT batch.
# Each block: TRUNCATE then INSERT...SELECT from bronze.
# =============================================================================

TRANSFORM_SQL = r"""
-- ---------------------------------------------------------------- empresas
TRUNCATE silver.empresas;
INSERT INTO silver.empresas (empresa_id, empresa_name)
SELECT DISTINCT ON (id_empresa)
    NULLIF(TRIM(id_empresa), '')::INT,
    NULLIF(TRIM(empresa), '')
FROM bronze.ac_empresas
WHERE NULLIF(TRIM(id_empresa), '') IS NOT NULL
ORDER BY id_empresa, ingested_at DESC;

-- --------------------------------------------------------------- geografia
TRUNCATE silver.geografia;
INSERT INTO silver.geografia (country_code, country_name, department_id, department_name, department_country)
SELECT DISTINCT ON (TRIM(id_pais), NULLIF(TRIM(id_departamento), '')::INT)
    NULLIF(TRIM(id_pais), ''),
    NULLIF(TRIM(pais), ''),
    NULLIF(TRIM(id_departamento), '')::INT,
    NULLIF(TRIM(departamento), ''),
    NULLIF(TRIM(departamento_pais), '')
FROM bronze.ac_geografia
WHERE NULLIF(TRIM(id_pais), '') IS NOT NULL
  AND NULLIF(TRIM(id_departamento), '') IS NOT NULL
ORDER BY TRIM(id_pais), NULLIF(TRIM(id_departamento), '')::INT, ingested_at DESC;

-- ---------------------------------------------------------------- clientes
TRUNCATE silver.clientes;
INSERT INTO silver.clientes (
    customer_branch_id, customer_id, branch_id,
    trade_name, legal_name,
    address, country_code, country_name, department, district,
    price_list, sales_zone, delivery_zone,
    category_1, category_1_1, category_2, category_2_1,
    status, latitude, longitude, rut,
    customer_type_code, customer_type, is_prospect
)
SELECT DISTINCT ON (TRIM(id_sucursal))
    TRIM(id_sucursal)                                          AS customer_branch_id,
    NULLIF(TRIM(cliente), '')::INT                             AS customer_id,
    NULLIF(TRIM(sucursal), '')::INT                            AS branch_id,
    -- nombre/nombre_cliente are duplicates in source; nombre is the trade name
    NULLIF(TRIM(nombre), '')                                   AS trade_name,
    NULLIF(TRIM(razon), '')                                    AS legal_name,
    NULLIF(TRIM(direccion), '')                                AS address,
    NULLIF(TRIM(id_pais), '')                                  AS country_code,
    NULLIF(TRIM(pais), '')                                     AS country_name,
    NULLIF(TRIM(departamento), '')                             AS department,
    NULLIF(TRIM(barrio), '')                                   AS district,
    NULLIF(TRIM(lista), '')                                    AS price_list,
    NULLIF(TRIM(zonaventa), '')                                AS sales_zone,
    NULLIF(TRIM(zonareparto), '')                              AS delivery_zone,
    NULLIF(TRIM(categoria1), '')                               AS category_1,
    NULLIF(TRIM(categoria1_1), '')                             AS category_1_1,
    NULLIF(TRIM(categoria2), '')                               AS category_2,
    NULLIF(TRIM(categoria2_1), '')                             AS category_2_1,
    NULLIF(TRIM(estado), '')                                   AS status,
    NULLIF(TRIM(latitud), '0')::NUMERIC(10,7)                  AS latitude,
    NULLIF(TRIM(longitud), '0')::NUMERIC(10,7)                 AS longitude,
    NULLIF(TRIM(rut), '')                                      AS rut,
    NULLIF(TRIM(codigo_tipocliente), '')::INT                  AS customer_type_code,
    NULLIF(TRIM(tipo_cliente), '')                             AS customer_type,
    (TRIM(es_prospect) IN ('Si','SI','si','S','TRUE','true','1'))::BOOLEAN AS is_prospect
FROM bronze.ac_clientes
WHERE NULLIF(TRIM(id_sucursal), '') IS NOT NULL
ORDER BY TRIM(id_sucursal), ingested_at DESC;

-- --------------------------------------------------------------- articulos
TRUNCATE silver.articulos;
INSERT INTO silver.articulos (
    product_id, product_name, product_code, product_line, is_active,
    family, brand, default_supplier, empresa, tax_class,
    measure, volume, unit_of_measure, size,
    category_1, category_1_1, category_2, category_2_1,
    category_3, category_3_1, category_3_2
)
SELECT DISTINCT ON (TRIM(id_articulo))
    TRIM(id_articulo)                                          AS product_id,
    NULLIF(TRIM(articulo), '')                                 AS product_name,
    NULLIF(TRIM(articulo_cod), '')                             AS product_code,
    NULLIF(TRIM(linea), '')                                    AS product_line,
    (TRIM(activo) IN ('Si','SI','si','S','TRUE','true','1'))::BOOLEAN AS is_active,
    NULLIF(TRIM(familia), '')                                  AS family,
    NULLIF(TRIM(marca), '')                                    AS brand,
    NULLIF(TRIM(proveedor), '')                                AS default_supplier,
    NULLIF(TRIM(empresa), '')                                  AS empresa,
    NULLIF(TRIM(impuesto), '')                                 AS tax_class,
    NULLIF(TRIM(medida), '')::NUMERIC(12,3)                    AS measure,
    NULLIF(TRIM(volumen), '')::NUMERIC(12,3)                   AS volume,
    NULLIF(TRIM(unidad_medida), '')                            AS unit_of_measure,
    NULLIF(TRIM("tamaño"), '')                                 AS size,
    NULLIF(TRIM(categoria1), '')                               AS category_1,
    NULLIF(TRIM(categoria1_1), '')                             AS category_1_1,
    NULLIF(TRIM(categoria2), '')                               AS category_2,
    NULLIF(TRIM(categoria2_1), '')                             AS category_2_1,
    NULLIF(TRIM(categoria3), '')                               AS category_3,
    NULLIF(TRIM(categoria3_1), '')                             AS category_3_1,
    NULLIF(TRIM(categoria3_2), '')                             AS category_3_2
FROM bronze.ac_articulos
WHERE TRIM(empresa) = 'Penicor'
  AND NULLIF(TRIM(id_articulo), '') IS NOT NULL
ORDER BY TRIM(id_articulo), ingested_at DESC;

-- ------------------------------------------------------------- proveedores
TRUNCATE silver.proveedores;
INSERT INTO silver.proveedores (supplier_id, supplier_name, legal_name, rut, address, price_list, category)
SELECT DISTINCT ON (NULLIF(TRIM(id_proveedor), '')::INT)
    NULLIF(TRIM(id_proveedor), '')::INT                        AS supplier_id,
    NULLIF(TRIM(nombre), '')                                   AS supplier_name,
    NULLIF(TRIM(razon), '')                                    AS legal_name,
    NULLIF(TRIM(rut), '')                                      AS rut,
    NULLIF(TRIM(direccion), '')                                AS address,
    NULLIF(TRIM(lista), '')                                    AS price_list,
    NULLIF(TRIM(categoria), '')                                AS category
FROM bronze.ac_proveedores
WHERE NULLIF(TRIM(id_proveedor), '') IS NOT NULL
ORDER BY NULLIF(TRIM(id_proveedor), '')::INT, ingested_at DESC;

-- -------------------------------------------------------------- vendedores
TRUNCATE silver.vendedores;
INSERT INTO silver.vendedores (
    salesperson_id, salesperson_name, invoice_series, warehouse_id,
    supervisor_id, empresa, delivery_zone, sales_zone, salesperson_type
)
SELECT DISTINCT ON (NULLIF(TRIM(id_vendedor), '')::INT)
    NULLIF(TRIM(id_vendedor), '')::INT                         AS salesperson_id,
    NULLIF(TRIM(vendedor), '')                                 AS salesperson_name,
    NULLIF(TRIM(serie), '')                                    AS invoice_series,
    NULLIF(TRIM(deposito), '')::INT                            AS warehouse_id,
    NULLIF(TRIM(supervisor), '')::INT                          AS supervisor_id,
    NULLIF(TRIM(empresa), '')                                  AS empresa,
    NULLIF(TRIM(zona_reparto), '')                             AS delivery_zone,
    NULLIF(TRIM(zona_venta), '')                               AS sales_zone,
    NULLIF(TRIM(tipo_vendedor), '')                            AS salesperson_type
FROM bronze.ac_vendedores
WHERE TRIM(empresa) = 'Penicor'
  AND NULLIF(TRIM(id_vendedor), '') IS NOT NULL
ORDER BY NULLIF(TRIM(id_vendedor), '')::INT, ingested_at DESC;

-- ------------------------------------------------------------------ routes
TRUNCATE silver.routes;
INSERT INTO silver.routes (route_id, route_name)
SELECT DISTINCT ON (NULLIF(TRIM(id_ruta), '')::INT)
    NULLIF(TRIM(id_ruta), '')::INT                             AS route_id,
    NULLIF(TRIM(ruta), '')                                     AS route_name
FROM bronze.ac_ruta
WHERE NULLIF(TRIM(id_ruta), '') IS NOT NULL
ORDER BY NULLIF(TRIM(id_ruta), '')::INT, ingested_at DESC;

-- ------------------------------------------------------- route_assignments
TRUNCATE silver.route_assignments;
INSERT INTO silver.route_assignments (salesperson_id, route_id, customer_branch_id, visit_order)
SELECT DISTINCT ON (NULLIF(TRIM(vendedor), '')::INT, NULLIF(TRIM(id_ruta), '')::INT, TRIM(id_sucursal))
    NULLIF(TRIM(vendedor), '')::INT                            AS salesperson_id,
    NULLIF(TRIM(id_ruta), '')::INT                             AS route_id,
    TRIM(id_sucursal)                                          AS customer_branch_id,
    NULLIF(TRIM(orden), '')::INT                               AS visit_order
FROM bronze.ac_rutas
WHERE NULLIF(TRIM(vendedor), '') IS NOT NULL
  AND NULLIF(TRIM(id_ruta), '') IS NOT NULL
  AND NULLIF(TRIM(id_sucursal), '') IS NOT NULL
ORDER BY NULLIF(TRIM(vendedor), '')::INT,
         NULLIF(TRIM(id_ruta), '')::INT,
         TRIM(id_sucursal),
         ingested_at DESC;

-- ------------------------------------------------------------------ ventas
TRUNCATE silver.ventas;
INSERT INTO silver.ventas (
    line_id, invoice_id, fecha, document_type, salesperson_id,
    currency_code, exchange_rate, price_list,
    customer_id, customer_branch_id, product_id,
    affects_sale, measure_value, volume, quantity, cases,
    list_price_cents, unit_price_cents, subtotal_cents,
    discount_cents, net_cents, tax_cents, total_cents
)
SELECT DISTINCT ON (TRIM(id_renglon))
    TRIM(id_renglon)                                           AS line_id,
    TRIM(id_factura)                                           AS invoice_id,
    fecha::DATE                                                AS fecha,
    TRIM(documento)                                            AS document_type,
    NULLIF(TRIM(id_vendedor), '')::INT                         AS salesperson_id,
    TRIM(moneda)                                               AS currency_code,
    NULLIF(TRIM(cotizacion), '')::NUMERIC(18,8)                AS exchange_rate,
    NULLIF(TRIM(lista), '')                                    AS price_list,
    NULLIF(TRIM(id_cliente), '')::INT                          AS customer_id,
    NULLIF(TRIM(id_sucursal), '')                              AS customer_branch_id,
    NULLIF(TRIM(id_articulo), '')                              AS product_id,
    NULLIF(TRIM(afecta_venta), '')::SMALLINT                   AS affects_sale,
    NULLIF(TRIM(valor_medida), '')::NUMERIC(12,3)              AS measure_value,
    NULLIF(TRIM(volumen), '')::NUMERIC(12,3)                   AS volume,
    NULLIF(TRIM(cantidad), '')::NUMERIC(12,3)                  AS quantity,
    NULLIF(TRIM(cajones), '')::NUMERIC(12,3)                   AS cases,
    (NULLIF(TRIM(preciolista), '')::NUMERIC * 100)::BIGINT     AS list_price_cents,
    (NULLIF(TRIM(preciounitario), '')::NUMERIC * 100)::BIGINT  AS unit_price_cents,
    (NULLIF(TRIM(subtotal), '')::NUMERIC * 100)::BIGINT        AS subtotal_cents,
    (NULLIF(TRIM(descuentos), '')::NUMERIC * 100)::BIGINT      AS discount_cents,
    (NULLIF(TRIM(neto), '')::NUMERIC * 100)::BIGINT            AS net_cents,
    (NULLIF(TRIM(impuestos), '')::NUMERIC * 100)::BIGINT       AS tax_cents,
    (NULLIF(TRIM(total), '')::NUMERIC * 100)::BIGINT           AS total_cents
FROM bronze.ac_ventas
WHERE TRIM(empresa) = 'Penicor'
  AND NULLIF(TRIM(id_renglon), '') IS NOT NULL
ORDER BY TRIM(id_renglon), ingested_at DESC;

-- ----------------------------------------------------------------- compras
TRUNCATE silver.compras;
INSERT INTO silver.compras (
    line_id, invoice_id, fecha, document_type, salesperson_id,
    currency_code, exchange_rate, price_list,
    supplier_id, product_id,
    affects_sale, measure_value, volume, quantity,
    list_price_cents, unit_price_cents, subtotal_cents,
    discount_cents, net_cents, tax_cents, total_cents
)
SELECT DISTINCT ON (TRIM(id_renglon))
    TRIM(id_renglon)                                           AS line_id,
    TRIM(id_factura)                                           AS invoice_id,
    fecha::DATE                                                AS fecha,
    TRIM(documento)                                            AS document_type,
    NULLIF(TRIM(id_vendedor), '')::INT                         AS salesperson_id,
    TRIM(moneda)                                               AS currency_code,
    NULLIF(TRIM(cotizacion), '')::NUMERIC(18,8)                AS exchange_rate,
    NULLIF(TRIM(lista), '')                                    AS price_list,
    NULLIF(TRIM(id_proveedor), '')::INT                        AS supplier_id,
    NULLIF(TRIM(id_articulo), '')                              AS product_id,
    NULLIF(TRIM(afecta_venta), '')::SMALLINT                   AS affects_sale,
    NULLIF(TRIM(valor_medida), '')::NUMERIC(12,3)              AS measure_value,
    NULLIF(TRIM(volumen), '')::NUMERIC(12,3)                   AS volume,
    NULLIF(TRIM(cantidad), '')::NUMERIC(12,3)                  AS quantity,
    (NULLIF(TRIM(preciolista), '')::NUMERIC * 100)::BIGINT     AS list_price_cents,
    (NULLIF(TRIM(preciounitario), '')::NUMERIC * 100)::BIGINT  AS unit_price_cents,
    (NULLIF(TRIM(subtotal), '')::NUMERIC * 100)::BIGINT        AS subtotal_cents,
    (NULLIF(TRIM(descuentos), '')::NUMERIC * 100)::BIGINT      AS discount_cents,
    (NULLIF(TRIM(neto), '')::NUMERIC * 100)::BIGINT            AS net_cents,
    (NULLIF(TRIM(impuestos), '')::NUMERIC * 100)::BIGINT       AS tax_cents,
    (NULLIF(TRIM(total), '')::NUMERIC * 100)::BIGINT           AS total_cents
FROM bronze.ac_compras
WHERE TRIM(empresa) = 'Penicor'
  AND NULLIF(TRIM(id_renglon), '') IS NOT NULL
ORDER BY TRIM(id_renglon), ingested_at DESC;

-- ------------------------------------------------------------------- pagos
TRUNCATE silver.pagos;
INSERT INTO silver.pagos (
    payment_id, fecha, document_type, salesperson_id,
    currency_code, exchange_rate, customer_id, customer_branch_id,
    affects_ar, amount_cents
)
SELECT DISTINCT ON (TRIM(id_pago))
    TRIM(id_pago)                                              AS payment_id,
    fecha::DATE                                                AS fecha,
    TRIM(documento)                                            AS document_type,
    NULLIF(TRIM(id_vendedor), '')::INT                         AS salesperson_id,
    TRIM(moneda)                                               AS currency_code,
    NULLIF(TRIM(cotizacion), '')::NUMERIC(18,8)                AS exchange_rate,
    NULLIF(TRIM(id_cliente), '')::INT                          AS customer_id,
    NULLIF(TRIM(id_sucursal), '')                              AS customer_branch_id,
    NULLIF(TRIM(afectacion), '')::SMALLINT                     AS affects_ar,
    (NULLIF(TRIM(pagos), '')::NUMERIC * 100)::BIGINT           AS amount_cents
FROM bronze.ac_pagos
WHERE TRIM(empresa) = 'Penicor'
  AND NULLIF(TRIM(id_pago), '') IS NOT NULL
ORDER BY TRIM(id_pago), ingested_at DESC;

-- --------------------------------------------------- deuda_por_cliente
TRUNCATE silver.deuda_por_cliente;
INSERT INTO silver.deuda_por_cliente (
    customer_branch_id, document_id, document_date, due_date,
    document_type, cfe_document, salesperson_id,
    currency_code, exchange_rate, amount_cents, outstanding_cents
)
SELECT DISTINCT ON (TRIM(id_sucursal), TRIM(id_documento))
    TRIM(id_sucursal)                                          AS customer_branch_id,
    TRIM(id_documento)                                         AS document_id,
    fecha::DATE                                                AS document_date,
    NULLIF(vencimiento, '')::DATE                              AS due_date,
    TRIM(documento)                                            AS document_type,
    NULLIF(TRIM(documento_cfe), '')                            AS cfe_document,
    NULLIF(TRIM(id_vendedor), '')::INT                         AS salesperson_id,
    TRIM(moneda)                                               AS currency_code,
    NULLIF(TRIM(cotiza), '')::NUMERIC(18,8)                    AS exchange_rate,
    (NULLIF(TRIM(importe), '')::NUMERIC * 100)::BIGINT         AS amount_cents,
    (NULLIF(TRIM(deuda), '')::NUMERIC * 100)::BIGINT           AS outstanding_cents
FROM bronze.ac_deuda_por_cliente
WHERE TRIM(empresa) = 'Penicor'
  AND NULLIF(TRIM(id_sucursal), '') IS NOT NULL
  AND NULLIF(TRIM(id_documento), '') IS NOT NULL
ORDER BY TRIM(id_sucursal), TRIM(id_documento), ingested_at DESC;

-- ----------------------------------------------------------------- visitas
TRUNCATE silver.visitas;
INSERT INTO silver.visitas (
    visit_id, fecha, start_at, end_at,
    salesperson_id, customer_id, customer_branch_id,
    visit_type_id, visit_type, notes,
    duration_minutes, duration_hours, latitude, longitude
)
SELECT DISTINCT ON (TRIM(id_visita))
    TRIM(id_visita)                                            AS visit_id,
    NULLIF(fecha, '')::DATE                                    AS fecha,
    NULLIF(fecha_inicio, '')::TIMESTAMPTZ                      AS start_at,
    NULLIF(fecha_fin, '')::TIMESTAMPTZ                         AS end_at,
    NULLIF(TRIM(id_vendedor), '')::INT                         AS salesperson_id,
    NULLIF(TRIM(id_cliente), '')::INT                          AS customer_id,
    NULLIF(TRIM(id_sucursal), '')                              AS customer_branch_id,
    NULLIF(TRIM(id_tipo_visita), '')::INT                      AS visit_type_id,
    NULLIF(TRIM(tipo_visita), '')                              AS visit_type,
    NULLIF(TRIM(observacion), '')                              AS notes,
    NULLIF(TRIM(duracion_min), '')::NUMERIC(10,2)              AS duration_minutes,
    NULLIF(TRIM(duracion_hs), '')::NUMERIC(10,2)               AS duration_hours,
    NULLIF(TRIM(latitud), '0')::NUMERIC(10,7)                  AS latitude,
    NULLIF(TRIM(longitud), '0')::NUMERIC(10,7)                 AS longitude
FROM bronze.ac_visitas
WHERE NULLIF(TRIM(id_visita), '') IS NOT NULL
ORDER BY TRIM(id_visita), ingested_at DESC;

-- ------------------------------------------------------------------- stock
TRUNCATE silver.stock;
INSERT INTO silver.stock (
    warehouse_id, warehouse_name, product_id, product_name,
    quantity, min_quantity, is_understocked
)
SELECT DISTINCT ON (NULLIF(TRIM(id_deposito), '')::INT, TRIM(id_articulo))
    NULLIF(TRIM(id_deposito), '')::INT                         AS warehouse_id,
    TRIM("Deposito")                                           AS warehouse_name,
    TRIM(id_articulo)                                          AS product_id,
    TRIM("Articulo")                                           AS product_name,
    NULLIF(TRIM("Cantidad"), '')::NUMERIC(12,3)                AS quantity,
    COALESCE(NULLIF(TRIM("Cantidad_min"), '')::NUMERIC(12,3), 0) AS min_quantity,
    (TRIM("Con_Faltante") IN ('Si','SI','si','S','TRUE','true','1'))::BOOLEAN AS is_understocked
FROM bronze.ac_stock
WHERE NULLIF(TRIM(id_deposito), '') IS NOT NULL
  AND NULLIF(TRIM(id_articulo), '') IS NOT NULL
ORDER BY NULLIF(TRIM(id_deposito), '')::INT, TRIM(id_articulo), ingested_at DESC;
"""


VERIFY_TABLES = [
    "silver.empresas",
    "silver.geografia",
    "silver.clientes",
    "silver.articulos",
    "silver.proveedores",
    "silver.vendedores",
    "silver.routes",
    "silver.route_assignments",
    "silver.ventas",
    "silver.compras",
    "silver.pagos",
    "silver.deuda_por_cliente",
    "silver.visitas",
    "silver.stock",
]


def main() -> int:
    load_env()

    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--connection-string", "-c",
                    default=os.environ.get("PENICOR_DB_URL") or DEFAULT_CONN,
                    help=f"Postgres URL (default: ${{PENICOR_DB_URL}} or {DEFAULT_CONN})")
    ap.add_argument("--schema-file", type=Path, default=DEFAULT_SCHEMA_FILE,
                    help=f"Silver DDL file (default: {DEFAULT_SCHEMA_FILE})")
    ap.add_argument("--schema-only", action="store_true",
                    help="Apply schema and exit; don't run transforms.")
    args = ap.parse_args()

    try:
        from sqlalchemy import create_engine, text  # type: ignore
    except ImportError as e:
        print(f"[silver] missing dep: {e}. Install with:  uv pip install -e \".[etl]\"")
        return 2

    if not args.schema_file.exists():
        print(f"[silver] schema file not found: {args.schema_file}")
        return 1

    print(f"[silver] connecting to {redact(args.connection_string)} ...")
    try:
        engine = create_engine(args.connection_string, isolation_level="AUTOCOMMIT")
        with engine.connect() as conn:
            conn.execute(text("SELECT 1")).scalar()
    except Exception as e:  # noqa: BLE001
        print(f"[silver] connection FAILED: {e}")
        return 3

    t0 = time.time()
    print(f"[silver] applying schema {args.schema_file.relative_to(REPO_ROOT)} ...")
    sql = args.schema_file.read_text(encoding="utf-8")
    with engine.connect() as conn:
        conn.exec_driver_sql(sql)

    if args.schema_only:
        print(f"[silver] schema applied in {time.time() - t0:.1f}s (--schema-only)")
        return 0

    print(f"[silver] running bronze -> silver transforms ...")
    t1 = time.time()
    try:
        with engine.connect() as conn:
            conn.exec_driver_sql(TRANSFORM_SQL)
    except Exception as e:  # noqa: BLE001
        print(f"[silver] transform FAILED: {e}")
        return 4

    print(f"[silver] transforms done in {time.time() - t1:.1f}s")

    print(f"[silver] verifying row counts:")
    with engine.connect() as conn:
        for table in VERIFY_TABLES:
            n = conn.execute(text(f"SELECT count(*) FROM {table}")).scalar()
            print(f"          {table:35s} {n:>7d}")

    print(f"[silver] total elapsed: {time.time() - t0:.1f}s")
    return 0


def load_env() -> None:
    try:
        from dotenv import load_dotenv  # type: ignore
        env_path = REPO_ROOT / ".env"
        if env_path.exists():
            load_dotenv(env_path)
    except ImportError:
        pass


def redact(url: str) -> str:
    if "://" not in url or "@" not in url:
        return url
    scheme, rest = url.split("://", 1)
    if "@" not in rest:
        return url
    creds, host = rest.rsplit("@", 1)
    if ":" not in creds:
        return url
    user, _ = creds.split(":", 1)
    return f"{scheme}://{user}:***@{host}"


if __name__ == "__main__":
    sys.exit(main())
