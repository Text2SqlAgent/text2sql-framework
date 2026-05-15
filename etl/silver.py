"""Silver layer — transform bronze.ac_* into typed, cleaned silver tables."""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SILVER_SCHEMA_FILE = REPO_ROOT / "etl" / "altocontrol_silver_schema.sql"

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
    TRIM(id_sucursal),
    NULLIF(TRIM(cliente), '')::INT,
    NULLIF(TRIM(sucursal), '')::INT,
    NULLIF(TRIM(nombre), ''),
    NULLIF(TRIM(razon), ''),
    NULLIF(TRIM(direccion), ''),
    NULLIF(TRIM(id_pais), ''),
    NULLIF(TRIM(pais), ''),
    NULLIF(TRIM(departamento), ''),
    NULLIF(TRIM(barrio), ''),
    NULLIF(TRIM(lista), ''),
    NULLIF(TRIM(zonaventa), ''),
    NULLIF(TRIM(zonareparto), ''),
    NULLIF(TRIM(categoria1), ''),
    NULLIF(TRIM(categoria1_1), ''),
    NULLIF(TRIM(categoria2), ''),
    NULLIF(TRIM(categoria2_1), ''),
    NULLIF(TRIM(estado), ''),
    NULLIF(TRIM(latitud), '0')::NUMERIC(10,7),
    NULLIF(TRIM(longitud), '0')::NUMERIC(10,7),
    NULLIF(TRIM(rut), ''),
    NULLIF(TRIM(codigo_tipocliente), '')::INT,
    NULLIF(TRIM(tipo_cliente), ''),
    (TRIM(es_prospect) IN ('Si','SI','si','S','TRUE','true','1'))::BOOLEAN
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
    TRIM(id_articulo),
    NULLIF(TRIM(articulo), ''),
    NULLIF(TRIM(articulo_cod), ''),
    NULLIF(TRIM(linea), ''),
    (TRIM(activo) IN ('Si','SI','si','S','TRUE','true','1'))::BOOLEAN,
    NULLIF(TRIM(familia), ''),
    NULLIF(TRIM(marca), ''),
    NULLIF(TRIM(proveedor), ''),
    NULLIF(TRIM(empresa), ''),
    NULLIF(TRIM(impuesto), ''),
    NULLIF(TRIM(medida), '')::NUMERIC(12,3),
    NULLIF(TRIM(volumen), '')::NUMERIC(12,3),
    NULLIF(TRIM(unidad_medida), ''),
    NULLIF(TRIM("tamaño"), ''),
    NULLIF(TRIM(categoria1), ''),
    NULLIF(TRIM(categoria1_1), ''),
    NULLIF(TRIM(categoria2), ''),
    NULLIF(TRIM(categoria2_1), ''),
    NULLIF(TRIM(categoria3), ''),
    NULLIF(TRIM(categoria3_1), ''),
    NULLIF(TRIM(categoria3_2), '')
FROM bronze.ac_articulos
WHERE TRIM(empresa) = 'Penicor'
  AND NULLIF(TRIM(id_articulo), '') IS NOT NULL
ORDER BY TRIM(id_articulo), ingested_at DESC;

-- ------------------------------------------------------------- proveedores
TRUNCATE silver.proveedores;
INSERT INTO silver.proveedores (supplier_id, supplier_name, legal_name, rut, address, price_list, category)
SELECT DISTINCT ON (NULLIF(TRIM(id_proveedor), '')::INT)
    NULLIF(TRIM(id_proveedor), '')::INT,
    NULLIF(TRIM(nombre), ''),
    NULLIF(TRIM(razon), ''),
    NULLIF(TRIM(rut), ''),
    NULLIF(TRIM(direccion), ''),
    NULLIF(TRIM(lista), ''),
    NULLIF(TRIM(categoria), '')
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
    NULLIF(TRIM(id_vendedor), '')::INT,
    NULLIF(TRIM(vendedor), ''),
    NULLIF(TRIM(serie), ''),
    NULLIF(TRIM(deposito), '')::INT,
    NULLIF(TRIM(supervisor), '')::INT,
    NULLIF(TRIM(empresa), ''),
    NULLIF(TRIM(zona_reparto), ''),
    NULLIF(TRIM(zona_venta), ''),
    NULLIF(TRIM(tipo_vendedor), '')
FROM bronze.ac_vendedores
WHERE TRIM(empresa) = 'Penicor'
  AND NULLIF(TRIM(id_vendedor), '') IS NOT NULL
ORDER BY NULLIF(TRIM(id_vendedor), '')::INT, ingested_at DESC;

-- ------------------------------------------------------------------ routes
TRUNCATE silver.routes;
INSERT INTO silver.routes (route_id, route_name)
SELECT DISTINCT ON (NULLIF(TRIM(id_ruta), '')::INT)
    NULLIF(TRIM(id_ruta), '')::INT,
    NULLIF(TRIM(ruta), '')
FROM bronze.ac_ruta
WHERE NULLIF(TRIM(id_ruta), '') IS NOT NULL
ORDER BY NULLIF(TRIM(id_ruta), '')::INT, ingested_at DESC;

-- ------------------------------------------------------- route_assignments
TRUNCATE silver.route_assignments;
INSERT INTO silver.route_assignments (salesperson_id, route_id, customer_branch_id, visit_order)
SELECT DISTINCT ON (NULLIF(TRIM(vendedor), '')::INT, NULLIF(TRIM(id_ruta), '')::INT, TRIM(id_sucursal))
    NULLIF(TRIM(vendedor), '')::INT,
    NULLIF(TRIM(id_ruta), '')::INT,
    TRIM(id_sucursal),
    NULLIF(TRIM(orden), '')::INT
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
    TRIM(id_renglon),
    TRIM(id_factura),
    fecha::DATE,
    TRIM(documento),
    NULLIF(TRIM(id_vendedor), '')::INT,
    TRIM(moneda),
    NULLIF(TRIM(cotizacion), '')::NUMERIC(18,8),
    NULLIF(TRIM(lista), ''),
    NULLIF(TRIM(id_cliente), '')::INT,
    NULLIF(TRIM(id_sucursal), ''),
    NULLIF(TRIM(id_articulo), ''),
    NULLIF(TRIM(afecta_venta), '')::SMALLINT,
    NULLIF(TRIM(valor_medida), '')::NUMERIC(12,3),
    NULLIF(TRIM(volumen), '')::NUMERIC(12,3),
    NULLIF(TRIM(cantidad), '')::NUMERIC(12,3),
    NULLIF(TRIM(cajones), '')::NUMERIC(12,3),
    (NULLIF(TRIM(preciolista), '')::NUMERIC * 100)::BIGINT,
    (NULLIF(TRIM(preciounitario), '')::NUMERIC * 100)::BIGINT,
    (NULLIF(TRIM(subtotal), '')::NUMERIC * 100)::BIGINT,
    (NULLIF(TRIM(descuentos), '')::NUMERIC * 100)::BIGINT,
    (NULLIF(TRIM(neto), '')::NUMERIC * 100)::BIGINT,
    (NULLIF(TRIM(impuestos), '')::NUMERIC * 100)::BIGINT,
    (NULLIF(TRIM(total), '')::NUMERIC * 100)::BIGINT
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
    TRIM(id_renglon),
    TRIM(id_factura),
    fecha::DATE,
    TRIM(documento),
    NULLIF(TRIM(id_vendedor), '')::INT,
    TRIM(moneda),
    NULLIF(TRIM(cotizacion), '')::NUMERIC(18,8),
    NULLIF(TRIM(lista), ''),
    NULLIF(TRIM(id_proveedor), '')::INT,
    NULLIF(TRIM(id_articulo), ''),
    NULLIF(TRIM(afecta_venta), '')::SMALLINT,
    NULLIF(TRIM(valor_medida), '')::NUMERIC(12,3),
    NULLIF(TRIM(volumen), '')::NUMERIC(12,3),
    NULLIF(TRIM(cantidad), '')::NUMERIC(12,3),
    (NULLIF(TRIM(preciolista), '')::NUMERIC * 100)::BIGINT,
    (NULLIF(TRIM(preciounitario), '')::NUMERIC * 100)::BIGINT,
    (NULLIF(TRIM(subtotal), '')::NUMERIC * 100)::BIGINT,
    (NULLIF(TRIM(descuentos), '')::NUMERIC * 100)::BIGINT,
    (NULLIF(TRIM(neto), '')::NUMERIC * 100)::BIGINT,
    (NULLIF(TRIM(impuestos), '')::NUMERIC * 100)::BIGINT,
    (NULLIF(TRIM(total), '')::NUMERIC * 100)::BIGINT
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
    TRIM(id_pago),
    fecha::DATE,
    TRIM(documento),
    NULLIF(TRIM(id_vendedor), '')::INT,
    TRIM(moneda),
    NULLIF(TRIM(cotizacion), '')::NUMERIC(18,8),
    NULLIF(TRIM(id_cliente), '')::INT,
    NULLIF(TRIM(id_sucursal), ''),
    NULLIF(TRIM(afectacion), '')::SMALLINT,
    (NULLIF(TRIM(pagos), '')::NUMERIC * 100)::BIGINT
FROM bronze.ac_pagos
WHERE TRIM(empresa) = 'Penicor'
  AND NULLIF(TRIM(id_pago), '') IS NOT NULL
ORDER BY TRIM(id_pago), ingested_at DESC;

-- -------------------------------------------------------- deuda_por_cliente
TRUNCATE silver.deuda_por_cliente;
INSERT INTO silver.deuda_por_cliente (
    customer_branch_id, document_id, document_date, due_date,
    document_type, cfe_document, salesperson_id,
    currency_code, exchange_rate, amount_cents, outstanding_cents
)
SELECT DISTINCT ON (TRIM(id_sucursal), TRIM(id_documento))
    TRIM(id_sucursal),
    TRIM(id_documento),
    fecha::DATE,
    NULLIF(vencimiento, '')::DATE,
    TRIM(documento),
    NULLIF(TRIM(documento_cfe), ''),
    NULLIF(TRIM(id_vendedor), '')::INT,
    TRIM(moneda),
    NULLIF(TRIM(cotiza), '')::NUMERIC(18,8),
    (NULLIF(TRIM(importe), '')::NUMERIC * 100)::BIGINT,
    (NULLIF(TRIM(deuda), '')::NUMERIC * 100)::BIGINT
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
    TRIM(id_visita),
    NULLIF(fecha, '')::DATE,
    NULLIF(fecha_inicio, '')::TIMESTAMPTZ,
    NULLIF(fecha_fin, '')::TIMESTAMPTZ,
    NULLIF(TRIM(id_vendedor), '')::INT,
    NULLIF(TRIM(id_cliente), '')::INT,
    NULLIF(TRIM(id_sucursal), ''),
    NULLIF(TRIM(id_tipo_visita), '')::INT,
    NULLIF(TRIM(tipo_visita), ''),
    NULLIF(TRIM(observacion), ''),
    NULLIF(TRIM(duracion_min), '')::NUMERIC(10,2),
    NULLIF(TRIM(duracion_hs), '')::NUMERIC(10,2),
    NULLIF(TRIM(latitud), '0')::NUMERIC(10,7),
    NULLIF(TRIM(longitud), '0')::NUMERIC(10,7)
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
    NULLIF(TRIM(id_deposito), '')::INT,
    TRIM(deposito),
    TRIM(id_articulo),
    TRIM(articulo),
    NULLIF(TRIM(cantidad), '')::NUMERIC(12,3),
    COALESCE(NULLIF(TRIM(cantidad_min), '')::NUMERIC(12,3), 0),
    (TRIM(con_faltante) IN ('Si','SI','si','S','TRUE','true','1'))::BOOLEAN
FROM bronze.ac_stock
WHERE NULLIF(TRIM(id_deposito), '') IS NOT NULL
  AND NULLIF(TRIM(id_articulo), '') IS NOT NULL
ORDER BY NULLIF(TRIM(id_deposito), '')::INT, TRIM(id_articulo), ingested_at DESC;
"""

TABLES = [
    "silver.empresas", "silver.geografia", "silver.clientes",
    "silver.articulos", "silver.proveedores", "silver.vendedores",
    "silver.routes", "silver.route_assignments",
    "silver.ventas", "silver.compras", "silver.pagos",
    "silver.deuda_por_cliente", "silver.visitas", "silver.stock",
]


def run(target_conn_str: str) -> int:
    """Transform bronze → silver. Returns total rows across all silver tables."""
    from sqlalchemy import create_engine, text

    engine = create_engine(target_conn_str, isolation_level="AUTOCOMMIT")

    if SILVER_SCHEMA_FILE.exists():
        print("[silver] applying schema ...")
        with engine.connect() as conn:
            conn.exec_driver_sql(SILVER_SCHEMA_FILE.read_text(encoding="utf-8"))

    print("[silver] transforming bronze → silver ...")
    with engine.connect() as conn:
        conn.exec_driver_sql(TRANSFORM_SQL)

    total = 0
    with engine.connect() as conn:
        for table in TABLES:
            n = int(conn.execute(text(f"SELECT count(*) FROM {table}")).scalar() or 0)
            print(f"[silver]   {table:35s} {n:>7,}")
            total += n

    print(f"[silver] done — {total:,} rows total")
    return total
