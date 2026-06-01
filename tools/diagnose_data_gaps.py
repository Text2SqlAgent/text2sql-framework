"""Quick diagnostic queries to root-cause the data gaps in the validation report.

Checks:
  1. silver.articulos: how many rows have family / brand populated?
  2. silver.clientes: how many rows have sales_zone populated?
  3. silver.ventas: date range; cases vs quantity for kg products.
  4. bronze.ac_articulos / bronze.ac_clientes / bronze.ac_ventas: same checks at bronze
     to determine whether silver dropped data or extract didn't carry it.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

QUERIES: list[tuple[str, str]] = [
    ("silver.articulos: family/brand population",
     """
     SELECT
       COUNT(*) AS total,
       COUNT(family) AS with_family,
       COUNT(brand)  AS with_brand,
       COUNT(unit_of_measure) AS with_unit_of_measure
     FROM silver.articulos
     """),
    ("silver.articulos: distinct family / brand sample",
     """
     SELECT family, brand, unit_of_measure, COUNT(*) AS n
     FROM silver.articulos
     GROUP BY family, brand, unit_of_measure
     ORDER BY n DESC
     LIMIT 10
     """),
    ("bronze.ac_articulos: familia/marca population",
     """
     SELECT
       COUNT(*) AS total,
       COUNT(NULLIF(TRIM(familia), '')) AS with_familia,
       COUNT(NULLIF(TRIM(marca), '')) AS with_marca,
       COUNT(NULLIF(TRIM(unidad_medida), '')) AS with_unit_of_measure,
       COUNT(*) FILTER (WHERE TRIM(empresa) = 'Penicor') AS penicor_rows
     FROM bronze.ac_articulos
     """),
    ("bronze.ac_articulos: top-selling products' family/marca",
     """
     SELECT articulo, familia, marca, unidad_medida
     FROM bronze.ac_articulos
     WHERE TRIM(empresa) = 'Penicor'
       AND TRIM(articulo) IN ('Milanesa de Pollo - Kg', 'Filet entero Gril - Kg',
                               'Cubos de Pollo  Kg', 'BURRITOS POLLO Y QUESO x Ud 165g')
     LIMIT 10
     """),
    ("silver.clientes: sales_zone population",
     """
     SELECT
       COUNT(*) AS total,
       COUNT(sales_zone) AS with_sales_zone,
       COUNT(delivery_zone) AS with_delivery_zone,
       COUNT(department)    AS with_department
     FROM silver.clientes
     """),
    ("bronze.ac_clientes: zonaventa population",
     """
     SELECT
       COUNT(*) AS total,
       COUNT(NULLIF(TRIM(zonaventa), '')) AS with_zonaventa,
       COUNT(NULLIF(TRIM(zonareparto), '')) AS with_zonareparto,
       COUNT(NULLIF(TRIM(departamento), '')) AS with_departamento
     FROM bronze.ac_clientes
     """),
    ("silver.ventas: date range + cases/quantity comparison",
     """
     SELECT
       MIN(fecha) AS min_date,
       MAX(fecha) AS max_date,
       COUNT(*) AS total_lines,
       COUNT(*) FILTER (WHERE cases IS NOT NULL AND cases > 0) AS lines_with_cases,
       COUNT(*) FILTER (WHERE cases = quantity) AS cases_eq_quantity,
       COUNT(*) FILTER (WHERE cases = quantity AND quantity > 0) AS cases_eq_quantity_pos
     FROM silver.ventas
     """),
    ("silver.ventas: month distribution",
     """
     SELECT TO_CHAR(DATE_TRUNC('month', fecha), 'YYYY-MM') AS month, COUNT(*) AS lines
     FROM silver.ventas
     GROUP BY 1
     ORDER BY 1
     """),
    ("bronze.ac_ventas: month distribution (Penicor only)",
     """
     SELECT TO_CHAR(DATE_TRUNC('month', fecha::DATE), 'YYYY-MM') AS month, COUNT(*) AS lines
     FROM bronze.ac_ventas
     WHERE TRIM(empresa) = 'Penicor'
     GROUP BY 1
     ORDER BY 1
     """),
    ("silver.ventas: cases vs quantity for kg products",
     """
     SELECT a.product_name, a.unit_of_measure,
            COUNT(*) AS lines,
            SUM(v.quantity) AS sum_qty,
            SUM(v.cases) AS sum_cases,
            COUNT(*) FILTER (WHERE v.cases = v.quantity) AS cases_eq_qty
     FROM silver.ventas v
     JOIN silver.articulos a ON a.product_id = v.product_id
     WHERE a.product_name ILIKE '%Kg%'
        OR a.unit_of_measure ILIKE '%kg%'
     GROUP BY a.product_name, a.unit_of_measure
     ORDER BY lines DESC
     LIMIT 10
     """),
]


def main() -> int:
    load_env()
    db = os.environ.get("PENICOR_DB_URL")
    if not db:
        print("[diag] PENICOR_DB_URL not set", file=sys.stderr)
        return 1

    from sqlalchemy import create_engine, text
    eng = create_engine(db, isolation_level="AUTOCOMMIT")
    with eng.connect() as conn:
        for title, q in QUERIES:
            print(f"=== {title}")
            try:
                rows = conn.execute(text(q)).mappings().all()
            except Exception as e:
                print(f"    ERROR: {e}")
                continue
            if not rows:
                print("    (no rows)")
                continue
            for r in rows:
                print("   ", dict(r))
            print()
    return 0


def load_env() -> None:
    try:
        from dotenv import load_dotenv
        load_dotenv(REPO_ROOT / ".env")
    except ImportError:
        pass


if __name__ == "__main__":
    sys.exit(main())
