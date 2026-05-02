"""Bronze layer — extract directly from Azure SQL Server into Postgres bronze tables.

Connects to the client DB via pyodbc, executes each PBI_tabla_* stored procedure,
and upserts rows into bronze.ac_* tables. No intermediate files.

Each run is an upsert keyed on the table's natural business key, so re-running
is safe and incremental loads don't produce duplicates.
"""

from __future__ import annotations

import os
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
BRONZE_SCHEMA_FILE = REPO_ROOT / "etl" / "altocontrol_bronze_schema.sql"

# (stored_procedure, bronze_table)
TABLES: list[tuple[str, str]] = [
    ("PBI_tabla_ventas",            "ac_ventas"),
    ("PBI_tabla_compras",           "ac_compras"),
    ("PBI_tabla_pagos",             "ac_pagos"),
    ("PBI_tabla_visitas",           "ac_visitas"),
    ("PBI_tabla_stock",             "ac_stock"),
    ("PBI_tabla_deuda_por_cliente", "ac_deuda_por_cliente"),
    ("PBI_tabla_articulos",         "ac_articulos"),
    ("PBI_tabla_clientes",          "ac_clientes"),
    ("PBI_tabla_empresas",          "ac_empresas"),
    ("PBI_tabla_geografia",         "ac_geografia"),
    ("PBI_tabla_proveedores",       "ac_proveedores"),
    ("PBI_tabla_ruta",              "ac_ruta"),
    ("PBI_tabla_rutas",             "ac_rutas"),
    ("PBI_tabla_vendedores",        "ac_vendedores"),
]

# Natural business keys used for ON CONFLICT DO UPDATE
UPSERT_KEYS: dict[str, list[str]] = {
    "ac_ventas":            ["id_renglon"],
    "ac_compras":           ["id_renglon"],
    "ac_pagos":             ["id_pago"],
    "ac_visitas":           ["id_visita"],
    "ac_stock":             ["id_deposito", "id_articulo"],
    "ac_deuda_por_cliente": ["id_sucursal", "id_documento"],
    "ac_articulos":         ["id_articulo"],
    "ac_clientes":          ["id_sucursal"],
    "ac_empresas":          ["id_empresa"],
    "ac_geografia":         ["id_pais", "id_departamento"],
    "ac_proveedores":       ["id_proveedor"],
    "ac_ruta":              ["id_ruta"],
    "ac_rutas":             ["id_sucursal", "id_ruta"],
    "ac_vendedores":        ["id_vendedor"],
}


def _detect_driver(pyodbc) -> str | None:
    preferred = ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server"]
    available = pyodbc.drivers()
    for d in preferred:
        if d in available:
            return d
    return None


def _source_conn() -> object:
    import pyodbc  # type: ignore

    driver = os.environ.get("ALTOCONTROL_DRIVER") or _detect_driver(pyodbc)
    if not driver:
        raise RuntimeError(
            "No SQL Server ODBC driver found. Install 'ODBC Driver 18 for SQL Server' "
            "or set ALTOCONTROL_DRIVER in .env."
        )

    server   = os.environ["ALTOCONTROL_SERVER"]
    database = os.environ["ALTOCONTROL_DATABASE"]
    user     = os.environ["ALTOCONTROL_USER"]
    password = os.environ["ALTOCONTROL_PASSWORD"]

    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    )
    conn = pyodbc.connect(conn_str, timeout=30)
    conn.execute("SET NOCOUNT ON; SET LANGUAGE Español;")
    return conn


def _upsert(engine, table_name: str, df, key_cols: list[str], chunksize: int = 500) -> None:
    from sqlalchemy import MetaData, Table
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    meta = MetaData()
    t = Table(table_name, meta, schema="bronze", autoload_with=engine)
    update_cols = [c for c in df.columns if c not in key_cols]

    records = df.to_dict(orient="records")
    for i in range(0, len(records), chunksize):
        chunk = records[i : i + chunksize]
        stmt = pg_insert(t).values(chunk)
        stmt = stmt.on_conflict_do_update(
            index_elements=key_cols,
            set_={col: stmt.excluded[col] for col in update_cols},
        )
        with engine.begin() as conn:
            conn.execute(stmt)


def run(
    target_conn_str: str,
    start_date: date | None = None,
    end_date: date | None = None,
    limit: int | None = None,
) -> int:
    """Extract all tables from Azure SQL and upsert into Postgres bronze.

    limit: if set, keeps only the first N rows per table (useful for dev/testing).

    Returns total rows upserted across all tables.
    """
    import pandas as pd
    from sqlalchemy import create_engine

    print("[bronze] connecting to source (Azure SQL) ...")
    src = _source_conn()
    print("[bronze] connected.")

    engine = create_engine(target_conn_str, isolation_level="AUTOCOMMIT")

    # Apply bronze schema (idempotent — CREATE TABLE/INDEX IF NOT EXISTS)
    if BRONZE_SCHEMA_FILE.exists():
        print("[bronze] applying schema ...")
        with engine.connect() as conn:
            conn.exec_driver_sql(BRONZE_SCHEMA_FILE.read_text(encoding="utf-8"))

    total_rows = 0
    batch_id = str(uuid.uuid4())
    ingested_at = datetime.now(timezone.utc)

    for sp_name, table_name in TABLES:
        cursor = src.cursor()
        print(f"[bronze] EXEC dbo.{sp_name} ...", end=" ", flush=True)
        cursor.execute(f"EXEC dbo.{sp_name}")

        raw_columns = [d[0].lower() for d in cursor.description]
        # Deduplicate column names — some SPs return the same name twice
        seen: dict[str, int] = {}
        columns = []
        for col in raw_columns:
            if col in seen:
                seen[col] += 1
                columns.append(f"{col}_{seen[col]}")
            else:
                seen[col] = 0
                columns.append(col)

        rows = cursor.fetchall()
        cursor.close()

        if not rows:
            print("0 rows")
            continue

        df = pd.DataFrame.from_records(rows, columns=columns)

        if limit is not None:
            df = df.head(limit)

        # All bronze columns are TEXT — stringify everything except metadata cols
        meta_cols = {"ingested_at", "etl_batch_id", "source_system", "source_record_id"}
        for col in df.columns:
            if col not in meta_cols:
                df[col] = df[col].astype(str).replace({"None": None, "nan": None})

        df["ingested_at"] = ingested_at
        df["etl_batch_id"] = batch_id
        df["source_system"] = "altocontrol"
        df["source_record_id"] = [str(uuid.uuid4()) for _ in range(len(df))]

        key_cols = UPSERT_KEYS.get(table_name, [])
        if key_cols:
            _upsert(engine, table_name, df, key_cols)
        else:
            # Fallback for any table without a defined key
            with engine.begin() as conn:
                from sqlalchemy import text
                conn.execute(text(f"TRUNCATE bronze.{table_name}"))
            df.to_sql(table_name, engine, schema="bronze", if_exists="append",
                      index=False, method="multi", chunksize=500)

        total_rows += len(df)
        print(f"{len(df):,} rows")

    src.close()
    print(f"[bronze] done — {total_rows:,} rows total")
    return total_rows
