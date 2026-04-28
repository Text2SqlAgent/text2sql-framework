"""load_altocontrol_bronze.py — Load AltoControl CSVs into Postgres bronze layer.

Reads the CSVs produced by extract_altocontrol.py from db/bronze/altocontrol/,
applies the bronze DDL (idempotent), TRUNCATEs each ac_* table, and bulk-inserts
the CSV contents.

Designed for fast iteration: every run is a clean reload, so silver/gold
transforms downstream always see a consistent state.

PREREQS
-------
1. A Postgres 16 instance reachable from this machine. Easiest:
     docker compose up -d postgres
   (See compose.yml at the repo root.)

2. Python deps (managed via uv + pyproject.toml):
     uv pip install -e ".[etl]"

3. CSVs already produced by:
     python etl/extract_altocontrol.py

USAGE
-----
    # Default: loads from db/bronze/altocontrol/ into the local Docker Postgres
    python etl/load_altocontrol_bronze.py

    # Custom connection
    python etl/load_altocontrol_bronze.py \\
        --connection-string "postgresql://etl_writer:pwd@host:5432/customer_demo"

    # Custom CSV directory (e.g. another dump location)
    python etl/load_altocontrol_bronze.py --csv-dir D:/penicor_dump

    # Apply schema only, skip the data load
    python etl/load_altocontrol_bronze.py --schema-only
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
ETL_DIR = REPO_ROOT / "etl"
DEFAULT_CSV_DIR = REPO_ROOT / "db" / "bronze" / "altocontrol"
DEFAULT_SCHEMA_FILE = ETL_DIR / "altocontrol_bronze_schema.sql"
DEFAULT_CONN = "postgresql+psycopg://penicor:penicor@localhost:5433/penicor"

# Mapping CSV-shortname -> bronze table.
# Order matters only for the per-line log; truncation is per-table.
CSV_TO_TABLE: list[tuple[str, str]] = [
    ("ventas",            "bronze.ac_ventas"),
    ("compras",           "bronze.ac_compras"),
    ("pagos",             "bronze.ac_pagos"),
    ("visitas",           "bronze.ac_visitas"),
    ("stock",             "bronze.ac_stock"),
    ("deuda_por_cliente", "bronze.ac_deuda_por_cliente"),
    ("articulos",         "bronze.ac_articulos"),
    ("clientes",          "bronze.ac_clientes"),
    ("empresas",          "bronze.ac_empresas"),
    ("geografia",         "bronze.ac_geografia"),
    ("proveedores",       "bronze.ac_proveedores"),
    ("ruta",              "bronze.ac_ruta"),
    ("rutas",             "bronze.ac_rutas"),
    ("vendedores",        "bronze.ac_vendedores"),
]


def main() -> int:
    load_env()

    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--connection-string", "-c",
                    default=os.environ.get("PENICOR_DB_URL") or DEFAULT_CONN,
                    help=f"Postgres URL (default: ${{PENICOR_DB_URL}} or {DEFAULT_CONN})")
    ap.add_argument("--csv-dir", type=Path, default=DEFAULT_CSV_DIR,
                    help=f"Directory containing the AltoControl CSVs (default: {DEFAULT_CSV_DIR})")
    ap.add_argument("--schema-file", type=Path, default=DEFAULT_SCHEMA_FILE,
                    help=f"Bronze DDL file (default: {DEFAULT_SCHEMA_FILE})")
    ap.add_argument("--schema-only", action="store_true",
                    help="Apply schema and exit; don't load CSVs.")
    ap.add_argument("--only", type=str, default=None,
                    help="Comma-separated list of CSV shortnames to load (default: all)")
    args = ap.parse_args()

    try:
        import pandas as pd  # type: ignore
        from sqlalchemy import create_engine, text  # type: ignore
    except ImportError as e:
        print(f"[load] missing dep: {e}. Install with:  uv pip install -e \".[etl]\"")
        return 2

    if not args.schema_file.exists():
        print(f"[load] schema file not found: {args.schema_file}")
        return 1

    print(f"[load] connecting to {redact(args.connection_string)} ...")
    try:
        engine = create_engine(args.connection_string, isolation_level="AUTOCOMMIT")
        with engine.connect() as conn:
            v = conn.execute(text("SELECT version()")).scalar()
            print(f"[load] connected: {v}")
    except Exception as e:  # noqa: BLE001
        print(f"[load] connection FAILED: {e}")
        print("\nTroubleshooting:")
        print("  - Is Postgres up? (docker compose ps)")
        print("  - Is the connection string correct? (PENICOR_DB_URL env var or --connection-string)")
        print("  - Are deps installed? (pip install pandas sqlalchemy psycopg[binary])")
        return 3

    t0 = time.time()
    batch_id = uuid.uuid4()

    print(f"[load] applying schema {args.schema_file.relative_to(REPO_ROOT)} ...")
    sql = args.schema_file.read_text(encoding="utf-8")
    with engine.connect() as conn:
        conn.exec_driver_sql(sql)

    if args.schema_only:
        print(f"[load] schema applied in {time.time() - t0:.1f}s (--schema-only)")
        return 0

    selected = filter_targets(CSV_TO_TABLE, args.only)
    if not selected:
        print(f"[load] no targets matched --only={args.only!r}")
        return 1

    print(f"[load] batch_id={batch_id}")
    print(f"[load] csv-dir = {args.csv_dir.relative_to(REPO_ROOT) if args.csv_dir.is_relative_to(REPO_ROOT) else args.csv_dir}")

    # Verify CSVs exist before truncating anything.
    missing = [s for s, _ in selected if not (args.csv_dir / f"{s}.csv").exists()]
    if missing:
        print(f"[load] CSVs missing: {missing}")
        print(f"[load] re-run extract_altocontrol.py first")
        return 1

    failures: list[tuple[str, str]] = []
    for short_name, table in selected:
        csv_path = args.csv_dir / f"{short_name}.csv"
        try:
            t1 = time.time()
            n = load_one(
                engine=engine,
                pd=pd,
                csv_path=csv_path,
                table=table,
                batch_id=batch_id,
                short_name=short_name,
            )
            elapsed = time.time() - t1
            print(f"[load]   {table:36s}  {n:>6d} rows  {elapsed:.1f}s")
        except Exception as e:  # noqa: BLE001
            print(f"[load]   {table:36s}  !! FAILED: {e}")
            failures.append((table, str(e)))

    print(f"[load] total elapsed: {time.time() - t0:.1f}s")

    if failures:
        print(f"\n[load] {len(failures)} table(s) failed:")
        for t, e in failures:
            print(f"    {t}: {e}")
        return 4
    return 0


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def load_env() -> None:
    try:
        from dotenv import load_dotenv  # type: ignore
        env_path = REPO_ROOT / ".env"
        if env_path.exists():
            load_dotenv(env_path)
    except ImportError:
        pass


def redact(url: str) -> str:
    """Hide the password segment in a connection URL for logs."""
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


def filter_targets(
    targets: list[tuple[str, str]],
    only: str | None,
) -> list[tuple[str, str]]:
    if not only:
        return targets
    wanted = {s.strip() for s in only.split(",") if s.strip()}
    return [t for t in targets if t[0] in wanted]


def load_one(
    *,
    engine,
    pd,
    csv_path: Path,
    table: str,
    batch_id: uuid.UUID,
    short_name: str,
) -> int:
    """Read CSV, add audit columns, TRUNCATE the table, bulk-insert."""
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False, na_values=[""])

    # If empty CSV (e.g. visitas), still TRUNCATE so re-runs are clean.
    df["etl_batch_id"] = str(batch_id)
    df["source_record_id"] = [
        f"{short_name}_{i+1:08d}" for i in range(len(df))
    ]

    # `ingested_at` and `source_system` come from column DEFAULTs; don't supply.

    with engine.begin() as conn:
        conn.exec_driver_sql(f"TRUNCATE {table};")
        if len(df):
            df.to_sql(
                name=table.split(".")[1],
                schema=table.split(".")[0],
                con=conn,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=500,
            )
    return len(df)


if __name__ == "__main__":
    sys.exit(main())
