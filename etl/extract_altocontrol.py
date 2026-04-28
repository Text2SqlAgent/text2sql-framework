"""extract_altocontrol.py — Dump AltoControl stored-procedure results to CSV.

Runs each PBI_tabla_* stored procedure once against Penicor's Azure SQL
instance, optionally caps row count, writes one CSV per SP into
db/bronze/altocontrol/. Also writes a _manifest.json with timestamps,
row counts, and column lists so we know exactly what was pulled.

Designed for POC — pull a representative slice of Penicor's data so the
bronze->silver->gold pipeline can be developed locally without VPN
dependency. Re-run later (with --cap 0) to refresh, or once the full DB
dump arrives use that instead.

PREREQS
-------
1. A SQL Server ODBC driver. Auto-detected — Driver 18 preferred, but
   Driver 17 (commonly already installed on Windows) works fine. Check:
     Get-OdbcDriver | Where-Object Name -like "*SQL Server*"

2. Python deps (managed via uv + pyproject.toml):
     uv pip install -e ".[etl]"

3. WireGuard tunnel to br-sao must be up (the Azure SQL firewall whitelists
   that exit IP). Verify with `Get-NetAdapter -Name br-sao`.

4. Copy .env.example -> .env and fill in:
     ALTOCONTROL_SERVER=logicoac.database.windows.net
     ALTOCONTROL_DATABASE=Penicor
     ALTOCONTROL_USER=...
     ALTOCONTROL_PASSWORD=...
   Optional: ALTOCONTROL_DRIVER=... to pin a specific ODBC driver.

USAGE
-----
    python etl/extract_altocontrol.py
    python etl/extract_altocontrol.py --output-dir D:/penicor_dump --cap 10000
    python etl/extract_altocontrol.py --only ventas,clientes
    python etl/extract_altocontrol.py --cap 0    # no cap, full pull
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT_DIR = REPO_ROOT / "db" / "bronze" / "altocontrol"

# (sp_name, output_filename_without_ext, default_cap_or_None)
# Caps reflect the POC plan in project_penicor.md / project_text2sql.md:
# - Transaction tables: cap to ~5k rows for fast iteration on bronze/silver/gold.
#   PBI_tabla_ventas appears to be ordered fecha DESC (per PowerBI screenshot
#   showing all-2026-04-28 rows on top), so first-N gives roughly the most
#   recent ~14 days — a coherent slice.
# - Master / pre-aggregated tables: pulled in full so foreign keys in the
#   transaction sample resolve.
SPS: list[tuple[str, str, int | None]] = [
    # transactions (capped)
    ("dbo.PBI_tabla_ventas",             "ventas",              5000),
    ("dbo.PBI_tabla_compras",            "compras",             5000),
    ("dbo.PBI_tabla_pagos",              "pagos",               5000),
    ("dbo.PBI_tabla_visitas",            "visitas",             5000),
    ("dbo.PBI_tabla_stock",              "stock",               5000),
    # likely small / pre-aggregated — full pull
    ("dbo.PBI_tabla_deuda_por_cliente",  "deuda_por_cliente",   None),
    # masters — full pull
    ("dbo.PBI_tabla_articulos",          "articulos",           None),
    ("dbo.PBI_tabla_clientes",           "clientes",            None),
    ("dbo.PBI_tabla_empresas",           "empresas",            None),
    ("dbo.PBI_tabla_geografia",          "geografia",           None),
    ("dbo.PBI_tabla_proveedores",        "proveedores",         None),
    ("dbo.PBI_tabla_ruta",               "ruta",                None),
    ("dbo.PBI_tabla_rutas",              "rutas",               None),
    ("dbo.PBI_tabla_vendedores",         "vendedores",          None),
]


def main() -> int:
    # Load .env first so env vars are available for argparse defaults / env override flags
    load_env()

    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR,
                    help=f"Directory to write CSVs into (default: {DEFAULT_OUTPUT_DIR})")
    ap.add_argument("--cap", type=int, default=None,
                    help="Override cap on transaction tables (0 = no cap, full pull)")
    ap.add_argument("--only", type=str, default=None,
                    help="Comma-separated list of SP short-names to run, e.g. 'ventas,clientes'")
    ap.add_argument("--driver", type=str, default=None,
                    help="ODBC driver name. Default: ALTOCONTROL_DRIVER env var, "
                         "or auto-detect (prefers Driver 18, falls back to 17).")
    ap.add_argument("--timeout", type=int, default=60,
                    help="Per-SP query timeout in seconds (default: 60)")
    args = ap.parse_args()

    # Lazy imports so --help works without the deps installed
    try:
        import pyodbc  # type: ignore
        import pandas as pd  # type: ignore
    except ImportError as e:
        print(f"[extract] missing dep: {e}. Install with:  uv pip install -e \".[etl]\"")
        return 2

    driver = args.driver or os.environ.get("ALTOCONTROL_DRIVER") or detect_odbc_driver(pyodbc)
    if not driver:
        print("[extract] no SQL Server ODBC driver found. Install one of:")
        print("  - ODBC Driver 18 for SQL Server (preferred)")
        print("  - ODBC Driver 17 for SQL Server")
        print("Or set ALTOCONTROL_DRIVER in .env to the exact driver name.")
        return 2
    print(f"[extract] using ODBC driver: {driver!r}")

    server   = require_env("ALTOCONTROL_SERVER")
    database = require_env("ALTOCONTROL_DATABASE")
    user     = require_env("ALTOCONTROL_USER")
    password = require_env("ALTOCONTROL_PASSWORD")

    args.output_dir.mkdir(parents=True, exist_ok=True)

    selected = filter_sps(SPS, args.only)
    if not selected:
        print(f"[extract] no SPs matched --only={args.only!r}. Available: "
              + ", ".join(s for _, s, _ in SPS))
        return 1

    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=no;"
        f"Connection Timeout=30;"
    )

    print(f"[extract] connecting to {server} / {database} as {user} ...")
    t0 = time.time()
    try:
        conn = pyodbc.connect(conn_str, timeout=30)
    except pyodbc.Error as e:
        print(f"[extract] connection FAILED: {e}")
        print("\nTroubleshooting:")
        print("  - Is the br-sao WireGuard tunnel up? (Get-NetAdapter -Name br-sao)")
        print(f"  - Is the ODBC driver name correct? Trying {driver!r}.")
        print("    Available drivers: " + ", ".join(pyodbc.drivers()))
        print("  - Are credentials in .env correct?")
        return 3

    print(f"[extract] connected in {time.time() - t0:.1f}s")
    conn.timeout = args.timeout  # per-query timeout (applies to all cursors on this conn)

    # Session settings:
    #   NOCOUNT ON  -> suppress rowcount messages so SPs that do INSERT INTO #temp
    #                  followed by SELECT don't return the INSERT's empty result
    #                  set first.
    #   LANGUAGE Español -> match Penicor's locale; SPs convert varchar dates in
    #                  DMY format (e.g. '13/04/2026'), which fails as out-of-range
    #                  under the default English/MDY language.
    setup = conn.cursor()
    setup.execute("SET NOCOUNT ON; SET LANGUAGE Español;")
    setup.close()

    manifest: dict = {
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        "server": server,
        "database": database,
        "tables": {},
    }

    overall_t0 = time.time()
    failures: list[tuple[str, str]] = []

    for sp_name, short_name, default_cap in selected:
        cap = resolve_cap(default_cap, args.cap)
        out_path = args.output_dir / f"{short_name}.csv"
        print(f"[extract] {short_name:24s}  EXEC {sp_name}  "
              f"(cap={cap if cap else 'none'})", flush=True)
        try:
            t1 = time.time()
            n_rows, n_cols, columns = run_sp_to_csv(
                conn=conn,
                sp_name=sp_name,
                out_path=out_path,
                cap=cap,
                timeout=args.timeout,
                pd=pd,
            )
            elapsed = time.time() - t1
            print(f"            -> {n_rows:>6d} rows, {n_cols:>3d} cols, {elapsed:.1f}s, "
                  f"{out_path.relative_to(REPO_ROOT)}")
            manifest["tables"][short_name] = {
                "sp": sp_name,
                "rows": n_rows,
                "cols": n_cols,
                "columns": columns,
                "cap_applied": cap,
                "elapsed_seconds": round(elapsed, 2),
                "output": str(out_path.relative_to(REPO_ROOT).as_posix()),
            }
        except Exception as e:  # noqa: BLE001 — top-level catch-all to keep going
            print(f"            !! FAILED: {e}")
            failures.append((short_name, str(e)))
            manifest["tables"][short_name] = {
                "sp": sp_name,
                "error": str(e),
            }

    conn.close()

    manifest_path = args.output_dir / "_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\n[extract] manifest -> {manifest_path.relative_to(REPO_ROOT)}")
    print(f"[extract] total elapsed: {time.time() - overall_t0:.1f}s")

    if failures:
        print(f"\n[extract] {len(failures)} table(s) failed:")
        for name, err in failures:
            print(f"    {name}: {err}")
        return 4
    return 0


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def load_env() -> None:
    """Load .env from repo root if python-dotenv is installed; otherwise rely on os.environ."""
    try:
        from dotenv import load_dotenv  # type: ignore
        env_path = REPO_ROOT / ".env"
        if env_path.exists():
            load_dotenv(env_path)
    except ImportError:
        pass


def detect_odbc_driver(pyodbc) -> str | None:
    """Return the best available SQL Server ODBC driver, or None if none installed.
    Prefers newer drivers (Driver 18 > 17 > older fallbacks)."""
    available = pyodbc.drivers()
    preferences = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "ODBC Driver 13 for SQL Server",
        "SQL Server Native Client 11.0",
        "SQL Server",
    ]
    for d in preferences:
        if d in available:
            return d
    return None


def require_env(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        raise SystemExit(
            f"[extract] missing required env var {name!r}. "
            f"Set it in .env or your shell. See .env.example."
        )
    return val


def filter_sps(
    sps: list[tuple[str, str, int | None]],
    only: str | None,
) -> list[tuple[str, str, int | None]]:
    if not only:
        return sps
    wanted = {s.strip() for s in only.split(",") if s.strip()}
    return [t for t in sps if t[1] in wanted]


def resolve_cap(default_cap: int | None, override: int | None) -> int | None:
    """If --cap was passed, it overrides only the *capped* tables (not masters).
    --cap 0 means 'no cap'."""
    if override is None:
        return default_cap
    if default_cap is None:
        return None  # masters always full
    if override == 0:
        return None
    return override


def run_sp_to_csv(
    *,
    conn,
    sp_name: str,
    out_path: Path,
    cap: int | None,
    timeout: int,
    pd,
) -> tuple[int, int, list[str]]:
    """Execute a stored procedure and stream up to `cap` rows into a CSV.

    Uses the cursor directly with fetchmany so we don't pull more rows than
    we need over the wire. Per-query timeout is set at the connection level
    in main() (pyodbc cursors don't have their own timeout attribute).
    """
    _ = timeout  # signature kept for clarity; actual timeout lives on conn
    cursor = conn.cursor()
    cursor.execute(f"EXEC {sp_name}")

    # Some SPs do INSERT/UPDATE before the final SELECT. Even with SET NOCOUNT ON,
    # be robust: skip past any result sets that have no column metadata.
    while cursor.description is None:
        if not cursor.nextset():
            cursor.close()
            out_path.write_text("", encoding="utf-8")
            return 0, 0, []

    columns = [c[0] for c in cursor.description]

    if cap is None:
        rows = cursor.fetchall()
    else:
        rows = cursor.fetchmany(cap)
    cursor.close()

    # Convert pyodbc.Row tuples into plain tuples for pandas
    df = pd.DataFrame.from_records(
        [tuple(r) for r in rows],
        columns=columns,
    )

    df.to_csv(
        out_path,
        index=False,
        encoding="utf-8",
        date_format="%Y-%m-%d %H:%M:%S",  # consistent timestamp format for bronze loader
    )
    return len(df), len(columns), columns


if __name__ == "__main__":
    sys.exit(main())
