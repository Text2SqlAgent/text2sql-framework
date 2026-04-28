"""apply_altocontrol_gold.py — apply gold.v_* views over silver for Penicor.

Idempotent. Just runs altocontrol_gold_views.sql against the local Postgres.
Re-run any time the SQL file changes; CREATE OR REPLACE VIEW handles the rest.

USAGE
-----
    python etl/apply_altocontrol_gold.py
    python etl/apply_altocontrol_gold.py --connection-string "postgresql+psycopg://..."
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_VIEWS_FILE = REPO_ROOT / "etl" / "altocontrol_gold_views.sql"
DEFAULT_CONN = "postgresql+psycopg://penicor:penicor@localhost:5433/penicor"

VERIFY_VIEWS = [
    "gold.v_sales",
    "gold.v_purchases",
    "gold.v_payments",
    "gold.v_open_ar",
    "gold.v_inventory",
    "gold.v_visits",
    "gold.v_customers",
    "gold.v_revenue_monthly",
    "gold.v_revenue_by_customer",
    "gold.v_revenue_by_product",
    "gold.v_revenue_by_salesperson",
    "gold.v_payments_monthly",
    "gold.v_ar_aging",
    "gold.v_ar_total",
    "gold.v_inventory_low",
]


def main() -> int:
    load_env()

    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--connection-string", "-c",
                    default=os.environ.get("PENICOR_DB_URL") or DEFAULT_CONN,
                    help=f"Postgres URL (default: ${{PENICOR_DB_URL}} or {DEFAULT_CONN})")
    ap.add_argument("--views-file", type=Path, default=DEFAULT_VIEWS_FILE,
                    help=f"Gold views DDL file (default: {DEFAULT_VIEWS_FILE})")
    args = ap.parse_args()

    try:
        from sqlalchemy import create_engine, text  # type: ignore
    except ImportError as e:
        print(f"[gold] missing dep: {e}. Install with:  uv pip install -e \".[etl]\"")
        return 2

    if not args.views_file.exists():
        print(f"[gold] views file not found: {args.views_file}")
        return 1

    print(f"[gold] connecting to {redact(args.connection_string)} ...")
    try:
        engine = create_engine(args.connection_string, isolation_level="AUTOCOMMIT")
        with engine.connect() as conn:
            conn.execute(text("SELECT 1")).scalar()
    except Exception as e:  # noqa: BLE001
        print(f"[gold] connection FAILED: {e}")
        return 3

    t0 = time.time()
    print(f"[gold] applying views {args.views_file.relative_to(REPO_ROOT)} ...")
    sql = args.views_file.read_text(encoding="utf-8")
    try:
        with engine.connect() as conn:
            conn.exec_driver_sql(sql)
    except Exception as e:  # noqa: BLE001
        print(f"[gold] FAILED: {e}")
        return 4

    print(f"[gold] applied in {time.time() - t0:.1f}s")
    print(f"[gold] verifying views (row counts):")
    with engine.connect() as conn:
        for view in VERIFY_VIEWS:
            try:
                n = conn.execute(text(f"SELECT count(*) FROM {view}")).scalar()
                print(f"          {view:38s} {n:>8d}")
            except Exception as e:  # noqa: BLE001
                print(f"          {view:38s} ERROR: {e}")

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
