"""Gold layer — apply CREATE OR REPLACE VIEW statements over silver.

Reads altocontrol_gold_views.sql and executes it against Postgres.
Idempotent: views are always fully replaced.
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
GOLD_VIEWS_FILE = REPO_ROOT / "etl" / "altocontrol_gold_views.sql"


def run(target_conn_str: str) -> int:
    """Apply gold views and return the count of views in the gold schema."""
    from sqlalchemy import create_engine, text

    if not GOLD_VIEWS_FILE.exists():
        raise FileNotFoundError(f"Gold views file not found: {GOLD_VIEWS_FILE}")

    print("[gold] applying views ...")
    sql = GOLD_VIEWS_FILE.read_text(encoding="utf-8")
    engine = create_engine(target_conn_str, isolation_level="AUTOCOMMIT")

    with engine.connect() as conn:
        conn.exec_driver_sql(sql)

    with engine.connect() as conn:
        n = conn.execute(
            text("SELECT count(*) FROM information_schema.views WHERE table_schema = 'gold'")
        ).scalar()

    count = int(n or 0)
    print(f"[gold] done — {count} views")
    return count
