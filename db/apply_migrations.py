"""Apply gold-layer migrations in order against a customer Postgres database.

Usage:
    python db/apply_migrations.py "postgresql://postgres:pwd@host:5432/customer_db"

What it does:
    Reads every .sql file in db/migrations/, sorted by filename, and
    executes each as a single statement block inside its own transaction.
    Logs which files ran and how long each took.

Requirements:
    pip install sqlalchemy psycopg2-binary

Caveats:
    * No migration-history table. Re-running is safe because every migration
      is written idempotently (CREATE IF NOT EXISTS, ON CONFLICT DO NOTHING,
      DROP MATERIALIZED VIEW IF EXISTS, etc.).
    * Run as a Postgres superuser (the 'postgres' user, or your RDS master).
    * 0007_seeds.sql will fail if gold.tenant_config has no row. Insert it
      first — see db/README.md.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

from sqlalchemy import create_engine, text


def apply(connection_string: str, migrations_dir: Path) -> None:
    files = sorted(migrations_dir.glob("*.sql"))
    if not files:
        raise SystemExit(f"No .sql files found in {migrations_dir}")

    print(f"Applying {len(files)} migrations to {connection_string.split('@')[-1]}")
    engine = create_engine(connection_string, isolation_level="AUTOCOMMIT")

    for path in files:
        sql = path.read_text(encoding="utf-8")
        print(f"  → {path.name} ({len(sql):,} chars) … ", end="", flush=True)
        start = time.time()
        try:
            with engine.connect() as conn:
                # Use exec_driver_sql so DO $$ ... $$ blocks aren't parsed as
                # a single statement by SQLAlchemy.
                conn.exec_driver_sql(sql)
        except Exception as e:
            elapsed = time.time() - start
            print(f"FAILED in {elapsed:.1f}s")
            print(f"\nError applying {path.name}:\n{e}")
            raise SystemExit(1)
        print(f"ok ({time.time() - start:.1f}s)")

    engine.dispose()
    print("Done.")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(__doc__)
        sys.exit(2)

    conn = sys.argv[1]
    here = Path(__file__).parent / "migrations"
    apply(conn, here)
