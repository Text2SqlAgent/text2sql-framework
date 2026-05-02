"""ETL pipeline orchestrator: bronze → silver → gold.

Usage (CLI):
    uv run python -m etl.pipeline
    uv run python -m etl.pipeline --layers bronze silver

Usage (Python):
    from etl.pipeline import run_pipeline
    result = run_pipeline()
    print(result.bronze_rows, result.silver_rows, result.gold_views)
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Literal

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONN = os.environ.get("APP_DB_URL", "postgresql+psycopg://penicor:penicor@localhost:5433/penicor")


@dataclass
class PipelineResult:
    bronze_rows: int = 0
    silver_rows: int = 0
    gold_views: int = 0
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    finished_at: datetime | None = None
    errors: list[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return not self.errors

    @property
    def duration_seconds(self) -> float | None:
        if self.finished_at and self.started_at:
            return (self.finished_at - self.started_at).total_seconds()
        return None


def run_pipeline(
    layers: list[Literal["bronze", "silver", "gold"]] | None = None,
    target_conn_str: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    limit: int | None = None,
) -> PipelineResult:
    from etl import bronze, silver, gold

    if layers is None:
        layers = ["bronze", "silver", "gold"]

    conn = target_conn_str or DEFAULT_CONN
    result = PipelineResult(started_at=datetime.now(timezone.utc))

    if "bronze" in layers:
        t0 = time.time()
        try:
            result.bronze_rows = bronze.run(conn, start_date=start_date, end_date=end_date, limit=limit)
            print(f"[pipeline] bronze done in {time.time()-t0:.1f}s — {result.bronze_rows:,} rows")
        except Exception as e:
            result.errors.append(f"bronze: {e}")
            result.finished_at = datetime.now(timezone.utc)
            return result

    if "silver" in layers:
        t0 = time.time()
        try:
            result.silver_rows = silver.run(conn)
            print(f"[pipeline] silver done in {time.time()-t0:.1f}s — {result.silver_rows:,} rows")
        except Exception as e:
            result.errors.append(f"silver: {e}")
            result.finished_at = datetime.now(timezone.utc)
            return result

    if "gold" in layers:
        t0 = time.time()
        try:
            result.gold_views = gold.run(conn)
            print(f"[pipeline] gold done in {time.time()-t0:.1f}s — {result.gold_views} views")
        except Exception as e:
            result.errors.append(f"gold: {e}")

    result.finished_at = datetime.now(timezone.utc)
    return result


def _main() -> int:
    ap = argparse.ArgumentParser(description="Run the ETL pipeline.")
    ap.add_argument(
        "--layers",
        nargs="+",
        choices=["bronze", "silver", "gold"],
        default=["bronze", "silver", "gold"],
    )
    ap.add_argument("--connection-string", "-c", default=None)
    ap.add_argument("--start", type=date.fromisoformat, metavar="YYYY-MM-DD", default=None,
                    help="Pass @start_date to every stored procedure (SP must accept it).")
    ap.add_argument("--end", type=date.fromisoformat, metavar="YYYY-MM-DD", default=None,
                    help="Pass @end_date to every stored procedure (SP must accept it).")
    ap.add_argument("--limit", type=int, metavar="N", default=None,
                    help="Keep only the first N rows per table (useful for dev/testing).")
    args = ap.parse_args()

    try:
        from dotenv import load_dotenv
        env_path = REPO_ROOT / ".env"
        if env_path.exists():
            load_dotenv(env_path, override=True)
    except ImportError:
        pass

    result = run_pipeline(
        layers=args.layers,
        target_conn_str=args.connection_string,
        start_date=args.start,
        end_date=args.end,
        limit=args.limit,
    )

    if result.errors:
        print(f"[pipeline] FAILED: {'; '.join(result.errors)}", file=sys.stderr)
        return 1

    print(
        f"[pipeline] done in {result.duration_seconds:.1f}s — "
        f"bronze={result.bronze_rows:,} silver={result.silver_rows:,} gold={result.gold_views}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(_main())
