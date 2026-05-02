"""Ingest job — orchestrates the ETL pipeline and records results to the DB.

Decouples the heavy execution logic from the HTTP layer so the router stays thin.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Literal

from sqlalchemy.orm import Session

from api.db import PipelineRun
from etl.pipeline import PipelineResult, run_pipeline


def start_run(
    db: Session,
    start_date: date,
    end_date: date,
    layers: list[str],
) -> PipelineRun:
    """Create a pipeline_runs row with status='running' and flush it."""
    run = PipelineRun(
        started_at=datetime.now(timezone.utc),
        start_date=start_date,
        end_date=end_date,
        layers=layers,
        status="running",
        triggered_by="api",
    )
    db.add(run)
    db.commit()
    db.refresh(run)
    return run


def execute_run(
    run: PipelineRun,
    layers: list[Literal["bronze", "silver", "gold"]],
    start_date: date,
    end_date: date,
    db: Session,
) -> PipelineRun:
    """Run the pipeline and update the DB row with the outcome."""
    try:
        result: PipelineResult = run_pipeline(
            start_date=start_date,
            end_date=end_date,
            layers=layers,
        )
    except Exception as e:
        run.status = "failed"
        run.finished_at = datetime.now(timezone.utc)
        run.error_detail = str(e)
        db.commit()
        raise

    run.status = "success" if not result.errors else "failed"
    run.finished_at = result.finished_at
    run.bronze_rows = result.bronze_rows
    run.silver_rows = result.silver_rows
    run.gold_views = result.gold_views
    run.error_detail = "; ".join(result.errors) if result.errors else None
    db.commit()
    db.refresh(run)
    return run
