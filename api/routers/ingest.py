"""Ingest endpoints — thin HTTP layer over api.jobs.ingest."""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session

from api.auth import require_api_key
from api.db import PipelineRun, get_session
from api.jobs import ingest as ingest_job

router = APIRouter(tags=["ingest"], dependencies=[Depends(require_api_key)])


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class IngestRequest(BaseModel):
    start_date: date
    end_date: date
    layers: list[Literal["bronze", "silver", "gold"]] = ["bronze", "silver", "gold"]


class IngestResponse(BaseModel):
    run_id: str
    status: str
    start_date: date
    end_date: date
    layers: list[str]
    bronze_rows: Optional[int]
    silver_rows: Optional[int]
    gold_views: Optional[int]
    duration_seconds: Optional[float]
    errors: list[str]
    started_at: datetime
    finished_at: Optional[datetime]


def _to_response(run: PipelineRun) -> IngestResponse:
    duration = None
    if run.started_at and run.finished_at:
        duration = round((run.finished_at - run.started_at).total_seconds(), 2)
    return IngestResponse(
        run_id=str(run.id),
        status=run.status,
        start_date=run.start_date,
        end_date=run.end_date,
        layers=list(run.layers or []),
        bronze_rows=run.bronze_rows,
        silver_rows=run.silver_rows,
        gold_views=run.gold_views,
        duration_seconds=duration,
        errors=[run.error_detail] if run.error_detail else [],
        started_at=run.started_at,
        finished_at=run.finished_at,
    )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.post("/ingest", response_model=IngestResponse)
def run_ingest(req: IngestRequest, db: Session = Depends(get_session)) -> IngestResponse:
    if req.start_date > req.end_date:
        raise HTTPException(status_code=422, detail="start_date must be <= end_date")

    run = ingest_job.start_run(db, req.start_date, req.end_date, req.layers)
    run = ingest_job.execute_run(run, req.layers, req.start_date, req.end_date, db)
    return _to_response(run)


@router.get("/ingest", response_model=list[IngestResponse])
def list_runs(
    limit: int = Query(default=20, ge=1, le=200),
    db: Session = Depends(get_session),
) -> list[IngestResponse]:
    runs = db.scalars(
        select(PipelineRun).order_by(PipelineRun.started_at.desc()).limit(limit)
    ).all()
    return [_to_response(r) for r in runs]


@router.get("/ingest/{run_id}", response_model=IngestResponse)
def get_run(run_id: str, db: Session = Depends(get_session)) -> IngestResponse:
    run = db.scalar(select(PipelineRun).where(PipelineRun.id == run_id))
    if run is None:
        raise HTTPException(status_code=404, detail="Pipeline run not found")
    return _to_response(run)
