"""Pydantic request/response models for the chat API."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class AskRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=2000)
    user_id: str | None = None
    user_role: str | None = None
    session_id: str | None = None
    max_rows: int | None = Field(default=200, ge=1, le=10_000)


class AskResponse(BaseModel):
    question: str
    sql: str
    data: list[dict[str, Any]]
    columns: list[str]
    row_count: int
    truncated: bool                              # data was capped by max_rows
    error: str | None
    commentary: str
    canonical_query: str | None
    canonical_score: float | None
    tool_calls_made: int
    iterations: int
    duration_seconds: float
    input_tokens: int
    output_tokens: int


class HealthResponse(BaseModel):
    status: str
    db: str
    canonical_count: int
    tracer_enabled: bool


class TraceSummary(BaseModel):
    question: str
    final_sql: str
    success: bool
    canonical_query: str | None
    duration_seconds: float
    user_id: str | None
    user_role: str | None
    started_at: float
