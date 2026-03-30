"""Pydantic models for analysis reports — schema recommendations and example suggestions."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class SchemaRecommendation(BaseModel):
    """A single recommendation to improve the database schema for LLM comprehension."""

    type: Literal["rename_column", "add_description", "add_foreign_key", "rename_table"]
    table: str
    column: str | None = None
    current_name: str | None = None
    suggested_name: str | None = None
    suggested_description: str | None = None
    evidence: str = Field(description="Why this recommendation was generated")
    trace_count: int = Field(description="How many traces support this recommendation")
    priority: Literal["high", "medium", "low"] = "medium"


class ExampleSuggestion(BaseModel):
    """A suggested example scenario to add to scenarios.md."""

    scenario_name: str = Field(description="Heading for the scenario (e.g. 'monthly recurring revenue')")
    scenario_content: str = Field(description="Markdown body explaining tables, columns, joins, and SQL patterns")
    tables_covered: list[str] = Field(default_factory=list)
    columns_covered: list[str] = Field(default_factory=list)
    joins_covered: list[str] = Field(default_factory=list, description="e.g. ['orders JOIN customers ON customer_id']")
    evidence_queries: list[str] = Field(default_factory=list, description="Failing questions that prompted this")
    estimated_impact: int = Field(default=0, description="Number of failing traces this could help")


class AnalysisReport(BaseModel):
    """Full analysis report combining schema and example recommendations."""

    schema_recommendations: list[SchemaRecommendation] = Field(default_factory=list)
    example_suggestions: list[ExampleSuggestion] = Field(default_factory=list)
    summary: str = ""
    traces_analyzed: int = 0
    success_rate: float = 0.0
    top_failure_patterns: list[str] = Field(default_factory=list)
