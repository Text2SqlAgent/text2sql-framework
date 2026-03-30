"""Analysis engine — generates schema and example recommendations from traces.

Purely deterministic pipeline (no LLM):
1. Pre-process — aggregate stats from traces
2. Cluster failures — group failing traces by business concept
3. Generate recommendations — turn patterns into actionable suggestions
"""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from typing import List

from text2sql.connection import Database
from text2sql.examples import ExampleStore
from text2sql.models import AnalysisReport, SchemaRecommendation, ExampleSuggestion
from text2sql.tracing import QueryTrace


class AnalysisEngine:
    """Analyzes traces to produce schema and example recommendations."""

    def __init__(
        self,
        db: Database,
        traces: List[QueryTrace],
        example_store: ExampleStore | None = None,
    ):
        self.db = db
        self.traces = traces
        self.example_store = example_store

    def run(self) -> AnalysisReport:
        """Run the full analysis pipeline."""
        if not self.traces:
            return AnalysisReport(summary="No traces to analyze.")

        stats = self._preprocess()
        clusters = self._cluster_failures()
        schema_recs = self._generate_schema_recommendations(stats)
        example_recs = self._generate_example_suggestions(clusters)

        total = len(self.traces)
        successes = sum(1 for t in self.traces if t.success)

        return AnalysisReport(
            schema_recommendations=schema_recs,
            example_suggestions=example_recs,
            traces_analyzed=total,
            success_rate=round(successes / total, 3) if total else 0,
            top_failure_patterns=[c["concept"] for c in clusters[:5]],
            summary=self._build_summary(schema_recs, example_recs, total, successes),
        )

    # ── Step 1: Deterministic Pre-processing ──

    def _preprocess(self) -> dict:
        """Compute aggregate stats from traces."""
        search_term_counts = Counter()  # type: Counter[str]
        column_mismatch = []  # type: list[dict]
        join_failures = []  # type: list[dict]
        error_types = Counter()  # type: Counter[str]
        column_confusion = Counter()  # type: Counter[str]

        for trace in self.traces:
            for term in trace.search_terms_used:
                search_term_counts[term] += 1

            for svf in trace.columns_searched_vs_found:
                if svf.columns_returned and svf.column_used_in_sql:
                    if svf.search_term.lower() != svf.column_used_in_sql.lower():
                        column_mismatch.append({
                            "search_term": svf.search_term,
                            "column_used": svf.column_used_in_sql,
                            "columns_returned": svf.columns_returned,
                            "question": trace.question,
                        })
                elif svf.columns_returned and not svf.column_used_in_sql:
                    for col in svf.columns_returned:
                        column_confusion[col] += 1

            for ja in trace.join_attempts:
                if not ja.success:
                    join_failures.append({
                        "left": ja.left_table,
                        "right": ja.right_table,
                        "condition": ja.join_condition,
                        "error": ja.error_message[:200],
                        "question": trace.question,
                    })

            for err in trace.sql_errors_structured:
                error_types[err.error_type] += 1

            for err in trace.sql_errors_structured:
                for col in re.findall(r'(?:column|field)\s+"?(\w+)"?', err.raw_message, re.I):
                    column_confusion[col] += 1

        return {
            "search_term_counts": dict(search_term_counts.most_common(20)),
            "column_mismatches": column_mismatch,
            "join_failures": join_failures,
            "error_types": dict(error_types),
            "column_confusion": dict(column_confusion.most_common(15)),
        }

    # ── Step 2: Cluster Failures ──

    def _cluster_failures(self) -> list[dict]:
        """Group failing traces by business concept using question text + tables involved."""
        failing = [t for t in self.traces if not t.success]
        if not failing:
            return []

        clusters = defaultdict(list)  # type: defaultdict[str, list[QueryTrace]]
        for trace in failing:
            concept = _extract_business_concept(trace)
            clusters[concept].append(trace)

        result = []
        for concept, traces in sorted(clusters.items(), key=lambda x: -len(x[1])):
            questions = [t.question for t in traces]
            tables = set()
            for t in traces:
                for ja in t.join_attempts:
                    tables.add(ja.left_table)
                    tables.add(ja.right_table)
                for err in t.sql_errors_structured:
                    for tbl in re.findall(r'\bFROM\s+(\w+)', err.sql_attempted, re.I):
                        tables.add(tbl)
                    for tbl in re.findall(r'\bJOIN\s+(\w+)', err.sql_attempted, re.I):
                        tables.add(tbl)
            tables.discard("")

            result.append({
                "concept": concept,
                "count": len(traces),
                "questions": questions,
                "tables_involved": sorted(tables),
                "errors": [e.raw_message for t in traces for e in t.sql_errors_structured][:5],
            })

        return result

    # ── Step 3: Generate Recommendations ──

    def _generate_schema_recommendations(self, stats: dict) -> list[SchemaRecommendation]:
        """Generate schema recommendations from pre-processed stats."""
        recs = []

        # Column naming issues: search term didn't match the column name used
        mismatch_groups = defaultdict(list)  # type: defaultdict[str, list[dict]]
        for m in stats["column_mismatches"]:
            key = (m["column_used"],)
            mismatch_groups[key].append(m)

        for key, mismatches in mismatch_groups.items():
            col_used = mismatches[0]["column_used"]
            search_terms = list({m["search_term"] for m in mismatches})
            count = len(mismatches)
            priority = "high" if count >= 3 else "medium" if count >= 2 else "low"

            # Try to find the table from the trace context
            table = ""
            for m in mismatches:
                for col_name in m["columns_returned"]:
                    if col_name == col_used:
                        table = col_used  # best guess
                        break

            recs.append(SchemaRecommendation(
                type="add_description",
                table=table,
                column=col_used,
                suggested_description=f"Consider adding a description. Users search for: {', '.join(search_terms)}",
                evidence=f"LLM searched for {search_terms} but column is named '{col_used}' ({count} traces)",
                trace_count=count,
                priority=priority,
            ))

        # Columns with high confusion scores
        for col, count in stats["column_confusion"].items():
            if count >= 2:
                priority = "high" if count >= 5 else "medium" if count >= 3 else "low"
                recs.append(SchemaRecommendation(
                    type="add_description",
                    table="",
                    column=col,
                    suggested_description=f"Column caused {count} confusion events — consider renaming or adding a description",
                    evidence=f"Column '{col}' involved in {count} errors or required extra exploration",
                    trace_count=count,
                    priority=priority,
                ))

        # Join failures — suggest missing foreign keys
        join_groups = defaultdict(list)  # type: defaultdict[tuple, list[dict]]
        for j in stats["join_failures"]:
            key = (j["left"], j["right"])
            join_groups[key].append(j)

        for (left, right), failures in join_groups.items():
            count = len(failures)
            priority = "high" if count >= 3 else "medium" if count >= 2 else "low"
            recs.append(SchemaRecommendation(
                type="add_foreign_key",
                table=left,
                column=None,
                suggested_description=f"Add foreign key between {left} and {right}",
                evidence=f"Join between {left} and {right} failed {count} times: {failures[0]['error'][:100]}",
                trace_count=count,
                priority=priority,
            ))

        return recs

    def _generate_example_suggestions(self, clusters: list[dict]) -> list[ExampleSuggestion]:
        """Generate example suggestions from failure clusters."""
        suggestions = []

        # Only suggest examples for clusters with 3+ distinct failing questions
        for cluster in clusters:
            if cluster["count"] < 3:
                continue

            # Check if existing examples already cover these tables
            tables = cluster["tables_involved"]
            if self.example_store:
                existing = self.example_store.list_scenarios()
                # Simple overlap check — skip if a scenario name matches the concept
                if any(cluster["concept"] in s or s in cluster["concept"] for s in existing):
                    continue

            suggestions.append(ExampleSuggestion(
                scenario_name=cluster["concept"],
                scenario_content=(
                    f"Tables: {', '.join(tables)}\n"
                    f"Common errors: {'; '.join(cluster['errors'][:3])}"
                ),
                tables_covered=tables,
                evidence_queries=cluster["questions"][:5],
                estimated_impact=cluster["count"],
            ))

        return suggestions

    def _build_summary(
        self,
        schema_recs: list[SchemaRecommendation],
        example_recs: list[ExampleSuggestion],
        total: int,
        successes: int,
    ) -> str:
        parts = [
            f"Analyzed {total} traces ({successes} successful, "
            f"{total - successes} failed, {round(successes/total*100, 1)}% success rate).",
        ]
        if schema_recs:
            high = sum(1 for r in schema_recs if r.priority == "high")
            parts.append(
                f"Found {len(schema_recs)} schema recommendations "
                f"({high} high priority)."
            )
        if example_recs:
            impact = sum(r.estimated_impact for r in example_recs)
            parts.append(
                f"Suggested {len(example_recs)} new example scenarios "
                f"(estimated to help {impact} failing queries)."
            )
        if not schema_recs and not example_recs:
            parts.append("No recommendations — schema and examples look good.")
        return " ".join(parts)


# ── Helpers ──

def _extract_business_concept(trace):
    # type: (QueryTrace) -> str
    """Extract a short business concept label from a failing trace's question."""
    q = trace.question.lower()
    for word in ["what", "which", "how", "many", "much", "show", "me", "the",
                 "all", "list", "find", "get", "are", "is", "was", "were",
                 "do", "does", "can", "could", "would", "a", "an", "of", "in",
                 "for", "to", "by", "with", "and", "or", "that", "this", "?",
                 "total", "top", "per", "each", "every"]:
        q = q.replace(f" {word} ", " ")
    q = re.sub(r'[^\w\s]', '', q).strip()
    words = [w for w in q.split() if len(w) > 2][:4]
    return " ".join(words) if words else "unknown"
