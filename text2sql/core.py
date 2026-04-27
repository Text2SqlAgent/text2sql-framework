"""Main entry point — the TextSQL class."""

from __future__ import annotations

import warnings

from text2sql.canonical import CanonicalQueryStore
from text2sql.connection import Database
from text2sql.examples import ExampleStore
from text2sql.generate import SQLGenerator, SQLResult
from text2sql.tracing import Tracer


class TextSQL:
    """
    Ask your database questions in plain English.

    Built on LangChain Deep Agents — the LLM gets pre-loaded tools to explore
    your schema, write SQL, execute it, and self-correct. Context compaction is
    handled automatically for large schemas.

    Usage:
        engine = TextSQL("sqlite:///mydb.db")
        result = engine.ask("Top 5 customers by revenue?")
        print(result.sql)
        print(result.data)

    With instructions + examples + tracing:
        engine = TextSQL(
            "postgresql://...",
            instructions="Revenue = net revenue after refunds.",
            examples="scenarios.md",
            trace_file="traces/queries.jsonl",
        )

    With canonical queries (vetted SQL templates that bypass the agent):
        engine = TextSQL(
            "postgresql://...",
            canonical_queries="canonical.md",
        )
        # "How much are we owed?" matches the AR canonical query and runs
        # the vetted SQL directly — deterministic, fast, no LLM cost.

    With user context (audit trail in traces):
        result = engine.ask(
            "Top customer this month",
            user_id="alice@acme.com",
            user_role="finance_manager",
        )

    Auto-sync traces to the dashboard:
        engine = TextSQL(
            "sqlite:///mydb.db",
            api_key="t2s_live_abc123..."
        )

    Analyze traces for schema and example recommendations:
        report = engine.analyze()
        for rec in report.schema_recommendations:
            print(rec.table, rec.column, rec.suggested_name)
    """

    def __init__(
        self,
        connection_string: str,
        model: str = "anthropic:claude-sonnet-4-6",
        instructions: str | None = None,
        examples: str | None = None,
        canonical_queries: str | None = None,
        canonical_threshold: float = 0.6,
        metadata_hint: str | None = None,
        trace_file: str | None = None,
        api_key: str | None = None,
        api_url: str | None = None,
        enforce_read_only: bool = False,
    ):
        self.db = Database(connection_string)

        if enforce_read_only:
            self.db.verify_read_only(raise_on_writable=True)
        else:
            # Soft check — warn if the connection is writable
            try:
                if self.db.verify_read_only(raise_on_writable=False) is False:
                    warnings.warn(
                        "Database connection appears to be writable. The execute_sql tool "
                        "blocks destructive SQL via regex, but for production deployments "
                        "you should also connect with a read-only DB user. Pass "
                        "enforce_read_only=True to fail fast on writable connections.",
                        stacklevel=2,
                    )
            except Exception:
                # Non-fatal — verification is best-effort
                pass

        self.example_store = None
        if examples:
            self.example_store = ExampleStore(examples)

        self.canonical_store = None
        if canonical_queries:
            self.canonical_store = CanonicalQueryStore(
                canonical_queries,
                match_threshold=canonical_threshold,
            )

        # Enable tracing if trace_file or api_key is set
        if trace_file or api_key:
            self.tracer = Tracer(
                output_path=trace_file,
                api_key=api_key,
                api_url=api_url,
            )
        else:
            self.tracer = None

        self.generator = SQLGenerator(
            db=self.db,
            model=model,
            instructions=instructions,
            custom_metadata=metadata_hint,
            example_store=self.example_store,
            tracer=self.tracer,
        )


    def ask(
        self,
        question: str,
        max_rows: int | None = None,
        user_id: str | None = None,
        user_role: str | None = None,
        metadata: dict | None = None,
    ) -> SQLResult:
        """Ask a natural language question. Returns SQL and results.

        Args:
            question: The natural language question.
            max_rows: Max rows to return in the result. If None, returns all rows.
                     This controls the final result only — the LLM still sees a
                     preview during exploration/testing.
            user_id: Optional caller identity (email, user ID, etc.) — recorded
                     in traces for audit purposes.
            user_role: Optional caller role (e.g. "finance_manager") — recorded
                     in traces.
            metadata: Optional free-form dict — recorded in traces. Useful for
                     correlating queries with sessions, requests, or tenants.
        """
        # Try canonical match first
        if self.canonical_store is not None:
            match = self.canonical_store.match(question)
            if match is not None:
                return self._run_canonical(
                    question=question,
                    match=match,
                    max_rows=max_rows,
                    user_id=user_id,
                    user_role=user_role,
                    metadata=metadata,
                )

        return self.generator.ask(
            question,
            max_rows=max_rows,
            user_id=user_id,
            user_role=user_role,
            metadata=metadata,
        )

    def _run_canonical(
        self,
        question: str,
        match,
        max_rows: int | None,
        user_id: str | None,
        user_role: str | None,
        metadata: dict | None,
    ) -> SQLResult:
        """Execute a canonical SQL template directly, bypassing the agent."""
        if self.tracer:
            self.tracer.start_query(question)
            self.tracer.attach_user_context(
                user_id=user_id,
                user_role=user_role,
                metadata=metadata,
            )

        sql = match.query.sql
        error = None
        data: list = []
        try:
            rows = self.db.execute(sql)
            if max_rows is not None:
                rows = rows[:max_rows]
            data = rows
        except Exception as e:
            error = f"Canonical query execution failed: {e}"

        if self.tracer:
            self.tracer.mark_canonical(match.query.name, match.score)
            self.tracer.end_query(
                sql=sql,
                success=error is None,
                error=error,
                iterations=0,
            )

        return SQLResult(
            question=question,
            sql=sql,
            data=data,
            error=error,
            commentary=(
                f"[canonical:{match.query.name} score={match.score:.2f}] "
                + (match.query.description or "")
            ).strip(),
            tool_calls_made=0,
            iterations=0,
            input_tokens=0,
            output_tokens=0,
        )

    def analyze(self, trace_file: str | None = None):
        """Analyze traces and produce schema + example recommendations.

        Args:
            trace_file: Path to a JSONL trace file. If None, uses traces from
                       the current session (requires tracing to be enabled).

        Returns:
            AnalysisReport with schema_recommendations and example_suggestions.
        """
        from text2sql.analyze import AnalysisEngine

        if trace_file:
            traces = Tracer.load_traces(trace_file)
        elif self.tracer:
            traces = self.tracer.traces
        else:
            from text2sql.models import AnalysisReport
            return AnalysisReport(
                summary="No traces available. Enable tracing with trace_file= "
                        "or pass a trace file to analyze()."
            )

        engine = AnalysisEngine(
            db=self.db,
            traces=traces,
            example_store=self.example_store,
        )
        return engine.run()

    def trace_summary(self) -> dict:
        """Aggregate trace stats across all queries in this session."""
        if not self.tracer:
            return {"error": "Tracing not enabled. Pass trace_file= to TextSQL()."}
        return self.tracer.summary()

    def example_report(self) -> list:
        """Per-scenario breakdown: lookups vs. actual usage in SQL."""
        if not self.tracer:
            return []
        return self.tracer.example_report()
