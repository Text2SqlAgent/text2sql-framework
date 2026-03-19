"""Main entry point — the TextSQL class."""

from __future__ import annotations

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
        metadata_hint: str | None = None,
        trace_file: str | None = None,
        api_key: str | None = None,
        api_url: str | None = None,
    ):
        self.db = Database(connection_string)

        self.example_store = None
        if examples:
            self.example_store = ExampleStore(examples)

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


    def ask(self, question: str, max_rows: int | None = None) -> SQLResult:
        """Ask a natural language question. Returns SQL and results.

        Args:
            max_rows: Max rows to return in the result. If None, returns all rows.
                     This controls the final result only — the LLM still sees a
                     preview during exploration/testing.
        """
        return self.generator.ask(question, max_rows=max_rows)

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
