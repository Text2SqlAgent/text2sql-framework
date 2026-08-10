"""Main entry point — the TextSQL class."""

from __future__ import annotations

from text2sql.connection import Database
from text2sql.examples import ExampleStore
from text2sql.generate import SQLGenerator, SQLResult
from text2sql.tracing import Tracer
from text2sql.state import MemoryStateStore, SQLiteStateStore, SQLAlchemyStateStore, StateStore


class TextSQL:
    """
    Ask your database questions in plain English.

    A native tool-calling agent gives the LLM pre-loaded tools to explore your
    schema, write SQL, execute it, and self-correct. By default it talks directly
    to the anthropic/openai SDKs (no LangChain required). Pass
    ``agent_backend="langchain"`` to use the legacy deepagents harness instead
    (requires the ``langchain`` extra).

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

    Experimental Python-first mode (one persistent Python tool; local state by
    default; restricted but not a hardened isolation boundary):
        engine = TextSQL(
            "sqlite:///analytics.db",
            agent_mode="python",
        )

    Put Python-agent state and traces in an explicit separate database:
        engine = TextSQL(
            "sqlite:///analytics.db",
            agent_mode="python",
            state_store="postgresql://.../agent_state",
        )

    Auto-sync traces to the dashboard:
        engine = TextSQL(
            "sqlite:///mydb.db",
            api_key="t2s_live_abc123..."
        )

    Store traces in your own database (no separate service or connection string —
    reuses the connection above and writes to ``text2sql_traces`` and
    ``text2sql_tool_calls``, created on first write):
        engine = TextSQL(
            "postgresql://...",
            trace_to_db=True,
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
        agent_backend: str = "native",
        trace_to_db: bool = False,
        enable_python_sandbox: bool = False,
        agent_mode: str = "tools",
        state_store: str | bool | StateStore | None = None,
        workspace_dir: str = ".text2sql",
        allow_self_modification: bool = False,
    ):
        self.db = Database(connection_string)
        self.state_db = None

        self.example_store = None
        if examples:
            self.example_store = ExampleStore(examples)

        # Python-first mode persists locally by default. The source database is
        # never written unless state_store="database" (or trace_to_db=True) is
        # explicit. A separate SQLAlchemy URL can be used for state instead.
        resolved_trace_file = trace_file
        trace_db = self.db if trace_to_db else None
        self.state_store = None
        if agent_mode == "python":
            resolved_store = "local" if state_store is None else state_store
            if resolved_store == "local":
                self.state_store = SQLiteStateStore(f"{workspace_dir}/state.db")
                resolved_trace_file = resolved_trace_file or f"{workspace_dir}/traces.jsonl"
            elif resolved_store == "database":
                self.state_store = SQLAlchemyStateStore(self.db.engine)
                trace_db = self.db
            elif isinstance(resolved_store, str) and "://" in resolved_store:
                self.state_db = Database(resolved_store)
                self.state_store = SQLAlchemyStateStore(self.state_db.engine)
                trace_db = self.state_db
            elif resolved_store in (False, "none"):
                self.state_store = MemoryStateStore()
            elif all(hasattr(resolved_store, name) for name in ("get", "put", "delete", "list")):
                self.state_store = resolved_store
            else:
                raise ValueError(
                    "state_store must be 'local', 'database', a SQLAlchemy URL, False, or a StateStore"
                )

        if resolved_trace_file or api_key or trace_db:
            self.tracer = Tracer(
                output_path=resolved_trace_file,
                api_key=api_key,
                api_url=api_url,
                db=trace_db,
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
            agent_backend=agent_backend,
            enable_python_sandbox=enable_python_sandbox,
            agent_mode=agent_mode,
            state_store=self.state_store,
            allow_self_modification=allow_self_modification,
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
