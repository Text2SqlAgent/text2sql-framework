"""Database connection wrapper using SQLAlchemy."""

from __future__ import annotations

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine, Inspector


class Database:
    """Thin wrapper around a SQLAlchemy engine."""

    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.engine: Engine = create_engine(connection_string)

    def execute(
        self, sql: str, params: dict | None = None, max_rows: int | None = None
    ) -> list[dict]:
        """Execute SQL and return rows as dictionaries.

        ``max_rows`` is applied while fetching, not after materializing the full
        result set. Model-facing database capabilities always set this limit.
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(sql), params or {})
            if result.returns_rows:
                columns = list(result.keys())
                rows = result.fetchmany(max_rows) if max_rows is not None else result.fetchall()
                return [dict(zip(columns, row)) for row in rows]
            return []

    def execute_read_only(
        self, sql: str, params: dict | None = None, max_rows: int | None = None
    ) -> list[dict]:
        """Execute read-only SQL with lexical and database-level guards where supported.

        Databricks SQL does not expose a read-only transaction through its
        SQLAlchemy dialect, so least-privilege credentials remain mandatory.
        """
        # Import locally to avoid a module cycle: tools only imports Database
        # under TYPE_CHECKING. Keep this guard here so direct callers cannot
        # bypass the workspace's identical allowlist.
        from text2sql.tools import _is_read_only
        if not _is_read_only(sql):
            raise ValueError("Only read-only SQL statements are allowed")

        with self.engine.connect() as conn:
            dialect = self.dialect
            sqlite_guard = dialect == "sqlite"
            if sqlite_guard:
                conn.exec_driver_sql("PRAGMA query_only=ON")
            elif dialect in {"postgresql", "mysql", "mariadb"}:
                conn.exec_driver_sql("SET TRANSACTION READ ONLY")
            try:
                result = conn.execute(text(sql), params or {})
                if not result.returns_rows:
                    return []
                columns = list(result.keys())
                rows = result.fetchmany(max_rows) if max_rows is not None else result.fetchall()
                return [dict(zip(columns, row)) for row in rows]
            finally:
                # SQLite's connection-level flag survives pool checkout unless
                # explicitly reset; framework-owned trace/state writes use the
                # unrestricted internal connection afterward.
                if sqlite_guard:
                    conn.exec_driver_sql("PRAGMA query_only=OFF")

    def get_inspector(self) -> Inspector:
        """Return a SQLAlchemy Inspector for schema introspection."""
        return inspect(self.engine)

    @property
    def dialect(self) -> str:
        """Return the database dialect name (e.g. 'sqlite', 'postgresql')."""
        return self.engine.dialect.name

    def test_connection(self) -> bool:
        """Verify the connection works."""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception:
            return False

    def get_schema_summary(self) -> dict:
        """Return structured schema summary using SQLAlchemy Inspector.

        Returns a dict of tables, each with columns (name, type, nullable,
        comment/description), primary keys, and foreign keys.
        """
        inspector = self.get_inspector()
        schema = {}

        for table_name in inspector.get_table_names():
            columns = []
            for col in inspector.get_columns(table_name):
                columns.append({
                    "name": col["name"],
                    "type": str(col["type"]),
                    "nullable": col.get("nullable", True),
                    "comment": col.get("comment") or "",
                })

            pk = inspector.get_pk_constraint(table_name)
            pk_columns = pk.get("constrained_columns", []) if pk else []

            fks = []
            for fk in inspector.get_foreign_keys(table_name):
                fks.append({
                    "constrained_columns": fk.get("constrained_columns", []),
                    "referred_table": fk.get("referred_table", ""),
                    "referred_columns": fk.get("referred_columns", []),
                })

            table_comment = ""
            try:
                tc = inspector.get_table_comment(table_name)
                table_comment = tc.get("text") or "" if tc else ""
            except (NotImplementedError, Exception):
                pass

            schema[table_name] = {
                "columns": columns,
                "primary_keys": pk_columns,
                "foreign_keys": fks,
                "comment": table_comment,
            }

        return schema
