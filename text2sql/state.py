"""Pluggable JSON state stores for Python-first agent state."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


class StateStore(Protocol):
    def get(self, namespace: str, key: str, default: Any = None) -> Any: ...
    def put(self, namespace: str, key: str, value: Any) -> None: ...
    def delete(self, namespace: str, key: str) -> None: ...
    def list(self, namespace: str, prefix: str = "") -> dict[str, Any]: ...


_CREATE_STATE = """
CREATE TABLE IF NOT EXISTS text2sql_state (
    namespace VARCHAR(255) NOT NULL,
    state_key VARCHAR(255) NOT NULL,
    value_json TEXT NOT NULL,
    updated_at VARCHAR(32) NOT NULL,
    PRIMARY KEY (namespace, state_key)
)
"""


class SQLAlchemyStateStore:
    """JSON state in any SQLAlchemy-supported database.

    Pass a separate state URL normally. Passing the analytics database engine is
    supported but should always be an explicit user choice.
    """

    def __init__(self, url_or_engine: str | Engine):
        self.engine = (
            url_or_engine if isinstance(url_or_engine, Engine) else create_engine(url_or_engine)
        )
        with self.engine.begin() as conn:
            conn.execute(text(_CREATE_STATE))

    @staticmethod
    def _encoded(value: Any) -> str:
        try:
            return json.dumps(value, ensure_ascii=False)
        except (TypeError, ValueError) as exc:
            raise TypeError("Agent state must be JSON-serializable") from exc

    def get(self, namespace: str, key: str, default: Any = None) -> Any:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT value_json FROM text2sql_state WHERE namespace=:n AND state_key=:k"),
                {"n": namespace, "k": key},
            ).first()
        return json.loads(row[0]) if row else default

    def put(self, namespace: str, key: str, value: Any) -> None:
        encoded = self._encoded(value)
        values = {
            "n": namespace, "k": key, "v": encoded,
            "u": datetime.now(timezone.utc).isoformat(),
        }
        dialect = self.engine.dialect.name
        if dialect in {"sqlite", "postgresql"}:
            statement = (
                "INSERT INTO text2sql_state (namespace,state_key,value_json,updated_at) "
                "VALUES (:n,:k,:v,:u) ON CONFLICT (namespace,state_key) DO UPDATE SET "
                "value_json=:v, updated_at=:u"
            )
        elif dialect in {"mysql", "mariadb"}:
            statement = (
                "INSERT INTO text2sql_state (namespace,state_key,value_json,updated_at) "
                "VALUES (:n,:k,:v,:u) ON DUPLICATE KEY UPDATE "
                "value_json=:v, updated_at=:u"
            )
        else:
            # Lowest-common-denominator fallback. Main supported stores above use
            # an atomic dialect upsert.
            with self.engine.begin() as conn:
                updated = conn.execute(text(
                    "UPDATE text2sql_state SET value_json=:v,updated_at=:u "
                    "WHERE namespace=:n AND state_key=:k"
                ), values)
                if not updated.rowcount:
                    conn.execute(text(
                        "INSERT INTO text2sql_state (namespace,state_key,value_json,updated_at) "
                        "VALUES (:n,:k,:v,:u)"
                    ), values)
            return
        with self.engine.begin() as conn:
            conn.execute(text(statement), values)

    def delete(self, namespace: str, key: str) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text("DELETE FROM text2sql_state WHERE namespace=:n AND state_key=:k"),
                {"n": namespace, "k": key},
            )

    def list(self, namespace: str, prefix: str = "") -> dict[str, Any]:
        escaped = prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("SELECT state_key,value_json FROM text2sql_state "
                     "WHERE namespace=:n AND state_key LIKE :p ESCAPE '\\' ORDER BY state_key"),
                {"n": namespace, "p": escaped + "%"},
            ).fetchall()
        return {row[0]: json.loads(row[1]) for row in rows}


class SQLiteStateStore(SQLAlchemyStateStore):
    """Local SQLite state, used by Python-first mode by default."""

    def __init__(self, path: str | Path = ".text2sql/state.db"):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        super().__init__(f"sqlite:///{path}")
        self.path = path


class MemoryStateStore:
    """Non-persistent state store for tests and explicitly stateless agents."""

    def __init__(self):
        self._values: dict[tuple[str, str], Any] = {}

    def get(self, namespace: str, key: str, default: Any = None) -> Any:
        return self._values.get((namespace, key), default)

    def put(self, namespace: str, key: str, value: Any) -> None:
        encoded = json.dumps(value, ensure_ascii=False)
        self._values[(namespace, key)] = json.loads(encoded)

    def delete(self, namespace: str, key: str) -> None:
        self._values.pop((namespace, key), None)

    def list(self, namespace: str, prefix: str = "") -> dict[str, Any]:
        return {
            key: value
            for (ns, key), value in sorted(self._values.items())
            if ns == namespace and key.startswith(prefix)
        }
