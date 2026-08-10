"""text2sql - Text-to-SQL with tool-based schema retrieval and a native agent loop."""

from text2sql.core import TextSQL
from text2sql.connection import Database
from text2sql.generate import SQLResult
from text2sql.tracing import Tracer
from text2sql.state import MemoryStateStore, SQLiteStateStore, SQLAlchemyStateStore
from text2sql.subagent import ExternalAgentSession

__version__ = "0.6.0"
__all__ = ["TextSQL", "Database", "SQLResult", "Tracer", "MemoryStateStore", "SQLiteStateStore", "SQLAlchemyStateStore", "ExternalAgentSession"]

try:
    from text2sql.middleware import Text2SqlMiddleware
    __all__.append("Text2SqlMiddleware")
except ImportError:
    pass
