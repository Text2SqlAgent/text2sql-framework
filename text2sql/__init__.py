"""text2sql - Text-to-SQL with tool-based schema retrieval and a native agent loop."""

from text2sql.core import TextSQL
from text2sql.connection import Database
from text2sql.generate import SQLResult
from text2sql.tracing import Tracer

__version__ = "0.3.0"
__all__ = ["TextSQL", "Database", "SQLResult", "Tracer"]

try:
    from text2sql.middleware import Text2SqlMiddleware
    __all__.append("Text2SqlMiddleware")
except ImportError:
    pass
