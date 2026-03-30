"""text2sql - Text-to-SQL with tool-based schema retrieval, powered by Deep Agents."""

from text2sql.core import TextSQL
from text2sql.connection import Database
from text2sql.generate import SQLResult
from text2sql.tracing import Tracer

__version__ = "0.2.0"
__all__ = ["TextSQL", "Database", "SQLResult", "Tracer"]
