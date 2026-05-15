"""SQLAlchemy ORM models and session factory for the app database.

All persistent state for conversations, API keys, and pipeline runs lives here.
The engine is lazily initialised on first use so the module is safe to import
at startup before environment variables are loaded.
"""

from __future__ import annotations

import os
from typing import Generator

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    String,
    Text,
    create_engine,
    func,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker


# ---------------------------------------------------------------------------
# ORM models
# ---------------------------------------------------------------------------

class Base(DeclarativeBase):
    pass


class Conversation(Base):
    __tablename__ = "conversations"

    id = Column(UUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid())
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    title = Column(Text)
    user_id = Column(String(255), index=True)
    user_role = Column(String(255))
    metadata_ = Column("metadata", JSONB, nullable=False, server_default="{}")


class Message(Base):
    __tablename__ = "messages"

    id = Column(UUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid())
    conversation_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    role = Column(String(20), nullable=False)  # 'user' | 'assistant'
    content = Column(Text, nullable=False)
    sql_generated = Column(Text)
    row_count = Column(Integer)
    tool_calls_made = Column(Integer)
    iterations = Column(Integer)
    input_tokens = Column(Integer)
    output_tokens = Column(Integer)
    duration_seconds = Column(Float)
    is_canonical = Column(Boolean, server_default="false")
    error = Column(Text)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())



class PipelineRun(Base):
    __tablename__ = "pipeline_runs"

    id = Column(UUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid())
    started_at = Column(DateTime(timezone=True), nullable=False)
    finished_at = Column(DateTime(timezone=True))
    start_date = Column(Date, nullable=False)
    end_date = Column(Date, nullable=False)
    layers = Column(ARRAY(Text), nullable=False)
    status = Column(String(20), nullable=False)  # 'running' | 'success' | 'failed'
    bronze_rows = Column(Integer)
    silver_rows = Column(Integer)
    gold_views = Column(Integer)
    error_detail = Column(Text)
    triggered_by = Column(String(50))  # 'api' | 'cli'


# ---------------------------------------------------------------------------
# Engine + session factory (lazy)
# ---------------------------------------------------------------------------

_engine = None
_SessionLocal: sessionmaker | None = None


def _app_db_url() -> str:
    url = os.environ.get("APP_DB_URL") or os.environ.get("TEXT2SQL_DB")
    if not url:
        raise RuntimeError(
            "APP_DB_URL environment variable is required for conversation/key/pipeline storage. "
            "Example: postgresql+psycopg://user:pass@localhost:5432/appdb"
        )
    return url


def get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(_app_db_url(), pool_pre_ping=True)
    return _engine


def get_session() -> Generator[Session, None, None]:
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(bind=get_engine(), autocommit=False, autoflush=False)
    db = _SessionLocal()
    try:
        yield db
    finally:
        db.close()
