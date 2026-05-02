"""Conversation management endpoints.

All routes require a valid API key (X-API-Key header).
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from api.auth import require_api_key
from api.db import Conversation, Message, get_session

router = APIRouter(
    prefix="/conversations",
    tags=["conversations"],
    dependencies=[Depends(require_api_key)],
)


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------

class ConversationCreate(BaseModel):
    user_id: Optional[str] = None
    user_role: Optional[str] = None
    title: Optional[str] = None


class ConversationSummary(BaseModel):
    id: str
    title: Optional[str]
    created_at: datetime
    updated_at: datetime
    message_count: int


class MessageOut(BaseModel):
    id: str
    role: str
    content: str
    sql_generated: Optional[str]
    row_count: Optional[int]
    duration_seconds: Optional[float]
    is_canonical: Optional[bool]
    error: Optional[str]
    created_at: datetime


class ConversationDetail(BaseModel):
    id: str
    title: Optional[str]
    user_id: Optional[str]
    created_at: datetime
    messages: list[MessageOut]


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.post("", response_model=ConversationSummary)
def create_conversation(
    req: ConversationCreate, db: Session = Depends(get_session)
) -> ConversationSummary:
    conv = Conversation(user_id=req.user_id, user_role=req.user_role, title=req.title)
    db.add(conv)
    db.commit()
    db.refresh(conv)
    return ConversationSummary(
        id=str(conv.id),
        title=conv.title,
        created_at=conv.created_at,
        updated_at=conv.updated_at,
        message_count=0,
    )


@router.get("", response_model=list[ConversationSummary])
def list_conversations(
    user_id: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_session),
) -> list[ConversationSummary]:
    stmt = (
        select(Conversation, func.count(Message.id).label("message_count"))
        .outerjoin(Message, Message.conversation_id == Conversation.id)
        .group_by(Conversation.id)
        .order_by(Conversation.updated_at.desc())
        .limit(limit)
        .offset(offset)
    )
    if user_id:
        stmt = stmt.where(Conversation.user_id == user_id)

    rows = db.execute(stmt).all()
    return [
        ConversationSummary(
            id=str(conv.id),
            title=conv.title,
            created_at=conv.created_at,
            updated_at=conv.updated_at,
            message_count=count,
        )
        for conv, count in rows
    ]


@router.get("/{conversation_id}", response_model=ConversationDetail)
def get_conversation(
    conversation_id: str, db: Session = Depends(get_session)
) -> ConversationDetail:
    conv = db.scalar(select(Conversation).where(Conversation.id == conversation_id))
    if conv is None:
        raise HTTPException(status_code=404, detail="Conversation not found")

    msgs = db.scalars(
        select(Message)
        .where(Message.conversation_id == conversation_id)
        .order_by(Message.created_at)
    ).all()

    return ConversationDetail(
        id=str(conv.id),
        title=conv.title,
        user_id=conv.user_id,
        created_at=conv.created_at,
        messages=[
            MessageOut(
                id=str(m.id),
                role=m.role,
                content=m.content,
                sql_generated=m.sql_generated,
                row_count=m.row_count,
                duration_seconds=m.duration_seconds,
                is_canonical=m.is_canonical,
                error=m.error,
                created_at=m.created_at,
            )
            for m in msgs
        ],
    )
