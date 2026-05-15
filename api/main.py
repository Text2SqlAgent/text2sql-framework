"""FastAPI application entry point.

Wires routers, middleware, and the startup/shutdown lifecycle.
All business logic lives in api/routers/, api/jobs/, and api/engine.py.

Environment variables (see .env.example for full list):
    TEXT2SQL_DB   Read-only Postgres URL for the agent.
    APP_DB_URL    Full-access Postgres URL for app tables (conversations, pipeline_runs).
    API_KEY       Shared secret required on X-API-Key header.

Migrations (run separately before starting the API):
    uv run alembic upgrade head

Run:
    uv run uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

# Load .env before any other import reads os.environ
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=True)

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.engine import build_engine, set_engine
from api.routers import ask, conversations, health, ingest


@asynccontextmanager
async def lifespan(app: FastAPI):
    set_engine(build_engine())
    yield
    set_engine(None)


app = FastAPI(
    title="text2sql API",
    version="0.2.0",
    description=(
        "Natural-language query engine with conversation history and ETL pipeline management. "
        "All endpoints except `/health`, `/traces`, and `/canonical` require `X-API-Key` header."
    ),
    openapi_tags=[
        {"name": "ask", "description": "Ask natural-language questions, get SQL + data back."},
        {"name": "conversations", "description": "Manage multi-turn conversation history."},
        {"name": "ingest", "description": "Run and monitor the bronze → silver → gold ETL pipeline."},
        {"name": "utility", "description": "Health check, traces, and canonical query list."},
    ],
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.environ.get("TEXT2SQL_CORS_ORIGINS", "*").split(","),
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router)
app.include_router(ask.router)
app.include_router(conversations.router)
app.include_router(ingest.router)
