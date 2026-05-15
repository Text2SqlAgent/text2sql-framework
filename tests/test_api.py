"""Smoke tests for the FastAPI app.

We don't connect to a real Postgres here — we monkey-patch a fake
TextSQL engine into api.main and exercise the request → response shape.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

import api.main as main_module
from text2sql.generate import SQLResult


class FakeTracer:
    def __init__(self):
        self.traces = []


class FakeCanonicalStore:
    queries = [
        SimpleNamespace(name="ar_aging", aliases=["accounts receivable aging"], description="AR aging buckets."),
        SimpleNamespace(name="top_customers_ytd", aliases=["top customers"], description="Top customers YTD."),
    ]


class FakeDB:
    def test_connection(self):
        return True


class FakeEngine:
    """Just enough of TextSQL for the API to drive."""
    def __init__(self):
        self.db = FakeDB()
        self.tracer = FakeTracer()
        self.canonical_store = FakeCanonicalStore()

    def ask(self, question, max_rows=None, user_id=None, user_role=None, metadata=None):
        # Pretend the AR aging canonical fired
        self.tracer.traces.append(SimpleNamespace(
            question=question,
            final_sql="SELECT * FROM gold.v_ar_aging",
            success=True,
            canonical_query="ar_aging",
            canonical_score=1.0,
            duration_seconds=0.04,
            user_id=user_id,
            user_role=user_role,
            start_time=1700000000.0,
        ))
        return SQLResult(
            question=question,
            sql="SELECT * FROM gold.v_ar_aging",
            data=[
                {"customer_name": "Acme", "total_owed": 1000.5, "currency_code": "PEN"},
                {"customer_name": "Beta", "total_owed":  250.0, "currency_code": "USD"},
            ],
            commentary="[canonical:ar_aging score=1.00]",
            tool_calls_made=0,
            iterations=0,
            input_tokens=0,
            output_tokens=0,
        )


@pytest.fixture
def client(monkeypatch):
    fake = FakeEngine()
    # Bypass lifespan startup by setting the module-level engine directly.
    monkeypatch.setattr(main_module, "_ENGINE", fake)
    return TestClient(main_module.app)


def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["db"] == "ok"
    assert body["canonical_count"] == 2
    assert body["tracer_enabled"] is True


def test_ask_basic(client):
    r = client.post("/ask", json={"question": "How much are we owed?"})
    assert r.status_code == 200
    body = r.json()
    assert body["question"] == "How much are we owed?"
    assert body["sql"].startswith("SELECT")
    assert body["row_count"] == 2
    assert body["columns"] == ["customer_name", "total_owed", "currency_code"]
    assert body["canonical_query"] == "ar_aging"
    assert body["canonical_score"] == 1.0
    assert body["error"] is None


def test_ask_with_user_context(client):
    r = client.post("/ask", json={
        "question": "AR aging",
        "user_id": "alice@acme.com",
        "user_role": "finance_manager",
        "session_id": "sess-1",
    })
    assert r.status_code == 200


def test_ask_csv(client):
    r = client.post("/ask/csv", json={"question": "How much are we owed?"})
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/csv")
    assert "attachment" in r.headers["content-disposition"]
    body = r.text
    assert "customer_name,total_owed,currency_code" in body
    assert "Acme,1000.5,PEN" in body


def test_canonical_listing(client):
    r = client.get("/canonical")
    assert r.status_code == 200
    body = r.json()
    assert len(body["queries"]) == 2
    assert body["queries"][0]["name"] == "ar_aging"


def test_traces_after_ask(client):
    client.post("/ask", json={"question": "test 1"})
    client.post("/ask", json={"question": "test 2"})
    r = client.get("/traces?limit=10")
    assert r.status_code == 200
    body = r.json()
    assert len(body) == 2
    # Most recent first
    assert body[0]["question"] == "test 2"


def test_ask_validation(client):
    # empty question
    r = client.post("/ask", json={"question": ""})
    assert r.status_code == 422
