"""Tests for the demo synth — determinism + shape."""

from __future__ import annotations

import datetime as dt

import pytest

from etl.synth import generate, _add_months


class TestDeterminism:
    def test_same_seed_same_output(self):
        a = generate(seed=42)
        b = generate(seed=42)
        assert a.customers == b.customers
        assert a.orders   == b.orders
        assert a.invoices == b.invoices
        assert a.payments == b.payments
        assert a.shipments == b.shipments
        assert a.expenses == b.expenses

    def test_different_seed_differs(self):
        a = generate(seed=42)
        b = generate(seed=43)
        assert a.customers != b.customers
        assert a.orders   != b.orders


class TestShape:
    @pytest.fixture(scope="class")
    def synth(self):
        return generate(seed=42)

    def test_volume_sanity(self, synth):
        # These bounds are wide — just check we're in the right ballpark.
        assert 40 <= len(synth.customers) <= 60
        assert 80 <= len(synth.products)  <= 120
        assert len(synth.warehouses) == 5
        assert 8  <= len(synth.vehicles)  <= 12
        assert 15 <= len(synth.employees) <= 25
        assert 800   < len(synth.orders)             < 2000
        assert 2000  < len(synth.order_items)        < 8000
        assert 600   < len(synth.invoices)           < 1500
        assert 600   < len(synth.shipments)          < 1500
        assert 3000  < len(synth.inventory_movements) < 10000
        assert 200   < len(synth.expenses)           < 800

    def test_currency_codes_consistent(self, synth):
        # Every monetary field references currency_code; that code should
        # be a 3-letter ISO string we know about.
        known = {"USD","EUR","GBP","JPY","PEN","MXN","COP","CLP","BRL","ARS"}
        for o in synth.orders[:50]:
            assert o["currency_code"] in known
        for inv in synth.invoices[:50]:
            assert inv["currency_code"] in known

    def test_invoice_amounts_consistent(self, synth):
        for inv in synth.invoices[:200]:
            assert inv["amount_paid_cents"] <= inv["amount_billed_cents"]
            if inv["status"] == "paid":
                assert inv["amount_paid_cents"] == inv["amount_billed_cents"]
            elif inv["status"] == "unpaid":
                assert inv["amount_paid_cents"] == 0

    def test_shipment_dates_logical(self, synth):
        for s in synth.shipments[:200]:
            ship_date = dt.date.fromisoformat(s["ship_date"])
            if s["delivered_date"]:
                d = dt.date.fromisoformat(s["delivered_date"])
                assert d >= ship_date
            if s["status"] == "delivered":
                assert s["delivered_at"] is not None

    def test_inventory_movements_signed_correctly(self, synth):
        for m in synth.inventory_movements[:500]:
            if m["movement_type"] == "receipt":
                assert m["quantity_delta"] > 0
            elif m["movement_type"] == "shipment":
                assert m["quantity_delta"] < 0

    def test_order_items_link_to_orders(self, synth):
        order_numbers = {o["order_number"] for o in synth.orders}
        for it in synth.order_items[:500]:
            assert it["order_number"] in order_numbers

    def test_payments_link_to_invoices(self, synth):
        invoice_numbers = {i["invoice_number"] for i in synth.invoices}
        for p in synth.payments[:500]:
            assert p["invoice_number"] in invoice_numbers

    def test_dates_within_window(self, synth):
        earliest = dt.date.fromisoformat(synth.config["earliest"])
        today    = dt.date.fromisoformat(synth.config["today"])
        for o in synth.orders[:200]:
            d = dt.date.fromisoformat(o["order_date"])
            assert earliest <= d <= today


class TestAddMonths:
    def test_basic(self):
        assert _add_months(dt.date(2026, 1, 15), 1) == dt.date(2026, 2, 1)
        assert _add_months(dt.date(2026, 1, 15), 12) == dt.date(2027, 1, 1)
        assert _add_months(dt.date(2026, 11, 1), 3) == dt.date(2027, 2, 1)

    def test_zero(self):
        assert _add_months(dt.date(2026, 5, 17), 0) == dt.date(2026, 5, 1)
