"""Deterministic synthetic data for a fictional logistics customer.

Stdlib only. Seeded RNG → identical output every run. Realistic enough to
exercise every canonical query and produce non-trivial AR aging / top
customer / late-delivery results.

Volumes (default):
    50  customers   100 products    5 warehouses    10 vehicles
    20  employees    5 suppliers
    ~1200 orders   ~3000 order items   ~1000 invoices   ~1800 payments
    ~800 shipments  ~5000 inventory movements  ~500 expenses

Usage:
    from etl.synth import generate
    data = generate(seed=42)
    # data is a dict with keys: customers, products, warehouses, ...
"""

from __future__ import annotations

import datetime as dt
import random
from dataclasses import dataclass, field
from typing import Any


# ---------- Reference data (hardcoded for full determinism) -----------------

FIRST_NAMES = [
    "Alice","Bob","Carol","David","Eve","Frank","Grace","Hank","Ivy","Jack",
    "Kate","Liam","Maria","Noah","Olivia","Pedro","Quinn","Rachel","Sam","Tina",
    "Ulysses","Vera","Walter","Ximena","Yasmin","Zach","Andrea","Bruno","Camila","Diego",
]
LAST_NAMES = [
    "Garcia","Rodriguez","Smith","Jones","Brown","Lopez","Martinez","Sanchez",
    "Castro","Morales","Vega","Romero","Diaz","Torres","Reyes","Flores",
    "Ramos","Cruz","Gonzalez","Hernandez","Vargas","Mendez","Silva","Rojas",
]
COMPANY_PREFIXES = [
    "Acme","Vertex","Atlas","Helios","Polaris","Quantum","Apex","Stellar",
    "Nimbus","Cobalt","Granite","Beacon","Cascade","Summit","Rampart","Meridian",
    "Pioneer","Tribune","Sentinel","Phoenix","Riverstone","Northwind","Westport","Eastline",
]
COMPANY_SUFFIXES = [
    "Logistics","Distribution","Trading","Imports","Holdings","Group","Industries",
    "Foods","Materials","Manufacturing","Wholesale","Retail S.A.","Comercial",
]
COUNTRY_CODES = ["PE","US","MX","CO","CL","BR","AR","EC"]
COUNTRY_WEIGHTS = [40, 15, 10, 10, 8, 7, 6, 4]  # sum=100, so each percent
CITIES_BY_COUNTRY = {
    "PE": ["Lima","Arequipa","Trujillo","Cusco","Piura"],
    "US": ["Miami","Houston","Los Angeles","Dallas"],
    "MX": ["Mexico City","Guadalajara","Monterrey"],
    "CO": ["Bogota","Medellin","Cali"],
    "CL": ["Santiago","Valparaiso"],
    "BR": ["Sao Paulo","Rio de Janeiro","Brasilia"],
    "AR": ["Buenos Aires","Cordoba"],
    "EC": ["Quito","Guayaquil"],
}

CUSTOMER_SEGMENTS = ["enterprise","smb","retail"]
SEGMENT_WEIGHTS  = [20, 50, 30]
PAYMENT_TERMS    = [None, 0, 15, 30, 45, 60, 90]
PAYMENT_TERM_WEIGHTS = [5, 10, 20, 35, 10, 15, 5]

PRODUCT_CATEGORIES = [
    ("Beverages",       ["Soda","Juice","Water","Coffee","Tea","Energy Drink"]),
    ("Snacks",          ["Chips","Cookies","Crackers","Nuts","Granola","Candy"]),
    ("Cleaning",        ["Detergent","Bleach","Soap","Disinfectant","Sponges","Wipes"]),
    ("Personal Care",   ["Shampoo","Toothpaste","Deodorant","Lotion","Razors","Soap Bars"]),
    ("Office Supplies", ["Paper A4","Pens Black","Pens Blue","Folders","Tape","Stapler"]),
    ("Electronics",     ["AA Batteries","AAA Batteries","Cables","Adapters","Bulbs"]),
    ("Hardware",        ["Screws Box","Hammer","Wrench","Drill Bits","Tape Measure"]),
    ("Pet Food",        ["Dog Food","Cat Food","Pet Treats"]),
    ("Frozen Goods",    ["Ice Cream","Frozen Vegetables","Frozen Pizza"]),
    ("Dairy",           ["Milk 1L","Cheese","Yogurt","Butter"]),
]

WAREHOUSE_NAMES = [
    ("LIMA-N",  "Lima Norte DC",         "Lima",     "PE"),
    ("LIMA-S",  "Lima Sur DC",           "Lima",     "PE"),
    ("AREQ-1",  "Arequipa Hub",          "Arequipa", "PE"),
    ("MIA-1",   "Miami Cross-Dock",      "Miami",    "US"),
    ("MEX-1",   "Mexico City Hub",       "Mexico City","MX"),
]

VEHICLE_TYPES = ["truck","van","semi-trailer"]
VEHICLE_TYPE_WEIGHTS = [50, 35, 15]

EXPENSE_CATEGORIES = [
    ("fuel",         60),
    ("maintenance",  20),
    ("rent",          5),
    ("utilities",     8),
    ("salaries",      2),
    ("t_and_e",      10),
    ("other",         5),
]

PAYMENT_METHODS = ["wire","ach","check","card","cash"]
PAYMENT_METHOD_WEIGHTS = [40, 30, 10, 15, 5]

SUPPLIER_NAMES = [
    "MegaFood Distributors","Pacific Trading Co","Andean Imports SAC",
    "Global Logistics Partners","Cono Sur Wholesale",
]


# ---------- Output dataclasses (mirror gold-layer fields, plus source ids) --

def _new_id():  # tiny helper for source IDs
    n = 0
    def gen(prefix=""):
        nonlocal n
        n += 1
        return f"{prefix}{n:06d}"
    return gen


@dataclass
class Synth:
    """All generated rows in one bag."""
    config:    dict[str, Any]                       = field(default_factory=dict)
    customers: list[dict[str, Any]]                 = field(default_factory=list)
    products:  list[dict[str, Any]]                 = field(default_factory=list)
    warehouses: list[dict[str, Any]]                = field(default_factory=list)
    vehicles:  list[dict[str, Any]]                 = field(default_factory=list)
    employees: list[dict[str, Any]]                 = field(default_factory=list)
    suppliers: list[dict[str, Any]]                 = field(default_factory=list)
    orders:    list[dict[str, Any]]                 = field(default_factory=list)
    order_items: list[dict[str, Any]]               = field(default_factory=list)
    invoices:  list[dict[str, Any]]                 = field(default_factory=list)
    payments:  list[dict[str, Any]]                 = field(default_factory=list)
    shipments: list[dict[str, Any]]                 = field(default_factory=list)
    shipment_items: list[dict[str, Any]]            = field(default_factory=list)
    inventory_movements: list[dict[str, Any]]       = field(default_factory=list)
    expenses:  list[dict[str, Any]]                 = field(default_factory=list)


# ---------- Generator -------------------------------------------------------

def generate(
    seed: int = 42,
    n_customers: int = 50,
    n_products: int = 100,
    n_employees: int = 20,
    months_back: int = 24,
    orders_per_month: int = 50,
    expenses_per_month: int = 20,
    base_currency: str = "PEN",
) -> Synth:
    """Build a complete demo dataset.

    Returns a Synth bag of dicts ready to insert into bronze tables.
    All amounts are integer minor units (cents).
    """
    rng = random.Random(seed)
    today = dt.date(2026, 4, 27)  # fixed reference date for reproducibility
    earliest = today.replace(day=1) - dt.timedelta(days=months_back * 31)
    # Snap to the first of that month
    earliest = earliest.replace(day=1)

    out = Synth()
    out.config = {
        "seed": seed,
        "today": today.isoformat(),
        "earliest": earliest.isoformat(),
        "base_currency": base_currency,
    }

    # ------ Customers ------
    customer_codes: list[str] = []
    for i in range(n_customers):
        country = _weighted(rng, COUNTRY_CODES, COUNTRY_WEIGHTS)
        city = rng.choice(CITIES_BY_COUNTRY[country])
        prefix = rng.choice(COMPANY_PREFIXES)
        suffix = rng.choice(COMPANY_SUFFIXES)
        name = f"{prefix} {suffix}"
        code = f"CUST-{i+1:04d}"
        customer_codes.append(code)
        out.customers.append({
            "customer_code":      code,
            "customer_name":      name,
            "customer_segment":   _weighted(rng, CUSTOMER_SEGMENTS, SEGMENT_WEIGHTS),
            "payment_terms_days": _weighted(rng, PAYMENT_TERMS, PAYMENT_TERM_WEIGHTS),
            "credit_limit_cents": rng.choice([None, 100_000_00, 500_000_00, 1_000_000_00, 5_000_000_00]),
            "default_currency_code": base_currency if country == "PE" else _country_currency(country),
            "email":              f"contact@{prefix.lower()}.example",
            "phone":              f"+{rng.randint(1, 99)}-{rng.randint(100,999)}-{rng.randint(1000,9999)}",
            "address_line_1":     f"{rng.randint(100, 9999)} {rng.choice(['Av','Calle','Jr'])}.  {rng.choice(LAST_NAMES)}",
            "city":               city,
            "country_code":       country,
            "is_active":          rng.random() > 0.05,   # 5% inactive
        })

    # ------ Products ------
    product_codes: list[str] = []
    for i in range(n_products):
        cat, items = rng.choice(PRODUCT_CATEGORIES)
        item = rng.choice(items)
        sku = f"SKU-{i+1:05d}"
        product_codes.append(sku)
        cost = rng.randint(50, 5000) * 10                    # 500..50000 cents
        price = int(cost * rng.uniform(1.25, 2.5))
        out.products.append({
            "sku":               sku,
            "product_name":      f"{item} #{i+1}",
            "product_category":  cat,
            "product_subcategory": item,
            "unit_weight_kg":    round(rng.uniform(0.05, 25.0), 3),
            "unit_volume_m3":    round(rng.uniform(0.0005, 0.5), 4),
            "unit_cost_cents":   cost,
            "unit_price_cents":  price,
            "reorder_threshold": rng.choice([10, 25, 50, 100, 250]),
            "is_active":         rng.random() > 0.03,
        })

    # ------ Warehouses ------
    for code, name, city, country in WAREHOUSE_NAMES:
        out.warehouses.append({
            "warehouse_code": code,
            "warehouse_name": name,
            "city":           city,
            "country_code":   country,
            "capacity_m3":    rng.choice([2000, 5000, 8000, 12000]),
            "is_active":      True,
        })

    # ------ Vehicles ------
    for i in range(10):
        vt = _weighted(rng, VEHICLE_TYPES, VEHICLE_TYPE_WEIGHTS)
        cap_kg = {"van": 1500, "truck": 8000, "semi-trailer": 25000}[vt]
        cap_m3 = {"van": 12,   "truck": 40,    "semi-trailer": 90}[vt]
        out.vehicles.append({
            "vehicle_code":  f"V-{i+1:03d}",
            "license_plate": f"ABC-{rng.randint(100,999)}",
            "vehicle_type":  vt,
            "capacity_kg":   cap_kg,
            "capacity_m3":   cap_m3,
            "is_active":     True,
        })

    # ------ Employees (drivers + admin) ------
    for i in range(n_employees):
        role = "driver" if i < (n_employees * 0.6) else rng.choice(["admin","ops","finance"])
        out.employees.append({
            "employee_code": f"EMP-{i+1:04d}",
            "full_name":     f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}",
            "email":         f"emp{i+1}@example.local",
            "department":    {"driver":"logistics","admin":"administration","ops":"operations","finance":"finance"}[role],
            "role_title":    role.capitalize(),
            "hire_date":     (earliest + dt.timedelta(days=rng.randint(0, 600))).isoformat(),
            "is_active":     rng.random() > 0.05,
        })

    # ------ Suppliers ------
    for i, name in enumerate(SUPPLIER_NAMES):
        out.suppliers.append({
            "supplier_code":      f"SUP-{i+1:03d}",
            "supplier_name":      name,
            "contact_email":      f"sales@{name.lower().replace(' ', '_')}.example",
            "country_code":       rng.choice(["PE","US","MX"]),
            "payment_terms_days": rng.choice([30, 45, 60]),
            "is_active":          True,
        })

    # ------ Orders + Items + Invoices + Payments + Shipments ------
    order_seq = 0
    invoice_seq = 0
    shipment_seq = 0

    n_months = months_back
    for m in range(n_months):
        month_start = _add_months(earliest, m)
        month_end   = _add_months(earliest, m + 1) - dt.timedelta(days=1)
        n_orders   = int(orders_per_month * rng.uniform(0.7, 1.3))   # +- 30% variance

        for _ in range(n_orders):
            order_seq += 1
            order_date = _rand_date_between(rng, month_start, month_end)
            cust = rng.choice([c for c in out.customers if c["is_active"]])
            warehouse = rng.choice(out.warehouses)
            currency = cust["default_currency_code"]

            order = {
                "order_number":  f"ORD-{order_seq:07d}",
                "customer_code": cust["customer_code"],
                "warehouse_code": warehouse["warehouse_code"],
                "order_date":    order_date.isoformat(),
                "ordered_at":    f"{order_date.isoformat()}T{rng.randint(8,18):02d}:{rng.randint(0,59):02d}:00Z",
                "status":        _weighted(rng, ["fulfilled","fulfilled","fulfilled","open","canceled","returned"], [60,15,10,8,4,3]),
                "currency_code": currency,
            }

            # Order items: 1..6 lines
            n_lines = rng.randint(1, 6)
            order_total = 0
            picked_products = rng.sample([p for p in out.products if p["is_active"]], n_lines)
            items_for_order = []
            for line_no, prod in enumerate(picked_products, start=1):
                qty = rng.choice([1,1,1,2,2,3,5,10,25])
                unit_price = int(prod["unit_price_cents"] * rng.uniform(0.95, 1.05))
                discount   = int(unit_price * qty * rng.choice([0,0,0,0.05,0.1])) if rng.random() < 0.2 else 0
                line_total = qty * unit_price - discount
                order_total += line_total
                items_for_order.append({
                    "order_number": order["order_number"],
                    "order_date":   order["order_date"],
                    "line_number":  line_no,
                    "sku":          prod["sku"],
                    "quantity":     qty,
                    "unit_price_cents": unit_price,
                    "discount_cents":   discount,
                    "line_total_cents": line_total,
                    "currency_code":    currency,
                })
            order["total_amount_cents"] = order_total
            order["line_count"] = len(items_for_order)
            out.orders.append(order)
            out.order_items.extend(items_for_order)

            # 80% of fulfilled orders get an invoice; opens/canceled don't
            if order["status"] in ("fulfilled","returned") and rng.random() < 0.9:
                invoice_seq += 1
                invoice_date = order_date + dt.timedelta(days=rng.randint(0, 5))
                terms = cust["payment_terms_days"] or 30
                due_date = invoice_date + dt.timedelta(days=terms)
                billed = order_total
                # Determine paid: most paid in full, some partial, some unpaid
                age_days = (today - invoice_date).days
                if age_days < terms - 5:
                    # Within terms: mostly unpaid/partial
                    pct = rng.choice([0, 0, 0.3, 0.5])
                elif age_days < terms + 30:
                    pct = rng.choice([0, 0.5, 1.0, 1.0])
                else:
                    pct = rng.choice([0, 0.7, 1.0, 1.0, 1.0])
                paid = int(billed * pct)

                if paid >= billed:
                    status = "paid"
                elif paid > 0:
                    status = "partial"
                else:
                    status = "unpaid"

                invoice = {
                    "invoice_number": f"INV-{invoice_seq:07d}",
                    "customer_code":  cust["customer_code"],
                    "order_number":   order["order_number"],
                    "invoice_date":   invoice_date.isoformat(),
                    "due_date":       due_date.isoformat(),
                    "status":         status,
                    "amount_billed_cents": billed,
                    "amount_paid_cents":   paid,
                    "currency_code":  currency,
                }
                out.invoices.append(invoice)

                # Payments: split paid amount into 1..3 receipts
                if paid > 0:
                    n_pay = rng.choice([1,1,1,2,2,3])
                    remaining = paid
                    for k in range(n_pay):
                        is_last = (k == n_pay - 1)
                        if is_last:
                            amt = remaining
                        else:
                            amt = int(remaining * rng.uniform(0.3, 0.7))
                            remaining -= amt
                        if amt <= 0:
                            continue
                        pay_date = invoice_date + dt.timedelta(days=rng.randint(1, max(2, age_days)))
                        if pay_date > today:
                            pay_date = today
                        out.payments.append({
                            "invoice_number":  invoice["invoice_number"],
                            "invoice_date":    invoice["invoice_date"],
                            "customer_code":   cust["customer_code"],
                            "payment_date":    pay_date.isoformat(),
                            "amount_cents":    amt,
                            "currency_code":   currency,
                            "payment_method":  _weighted(rng, PAYMENT_METHODS, PAYMENT_METHOD_WEIGHTS),
                            "reference_number": f"PAY-{order_seq:07d}-{k+1}",
                        })

            # 70% of fulfilled orders get a shipment
            if order["status"] in ("fulfilled","returned") and rng.random() < 0.85:
                shipment_seq += 1
                ship_date = order_date + dt.timedelta(days=rng.randint(0, 3))
                # transit: usually 1-3 days, sometimes very late
                if rng.random() < 0.05:
                    transit_days = rng.randint(8, 14)   # late
                else:
                    transit_days = rng.randint(1, 4)
                delivered = ship_date + dt.timedelta(days=transit_days)
                if delivered > today:
                    delivered_at = None
                    delivered_date = None
                    status = "in_transit"
                elif rng.random() < 0.02:
                    delivered_at = None
                    delivered_date = None
                    status = "lost"
                else:
                    delivered_at = f"{delivered.isoformat()}T{rng.randint(8,18):02d}:00:00Z"
                    delivered_date = delivered.isoformat()
                    status = "delivered"

                vehicle = rng.choice(out.vehicles)
                driver = rng.choice([e for e in out.employees if e["role_title"] == "Driver"])

                # destination = customer city/country
                dest_city = cust["city"]
                dest_country = cust["country_code"]
                origin_city = warehouse["city"]
                distance_km = rng.choice([50, 150, 300, 600, 1200, 2500]) if origin_city != dest_city else rng.randint(5, 80)
                weight_kg = round(sum(
                    next(p["unit_weight_kg"] for p in out.products if p["sku"] == it["sku"]) * it["quantity"]
                    for it in items_for_order
                ), 2)
                volume_m3 = round(sum(
                    next(p["unit_volume_m3"] for p in out.products if p["sku"] == it["sku"]) * it["quantity"]
                    for it in items_for_order
                ), 3)
                freight_cost = int(distance_km * weight_kg * rng.uniform(0.5, 1.5)) if weight_kg else 0

                shipment = {
                    "shipment_number":   f"SHP-{shipment_seq:07d}",
                    "order_number":      order["order_number"],
                    "warehouse_code":    warehouse["warehouse_code"],
                    "vehicle_code":      vehicle["vehicle_code"],
                    "driver_employee_code": driver["employee_code"],
                    "ship_date":         ship_date.isoformat(),
                    "shipped_at":        f"{ship_date.isoformat()}T{rng.randint(6,12):02d}:00:00Z",
                    "delivered_at":      delivered_at,
                    "delivered_date":    delivered_date,
                    "status":            status,
                    "origin_city":       origin_city,
                    "destination_city":  dest_city,
                    "destination_country_code": dest_country,
                    "distance_km":       distance_km,
                    "weight_kg":         weight_kg,
                    "volume_m3":         volume_m3,
                    "freight_cost_cents": freight_cost,
                    "currency_code":     currency,
                }
                out.shipments.append(shipment)
                for it in items_for_order:
                    out.shipment_items.append({
                        "shipment_number": shipment["shipment_number"],
                        "ship_date":       shipment["ship_date"],
                        "sku":             it["sku"],
                        "units_shipped":   it["quantity"],
                    })

    # ------ Inventory movements ------
    # Receipts: each warehouse receives each product several times across history
    for w in out.warehouses:
        for p in [p for p in out.products if p["is_active"]]:
            n_receipts = rng.randint(2, 8)
            for _ in range(n_receipts):
                day = _rand_date_between(rng, earliest, today)
                qty = rng.randint(50, 500)
                out.inventory_movements.append({
                    "sku":              p["sku"],
                    "warehouse_code":   w["warehouse_code"],
                    "event_at":         f"{day.isoformat()}T{rng.randint(6,18):02d}:00:00Z",
                    "movement_type":    "receipt",
                    "quantity_delta":   qty,
                    "reference_table":  None,
                    "reference_id":     None,
                    "reason":           "vendor receipt",
                })
    # Outbound from shipments
    for s in out.shipments:
        for si in [x for x in out.shipment_items if x["shipment_number"] == s["shipment_number"]]:
            out.inventory_movements.append({
                "sku":             si["sku"],
                "warehouse_code":  s["warehouse_code"],
                "event_at":        s["shipped_at"],
                "movement_type":   "shipment",
                "quantity_delta":  -si["units_shipped"],
                "reference_table": "fact_shipments",
                "reference_id":    None,   # filled in gold load via shipment_number lookup
                "reason":          "outbound shipment",
            })

    # ------ Expenses ------
    expense_seq = 0
    cat_choices, cat_weights = zip(*EXPENSE_CATEGORIES)
    for m in range(n_months):
        month_start = _add_months(earliest, m)
        month_end   = _add_months(earliest, m + 1) - dt.timedelta(days=1)
        n_exp = int(expenses_per_month * rng.uniform(0.7, 1.3))
        for _ in range(n_exp):
            expense_seq += 1
            cat = _weighted(rng, list(cat_choices), list(cat_weights))
            day = _rand_date_between(rng, month_start, month_end)
            amount = {
                "fuel":         rng.randint(50, 500) * 100,
                "maintenance":  rng.randint(100, 2000) * 100,
                "rent":         rng.randint(2000, 8000) * 100,
                "utilities":    rng.randint(200, 1500) * 100,
                "salaries":     rng.randint(15000, 35000) * 100,
                "t_and_e":      rng.randint(50, 800) * 100,
                "other":        rng.randint(20, 1500) * 100,
            }[cat]
            warehouse = rng.choice(out.warehouses) if cat in ("rent","utilities","other") else None
            vehicle   = rng.choice(out.vehicles)   if cat in ("fuel","maintenance") else None
            employee  = rng.choice(out.employees)  if cat in ("t_and_e","salaries") else None
            supplier  = rng.choice(out.suppliers)  if rng.random() < 0.5 else None
            out.expenses.append({
                "expense_id":    expense_seq,
                "expense_date":  day.isoformat(),
                "category":      cat,
                "description":   f"{cat} expense {expense_seq}",
                "amount_cents":  amount,
                "tax_amount_cents": int(amount * 0.18),  # 18% IGV
                "currency_code": base_currency,
                "warehouse_code": warehouse["warehouse_code"] if warehouse else None,
                "vehicle_code":   vehicle["vehicle_code"]    if vehicle  else None,
                "employee_code":  employee["employee_code"]  if employee else None,
                "supplier_code":  supplier["supplier_code"]  if supplier else None,
                "is_reimbursable": rng.random() < 0.1,
            })

    return out


# ---------- Helpers ---------------------------------------------------------

def _weighted(rng: random.Random, choices: list, weights: list):
    return rng.choices(choices, weights=weights, k=1)[0]


def _add_months(d: dt.date, n: int) -> dt.date:
    """Add n months to date d, returning the 1st of the resulting month."""
    y = d.year + (d.month - 1 + n) // 12
    m = (d.month - 1 + n) % 12 + 1
    return dt.date(y, m, 1)


def _rand_date_between(rng: random.Random, start: dt.date, end: dt.date) -> dt.date:
    delta = (end - start).days
    if delta <= 0:
        return start
    return start + dt.timedelta(days=rng.randint(0, delta))


def _country_currency(country: str) -> str:
    """Default invoicing currency by country."""
    return {
        "PE": "PEN",
        "US": "USD",
        "MX": "MXN",
        "CO": "COP",
        "CL": "CLP",
        "BR": "BRL",
        "AR": "ARS",
        "EC": "USD",
    }.get(country, "USD")
